{#- Silver: one column per line + macros (dbt/macros/). PK composta: nr_prescricao + nr_sequencia. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key=['nr_prescricao', 'nr_sequencia'],
    incremental_strategy='merge',
    merge_update_columns=[
      'ie_tipo_complemento', 'ie_situacao', 'ds_complemento', 'ds_titulo',
      'dt_complemento', 'dt_liberacao',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_prescr_medica_compl', 'nr_prescricao, nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_prescr_medica_compl') }} b
  {% if is_incremental() %}
  WHERE b._bronze_loaded_at > (
      SELECT COALESCE(MAX(s._silver_processed_at), CAST('1900-01-01' AS TIMESTAMP))
      FROM {{ this }} s
  )
  {% endif %}
)

, latest_by_pk AS (
  SELECT *
  FROM (
    SELECT
        b.*
      , ROW_NUMBER() OVER (
          PARTITION BY b.nr_prescricao, b.nr_sequencia
          ORDER BY
              CAST(COALESCE(b._cdc_ts_ms, 0) AS BIGINT) DESC
            , CAST(COALESCE(b.__source_txid, 0) AS BIGINT) DESC
        ) AS _rn
    FROM base b
  ) x
  WHERE _rn = 1
)

, shaped AS (
  SELECT
      d.nr_prescricao                                             AS nr_prescricao
    , d.nr_sequencia                                              AS nr_sequencia

    , {{ standardize_enum('d.ie_tipo_complemento', "'i'") }}      AS ie_tipo_complemento
    , {{ standardize_enum('d.ie_situacao', "'i'") }}              AS ie_situacao
    , {{ standardize_text_initcap('d.ds_complemento', "'indefinido'") }}     AS ds_complemento
    , {{ standardize_text_initcap('d.ds_titulo', "'indefinido'") }}          AS ds_titulo

    , {{ standardize_date('d.dh_complemento') }}                  AS dt_complemento
    , {{ standardize_date('d.dh_liberacao') }}                    AS dt_liberacao

    , {{ standardize_date('d.dh_atualizacao') }}                  AS dt_atualizacao
    , {{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }}         AS nm_usuario
    , {{ standardize_date('d.dh_atualizacao_nrec') }}             AS dt_atualizacao_nrec
    , {{ standardize_text_initcap('d.nm_usuario_nrec', "'indefinido'") }}    AS nm_usuario_nrec

    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)

, final AS (
  SELECT *
  FROM shaped
  WHERE NOT _is_deleted
)

SELECT * FROM final
