{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{#- Dimensao regras de consistencia - referenciada por PRESCR_MEDICA_ERRO.NR_REGRA -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'cd_estabelecimento',
      'ds_regra', 'ds_mensagem', 'ie_tipo_regra', 'ie_criticidade',
      'ie_exibe_farmacia', 'ie_situacao', 'ie_acao',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_regra_consiste_prescr', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_regra_consiste_prescr') }} b
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
          PARTITION BY b.nr_sequencia
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
      d.nr_sequencia                                              AS nr_sequencia
    , {{ fill_null_bigint('d.cd_estabelecimento', -1) }}          AS cd_estabelecimento

    , {{ standardize_text_initcap('d.ds_regra', "'indefinido'") }}           AS ds_regra
    , {{ standardize_text_initcap('d.ds_mensagem', "'indefinido'") }}        AS ds_mensagem
    , {{ standardize_enum('d.ie_tipo_regra', "'i'") }}            AS ie_tipo_regra
    , {{ standardize_enum('d.ie_criticidade', "'i'") }}           AS ie_criticidade
    , {{ standardize_enum('d.ie_exibe_farmacia', "'N'") }}        AS ie_exibe_farmacia
    , {{ standardize_enum('d.ie_situacao', "'i'") }}              AS ie_situacao
    , {{ standardize_enum('d.ie_acao', "'i'") }}                  AS ie_acao

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
