{#- Silver: one column per line + macros (dbt/macros/). Chave natural: cd_estabelecimento. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='cd_estabelecimento',
    incremental_strategy='merge',
    merge_update_columns=[
      'ie_lib_farm_auto', 'ie_cancel_prescr_auto', 'ie_consiste_estoque', 'ie_duplo_check',
      'ie_exige_lote_fabric', 'ie_controle_acesso', 'ds_parametros',
      'qt_tempo_validade', 'qt_limite_lote',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_parametros_farmacia', 'cd_estabelecimento', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_parametros_farmacia') }} b
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
          PARTITION BY b.cd_estabelecimento
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
      d.cd_estabelecimento                                        AS cd_estabelecimento

    , {{ standardize_enum('d.ie_lib_farm_auto', "'N'") }}         AS ie_lib_farm_auto
    , {{ standardize_enum('d.ie_cancel_prescr_auto', "'N'") }}    AS ie_cancel_prescr_auto
    , {{ standardize_enum('d.ie_consiste_estoque', "'N'") }}      AS ie_consiste_estoque
    , {{ standardize_enum('d.ie_duplo_check', "'N'") }}           AS ie_duplo_check
    , {{ standardize_enum('d.ie_exige_lote_fabric', "'N'") }}     AS ie_exige_lote_fabric
    , {{ standardize_enum('d.ie_controle_acesso', "'N'") }}       AS ie_controle_acesso
    , {{ standardize_text_initcap('d.ds_parametros', "'indefinido'") }}      AS ds_parametros
    , {{ normalize_decimal('d.qt_tempo_validade', 2, -1) }}       AS qt_tempo_validade
    , {{ normalize_decimal('d.qt_limite_lote', 2, -1) }}          AS qt_limite_lote

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
