{#- Silver: one column per line + macros (dbt/macros/). PK: cd_operacao_estoque. -#}
{#- Dimensao - 69 operacoes de estoque cadastradas -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='cd_operacao_estoque',
    incremental_strategy='merge',
    merge_update_columns=[
      'ds_operacao_estoque', 'ie_entrada_saida', 'ie_situacao', 'ie_consignado',
      'ie_tipo_operacao', 'ie_ajuste', 'ie_transferencia',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_operacao_estoque', 'cd_operacao_estoque', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_operacao_estoque') }} b
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
          PARTITION BY b.cd_operacao_estoque
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
      d.cd_operacao_estoque                                       AS cd_operacao_estoque

    , {{ standardize_text_initcap('d.ds_operacao_estoque', "'indefinido'") }} AS ds_operacao_estoque
    , {{ standardize_enum('d.ie_entrada_saida', "'i'") }}         AS ie_entrada_saida
    , {{ standardize_enum('d.ie_situacao', "'i'") }}              AS ie_situacao
    , {{ standardize_enum('d.ie_consignado', "'N'") }}            AS ie_consignado
    , {{ standardize_enum('d.ie_tipo_operacao', "'i'") }}         AS ie_tipo_operacao
    , {{ standardize_enum('d.ie_ajuste', "'N'") }}                AS ie_ajuste
    , {{ standardize_enum('d.ie_transferencia', "'N'") }}         AS ie_transferencia

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
