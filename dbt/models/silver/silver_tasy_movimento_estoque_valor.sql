{#- Silver: one column per line + macros (dbt/macros/). PK composta: nr_movimento_estoque + cd_tipo_valor. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key=['nr_movimento_estoque', 'cd_tipo_valor'],
    incremental_strategy='merge',
    merge_update_columns=[
      'vl_movimento', 'vl_unitario', 'vl_custo', 'vl_total',
      'qt_movimento',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_movimento_estoque_valor', 'nr_movimento_estoque, cd_tipo_valor', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_movimento_estoque_valor') }} b
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
          PARTITION BY b.nr_movimento_estoque, b.cd_tipo_valor
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
      d.nr_movimento_estoque                                      AS nr_movimento_estoque
      -- cd_tipo_valor: 99,26% = 7 (Custo medio ponderado), 0,72% = 1 (Custo compra NF)
    , d.cd_tipo_valor                                             AS cd_tipo_valor

      -- Valores monetarios
    , {{ normalize_decimal('d.vl_movimento', 2, -1) }}            AS vl_movimento
    , {{ normalize_decimal('d.vl_unitario', 4, -1) }}             AS vl_unitario
    , {{ normalize_decimal('d.vl_custo', 2, -1) }}                AS vl_custo
    , {{ normalize_decimal('d.vl_total', 2, -1) }}                AS vl_total

      -- Quantidades
    , {{ normalize_decimal('d.qt_movimento', 4, -1) }}            AS qt_movimento

      -- Auditoria TASY
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
