{#- Silver: one column per line + macros (dbt/macros/). PK: nr_movimento_estoque. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_movimento_estoque',
    incremental_strategy='merge',
    merge_update_columns=[
      'cd_material', 'cd_operacao_estoque', 'nr_prescricao', 'nr_atendimento',
      'cd_local_estoque', 'cd_local_estoque_dest', 'cd_setor_atendimento',
      'cd_estabelecimento', 'cd_unidade_medida', 'cd_unidade_medida_cons',
      'nr_ordem_compra', 'nr_lote_fabric',
      'ie_origem_documento', 'ie_tipo_movimento', 'ie_entrada_saida', 'ie_situacao',
      'ie_consignado',
      'qt_movimento', 'qt_estoque', 'vl_movimento', 'vl_unitario', 'pr_desconto',
      'dt_movimento_estoque', 'dt_mesano_referencia', 'dt_validade',
      'ds_observacao', 'ds_justificativa',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_movimento_estoque', 'nr_movimento_estoque', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_movimento_estoque') }} b
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
          PARTITION BY b.nr_movimento_estoque
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
    , {{ fill_null_bigint('d.cd_material', -1) }}                 AS cd_material
    , {{ fill_null_bigint('d.cd_operacao_estoque', -1) }}         AS cd_operacao_estoque
    , {{ fill_null_bigint('d.nr_prescricao', -1) }}               AS nr_prescricao
    , {{ fill_null_bigint('d.nr_atendimento', -1) }}              AS nr_atendimento
    , {{ fill_null_bigint('d.cd_local_estoque', -1) }}            AS cd_local_estoque
    , {{ fill_null_bigint('d.cd_local_estoque_dest', -1) }}       AS cd_local_estoque_dest
    , {{ fill_null_bigint('d.cd_setor_atendimento', -1) }}        AS cd_setor_atendimento
    , {{ fill_null_bigint('d.cd_estabelecimento', -1) }}          AS cd_estabelecimento
    , {{ fill_null_bigint('d.cd_unidade_medida', -1) }}           AS cd_unidade_medida
    , {{ fill_null_bigint('d.cd_unidade_medida_cons', -1) }}      AS cd_unidade_medida_cons
    , {{ fill_null_bigint('d.nr_ordem_compra', -1) }}             AS nr_ordem_compra
    , {{ fill_null_string('d.nr_lote_fabric', "'indefinido'") }}  AS nr_lote_fabric

      -- Categoricos - ie_origem_documento dominio 23 (92% = '3' Prescricao)
    , {{ standardize_enum('d.ie_origem_documento', "'i'") }}      AS ie_origem_documento
    , {{ standardize_enum('d.ie_tipo_movimento', "'i'") }}        AS ie_tipo_movimento
    , {{ standardize_enum('d.ie_entrada_saida', "'i'") }}         AS ie_entrada_saida
    , {{ standardize_enum('d.ie_situacao', "'i'") }}              AS ie_situacao
    , {{ standardize_enum('d.ie_consignado', "'N'") }}            AS ie_consignado

      -- Quantidades e valores (vl_movimento pode ser NULL aqui - valorizacao em MOVIMENTO_ESTOQUE_VALOR)
    , {{ normalize_decimal('d.qt_movimento', 4, -1) }}            AS qt_movimento
    , {{ normalize_decimal('d.qt_estoque', 4, -1) }}              AS qt_estoque
    , {{ normalize_decimal('d.vl_movimento', 2, -1) }}            AS vl_movimento
    , {{ normalize_decimal('d.vl_unitario', 4, -1) }}             AS vl_unitario
    , {{ normalize_decimal('d.pr_desconto', 2, -1) }}             AS pr_desconto

      -- Timestamps (dh_* -> dt_*)
    , {{ standardize_date('d.dh_movimento_estoque') }}            AS dt_movimento_estoque
    , {{ standardize_date('d.dh_mesano_referencia') }}            AS dt_mesano_referencia
    , {{ standardize_date('d.dh_validade') }}                     AS dt_validade

      -- Observacoes
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao
    , {{ standardize_text_initcap('d.ds_justificativa', "'indefinido'") }}   AS ds_justificativa

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
