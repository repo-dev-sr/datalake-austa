{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_atendimento', 'cd_material', 'cd_tab_preco_material', 'nr_seq_regra_ajuste_mat',
      'cd_unidade_medida', 'cd_convenio', 'cd_categoria', 'nr_prescricao',
      'nr_sequencia_prescricao', 'cd_setor_atendimento', 'cd_estabelecimento', 'cd_medico',
      'qt_material', 'vl_material', 'vl_unitario', 'vl_custo', 'vl_custo_medio',
      'vl_convenio', 'pr_desconto',
      'ie_situacao', 'ie_valor_informado', 'ie_tipo_material', 'ie_cobranca', 'ie_suspenso',
      'dt_atendimento', 'dt_conta', 'dt_validade',
      'ds_observacao', 'ds_complemento',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_material_atend_paciente', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_material_atend_paciente') }} b
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
    , {{ fill_null_bigint('d.nr_atendimento', -1) }}              AS nr_atendimento
    , {{ fill_null_bigint('d.cd_material', -1) }}                 AS cd_material
    , {{ fill_null_bigint('d.cd_tab_preco_material', -1) }}       AS cd_tab_preco_material
    , {{ fill_null_bigint('d.nr_seq_regra_ajuste_mat', -1) }}     AS nr_seq_regra_ajuste_mat
    , {{ fill_null_bigint('d.cd_unidade_medida', -1) }}           AS cd_unidade_medida
    , {{ fill_null_bigint('d.cd_convenio', -1) }}                 AS cd_convenio
    , {{ fill_null_bigint('d.cd_categoria', -1) }}                AS cd_categoria
    , {{ fill_null_bigint('d.nr_prescricao', -1) }}               AS nr_prescricao
    , {{ fill_null_bigint('d.nr_sequencia_prescricao', -1) }}     AS nr_sequencia_prescricao
    , {{ fill_null_bigint('d.cd_setor_atendimento', -1) }}        AS cd_setor_atendimento
    , {{ fill_null_bigint('d.cd_estabelecimento', -1) }}          AS cd_estabelecimento
    , {{ fill_null_bigint('d.cd_medico', -1) }}                   AS cd_medico

      -- Quantidades e valores (monetario)
    , {{ normalize_decimal('d.qt_material', 4, -1) }}             AS qt_material
    , {{ normalize_decimal('d.vl_material', 2, -1) }}             AS vl_material
    , {{ normalize_decimal('d.vl_unitario', 4, -1) }}             AS vl_unitario
    , {{ normalize_decimal('d.vl_custo', 2, -1) }}                AS vl_custo
    , {{ normalize_decimal('d.vl_custo_medio', 2, -1) }}          AS vl_custo_medio
    , {{ normalize_decimal('d.vl_convenio', 2, -1) }}             AS vl_convenio
    , {{ normalize_decimal('d.pr_desconto', 2, -1) }}             AS pr_desconto

      -- Categoricos
    , {{ standardize_enum('d.ie_situacao', "'i'") }}              AS ie_situacao
    , {{ standardize_enum('d.ie_valor_informado', "'N'") }}       AS ie_valor_informado
    , {{ standardize_enum('d.ie_tipo_material', "'i'") }}         AS ie_tipo_material
    , {{ standardize_enum('d.ie_cobranca', "'N'") }}              AS ie_cobranca
    , {{ standardize_enum('d.ie_suspenso', "'N'") }}              AS ie_suspenso

      -- Timestamps (dh_* -> dt_*)
    , {{ standardize_date('d.dh_atendimento') }}                  AS dt_atendimento
    , {{ standardize_date('d.dh_conta') }}                        AS dt_conta
    , {{ standardize_date('d.dh_validade') }}                     AS dt_validade

      -- Observacoes
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao
    , {{ standardize_text_initcap('d.ds_complemento', "'indefinido'") }}     AS ds_complemento

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
