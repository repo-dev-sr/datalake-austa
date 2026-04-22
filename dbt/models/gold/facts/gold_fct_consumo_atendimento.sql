-- Grain: 1 linha = 1 material consumido/cobrado por atendimento (MATERIAL_ATEND_PACIENTE.nr_sequencia)
-- Fonte: silver_context.consumo_material via {{ ref('consumo_material') }}
-- Camada: Gold | Processo: Consumo / Custo / Margem Farmácia | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · dim_medico (sk_medico) · dim_convenio (sk_convenio) · gold_dim_medicamento (sk_medicamento)
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_consumo_atendimento'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "consumo_atendimento"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('consumo_material') }}
    {% if is_incremental() %}
    WHERE _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não exposto em silver_context.consumo_material.
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_seq_consumo']) }}             AS sk_consumo_atendimento

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        , CASE
              WHEN dt_atendimento = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_atendimento, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data
        , {{ dbt_utils.generate_surrogate_key(['cd_medico']) }}                 AS sk_medico
        , {{ dbt_utils.generate_surrogate_key(['cd_convenio']) }}               AS sk_convenio
        , {{ dbt_utils.generate_surrogate_key(['cd_material']) }}               AS sk_medicamento

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_seq_consumo                                                         AS nk_consumo
        , nr_atendimento                                                         AS nk_atendimento
        , nr_prescricao                                                          AS nk_prescricao
        , cd_medico                                                              AS nk_medico
        , cd_convenio                                                            AS nk_convenio
        , cd_material                                                            AS nk_medicamento
        , cd_categoria                                                           AS nk_categoria_convenio

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_setor_atendimento
        , cd_estabelecimento
        , ie_tipo_material
        , ie_tipo_material_atend
        , ie_cobranca
        , ie_situacao
        , ie_suspenso
        , ie_valor_informado
        , ie_controlado_material
        , ie_alto_risco_material
        , ie_medicamento_material
        , ie_antibiotico_material
        , ie_quimioterapico_material
        , ie_curva_abc_material
        , cd_unidade_medida

        -- ── Métricas aditivas ────────────────────────────────────────────────────
        , qt_material
        , vl_material
        , vl_unitario
        , vl_custo
        , vl_custo_medio
        , vl_convenio

        -- ── Margem bruta (já calculada na Silver-Context quando ambos > 0) ───────
        -- Aditiva: pode ser somada em qualquer dimensão; registros NULL são ignorados por SUM
        , vl_margem_bruta

        -- ── Métrica não-aditiva ──────────────────────────────────────────────────
        -- ⚠️  pr_desconto: NÃO somar — recalcular em cada agregação como média ponderada
        , pr_desconto

        -- ── Alerta pré-calculado: margem negativa é sinal de custo > cobrança ────
        , CASE
              WHEN vl_margem_bruta IS NULL THEN 'SEM_VALORIZACAO'
              WHEN vl_margem_bruta < 0 THEN 'MARGEM_NEGATIVA'
              WHEN vl_margem_bruta = 0 THEN 'MARGEM_ZERO'
              ELSE 'MARGEM_POSITIVA'
          END                                                                    AS ie_nivel_alerta_margem

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
