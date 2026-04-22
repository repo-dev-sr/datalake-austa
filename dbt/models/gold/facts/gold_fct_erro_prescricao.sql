-- Grain: 1 linha = 1 erro de prescrição detectado (PRESCR_MEDICA_ERRO.nr_sequencia)
-- Fonte: silver_context.erro_prescricao via {{ ref('erro_prescricao') }}
-- Camada: Gold | Processo: Erros de Prescrição por Setor | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · dim_medico (sk_medico)
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_erro_prescricao'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "erro_prescricao"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('erro_prescricao') }}
    {% if is_incremental() %}
    WHERE _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não exposto em silver_context.erro_prescricao.
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_seq_erro']) }}                AS sk_erro_prescricao

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        , CASE
              WHEN dt_erro = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_erro, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data
        , CASE
              WHEN dt_resolucao = DATE '1900-01-01' OR dt_resolucao IS NULL THEN NULL
              ELSE CAST(DATE_FORMAT(dt_resolucao, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data_resolucao
        , {{ dbt_utils.generate_surrogate_key(['cd_medico']) }}                 AS sk_medico

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_seq_erro                                                            AS nk_erro
        , nr_prescricao                                                          AS nk_prescricao
        , nr_atendimento                                                         AS nk_atendimento
        , cd_medico                                                              AS nk_medico
        , cd_pessoa_fisica                                                       AS nk_paciente  -- FK futura → dim_paciente
        , nr_regra                                                               AS nk_regra

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_setor_atendimento
        , cd_estabelecimento
        , ie_tipo_erro
        , ie_criticidade
        , ie_origem
        , ie_status_resolucao
        , ie_tipo_regra
        , ie_criticidade_regra
        , ie_exibe_farmacia
        , ie_acao_regra
        , ds_regra

        -- ── Métricas aditivas ────────────────────────────────────────────────────
        , 1                                                                      AS qt_erro
        , CASE WHEN ie_status_resolucao = 'resolvido' THEN 1 ELSE 0 END          AS qt_erro_resolvido
        , CASE WHEN ie_status_resolucao = 'pendente'  THEN 1 ELSE 0 END          AS qt_erro_pendente
        , CASE WHEN ie_criticidade IN ('A', 'ALTA')   THEN 1 ELSE 0 END          AS qt_erro_alta_criticidade

        -- ── Métrica semi-aditiva ─────────────────────────────────────────────────
        -- ⚠️  qt_dias_resolucao: somar apenas dentro do mesmo erro.
        --     Em agregações usar AVG/MEDIAN por setor/mês.
        , qt_dias_resolucao

        -- ── Alerta pré-calculado ─────────────────────────────────────────────────
        , CASE
              WHEN ie_status_resolucao = 'pendente' AND ie_criticidade IN ('A', 'ALTA') THEN 'CRITICO_PENDENTE'
              WHEN ie_status_resolucao = 'pendente' THEN 'PENDENTE'
              WHEN qt_dias_resolucao > 7 THEN 'RESOLUCAO_TARDIA'
              ELSE 'RESOLVIDO'
          END                                                                    AS ie_nivel_alerta_erro

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
