-- Grain: 1 linha = 1 análise farmacêutica registrada (LOG_ANALISE_PRESCR.nr_sequencia)
-- Fonte: silver_context.analise_farmaceutica via {{ ref('analise_farmaceutica') }}
-- Camada: Gold | Processo: Intervenções Farmacêuticas / Farmácia Clínica | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · dim_medico (sk_medico)
-- Observação: volume baixo (~8 análises/dia em homologação) — cubo crítico para farmácia clínica
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_analise_farmaceutica'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "analise_farmaceutica"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('analise_farmaceutica') }}
    {% if is_incremental() %}
    WHERE _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não exposto em silver_context.analise_farmaceutica.
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_seq_analise']) }}             AS sk_analise_farmaceutica

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        , CASE
              WHEN dt_analise = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_analise, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data
        , {{ dbt_utils.generate_surrogate_key(['cd_medico']) }}                 AS sk_medico

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_seq_analise                                                         AS nk_analise
        , nr_prescricao                                                          AS nk_prescricao
        , nr_atendimento                                                         AS nk_atendimento
        , cd_medico                                                              AS nk_medico
        , cd_farmaceutico                                                        AS nk_farmaceutico
        , cd_pessoa_fisica                                                       AS nk_paciente  -- FK futura → dim_paciente

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_setor_atendimento
        , cd_estabelecimento
        , ie_tipo_analise
        , ie_resultado
        , ie_tem_intervencao

        -- ── Métricas aditivas ────────────────────────────────────────────────────
        , 1                                                                      AS qt_analise
        , CASE WHEN ie_tem_intervencao = 'S' THEN 1 ELSE 0 END                   AS qt_analise_com_intervencao
        , CASE WHEN ie_tem_intervencao = 'N' THEN 1 ELSE 0 END                   AS qt_analise_sem_intervencao
        , CASE WHEN ie_resultado = 'A' THEN 1 ELSE 0 END                         AS qt_intervencao_aceita
        , CASE WHEN ie_resultado = 'N' THEN 1 ELSE 0 END                         AS qt_intervencao_recusada

        -- ── Alerta pré-calculado ─────────────────────────────────────────────────
        , CASE
              WHEN ie_tem_intervencao = 'S' AND ie_resultado = 'N' THEN 'INTERVENCAO_RECUSADA'
              WHEN ie_tem_intervencao = 'S' AND ie_resultado = 'A' THEN 'INTERVENCAO_ACEITA'
              WHEN ie_tem_intervencao = 'S' THEN 'INTERVENCAO_SEM_RESULTADO'
              ELSE 'SEM_INTERVENCAO'
          END                                                                    AS ie_nivel_alerta_analise

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
