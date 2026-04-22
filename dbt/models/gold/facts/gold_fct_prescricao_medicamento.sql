-- Grain: 1 linha = 1 prescrição médica liberada com itens agregados (nr_prescricao)
-- Fonte: silver_context.prescricao via {{ ref('prescricao') }}
-- Camada: Gold | Processo: Universo de Prescrições Farmácia | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · dim_medico (sk_medico)
-- Filtros de negócio: já aplicados na Silver-Context (dt_liberacao not null, cd_funcao_origem NOT IN (924,950))
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_prescricao'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "prescricao"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('prescricao') }}
    {% if is_incremental() %}
    WHERE _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não exposto em silver_context.prescricao.
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_prescricao']) }}              AS sk_prescricao

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        , CASE
              WHEN dt_liberacao = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_liberacao, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data
        , {{ dbt_utils.generate_surrogate_key(['cd_medico']) }}                 AS sk_medico

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_prescricao                                                          AS nk_prescricao
        , nr_atendimento                                                         AS nk_atendimento
        , cd_medico                                                              AS nk_medico
        , cd_pessoa_fisica                                                       AS nk_paciente  -- FK futura → dim_paciente

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_setor_atendimento
        , cd_estabelecimento
        , cd_funcao_origem
        , ie_origem_inf
        , ie_adep
        , ie_prescr_emergencia
        , ie_recem_nato
        , ie_hemodialise
        , ie_tipo_prescricao
        , ie_situacao
        , ie_retroativa
        , ie_status_liberacao_farmacia
        , ie_tem_medicamento
        , ie_tem_controlado

        -- ── Métricas aditivas — contagens do lote de itens ───────────────────────
        , 1                                                                      AS qt_prescricao
        , qt_itens
        , qt_medicamentos
        , qt_materiais
        , qt_outros_itens
        , qt_itens_suspensos
        , qt_itens_controlados
        , qt_itens_urgentes

        -- ── Flags derivadas — indicadores booleanos convertidos em 0/1 ───────────
        -- Facilita SUM() em dashboards (evita CASE repetido no consumo)
        , CASE WHEN ie_tem_medicamento = 'S' THEN 1 ELSE 0 END                  AS qt_prescricao_com_medicamento
        , CASE WHEN ie_tem_controlado  = 'S' THEN 1 ELSE 0 END                  AS qt_prescricao_com_controlado
        , CASE WHEN ie_adep            = 'S' THEN 1 ELSE 0 END                  AS qt_prescricao_adep

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
