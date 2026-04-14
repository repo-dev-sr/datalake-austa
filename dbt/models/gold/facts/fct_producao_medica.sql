-- Grain: 1 linha = 1 procedimento realizado (PK natural: nr_sequencia do Tasy PROC_PACIENTE)
-- Fonte: silver_context.procedimento via {{ ref('procedimento') }}
-- Camada: Gold | Processo: Produtividade Médica | Constellation Austa
-- Dimensões: dim_tempo · dim_medico (solicitante + executor) · dim_procedimento · dim_convenio
-- Dimensão futura: dim_paciente (pendente silver_context.paciente)
{{
  config(
    materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_producao'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "producao_medica"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('procedimento') }}
    -- ⚠️  _is_deleted não está exposto em silver_context.procedimento (silver_context_audit_columns
    --     não repassa o campo da camada silver). Incluir filtro após exposição do campo.
    {% if is_incremental() %}
    WHERE _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_sequencia']) }}                AS sk_producao

        -- ── Foreign keys para dimensões ─────────────────────────────────────────
        -- sk_data: NULL quando dt_procedimento é sentinela (1900-01-01 do standardize_date)
        , CASE
              WHEN dt_procedimento = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_procedimento, 'yyyyMMdd') AS INT)
          END                                                                     AS sk_data

        , {{ dbt_utils.generate_surrogate_key(['cd_medico']) }}                  AS sk_medico_solicitante
        , {{ dbt_utils.generate_surrogate_key(['cd_medico_executor']) }}         AS sk_medico_executor
        , {{ dbt_utils.generate_surrogate_key(['cd_procedimento']) }}            AS sk_procedimento
        , {{ dbt_utils.generate_surrogate_key(['cd_convenio']) }}                AS sk_convenio

        -- sk_cid: NULL quando procedimento não possui CID informado (cd_doenca_cid = -1)
        , CASE
              WHEN cd_doenca_cid = -1 THEN NULL
              ELSE {{ dbt_utils.generate_surrogate_key(['cd_doenca_cid']) }}
          END                                                                    AS sk_cid

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_sequencia                                                            AS nk_sequencia_procedimento
        , nr_atendimento                                                         AS nk_atendimento
        , cd_pessoa_fisica                                                       AS nk_paciente  -- FK futura → dim_paciente
        , cd_medico                                                              AS nk_medico_solicitante
        , cd_medico_executor                                                     AS nk_medico_executor
        , cd_procedimento                                                        AS nk_procedimento
        , cd_convenio                                                            AS nk_convenio
        , NULLIF(cd_doenca_cid, -1)                                             AS nk_cid

        -- ── Dimensões degeneradas (sem tabela própria) ───────────────────────────
        , atend_ie_tipo_atendimento
        , atend_ie_clinica
        , ie_funcao_medico
        , ie_origem_proced
        , cd_setor_atendimento
        , cd_especialidade

        -- ── Métricas aditivas (podem ser somadas em qualquer dimensão) ───────────
        , qt_procedimento
        , vl_procedimento
        , vl_medico
        , vl_anestesista
        , vl_materiais
        , vl_auxiliares
        , vl_custo_operacional
        , vl_repasse_calc

        -- ── Métrica semi-aditiva ─────────────────────────────────────────────────
        -- ⚠️  nr_minuto_duracao: somar apenas dentro do mesmo atendimento/nr_sequencia.
        --     Ao agregar por médico ou dia use SUM com atenção a sobreposições de procedimentos.
        , nr_minuto_duracao

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
