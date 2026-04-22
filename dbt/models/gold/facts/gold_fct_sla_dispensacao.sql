-- Grain: 1 linha = 1 lote de dispensação com ciclo completo (ie_status_lote = 'R')
-- Fonte: silver_context.dispensacao via {{ ref('dispensacao') }}
-- Camada: Gold | Processo: SLA Dispensação Farmácia | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · gold_dim_medicamento (futuro — agregação por item)
-- Alerta: thresholds de 480/720 min derivados de regras_negocio.md seção "SLA Dispensacao por Etapa"
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_sla_dispensacao'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "sla_dispensacao"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('dispensacao') }}
    WHERE ie_status_lote = 'R'    -- ciclo completo garantido (regras_negocio.md)
    {% if is_incremental() %}
      AND _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não está exposto em silver_context.dispensacao (silver_context_audit_columns
    --     não repassa o campo da camada silver). Incluir filtro após exposição do campo.
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_lote']) }}                    AS sk_sla_dispensacao

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        -- sk_data: INTEIRO YYYYMMDD, sentinela 1900-01-01 → NULL
        , CASE
              WHEN dt_geracao_lote = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_geracao_lote, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data

        -- ── Natural keys preservadas com prefixo nk_ ─────────────────────────────
        , nr_lote                                                                AS nk_lote
        , nr_prescricao                                                          AS nk_prescricao
        , nr_atendimento                                                         AS nk_atendimento

        -- ── Dimensões degeneradas (sem tabela própria) ───────────────────────────
        , cd_setor_atendimento
        , cd_estabelecimento
        , ds_classificacao
        , ie_momento_classif
        , ie_tipo_classif
        , ds_turno
        , ie_status_lote
        , ie_urgente
        , ie_controlado
        , ie_dispensacao
        , ie_tipo_lote

        -- ── Métricas aditivas — SLAs em minutos por etapa ────────────────────────
        , qt_min_ger_atend
        , qt_min_atend_disp
        , qt_min_disp_entrega
        , qt_min_entrega_receb
        , qt_min_total_ciclo

        -- ── Métricas aditivas — volume do lote ───────────────────────────────────
        , qt_itens
        , qt_materiais_distintos
        , qt_itens_controlados
        , qt_itens_urgentes
        , qt_itens_substituidos
        , qt_dispensada_total
        , qt_real_disp_total

        -- ── Alertas pré-calculados (regras_negocio.md — SLA total > 720 min = 12h) ─
        , CASE
              WHEN qt_min_total_ciclo > 720 THEN 'VERMELHO'
              WHEN qt_min_total_ciclo > 480 THEN 'AMARELO'
              WHEN qt_min_total_ciclo IS NULL THEN 'SEM_DADO'
              ELSE 'VERDE'
          END                                                                    AS ie_nivel_alerta_sla

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
