-- Grain: 1 linha = 1 lote de dispensação cancelado ou suspenso (ie_status_lote IN ('C','CA','S'))
-- Fonte: silver_context.dispensacao via {{ ref('dispensacao') }}
-- Camada: Gold | Processo: Taxa de Cancelamento / Desperdício Operacional | Constellation Austa
-- Dimensões: dim_tempo (sk_data)
-- Alerta: desperdício operacional = cancelado APÓS início de dispensação (regras_negocio.md)
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_cancelamento_lote'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "cancelamento_lote"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('dispensacao') }}
    WHERE ie_status_lote IN ('C', 'CA', 'S')
    {% if is_incremental() %}
      AND _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não exposto em silver_context.dispensacao (gap do silver_context_audit_columns).
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_lote']) }}                    AS sk_cancelamento_lote

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        , CASE
              WHEN dt_geracao_lote = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_geracao_lote, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data_geracao
        , CASE
              WHEN dt_cancelamento = DATE '1900-01-01' OR dt_cancelamento IS NULL THEN NULL
              ELSE CAST(DATE_FORMAT(dt_cancelamento, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data_cancelamento

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_lote                                                                AS nk_lote
        , nr_prescricao                                                          AS nk_prescricao
        , nr_atendimento                                                         AS nk_atendimento

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_setor_atendimento
        , cd_estabelecimento
        , ds_classificacao
        , ie_momento_classif
        , ie_tipo_classif
        , ie_status_lote
        , ie_status_ciclo
        , ie_urgente
        , ie_controlado

        -- ── Métricas aditivas ────────────────────────────────────────────────────
        -- Contador unitário facilita SUM() para pct_cancelamento em camada de consumo
        , 1                                                                      AS qt_lote_cancelado
        , CASE WHEN ie_status_lote IN ('C', 'CA') THEN 1 ELSE 0 END              AS qt_cancelado_estrito
        , CASE WHEN ie_status_lote = 'S'          THEN 1 ELSE 0 END              AS qt_suspenso
        , CASE WHEN ie_cancelado_apos_disp = 'S'  THEN 1 ELSE 0 END              AS qt_desperdicio_operacional

        -- ── Métrica semi-aditiva ─────────────────────────────────────────────────
        -- ⚠️  qt_min_ger_cancel: minutos entre geração e cancelamento.
        --     Somar somente dentro do mesmo lote — usar AVG/MEDIAN em agregações por setor/mês.
        , qt_min_ger_cancel

        -- ── Alerta pré-calculado ─────────────────────────────────────────────────
        -- Desperdício = cancelou DEPOIS que farmácia já começou a trabalhar (regras_negocio.md)
        , CASE
              WHEN ie_cancelado_apos_disp = 'S' THEN 'DESPERDICIO'
              WHEN ie_status_lote IN ('C', 'CA') THEN 'CANCELADO_PREVENTIVO'
              WHEN ie_status_lote = 'S' THEN 'SUSPENSO'
              ELSE 'OUTRO'
          END                                                                    AS ie_nivel_alerta_cancel

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
