-- Grain: 1 linha = 1 item dispensado dentro de um lote (AP_LOTE_ITEM.nr_sequencia)
-- Fonte: silver_context.item_dispensacao via {{ ref('item_dispensacao') }}
-- Camada: Gold | Processo: Volume Dispensado por Medicamento e Setor | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · gold_dim_medicamento (sk_medicamento)
-- Filtros: ie_status_lote IN ('D','E','R') garante dispensação real (regras_negocio.md)
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_volume_dispensacao'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "volume_dispensacao"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('item_dispensacao') }}
    WHERE ie_status_lote IN ('D', 'E', 'R')
      AND qt_dispensada > 0               -- regras_negocio.md: exclui itens zerados e 52 negativos observados
    {% if is_incremental() %}
      AND _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não exposto em silver_context.item_dispensacao.
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_seq_item']) }}                AS sk_volume_dispensacao

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        , CASE
              WHEN dt_geracao_lote = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_geracao_lote, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data
        , {{ dbt_utils.generate_surrogate_key(['cd_material']) }}               AS sk_medicamento

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_seq_item                                                            AS nk_item_dispensacao
        , nr_seq_lote                                                            AS nk_lote
        , nr_prescricao                                                          AS nk_prescricao
        , nr_atendimento                                                         AS nk_atendimento
        , cd_material                                                            AS nk_medicamento

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_setor_atendimento
        , ie_status_lote
        , ie_status_item
        , ie_controlado
        , ie_urgente
        , ie_item_substituido
        , ie_vinculado_horario
        , cd_unidade_medida

        -- Flags clínicas do material (oriundas do cadastro — para drill-down imediato)
        , ie_controlado_material
        , ie_alto_risco_material
        , ie_medicamento_material
        , ie_antibiotico_material
        , ie_quimioterapico_material

        -- ── Métricas aditivas ────────────────────────────────────────────────────
        , 1                                                                      AS qt_item_dispensado
        , qt_dispensada
        , qt_real_disp
        , qt_prescrita
        , qt_estoque

        -- ── Alertas pré-calculados ───────────────────────────────────────────────
        , CASE
              WHEN ie_alto_risco_material    = 'S' THEN 'ALTO_RISCO'
              WHEN ie_quimioterapico_material = 'S' THEN 'QUIMIOTERAPICO'
              WHEN ie_controlado_material    = 'S' THEN 'CONTROLADO'
              WHEN ie_antibiotico_material   = 'S' THEN 'ANTIBIOTICO'
              ELSE 'COMUM'
          END                                                                    AS ie_classificacao_clinica

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
