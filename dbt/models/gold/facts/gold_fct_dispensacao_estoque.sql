-- Grain: 1 linha = 1 movimento de estoque valorizado (MOVIMENTO_ESTOQUE.nr_movimento_estoque)
-- Fonte: silver_context.movimento_estoque via {{ ref('movimento_estoque') }}
-- Camada: Gold | Processo: Dispensação Efetiva no Estoque (Lote → Movimento) | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · gold_dim_medicamento (sk_medicamento)
-- Benchmark (regras_negocio.md §22.5): 99,71% de cobertura AP_LOTE → MOVIMENTO_ESTOQUE
-- Filtros: ie_origem_documento='3' (Prescrição) + cd_operacao_estoque=3 (Consumo Paciente) = 92% dos movimentos
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_dispensacao_estoque'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "dispensacao_estoque"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('movimento_estoque') }}
    WHERE ie_tipo_funcional = 'dispensacao_paciente'   -- escopo: dispensação ao paciente
    {% if is_incremental() %}
      AND _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
    -- ⚠️  _is_deleted não exposto em silver_context.movimento_estoque.
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_movimento_estoque']) }}       AS sk_dispensacao_estoque

        -- ── Foreign keys para dimensões conformadas ──────────────────────────────
        , CASE
              WHEN dt_movimento_estoque = DATE '1900-01-01' THEN NULL
              ELSE CAST(DATE_FORMAT(dt_movimento_estoque, 'yyyyMMdd') AS INT)
          END                                                                    AS sk_data
        , {{ dbt_utils.generate_surrogate_key(['cd_material']) }}               AS sk_medicamento

        -- ── Natural keys preservadas ─────────────────────────────────────────────
        , nr_movimento_estoque                                                   AS nk_movimento_estoque
        , nr_prescricao                                                          AS nk_prescricao
        , nr_atendimento                                                         AS nk_atendimento
        , cd_material                                                            AS nk_medicamento

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_operacao_estoque
        , ds_operacao_estoque
        , cd_local_estoque
        , cd_local_estoque_dest
        , cd_setor_atendimento
        , cd_estabelecimento
        , ie_origem_documento
        , ie_tipo_movimento
        , ie_entrada_saida
        , ie_tipo_funcional
        , ie_situacao
        , ie_controlado_material
        , ie_medicamento_material
        , cd_unidade_medida
        , nr_lote_fabric

        -- ── Métricas aditivas ────────────────────────────────────────────────────
        , qt_movimento
        , vl_movimento
        , vl_unitario

        -- ── Valorização custo médio ponderado (cd_tipo_valor=7) ────────────────
        -- Nota (silver_context): 99,26% dos movimentos têm valorização; 0,74% podem ser NULL
        , vl_movimento_custo
        , vl_unitario_custo
        , vl_custo
        , vl_total_custo

        -- ── Flags derivadas ──────────────────────────────────────────────────────
        , CASE WHEN vl_movimento_custo IS NULL THEN 1 ELSE 0 END                 AS qt_movimento_sem_valorizacao

        -- ── Alerta pré-calculado ─────────────────────────────────────────────────
        , CASE
              WHEN vl_movimento_custo IS NULL THEN 'SEM_VALORIZACAO'
              WHEN vl_movimento_custo = 0 THEN 'VALOR_ZERO'
              WHEN ie_controlado_material = 'S' THEN 'CONTROLADO'
              ELSE 'NORMAL'
          END                                                                    AS ie_nivel_alerta_movimento

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
