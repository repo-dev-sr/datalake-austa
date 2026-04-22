-- Grain: 1 linha = 1 prescrição com cobertura calculada (itens prescritos vs dispensados via lote)
-- Fonte: silver_context.prescricao + silver_context.item_dispensacao
--        via {{ ref('prescricao') }} e {{ ref('item_dispensacao') }}
-- Camada: Gold | Processo: Cobertura Lote vs Dispensação Direta | Constellation Austa
-- Dimensões: dim_tempo (sk_data) · dim_medico (sk_medico)
-- Benchmark (regras_negocio.md): 78,44% via lote; 21,56% dispensação direta
{{
  config(
      materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_cobertura'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "farmacia", "cobertura_dispensacao"]
  )
}}

WITH prescricao AS (
    SELECT * FROM {{ ref('prescricao') }}
    WHERE ie_tem_medicamento = 'S'       -- só faz sentido calcular cobertura se há medicamento
    {% if is_incremental() %}
      AND _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
)

, itens_dispensados AS (
    SELECT
          item_dispensacao.nr_prescricao                                         AS nr_prescricao
        , COUNT(*)                                                               AS qt_itens_dispensados_via_lote
        , SUM(CASE WHEN item_dispensacao.ie_vinculado_horario = 'S' THEN 1 ELSE 0 END)
                                                                                 AS qt_itens_com_horario_vinculado
    FROM {{ ref('item_dispensacao') }} AS item_dispensacao
    WHERE item_dispensacao.ie_status_lote IN ('D', 'E', 'R')
      AND item_dispensacao.qt_dispensada > 0
    GROUP BY item_dispensacao.nr_prescricao
)

, joined AS (
    SELECT
          prescricao.nr_prescricao
        , prescricao.dt_liberacao
        , prescricao.nr_atendimento
        , prescricao.cd_medico
        , prescricao.cd_setor_atendimento
        , prescricao.cd_estabelecimento
        , prescricao.qt_medicamentos
        , prescricao.qt_itens_controlados
        , prescricao.qt_itens_urgentes
        , prescricao.ie_adep
        , COALESCE(itens_dispensados.qt_itens_dispensados_via_lote, 0)          AS qt_itens_dispensados_via_lote
        , COALESCE(itens_dispensados.qt_itens_com_horario_vinculado, 0)         AS qt_itens_com_horario_vinculado
    FROM prescricao
    LEFT JOIN itens_dispensados
      ON itens_dispensados.nr_prescricao = prescricao.nr_prescricao
)

, final AS (
    SELECT
        -- ── Surrogate key da fato ────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['nr_prescricao']) }}              AS sk_cobertura

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

        -- ── Dimensões degeneradas ────────────────────────────────────────────────
        , cd_setor_atendimento
        , cd_estabelecimento
        , ie_adep

        -- ── Métricas aditivas ────────────────────────────────────────────────────
        , qt_medicamentos                                                        AS qt_medicamentos_prescritos
        , qt_itens_dispensados_via_lote
        , qt_itens_com_horario_vinculado
        , qt_itens_controlados
        , qt_itens_urgentes

        -- ── Métrica não-aditiva ──────────────────────────────────────────────────
        -- ⚠️  pct_cobertura_lote: NÃO somar — recalcular em cada agregação
        --     usando SUM(qt_itens_dispensados_via_lote) * 100.0 / NULLIF(SUM(qt_medicamentos_prescritos), 0)
        , CASE
              WHEN qt_medicamentos > 0
              THEN CAST(qt_itens_dispensados_via_lote * 100.0 / qt_medicamentos AS DECIMAL(5,2))
              ELSE NULL
          END                                                                    AS pct_cobertura_lote

        -- ── Flags derivadas ──────────────────────────────────────────────────────
        , CASE
              WHEN qt_itens_dispensados_via_lote > 0 THEN 1
              ELSE 0
          END                                                                    AS qt_prescricao_com_lote
        , CASE
              WHEN qt_itens_dispensados_via_lote = 0 THEN 1
              ELSE 0
          END                                                                    AS qt_prescricao_sem_lote

        -- ── Alerta: prescrição com medicamento prescrito mas sem lote ──────────
        , CASE
              WHEN qt_itens_dispensados_via_lote = 0 AND qt_medicamentos > 0 THEN 'SEM_LOTE'
              WHEN qt_itens_dispensados_via_lote < qt_medicamentos           THEN 'COBERTURA_PARCIAL'
              ELSE 'COBERTURA_TOTAL'
          END                                                                    AS ie_nivel_cobertura

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM joined
)

SELECT * FROM final
