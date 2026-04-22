{#-
  Silver-Context: Dispensacao
  Consolida lote de dispensacao (AP_LOTE) com agregados de itens (AP_LOTE_ITEM),
  classificacao (CLASSIF_LOTE_DISP_FAR) e turno (REGRA_TURNO_DISP).
  Calcula SLAs em minutos entre cada etapa do ciclo G->A->D->E->R.
  Grao: 1 linha por lote de dispensacao (AP_LOTE.NR_SEQUENCIA).
  PK: nr_lote.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH ap_lote AS (
  SELECT * FROM {{ ref('silver_tasy_ap_lote') }}
)
, itens AS (
  SELECT
      ap_lote_item.nr_seq_lote                                                 AS nr_seq_lote
    , COUNT(*)                                                                 AS qt_itens
    , COUNT(DISTINCT ap_lote_item.cd_material)                                 AS qt_materiais_distintos
    , SUM(CASE WHEN ap_lote_item.ie_controlado = 'S' THEN 1 ELSE 0 END)        AS qt_itens_controlados
    , SUM(CASE WHEN ap_lote_item.ie_urgente = 'S' THEN 1 ELSE 0 END)           AS qt_itens_urgentes
    , SUM(CASE WHEN ap_lote_item.ie_item_substituido = 'S' THEN 1 ELSE 0 END)  AS qt_itens_substituidos
    , SUM(CASE WHEN ap_lote_item.qt_dispensada > 0 THEN ap_lote_item.qt_dispensada ELSE 0 END) AS qt_dispensada_total
    , SUM(CASE WHEN ap_lote_item.qt_real_disp > 0  THEN ap_lote_item.qt_real_disp  ELSE 0 END) AS qt_real_disp_total
  FROM {{ ref('silver_tasy_ap_lote_item') }} AS ap_lote_item
  GROUP BY ap_lote_item.nr_seq_lote
)
, classificacao AS (
  SELECT * FROM {{ ref('silver_tasy_classif_lote_disp_far') }}
)
, turno AS (
  SELECT * FROM {{ ref('silver_tasy_regra_turno_disp') }}
)

SELECT
    ap_lote.nr_sequencia                                                       AS nr_lote
  , ap_lote.nr_prescricao                                                      AS nr_prescricao
  , ap_lote.nr_atendimento                                                     AS nr_atendimento
  , ap_lote.cd_setor_atendimento                                               AS cd_setor_atendimento
  , ap_lote.cd_setor_ant                                                       AS cd_setor_ant
  , ap_lote.cd_estabelecimento                                                 AS cd_estabelecimento

    -- Classificacao do lote
  , ap_lote.nr_seq_classif                                                     AS nr_seq_classif
  , classificacao.ds_classificacao                                             AS ds_classificacao
  , classificacao.ie_momento                                                   AS ie_momento_classif
  , classificacao.ie_tipo_lote                                                 AS ie_tipo_classif

    -- Turno de dispensacao
  , ap_lote.nr_seq_turno                                                       AS nr_seq_turno
  , turno.ds_turno                                                             AS ds_turno
  , turno.hr_inicio                                                            AS hr_turno_inicio
  , turno.hr_fim                                                               AS hr_turno_fim

    -- Lote pai (desdobramento)
  , ap_lote.nr_seq_lote_sup                                                    AS nr_seq_lote_sup
  , ap_lote.nr_seq_embalagem                                                   AS nr_seq_embalagem
  , ap_lote.nr_seq_motivo_parcial                                              AS nr_seq_motivo_parcial
  , ap_lote.nr_seq_mot_desdobrar                                               AS nr_seq_mot_desdobrar

    -- Flags do lote
  , ap_lote.ie_status_lote                                                     AS ie_status_lote
  , ap_lote.ie_urgente                                                         AS ie_urgente
  , ap_lote.ie_controlado                                                      AS ie_controlado
  , ap_lote.ie_dispensacao                                                     AS ie_dispensacao
  , ap_lote.ie_tipo_lote                                                       AS ie_tipo_lote

    -- Timestamps do ciclo SLA (G -> I -> A -> D -> E -> R) + cancelamento/suspensao
    -- dt_inicio_dispensacao: 19,32% preenchido em cancelados (vs 0,02% dt_atend_farmacia) —
    -- usar para detectar desperdício operacional em gold_fct_cancelamento_lote
  , ap_lote.dt_geracao_lote                                                    AS dt_geracao_lote
  , ap_lote.dt_inicio_dispensacao                                              AS dt_inicio_dispensacao
  , ap_lote.dt_atend_farmacia                                                  AS dt_atend_farmacia
  , ap_lote.dt_disp_farmacia                                                   AS dt_disp_farmacia
  , ap_lote.dt_entrega_setor                                                   AS dt_entrega_setor
  , ap_lote.dt_recebimento_setor                                               AS dt_recebimento_setor
  , ap_lote.dt_cancelamento                                                    AS dt_cancelamento
  , ap_lote.dt_suspensao                                                       AS dt_suspensao

    -- SLAs em minutos por etapa (* 1440 converte dias em minutos)
    -- Calculado apenas quando ambas as datas existem
  , CASE
      WHEN ap_lote.dt_atend_farmacia IS NOT NULL AND ap_lote.dt_geracao_lote IS NOT NULL
      THEN CAST((UNIX_TIMESTAMP(ap_lote.dt_atend_farmacia) - UNIX_TIMESTAMP(ap_lote.dt_geracao_lote)) / 60.0 AS DECIMAL(12,2))
      ELSE NULL
    END                                                                        AS qt_min_ger_atend
  , CASE
      WHEN ap_lote.dt_disp_farmacia IS NOT NULL AND ap_lote.dt_atend_farmacia IS NOT NULL
      THEN CAST((UNIX_TIMESTAMP(ap_lote.dt_disp_farmacia) - UNIX_TIMESTAMP(ap_lote.dt_atend_farmacia)) / 60.0 AS DECIMAL(12,2))
      ELSE NULL
    END                                                                        AS qt_min_atend_disp
  , CASE
      WHEN ap_lote.dt_entrega_setor IS NOT NULL AND ap_lote.dt_disp_farmacia IS NOT NULL
      THEN CAST((UNIX_TIMESTAMP(ap_lote.dt_entrega_setor) - UNIX_TIMESTAMP(ap_lote.dt_disp_farmacia)) / 60.0 AS DECIMAL(12,2))
      ELSE NULL
    END                                                                        AS qt_min_disp_entrega
  , CASE
      WHEN ap_lote.dt_recebimento_setor IS NOT NULL AND ap_lote.dt_entrega_setor IS NOT NULL
      THEN CAST((UNIX_TIMESTAMP(ap_lote.dt_recebimento_setor) - UNIX_TIMESTAMP(ap_lote.dt_entrega_setor)) / 60.0 AS DECIMAL(12,2))
      ELSE NULL
    END                                                                        AS qt_min_entrega_receb
  , CASE
      WHEN ap_lote.dt_recebimento_setor IS NOT NULL AND ap_lote.dt_geracao_lote IS NOT NULL
      THEN CAST((UNIX_TIMESTAMP(ap_lote.dt_recebimento_setor) - UNIX_TIMESTAMP(ap_lote.dt_geracao_lote)) / 60.0 AS DECIMAL(12,2))
      ELSE NULL
    END                                                                        AS qt_min_total_ciclo
  , CASE
      WHEN ap_lote.dt_cancelamento IS NOT NULL AND ap_lote.dt_geracao_lote IS NOT NULL
      THEN CAST((UNIX_TIMESTAMP(ap_lote.dt_cancelamento) - UNIX_TIMESTAMP(ap_lote.dt_geracao_lote)) / 60.0 AS DECIMAL(12,2))
      ELSE NULL
    END                                                                        AS qt_min_ger_cancel

    -- Agregados dos itens
  , COALESCE(itens.qt_itens, 0)                                                AS qt_itens
  , COALESCE(itens.qt_materiais_distintos, 0)                                  AS qt_materiais_distintos
  , COALESCE(itens.qt_itens_controlados, 0)                                    AS qt_itens_controlados
  , COALESCE(itens.qt_itens_urgentes, 0)                                       AS qt_itens_urgentes
  , COALESCE(itens.qt_itens_substituidos, 0)                                   AS qt_itens_substituidos
  , itens.qt_dispensada_total                                                  AS qt_dispensada_total
  , itens.qt_real_disp_total                                                   AS qt_real_disp_total

    -- Derivadas de negocio (sem threshold, apenas categorizacao estrutural)
    -- Status simplificado: 'R' = ciclo completo, 'C'/'CA' = cancelado
  , CASE
      WHEN ap_lote.ie_status_lote = 'R'           THEN 'ciclo_completo'
      WHEN ap_lote.ie_status_lote IN ('C', 'CA')  THEN 'cancelado'
      WHEN ap_lote.ie_status_lote = 'S'           THEN 'suspenso'
      WHEN ap_lote.ie_status_lote = 'E'           THEN 'entregue'
      WHEN ap_lote.ie_status_lote = 'D'           THEN 'dispensado'
      WHEN ap_lote.ie_status_lote = 'A'           THEN 'atendido'
      WHEN ap_lote.ie_status_lote = 'G'           THEN 'gerado'
      ELSE 'outro'
    END                                                                        AS ie_status_ciclo

    -- Flag se foi cancelado apos inicio de dispensacao (desperdicio operacional).
    -- Usa dt_inicio_dispensacao (19,32% preenchido em cancelados) — NAO dt_disp_farmacia
    -- (que e quando a dispensacao concluiu, nao quando comecou).
    -- Validado pela Camada 7 em 2026-04-19 (DIFF-001): com dt_disp_farmacia disparava 33 lotes;
    -- com dt_inicio_dispensacao dispara ~31.891 (correto).
  , CASE
      WHEN ap_lote.ie_status_lote IN ('C','CA') AND ap_lote.dt_inicio_dispensacao IS NOT NULL THEN 'S'
      ELSE 'N'
    END                                                                        AS ie_cancelado_apos_disp

    -- Usuarios por etapa
  , ap_lote.nm_usuario_atendimento                                             AS nm_usuario_atendimento
  , ap_lote.nm_usuario_disp                                                    AS nm_usuario_disp
  , ap_lote.nm_usuario_entrega                                                 AS nm_usuario_entrega
  , ap_lote.nm_usuario_receb                                                   AS nm_usuario_receb

    -- Observacoes
  , ap_lote.ds_observacao                                                      AS ds_observacao
  , ap_lote.ds_justificativa                                                   AS ds_justificativa

    -- Auditoria TASY
  , ap_lote.dt_atualizacao                                                     AS dt_atualizacao
  , ap_lote.nm_usuario                                                         AS nm_usuario
  , ap_lote.dt_atualizacao_nrec                                                AS dt_atualizacao_nrec
  , ap_lote.nm_usuario_nrec                                                    AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM ap_lote
LEFT JOIN itens
  ON itens.nr_seq_lote = ap_lote.nr_sequencia
LEFT JOIN classificacao
  ON classificacao.nr_sequencia = ap_lote.nr_seq_classif
LEFT JOIN turno
  ON turno.nr_sequencia = ap_lote.nr_seq_turno
