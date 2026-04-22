{#-
  Silver-Context: Item Dispensacao
  Detalhe de cada item dispensado dentro de um lote AP_LOTE_ITEM com dados do lote pai,
  do material e do horario de administracao prescrito.
  Grao: 1 linha por item dispensado (AP_LOTE_ITEM.NR_SEQUENCIA).
  PK: nr_seq_item.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH ap_lote_item AS (
  SELECT * FROM {{ ref('silver_tasy_ap_lote_item') }}
)
, ap_lote AS (
  SELECT * FROM {{ ref('silver_tasy_ap_lote') }}
)
, material AS (
  SELECT * FROM {{ ref('silver_tasy_material') }}
)
, prescr_mat_hor AS (
  SELECT * FROM {{ ref('silver_tasy_prescr_mat_hor') }}
)

SELECT
    ap_lote_item.nr_sequencia                                                  AS nr_seq_item
  , ap_lote_item.nr_seq_lote                                                   AS nr_seq_lote
  , ap_lote_item.nr_seq_mat_hor                                                AS nr_seq_mat_hor
  , ap_lote_item.nr_prescricao                                                 AS nr_prescricao
  , ap_lote_item.nr_seq_prescr_mat                                             AS nr_seq_prescr_mat

    -- Contexto do lote pai (desnormalizado)
  , ap_lote.nr_atendimento                                                     AS nr_atendimento
  , ap_lote.cd_setor_atendimento                                               AS cd_setor_atendimento
  , ap_lote.ie_status_lote                                                     AS ie_status_lote
  , ap_lote.dt_geracao_lote                                                    AS dt_geracao_lote
  , ap_lote.dt_disp_farmacia                                                   AS dt_disp_farmacia_lote
  , ap_lote.dt_recebimento_setor                                               AS dt_recebimento_setor_lote

    -- Material (desnormalizado do cadastro mestre)
  , ap_lote_item.cd_material                                                   AS cd_material
  , ap_lote_item.cd_material_original                                          AS cd_material_original
  , material.ds_material                                                       AS ds_material
  , material.ds_reduzida                                                       AS ds_material_reduzida
  , material.ds_principio_ativo                                                AS ds_principio_ativo
  , material.cd_grupo_material                                                 AS cd_grupo_material
  , material.cd_subgrupo_material                                              AS cd_subgrupo_material
  , material.cd_classe_material                                                AS cd_classe_material
  , material.ie_tipo_material                                                  AS ie_tipo_material
  , material.ie_controlado                                                     AS ie_controlado_material
  , material.ie_alto_risco                                                     AS ie_alto_risco_material
  , material.ie_medicamento                                                    AS ie_medicamento_material
  , material.ie_antibiotico                                                    AS ie_antibiotico_material
  , material.ie_quimioterapico                                                 AS ie_quimioterapico_material

    -- Unidade de medida e local
  , ap_lote_item.cd_unidade_medida                                             AS cd_unidade_medida
  , ap_lote_item.cd_local_consistido                                           AS cd_local_consistido

    -- Quantidades (ja saneadas na Silver)
  , ap_lote_item.qt_dispensada                                                 AS qt_dispensada
  , ap_lote_item.qt_estoque                                                    AS qt_estoque
  , ap_lote_item.qt_prescrita                                                  AS qt_prescrita
  , ap_lote_item.qt_real_disp                                                  AS qt_real_disp

    -- Flags do item
  , ap_lote_item.ie_prescrito                                                  AS ie_prescrito
  , ap_lote_item.ie_urgente                                                    AS ie_urgente
  , ap_lote_item.ie_item_substituido                                           AS ie_item_substituido
  , ap_lote_item.ie_controlado                                                 AS ie_controlado
  , ap_lote_item.ie_gerado                                                     AS ie_gerado
  , ap_lote_item.ie_status_item                                                AS ie_status_item

    -- Timestamps do item
  , ap_lote_item.dt_dispensacao                                                AS dt_dispensacao
  , ap_lote_item.dt_geracao                                                    AS dt_geracao
  , ap_lote_item.dt_entrega                                                    AS dt_entrega
  , ap_lote_item.dt_recebimento                                                AS dt_recebimento
  , ap_lote_item.dt_validade                                                   AS dt_validade

    -- Horario prescrito (ponte para PRESCR_MAT_HOR)
    -- Cobertura: 100% AP_LOTE_ITEM -> PRESCR_MAT_HOR (cruzamentos.md)
  , prescr_mat_hor.dt_horario                                                  AS dt_horario_prescrito
  , prescr_mat_hor.dt_lib_horario                                              AS dt_lib_horario_prescrito
  , prescr_mat_hor.dt_administracao                                            AS dt_administracao_prescrito
  , prescr_mat_hor.ie_agrupador                                                AS ie_agrupador_prescrito
  , prescr_mat_hor.ie_status_execucao                                          AS ie_status_execucao_prescrito
  , prescr_mat_hor.ie_suspenso                                                 AS ie_suspenso_prescrito

    -- Derivada: tipo de dispensacao (via lote vs direta e fora do escopo deste grao,
    -- mas marcamos se o item tem vinculo com horario - substrato para 'cobertura lote vs direta')
  , CASE
      WHEN ap_lote_item.nr_seq_mat_hor IS NOT NULL AND ap_lote_item.nr_seq_mat_hor <> -1 THEN 'S'
      ELSE 'N'
    END                                                                        AS ie_vinculado_horario

    -- Observacoes
  , ap_lote_item.ds_observacao                                                 AS ds_observacao
  , ap_lote_item.ds_justificativa                                              AS ds_justificativa
  , ap_lote_item.nr_lote_fabric                                                AS nr_lote_fabric

    -- Auditoria TASY
  , ap_lote_item.dt_atualizacao                                                AS dt_atualizacao
  , ap_lote_item.nm_usuario                                                    AS nm_usuario
  , ap_lote_item.dt_atualizacao_nrec                                           AS dt_atualizacao_nrec
  , ap_lote_item.nm_usuario_nrec                                               AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM ap_lote_item
LEFT JOIN ap_lote
  ON ap_lote.nr_sequencia = ap_lote_item.nr_seq_lote
LEFT JOIN material
  ON material.cd_material = ap_lote_item.cd_material
LEFT JOIN prescr_mat_hor
  ON prescr_mat_hor.nr_sequencia = ap_lote_item.nr_seq_mat_hor
