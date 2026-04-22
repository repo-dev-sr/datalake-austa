{#-
  Silver-Context: Prescricao
  Join entre PRESCR_MEDICA (cabecalho) e PRESCR_MATERIAL (itens agregados por tipo).
  Grao: 1 linha por prescricao medica liberada.
  PK: nr_prescricao.
  Filtros de negocio aplicados aqui (exclusao de rascunhos e copias conforme regras_negocio.md).
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH prescricao AS (
  SELECT * FROM {{ ref('silver_tasy_prescr_medica') }}
  WHERE dt_liberacao IS NOT NULL
    AND cd_funcao_origem NOT IN (924, 950)
)
, itens AS (
  SELECT
      prescr_material.nr_prescricao                                              AS nr_prescricao
    , COUNT(*)                                                                   AS qt_itens
    , SUM(CASE WHEN prescr_material.ie_agrupador = '1' THEN 1 ELSE 0 END)        AS qt_medicamentos
    , SUM(CASE WHEN prescr_material.ie_agrupador = '2' THEN 1 ELSE 0 END)        AS qt_materiais
    , SUM(CASE WHEN prescr_material.ie_agrupador NOT IN ('1','2') THEN 1 ELSE 0 END) AS qt_outros_itens
    , SUM(CASE WHEN prescr_material.ie_suspenso = 'S' THEN 1 ELSE 0 END)         AS qt_itens_suspensos
    , SUM(CASE WHEN prescr_material.ie_controlado = 'S' THEN 1 ELSE 0 END)       AS qt_itens_controlados
    , SUM(CASE WHEN prescr_material.ie_urgencia = 'S' THEN 1 ELSE 0 END)         AS qt_itens_urgentes
    , MIN(prescr_material.dt_lib_material)                                       AS dt_primeira_lib_material
    , MAX(prescr_material.dt_lib_farmacia)                                       AS dt_ultima_lib_farmacia
  FROM {{ ref('silver_tasy_prescr_material') }} AS prescr_material
  WHERE prescr_material.ie_suspenso <> 'S'
  GROUP BY prescr_material.nr_prescricao
)
SELECT
    prescricao.nr_prescricao                                                   AS nr_prescricao
  , prescricao.nr_atendimento                                                  AS nr_atendimento
  , prescricao.cd_medico                                                       AS cd_medico
  , prescricao.cd_pessoa_fisica                                                AS cd_pessoa_fisica
  , prescricao.cd_setor_atendimento                                            AS cd_setor_atendimento
  , prescricao.cd_estabelecimento                                              AS cd_estabelecimento
  , prescricao.nr_seq_agrupador                                                AS nr_seq_agrupador
  , prescricao.nr_prescricao_original                                          AS nr_prescricao_original
  , prescricao.cd_funcao_origem                                                AS cd_funcao_origem

    -- Flags de negocio
  , prescricao.ie_origem_inf                                                   AS ie_origem_inf
  , prescricao.ie_adep                                                         AS ie_adep
  , prescricao.ie_prescr_emergencia                                            AS ie_prescr_emergencia
  , prescricao.ie_recem_nato                                                   AS ie_recem_nato
  , prescricao.ie_lib_farm                                                     AS ie_lib_farm
  , prescricao.ie_hemodialise                                                  AS ie_hemodialise
  , prescricao.ie_tipo_prescricao                                              AS ie_tipo_prescricao
  , prescricao.ie_situacao                                                     AS ie_situacao
  , prescricao.ie_retroativa                                                   AS ie_retroativa

    -- Timestamps do ciclo da prescricao
  , prescricao.dt_prescricao                                                   AS dt_prescricao
  , prescricao.dt_liberacao                                                    AS dt_liberacao
  , prescricao.dt_liberacao_medico                                             AS dt_liberacao_medico
  , prescricao.dt_liberacao_farmacia                                           AS dt_liberacao_farmacia
  , prescricao.dt_suspensao                                                    AS dt_suspensao
  , prescricao.dt_validade_prescr                                              AS dt_validade_prescr
  , prescricao.dt_entrega                                                      AS dt_entrega
  , prescricao.dt_primeiro_horario                                             AS dt_primeiro_horario

    -- Agregacoes dos itens (PRESCR_MATERIAL)
    -- Observacao: match PRESCR_MEDICA -> PRESCR_MATERIAL e 70,4% (cruzamentos.md)
    -- 29,6% das prescricoes sao de procedimento/recomendacao/dieta/gasoterapia sem item
  , COALESCE(itens.qt_itens, 0)                                                AS qt_itens
  , COALESCE(itens.qt_medicamentos, 0)                                         AS qt_medicamentos
  , COALESCE(itens.qt_materiais, 0)                                            AS qt_materiais
  , COALESCE(itens.qt_outros_itens, 0)                                         AS qt_outros_itens
  , COALESCE(itens.qt_itens_suspensos, 0)                                      AS qt_itens_suspensos
  , COALESCE(itens.qt_itens_controlados, 0)                                    AS qt_itens_controlados
  , COALESCE(itens.qt_itens_urgentes, 0)                                       AS qt_itens_urgentes
  , itens.dt_primeira_lib_material                                             AS dt_primeira_lib_material
  , itens.dt_ultima_lib_farmacia                                               AS dt_ultima_lib_farmacia

    -- Derivadas de negocio simples (sem alerta)
  , CASE
      WHEN prescricao.dt_liberacao_farmacia IS NOT NULL THEN 'liberada_farmacia'
      WHEN COALESCE(itens.qt_medicamentos, 0) > 0       THEN 'pendente_liberacao_farmacia'
      ELSE 'sem_medicamento'
    END                                                                        AS ie_status_liberacao_farmacia

  , CASE
      WHEN COALESCE(itens.qt_medicamentos, 0) > 0 THEN 'S'
      ELSE 'N'
    END                                                                        AS ie_tem_medicamento

  , CASE
      WHEN COALESCE(itens.qt_itens_controlados, 0) > 0 THEN 'S'
      ELSE 'N'
    END                                                                        AS ie_tem_controlado

    -- Observacoes
  , prescricao.ds_observacao                                                   AS ds_observacao
  , prescricao.ds_justificativa                                                AS ds_justificativa

    -- Auditoria TASY
  , prescricao.dt_atualizacao                                                  AS dt_atualizacao
  , prescricao.nm_usuario                                                      AS nm_usuario
  , prescricao.dt_atualizacao_nrec                                             AS dt_atualizacao_nrec
  , prescricao.nm_usuario_nrec                                                 AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM prescricao
LEFT JOIN itens
  ON itens.nr_prescricao = prescricao.nr_prescricao
