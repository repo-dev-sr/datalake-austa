{#-
  Silver-Context: Erro Prescricao
  Erros de prescricao detectados pelas regras de consistencia (PRESCR_MEDICA_ERRO),
  enriquecidos com descricao da regra (REGRA_CONSISTE_PRESCR) e contexto da prescricao
  (PRESCR_MEDICA: setor, medico, atendimento).
  Grao: 1 linha por erro de prescricao detectado.
  PK: nr_seq_erro.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH erro AS (
  SELECT * FROM {{ ref('silver_tasy_prescr_medica_erro') }}
)
, prescricao AS (
  SELECT * FROM {{ ref('silver_tasy_prescr_medica') }}
)
, regra AS (
  SELECT * FROM {{ ref('silver_tasy_regra_consiste_prescr') }}
)

SELECT
    erro.nr_sequencia                                                          AS nr_seq_erro
  , erro.nr_prescricao                                                         AS nr_prescricao
  , erro.nr_seq_medic                                                          AS nr_seq_medic
  , erro.nr_regra                                                              AS nr_regra

    -- Contexto da prescricao (desnormalizado)
    -- Cobertura 100% PRESCR_MEDICA_ERRO -> PRESCR_MEDICA (cruzamentos.md)
  , prescricao.nr_atendimento                                                  AS nr_atendimento
  , prescricao.cd_medico                                                       AS cd_medico
  , prescricao.cd_setor_atendimento                                            AS cd_setor_atendimento
  , prescricao.cd_pessoa_fisica                                                AS cd_pessoa_fisica
  , prescricao.cd_estabelecimento                                              AS cd_estabelecimento
  , prescricao.dt_prescricao                                                   AS dt_prescricao
  , prescricao.dt_liberacao                                                    AS dt_liberacao

    -- Regra de consistencia (desnormalizada)
  , regra.ds_regra                                                             AS ds_regra
  , regra.ds_mensagem                                                          AS ds_mensagem_regra
  , regra.ie_tipo_regra                                                        AS ie_tipo_regra
  , regra.ie_criticidade                                                       AS ie_criticidade_regra
  , regra.ie_exibe_farmacia                                                    AS ie_exibe_farmacia
  , regra.ie_acao                                                              AS ie_acao_regra

    -- Detalhes do erro
  , erro.ie_tipo_erro                                                          AS ie_tipo_erro
  , erro.ie_criticidade                                                        AS ie_criticidade
  , erro.ie_origem                                                             AS ie_origem
  , erro.ds_erro                                                               AS ds_erro
  , erro.ds_resolucao                                                          AS ds_resolucao

    -- Timestamps do ciclo do erro
  , erro.dt_erro                                                               AS dt_erro
  , erro.dt_resolucao                                                          AS dt_resolucao

    -- Derivada: status de resolucao
  , CASE
      WHEN erro.dt_resolucao IS NOT NULL THEN 'resolvido'
      ELSE 'pendente'
    END                                                                        AS ie_status_resolucao

    -- Derivada: SLA em dias ate resolucao (quando aplicavel)
  , CASE
      WHEN erro.dt_resolucao IS NOT NULL AND erro.dt_erro IS NOT NULL
      THEN DATEDIFF(erro.dt_resolucao, erro.dt_erro)
      ELSE NULL
    END                                                                        AS qt_dias_resolucao

    -- Auditoria TASY
  , erro.dt_atualizacao                                                        AS dt_atualizacao
  , erro.nm_usuario                                                            AS nm_usuario
  , erro.dt_atualizacao_nrec                                                   AS dt_atualizacao_nrec
  , erro.nm_usuario_nrec                                                       AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM erro
LEFT JOIN prescricao
  ON prescricao.nr_prescricao = erro.nr_prescricao
LEFT JOIN regra
  ON regra.nr_sequencia = erro.nr_regra
