{#-
  Silver-Context: Analise Farmaceutica
  Log de analise/intervencao farmaceutica (LOG_ANALISE_PRESCR) enriquecido com
  contexto da prescricao (setor, medico, atendimento).
  Grao: 1 linha por analise farmaceutica registrada.
  PK: nr_seq_analise.
  Observacao de volume: ~8 analises/dia (3.085 registros em 3 meses) - tabela de baixo volume
  mas critica para indicadores de farmacia clinica.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH analise AS (
  SELECT * FROM {{ ref('silver_tasy_log_analise_prescr') }}
)
, prescricao AS (
  SELECT * FROM {{ ref('silver_tasy_prescr_medica') }}
)

SELECT
    analise.nr_sequencia                                                       AS nr_seq_analise
  , analise.nr_prescricao                                                      AS nr_prescricao
  , analise.cd_farmaceutico                                                    AS cd_farmaceutico
  , analise.cd_pessoa_fisica                                                   AS cd_pessoa_fisica

    -- Contexto da prescricao (desnormalizado)
    -- Cobertura 100% LOG_ANALISE_PRESCR -> PRESCR_MEDICA (cruzamentos.md)
  , prescricao.nr_atendimento                                                  AS nr_atendimento
  , prescricao.cd_medico                                                       AS cd_medico
  , prescricao.cd_setor_atendimento                                            AS cd_setor_atendimento
  , prescricao.cd_estabelecimento                                              AS cd_estabelecimento
  , prescricao.dt_prescricao                                                   AS dt_prescricao
  , prescricao.dt_liberacao                                                    AS dt_liberacao_prescricao

    -- Detalhes da analise
  , analise.ie_tipo_analise                                                    AS ie_tipo_analise
  , analise.ie_resultado                                                       AS ie_resultado
  , analise.ds_analise                                                         AS ds_analise
  , analise.ds_observacao                                                      AS ds_observacao
  , analise.ds_intervencao                                                     AS ds_intervencao

    -- Timestamp da analise
  , analise.dt_analise                                                         AS dt_analise

    -- Derivada: houve intervencao registrada
  , CASE
      WHEN analise.ds_intervencao IS NOT NULL
        AND TRIM(analise.ds_intervencao) <> ''
        AND LOWER(analise.ds_intervencao) <> 'indefinido'
      THEN 'S'
      ELSE 'N'
    END                                                                        AS ie_tem_intervencao

    -- Auditoria TASY
  , analise.dt_atualizacao                                                     AS dt_atualizacao
  , analise.nm_usuario                                                         AS nm_usuario
  , analise.dt_atualizacao_nrec                                                AS dt_atualizacao_nrec
  , analise.nm_usuario_nrec                                                    AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM analise
LEFT JOIN prescricao
  ON prescricao.nr_prescricao = analise.nr_prescricao
