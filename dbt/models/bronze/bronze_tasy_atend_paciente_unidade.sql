-- Bronze ATEND_PACIENTE_UNIDADE: Raw Avro (stream) → Iceberg
-- Estado atual técnico por NR_SEQ_INTERNO com CDC incremental.
{{
  config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='merge',
    unique_key='NR_SEQ_INTERNO',
    on_schema_change='append_new_columns'
  )
}}
{% set raw_path = "s3a://" ~ var('datalake_bucket') ~ "/" ~ var('raw_tasy_stream_prefix') ~ var('raw_tasy_stream_topic_atend_paciente_unidade') ~ "/" %}
{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}
{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}

WITH target_watermark AS (
  {% if is_incremental() %}
    SELECT CAST(COALESCE(MAX(__ts_ms), 0) AS BIGINT) AS max_ts_ms
    FROM {{ this }}
  {% else %}
    SELECT CAST(0 AS BIGINT) AS max_ts_ms
  {% endif %}
),
params AS (
  SELECT
    CASE
      WHEN {{ cdc_reprocess_hours }} > 0 THEN
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - ({{ cdc_reprocess_hours }} * 3600)) * 1000
      ELSE
        GREATEST(
          0,
          (SELECT max_ts_ms FROM target_watermark) - ({{ cdc_lookback_hours }} * 3600 * 1000)
        )
    END AS wm_start_ms
),
raw_incremental AS (
  SELECT
    *,
    CASE
      WHEN __op = 'd' OR COALESCE(TRY_CAST(__deleted AS BOOLEAN), FALSE) THEN TRUE
      ELSE FALSE
    END AS is_deleted,
    CURRENT_TIMESTAMP() AS dt_criacao,
    CURRENT_TIMESTAMP() AS dt_atualizacao_lakehouse,
    '{{ var("fonte_sistema") }}' AS fonte_sistema
  FROM avro.`{{ raw_path }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
),
latest_by_pk AS (
  SELECT *
  FROM (
    SELECT
      r.*,
      ROW_NUMBER() OVER (
        PARTITION BY r.NR_SEQ_INTERNO
        ORDER BY
          CAST(COALESCE(r.__ts_ms, 0) AS BIGINT) DESC,
          CAST(COALESCE(r.__source_txId, 0) AS BIGINT) DESC,
          r.dt_atualizacao_lakehouse DESC
      ) AS rn
    FROM raw_incremental r
  ) x
  WHERE rn = 1
)
SELECT
  NR_SEQ_INTERNO,
  NR_ATENDIMENTO,
  NR_SEQUENCIA,
  CD_SETOR_ATENDIMENTO,
  CD_UNIDADE_BASICA,
  CD_UNIDADE_COMPL,
  DT_ENTRADA_UNIDADE,
  DT_ATUALIZACAO,
  NM_USUARIO,
  CD_TIPO_ACOMODACAO,
  DT_SAIDA_UNIDADE,
  NR_ATEND_DIA,
  DS_OBSERVACAO,
  NM_USUARIO_ORIGINAL,
  DT_SAIDA_INTERNO,
  IE_PASSAGEM_SETOR,
  NR_ACOMPANHANTE,
  IE_CALCULAR_DIF_DIARIA,
  NR_SEQ_MOTIVO_TRANSF,
  DT_ENTRADA_REAL,
  NM_USUARIO_REAL,
  IE_RADIACAO,
  NR_SEQ_MOTIVO_DIF,
  NR_SEQ_MOT_DIF_DIARIA,
  CD_UNIDADE_EXTERNA,
  DT_ALTA_MEDICO_SETOR,
  CD_MOTIVO_ALTA_SETOR,
  CD_PROCEDENCIA_SETOR,
  NR_SEQ_MOTIVO_INT,
  NR_SEQ_MOTIVO_INT_SUB,
  NR_SEQ_MOTIVO_PERM,
  DT_ATUALIZACAO_NREC,
  NM_USUARIO_NREC,
  NR_CIRURGIA,
  NR_SEQ_PEPO,
  NR_SEQ_AGRUPAMENTO,
  QT_TEMPO_PREV,
  NR_SEQ_UNID_ANT,
  DT_SAIDA_TEMPORARIA,
  DT_RETORNO_SAIDA_TEMPORARIA,
  ID_LEITO_TEMP_CROSS,
  CD_DEPARTAMENTO,
  CD_PROCEDENCIA_ATEND,
  NR_SEQ_CLASSIF_ESP,
  IE_ANZICS_GENERATED,
  CD_EVOLUCAO,
  DT_EST_RETURN,
  __deleted,
  __op,
  __ts_ms,
  __source_table,
  __source_txId,
  kafka_ingestion_source,
  year,
  month,
  day,
  hour,
  dt_criacao,
  dt_atualizacao_lakehouse,
  fonte_sistema,
  is_deleted
FROM latest_by_pk