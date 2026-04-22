{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.PRESCR_PROCEDIMENTO/" %}
{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}
{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}

WITH target_watermark AS (
  {% if is_incremental() %}
    SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms FROM {{ this }}
  {% else %}
    SELECT CAST(0 AS BIGINT) AS max_ts_ms
  {% endif %}
)
, params AS (
  SELECT
      max_ts_ms
    , GREATEST(0, max_ts_ms - ({{ cdc_lookback_hours }} * 3600 * 1000)) AS wm_start_ms
  FROM target_watermark
)
, raw_incremental AS (
  SELECT *
  FROM avro.`{{ raw_path }}`
  WHERE CAST(__ts_ms AS BIGINT) >= (SELECT wm_start_ms FROM params)
)

SELECT
    -- PK composta
    r.nr_prescricao
  , r.nr_sequencia

    -- FKs
  , r.cd_procedimento
  , r.ie_origem_proced
  , r.cd_setor_atendimento
  , r.cd_medico_exec
  , r.nr_seq_proc_interno

    -- Categoricos / flags
  , r.ie_status_exec
  , r.ie_suspenso
  , r.ie_urgencia
  , r.ie_agrupador
  , r.ie_lado
  , r.ie_amostra
  , r.ie_executar
  , r.ie_acm
  , r.ie_se_necessario

    -- Quantidades
  , r.qt_procedimento
  , r.qt_total

    -- Timestamps (DT_* -> dh_*)
  , CAST(r.dt_prev_execucao AS TIMESTAMP)       AS dh_prev_execucao
  , CAST(r.dt_execucao AS TIMESTAMP)            AS dh_execucao
  , CAST(r.dt_suspensao AS TIMESTAMP)           AS dh_suspensao
  , CAST(r.dt_lib_proc AS TIMESTAMP)            AS dh_lib_proc
  , CAST(r.dt_baixa AS TIMESTAMP)               AS dh_baixa
  , CAST(r.dt_primeiro_horario AS TIMESTAMP)    AS dh_primeiro_horario

    -- Observacoes
  , r.ds_observacao
  , r.ds_justificativa

    -- Auditoria TASY
  , CAST(r.dt_atualizacao AS TIMESTAMP)         AS dh_atualizacao
  , r.nm_usuario
  , CAST(r.dt_atualizacao_nrec AS TIMESTAMP)    AS dh_atualizacao_nrec
  , r.nm_usuario_nrec

    -- Auditoria CDC bronze (macro gera 8 colunas)
  {{ bronze_audit_columns(raw_path) }}

    -- Preservar __source_txid para desempate no ROW_NUMBER da Silver
  , r.__source_txid
FROM raw_incremental r
