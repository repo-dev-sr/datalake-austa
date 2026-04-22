{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.PRESCR_MATERIAL/" %}
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
  , r.cd_material
  , r.cd_unidade_medida
  , r.cd_unidade_medida_dose
  , r.cd_intervalo
  , r.nr_seq_solucao
  , r.nr_seq_superior

    -- Categoricos / flags
  , r.ie_agrupador
  , r.ie_suspenso
  , r.ie_acm
  , r.ie_urgencia
  , r.ie_se_necessario
  , r.ie_via_aplicacao
  , r.ie_administrar
  , r.ie_controlado
  , r.ie_origem_inf

    -- Quantidades e doses
  , r.qt_dose
  , r.qt_unitaria
  , r.qt_total_dispensar
  , r.qt_material
  , r.qt_dispensar
  , r.qt_conversao_dose

    -- Timestamps (DT_* -> dh_*)
  , CAST(r.dt_inicio_medic AS TIMESTAMP)        AS dh_inicio_medic
  , CAST(r.dt_fim_medic AS TIMESTAMP)           AS dh_fim_medic
  , CAST(r.dt_suspensao AS TIMESTAMP)           AS dh_suspensao
  , CAST(r.dt_lib_material AS TIMESTAMP)        AS dh_lib_material
  , CAST(r.dt_lib_farmacia AS TIMESTAMP)        AS dh_lib_farmacia
  , CAST(r.dt_primeiro_horario AS TIMESTAMP)    AS dh_primeiro_horario
  , CAST(r.dt_validade AS TIMESTAMP)            AS dh_validade

    -- Observacoes
  , r.ds_observacao
  , r.ds_justificativa
  , r.ds_dose_diferenciada

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
