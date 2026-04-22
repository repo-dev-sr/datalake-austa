{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.DISPENSACAO_QUIMIOTERAPIA/" %}
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
    -- PK
    r.nr_sequencia

    -- FKs
  , r.nr_atendimento
  , r.nr_prescricao
  , r.cd_material
  , r.nr_seq_item
  , r.cd_pessoa_fisica
  , r.cd_setor_atendimento

    -- Quantidades e valores
  , r.qt_material
  , r.qt_dispensada
  , r.qt_devolvida

    -- Flags / status
  , r.ie_status
  , r.ie_tipo_dispensacao
  , r.ie_controlado

    -- Timestamps (DT_* -> dh_*)
  , CAST(r.dt_dispensacao AS TIMESTAMP)         AS dh_dispensacao
  , CAST(r.dt_preparacao AS TIMESTAMP)          AS dh_preparacao
  , CAST(r.dt_cancelamento AS TIMESTAMP)        AS dh_cancelamento

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
