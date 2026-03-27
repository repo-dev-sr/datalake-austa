-- Bronze CONTA_PACIENTE: Raw Avro (stream) → Iceberg
-- Estado atual técnico por NR_ATENDIMENTO com CDC incremental.
{{
  config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='merge',
    unique_key='NR_ATENDIMENTO',
    on_schema_change='append_new_columns'
  )
}}
{% set raw_path = "s3a://" ~ var('datalake_bucket') ~ "/" ~ var('raw_tasy_stream_prefix') ~ var('raw_tasy_stream_topic_conta_paciente') ~ "/" %}
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
      WHEN __op = 'd' OR CAST(COALESCE(__deleted, FALSE) AS BOOLEAN) THEN TRUE
      ELSE FALSE
    END AS is_deleted,
    CURRENT_TIMESTAMP() AS dt_criacao,
    CURRENT_TIMESTAMP() AS dt_atualizacao_lakehouse,
    '{{ var("fonte_sistema") }}' AS fonte_sistema
  FROM avro.`{{ raw_path }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
),
latest_by_pk AS (
  SELECT r.*
  FROM (
    SELECT
      r.*,
      ROW_NUMBER() OVER (
        PARTITION BY r.NR_ATENDIMENTO
        ORDER BY
          CAST(COALESCE(r.__ts_ms, 0) AS BIGINT) DESC,
          CAST(COALESCE(r.__source_txId, 0) AS BIGINT) DESC,
          r.dt_atualizacao_lakehouse DESC
      ) AS rn
    FROM raw_incremental r
  ) ranked
  WHERE ranked.rn = 1
)
SELECT *
FROM latest_by_pk