{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.SUS_MUNICIPIO/" %}
{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}
{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}

WITH target_watermark AS (
  {% if is_incremental() %}
    SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms
    FROM {{ this }}
  {% else %}
    SELECT CAST(0 AS BIGINT) AS max_ts_ms
  {% endif %}
)
, params AS (
  SELECT
    CASE
      WHEN {{ cdc_reprocess_hours }} > 0 THEN
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - ({{ cdc_reprocess_hours }} * 3600)) * 1000
      ELSE
        GREATEST(
          0
          , (SELECT max_ts_ms FROM target_watermark) - ({{ cdc_lookback_hours }} * 3600 * 1000)
        )
    end  AS wm_start_ms
)
{{ bronze_raw_incremental_austa_flat(raw_path) }}
SELECT
    r.cd_municipio_ibge
  , r.cd_municipio_sinpas
  , r.ds_municipio
  , r.ds_unidade_federacao
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.nr_localidade
  , r.cd_orgao_emissor
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.cd_orgao_emissor_sihd
  , r.cd_medico_autorizador
  , r.ie_importacao_sus
  , r.cd_siafi
  , r.cd_municipio_tom
  , r.ds_abreviacao
  , r.nr_seq_coordenadoria
  , r.ds_municipio_guia_medico
  , r.cd_municipio_nfe
  , r.cd_municipio_bacen
  , r.nr_seq_municipio_mx
  , r.nr_seq_entidade_mx
  , r.cd_catalogo_nom
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
