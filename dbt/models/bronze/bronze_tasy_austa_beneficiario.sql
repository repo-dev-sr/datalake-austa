{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.AUSTA_BENEFICIARIO/" %}
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
    r.id_beneficiario
  , r.ds_situacao
  , r.nm_beneficiario
  , r.ds_sexo
  , r.dt_nascimento AS dh_nascimento
  , r.ds_municipio
  , r.ds_estado
  , r.nr_contrato
  , r.id_beneficiario_titular
  , r.ds_parentesco
  , r.ds_tipo_estipulante
  , r.nm_estipulante
  , r.nm_sub_estipulante
  , r.dt_contratacao AS dh_contratacao
  , r.dt_rescisao AS dh_rescisao
  , r.ds_motivo_rescisao
  , r.nr_produto
  , r.ds_produto
  , r.ds_tipo_regulamentacao
  , r.ds_tipo_contratacao
  , r.ds_padrao_acomodacao
  , r.ds_segmentacao_produto
  , r.ds_tipo_abrangencia
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
