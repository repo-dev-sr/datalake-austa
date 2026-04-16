{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.TISS_MOTIVO_GLOSA/" %}
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
{{ bronze_raw_incremental_austa_envelope(raw_path) }}
SELECT
    r.nr_sequencia
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.ie_grupo_glosa
  , r.cd_motivo_tiss
  , r.ds_motivo_tiss
  , r.ie_acao_pls
  , r.cd_convenio
  , r.cd_motivo_glosa
  , r.ie_fechar_conta
  , r.ie_importacao_ops
  , r.ie_regra_integracao_ops
  , r.ie_permite_alteracao_ops
  , r.ie_habilita_excecao_ops
  , r.ie_param_ops
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
