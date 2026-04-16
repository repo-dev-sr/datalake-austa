{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.TIPO_PESSOA_JURIDICA/" %}
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
    r.cd_tipo_pessoa
  , r.ds_tipo_pessoa
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.ie_situacao
  , r.cd_conta_pagamento
  , r.cd_conta_recebimento
  , r.ie_comercial_ops
  , r.ie_externo
  , r.ie_internacional
  , r.cd_estabelecimento
  , r.ie_atualiza_pj
  , r.ie_servico
  , r.ie_incrementar_pj
  , r.cd_conta_imp
  , r.ie_laboratorio_apoio
  , r.ie_orgao_publico
  , r.ie_nat_retencao_fonte
  , r.ie_cooper_medica
  , r.ie_empregador
  , r.ie_entidade_legal
  , r.tp_entidade_gov
  , r.tx_reducao_aliquota
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
