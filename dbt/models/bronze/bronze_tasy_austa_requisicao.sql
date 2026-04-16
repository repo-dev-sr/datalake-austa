{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.AUSTA_REQUISICAO/" %}
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
    r.id_requisicao_item
  , r.id_requisicao
  , r.id_requisicao_principal
  , r.id_prestador
  , r.id_beneficiario
  , r.dt_requisicao as dh_requisicao
  , r.dt_vencimento_sla as dh_vencimento_sla
  , r.dt_fim_analise as dh_fim_analise
  , r.ds_status_requisicao
  , r.ds_status_item
  , r.ds_classificacao_item
  , r.ds_tipo_servico
  , r.ds_tipo_guia
  , r.ds_medico
  , r.ds_cbo_solicitante
  , r.ds_especialidade
  , r.ds_carater_atendimento
  , r.ds_tipo_sadt
  , r.ds_tipo_consulta
  , r.ds_tipo_internacao
  , r.ds_regime_internacao
  , r.ds_regime_atendimento
  , r.ds_indicacao_clinica
  , r.ds_observacao
  , r.ds_classificacao_item_n1
  , r.ds_classificacao_item_n2
  , r.ds_classificacao_item_n3
  , r.cd_item
  , r.ds_item
  , r.qt_solicitada
  , r.qt_liberada
  , r.qt_executada
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
