{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.PLS_MENSALIDADE_SEGURADO/" %}
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
    r.nr_sequencia
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.nr_seq_segurado
  , r.vl_mensalidade
  , r.nr_seq_mensalidade
  , r.qt_idade
  , r.dt_mesano_referencia as dh_mesano_referencia
  , r.nr_parcela
  , r.cd_conta_deb
  , r.nr_seq_regra_ctb_mensal
  , r.nr_seq_plano
  , r.dt_mensalidade_retro as dh_mensalidade_retro
  , r.nr_parcela_contrato
  , r.nr_seq_contrato
  , r.nr_seq_reajuste
  , r.nr_seq_mensalidade_retro
  , r.vl_pre_estabelecido
  , r.vl_coparticipacao
  , r.vl_outros
  , r.vl_adicionais
  , r.vl_pos_estabelecido
  , r.vl_pro_rata_dia
  , r.vl_antecipacao
  , r.nr_seq_intercambio
  , r.nr_seq_segurado_preco
  , r.dt_inicio_cobertura as dh_inicio_cobertura
  , r.dt_fim_cobertura as dh_fim_cobertura
  , r.nr_seq_titular
  , r.nr_seq_parentesco
  , r.dt_rescisao_benef as dh_rescisao_benef
  , r.nr_seq_subestipulante
  , r.nr_seq_localizacao_benef
  , r.ie_rescisao_proporcional
  , r.nr_seq_lote
  , r.ie_tipo_estipulante
  , r.ie_pce_proporcional
  , r.ie_reativacao_proporcional
  , r.ie_remido
  , r.nr_seq_regra_susp_reaj_fx
  , r.ds_timezone
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
