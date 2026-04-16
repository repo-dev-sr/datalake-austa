{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.PLS_MENSALIDADE/" %}
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
  , r.dt_referencia as dh_referencia
  , r.vl_mensalidade
  , r.dt_vencimento as dh_vencimento
  , r.nr_parcela
  , r.nr_seq_pagador
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.cd_conta_imposto_rec
  , r.cd_conta_imposto_deb
  , r.cd_historico_imposto
  , r.nr_seq_lote
  , r.ds_observacao
  , r.nr_seq_contrato
  , r.ie_cancelamento
  , r.dt_cancelamento as dh_cancelamento
  , r.vl_coparticipacao
  , r.nr_seq_motivo_canc
  , r.nr_seq_forma_cobranca
  , r.cd_banco
  , r.cd_agencia_bancaria
  , r.ie_digito_agencia
  , r.cd_conta
  , r.ie_digito_conta
  , r.ie_endereco_boleto
  , r.nr_seq_conta_banco
  , r.nr_seq_cobranca
  , r.vl_outros
  , r.vl_pre_estabelecido
  , r.vl_pos_estabelecido
  , r.vl_adicionais
  , r.vl_pro_rata_dia
  , r.vl_antecipacao
  , r.qt_beneficiarios
  , r.nm_usuario_cancelamento
  , r.vl_taxa_boleto
  , r.nr_seq_pagador_fin
  , r.nr_seq_compl_pf_tel_adic
  , r.ie_nota_titulo
  , r.ie_tipo_formacao_preco
  , r.ie_apresentacao
  , r.nr_seq_compl_pj
  , r.dt_inicio_geracao as dh_inicio_geracao
  , r.dt_fim_geracao as dh_fim_geracao
  , r.qt_tempo_geracao
  , r.ie_proporcional
  , r.ie_gerar_cobr_escrit
  , r.nr_serie_mensalidade
  , r.ie_varios_titulos
  , r.nr_rps
  , r.cd_grupo_intercambio
  , r.nr_seq_cancel_rec_mens
  , r.ie_tipo_geracao_mens
  , r.nr_seq_tipo_compl_adic
  , r.nr_seq_conta_banco_deb_aut
  , r.ds_obs_cancelamento
  , r.ds_mensagem_quitacao
  , r.nr_seq_motivo_susp
  , r.ie_tipo_estipulante
  , r.ds_complemento
  , r.ie_indice_correcao
  , r.ds_timezone
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
