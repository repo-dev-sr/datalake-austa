{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.CONTA_PACIENTE/" %}
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
, raw_incremental AS (
  SELECT   *
  FROM avro.`{{ raw_path }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
)
SELECT
    nr_atendimento
  , dt_acerto_conta as dh_acerto_conta
  , ie_status_acerto
  , dt_periodo_inicial as dh_periodo_inicial
  , dt_periodo_final as dh_periodo_final
  , dt_atualizacao as dh_atualizacao
  , nm_usuario
  , cd_convenio_parametro
  , nr_protocolo
  , dt_mesano_referencia as dh_mesano_referencia
  , dt_mesano_contabil as dh_mesano_contabil
  , cd_convenio_calculo
  , cd_categoria_calculo
  , nr_interno_conta
  , nr_seq_protocolo
  , cd_categoria_parametro
  , ds_inconsistencia
  , dt_recalculo as dh_recalculo
  , cd_estabelecimento
  , ie_cancelamento
  , nr_lote_contabil
  , nr_seq_apresent
  , ie_complexidade
  , ie_tipo_guia
  , cd_autorizacao
  , vl_conta
  , vl_desconto
  , nr_seq_conta_origem
  , ie_tipo_nascimento
  , dt_cancelamento as dh_cancelamento
  , cd_proc_princ
  , nr_conta_convenio
  , dt_conta_definitiva as dh_conta_definitiva
  , dt_conta_protocolo as dh_conta_protocolo
  , nr_seq_conta_prot
  , nr_seq_pq_protocolo
  , ds_observacao
  , ie_tipo_atend_tiss
  , nr_seq_saida_consulta
  , nr_seq_saida_int
  , nr_seq_saida_spsadt
  , dt_geracao_tiss as dh_geracao_tiss
  , nr_lote_repasse
  , ie_reapresentacao_sus
  , nr_seq_ret_glosa
  , dt_prev_protocolo as dh_prev_protocolo
  , nr_conta_pacote_exced
  , dt_alta_tiss as dh_alta_tiss
  , dt_entrada_tiss as dh_entrada_tiss
  , ie_tipo_fatur_tiss
  , ie_tipo_consulta_tiss
  , cd_especialidade_conta
  , cd_plano_retorno_conv
  , ie_tipo_atend_conta
  , qt_dias_conta
  , cd_responsavel
  , nr_seq_estagio_conta
  , nm_usuario_original
  , nr_seq_audit_hist_glosa
  , vl_repasse_conta
  , nr_fechamento
  , nr_seq_tipo_fatura
  , nr_seq_apresentacao
  , nr_seq_motivo_cancel
  , dt_conferencia_sus as dh_conferencia_sus
  , cd_medico_conta
  , qt_dias_periodo
  , dt_geracao_resumo as dh_geracao_resumo
  , vl_conta_relat
  , ie_faec
  , nr_seq_apresent_sus
  , nr_seq_status_fat
  , nr_seq_status_mob
  , nr_seq_regra_fluxo
  , nr_conta_orig_desdob
  , dt_atual_conta_conv as dh_atual_conta_conv
  , pr_coseguro_hosp
  , pr_coseguro_honor
  , vl_deduzido
  , vl_base_conta
  , dt_definicao_conta as dh_definicao_conta
  , nr_seq_ordem
  , ie_complex_aih_orig
  , vl_coseguro_honor
  , nr_seq_tipo_cobranca
  , vl_coseguro_hosp
  , pr_coseg_nivel_hosp
  , vl_coseg_nivel_hosp
  , vl_maximo_coseguro
  , ie_claim_type
  , nr_codigo_controle
  , nr_seq_categoria_iva
  , dt_geracao_rel_conv as dh_geracao_rel_conv
  , nr_conta_lei_prov
  , ie_integration
  , nr_guia_prestador
  , id_delivery
  , ie_consist_prot
  , ie_status
  , cd_plano
  , dt_geracao_anexos_tiss as dh_geracao_anexos_tiss
  , dt_fim_ger_doc_tiss_relats as dh_fim_ger_doc_tiss_relats
  , dt_fim_ger_doc_tiss_laudos as dh_fim_ger_doc_tiss_laudos
  , dt_fim_ger_doc_tiss_exames as dh_fim_ger_doc_tiss_exames
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
