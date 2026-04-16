{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.CONVENIO/" %}
{% set raw_path_tasy = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.CONVENIO/" %}
{% set raw_path_austa = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.CONVENIO/" %}
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
{{ bronze_raw_incremental_union_tasy_austa(raw_path_tasy, raw_path_austa, 1) }}

SELECT
    cd_convenio
  , ds_convenio
  , dt_inclusao as dh_inclusao
  , ie_tipo_convenio
  , ie_situacao
  , cd_cgc
  , dt_atualizacao as dh_atualizacao
  , nm_usuario
  , dt_dia_vencimento as dh_dia_vencimento
  , cd_condicao_pagamento
  , cd_interno
  , cd_regional
  , dt_ref_valida as dh_ref_valida
  , qt_dias_tolerancia
  , ie_forma_calculo_diaria
  , cd_interface_envio
  , cd_interface_retorno
  , ie_exige_guia
  , ie_separa_conta
  , nr_digitos_codigo
  , ds_rotina_digito
  , ds_mascara_codigo
  , cd_processo_alta
  , dt_cancelamento as dh_cancelamento
  , ds_observacao
  , cd_conta_contabil
  , ie_classif_contabil
  , ie_glosa_atendimento
  , ie_codigo_convenio
  , ds_senha
  , cd_interface_autorizacao
  , ie_valor_contabil
  , ie_tipo_acomodacao
  , ie_preco_medio_material
  , qt_conta_protocolo
  , ie_titulo_receber
  , ie_agenda_consulta
  , ie_exige_data_ult_pagto
  , ie_doc_convenio
  , ie_origem_preco
  , ie_precedencia_preco
  , ie_agrup_item_interf
  , ie_conversao_mat
  , ie_doc_retorno
  , ie_rep_cod_usuario
  , ie_exige_orc_atend
  , ie_calc_porte
  , ie_exige_senha_atend
  , nr_multiplo_envio
  , ie_exige_plano
  , ie_partic_cirurgia
  , ie_exige_carteira_atend
  , ie_exige_validade_atend
  , ie_solic_exame_tasymed
  , dt_atualizacao_nrec as dh_atualizacao_nrec
  , nm_usuario_nrec
  , qt_dias_reapresentacao
  , ds_mascara_senha
  , ds_mascara_guia
  , qt_dia_fim_conta
  , ie_consiste_autor
  , vl_max_convenio
  , nr_vias_conta
  , ie_guia_unica_conta
  , cd_convenio_glosa
  , cd_categoria_glosa
  , nr_digito_plano
  , ie_exige_origem
  , qt_inicio_digito
  , nr_telefone_autor
  , ds_arquivo_logo_tiss
  , ie_exige_tipo_guia
  , ds_rotina_digito_guia
  , ie_venc_ultimo_dia
  , cd_fonte_remuner_cih
  , dt_dia_entrega as dh_dia_entrega
  , dt_entrega_prot as dh_entrega_prot
  , ds_cor
  , ds_rotina_senha
  , ie_consiste_biometria
  , cd_edicao_amb_refer
  , qt_dias_autorizacao
  , ie_cobertura
  , qt_dias_autor_proc
  , nr_prim_digitos
  , ie_imprime_estab_log
  , ie_agenda_web
  , ds_obs_nota
  , qt_dias_regurso_glosa
  , qt_dias_codificacao
  , qt_dias_term_agenda
  , ie_exige_empresa_agi
  , ie_forma_localizador
  , ds_mascara_compl
  , qt_dias_tol_entrega
  , cd_senha_plamta
  , qt_dias_cancel_plamta
  , ds_obs_nf_eletronica
  , ie_exige_empresa_ageweb
  , ds_arquivo_logo_conta
  , ie_matricula_integ
  , ie_nf_contab_rec_conv
  , ie_intermediario
  , ie_exige_titular
  , cd_integracao
  , ds_cor_html
  , nr_ddi_telefone
  , cd_tipo_convenio_mx
  , nr_doc_externo
  , cd_externo
  , nr_prioridade_padrao
  , ie_multiplo_arquivo
  , ie_utiliza_integracao_convenio
  , tx_outpatient_self_pay
  , tx_inpatient_self_pay
  , cd_tipo_contratacion
  , ds_outro_tipo
  , ie_valida_bono
  , cd_tipo_seguradora
  , ie_regra_nota_deb_cred
  , ie_rebilling
  , ie_coinsurance
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
