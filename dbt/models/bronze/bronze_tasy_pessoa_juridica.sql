{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.PESSOA_JURIDICA/" %}
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
    r.cd_cgc
  , r.ds_razao_social
  , r.nm_fantasia
  , r.cd_cep
  , r.ds_endereco
  , r.ds_bairro
  , r.ds_municipio
  , r.sg_estado
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.ds_complemento
  , r.nr_telefone
  , r.nr_endereco
  , r.nr_fax
  , r.ds_email
  , r.nm_pessoa_contato
  , r.nr_ramal_contato
  , r.nr_inscricao_estadual
  , r.cd_tipo_pessoa
  , r.cd_conta_contabil
  , r.ie_prod_fabric
  , r.ds_site_internet
  , r.ie_qualidade
  , r.cd_cond_pagto
  , r.qt_dia_prazo_entrega
  , r.ds_nome_abrev
  , r.ie_situacao
  , r.vl_minimo_nf
  , r.cd_pf_resp_tecnico
  , r.nr_alvara_sanitario
  , r.nr_autor_func
  , r.nr_inscricao_municipal
  , r.cd_ans
  , r.cd_referencia_fornec
  , r.ie_tipo_titulo
  , r.dt_validade_alvara_sanit as dh_validade_alvara_sanit
  , r.dt_validade_autor_func as dh_validade_autor_func
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.ie_alterar_senha
  , r.nr_seq_pais
  , r.cd_portador
  , r.cd_tipo_portador
  , r.cd_internacional
  , r.cd_cnes
  , r.cd_municipio_ibge
  , r.nr_seq_tipo_logradouro
  , r.ie_status_exportar
  , r.dt_integracao_externa as dh_integracao_externa
  , r.ie_tipo_tributacao
  , r.nr_certificado_boas_prat
  , r.nr_alvara_sanitario_munic
  , r.dt_validade_alvara_munic as dh_validade_alvara_munic
  , r.dt_validade_resp_tecnico as dh_validade_resp_tecnico
  , r.dt_validade_cert_boas_prat as dh_validade_cert_boas_prat
  , r.cd_cnpj_raiz
  , r.nr_ddi_telefone
  , r.nr_ddd_telefone
  , r.nr_ddi_fax
  , r.nr_ddd_fax
  , r.nr_registro_pls
  , r.ds_orgao_reg_resp_tecnico
  , r.nr_registro_resp_tecnico
  , r.ds_resp_tecnico
  , r.nr_seq_cnae
  , r.cd_cgc_mantenedora
  , r.cd_operadora_empresa
  , r.cd_sistema_ant
  , r.ds_senha
  , r.nr_seq_nat_juridica
  , r.ie_transporte
  , r.ie_forma_revisao
  , r.dt_ultima_revisao as dh_ultima_revisao
  , r.nm_usuario_revisao
  , r.ds_observacao
  , r.ds_observacao_compl
  , r.nr_ccm
  , r.dt_integracao as dh_integracao
  , r.ie_fornecedor_opme
  , r.ie_status_envio
  , r.ie_tipo_trib_municipal
  , r.nr_seq_regiao
  , r.nr_seq_ident_cnes
  , r.nr_cei
  , r.ie_empreendedor_individual
  , r.nr_matricula_cei
  , r.cd_cvm
  , r.cd_curp
  , r.cd_rfc
  , r.ds_orientacao_cobranca
  , r.dt_criacao as dh_criacao
  , r.nr_seq_idioma
  , r.nr_seq_tipo_asen
  , r.ie_tipo_inst_saude
  , r.nr_autor_transp_resid
  , r.nr_autor_receb_resid
  , r.nr_seq_pessoa_endereco
  , r.ie_tipo_secretaria
  , r.cd_uf_ibge
  , r.ds_email_nfe
  , r.ie_tipo_contribuicao
  , r.cd_facility_code
  , r.ds_latitude
  , r.ds_longitude
  , r.ie_tipo_contribuinte
  , r.dt_validade_pj as dh_validade_pj
  , r.ie_instituicao_medica
  , r.ds_resolucion
  , r.ie_regime_iva
  , r.cd_type_person
  , r.ie_registro_mercantil
  , r.cd_regime_fiscal
  , r.ie_tipo_situacao_iibb
  , r.nr_inscricao_iibb
  , r.ie_regime_ica
  , r.nr_registro_mercantil
  , r.dt_atualizacao_senha as dh_atualizacao_senha
  , r.ds_hist_senha
  , r.nr_seq_cat_estab_saude
  , r.cd_fiscal_internacional
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
