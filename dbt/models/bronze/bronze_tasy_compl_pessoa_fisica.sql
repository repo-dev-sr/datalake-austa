{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.COMPL_PESSOA_FISICA/" %}
{% set raw_path_tasy = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.COMPL_PESSOA_FISICA/" %}
{% set raw_path_austa = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.COMPL_PESSOA_FISICA/" %}
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
{{ bronze_raw_incremental_union_tasy_austa_flat(raw_path_tasy, raw_path_austa, 1) }}

SELECT
    cd_pessoa_fisica
  , nr_sequencia
  , ie_tipo_complemento
  , dt_atualizacao as dh_atualizacao
  , nm_usuario
  , nm_contato
  , ds_endereco
  , cd_cep
  , nr_endereco
  , ds_complemento
  , ds_bairro
  , ds_municipio
  , sg_estado
  , nr_telefone
  , nr_ramal
  , ds_observacao
  , ds_email
  , cd_empresa_refer
  , cd_profissao
  , nr_identidade
  , nr_cpf
  , cd_municipio_ibge
  , ds_setor_trabalho
  , ds_horario_trabalho
  , nr_matricula_trabalho
  , nr_seq_parentesco
  , ds_fone_adic
  , ds_fax
  , dt_atualizacao_nrec as dh_atualizacao_nrec
  , nm_usuario_nrec
  , cd_pessoa_fisica_ref
  , nr_seq_pais
  , ds_fonetica
  , cd_tipo_logradouro
  , nr_ddd_telefone
  , nr_ddi_telefone
  , nr_ddi_fax
  , nr_ddd_fax
  , ds_website
  , nm_contato_pesquisa
  , ie_nf_correio
  , qt_dependente
  , cd_zona_procedencia
  , ie_obriga_email
  , nr_seq_ident_cnes
  , ie_mala_direta
  , ie_tipo_sangue
  , ie_fator_rh
  , nr_seq_local_atend_med
  , cd_pessoa_end_ref
  , nr_seq_end_ref
  , ie_empresa_pagadora
  , nr_seq_regiao
  , ie_correspondencia
  , nr_ddi_fone_adic
  , nr_ddd_fone_adic
  , nr_seq_tipo_compl_adic
  , nr_telefone_celular
  , nr_ddd_celular
  , nr_ddi_celular
  , ie_nao_possui_email
  , ie_regime_casamento
  , ie_resp_legal
  , nr_seq_assentamento_mx
  , nr_seq_localizacao_mx
  , ds_compl_end
  , ie_custodiante
  , nr_seq_tipo_custodia
  , ie_contato_principal
  , nr_seq_pessoa_endereco
  , cd_cgc
  , ds_resumo_end
  , qt_tempo_atividade
  , nr_seq_tipo_doc_identificacao
  , nr_doc_identificacao_contato
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
