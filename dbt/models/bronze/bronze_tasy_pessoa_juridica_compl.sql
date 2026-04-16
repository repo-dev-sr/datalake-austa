{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.PESSOA_JURIDICA_COMPL/" %}
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
    r.cd_cgc
  , r.nr_sequencia
  , r.ie_tipo_complemento
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.ds_endereco
  , r.nr_endereco
  , r.ds_complemento
  , r.ds_bairro
  , r.ds_municipio
  , r.sg_estado
  , r.cd_cep
  , r.nr_telefone
  , r.nr_fax
  , r.ds_email
  , r.nr_ramal_contato
  , r.nm_pessoa_contato
  , r.nr_telefone_celular
  , r.cd_cbo_red
  , r.ds_cargo
  , r.cd_municipio_ibge
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.ie_recebe_prop
  , r.nr_ddd_telefone
  , r.ds_setor_contato
  , r.ds_identific_complemento
  , r.nr_seq_regiao
  , r.nr_seq_ident_cnes
  , r.ie_recebe_revista
  , r.ds_observacao
  , r.ie_gerente_canal
  , r.cd_pessoa_fisica_canal
  , r.ie_pls
  , r.ie_contato_ref
  , r.nr_seq_tipo_logradouro
  , r.ie_participa_eventos
  , r.ie_plc_customer_sponsor
  , r.ie_plc_plataform_manager
  , r.nr_ddi_telefone
  , r.nr_ddi_fax
  , r.nr_ddi_celular
  , r.ie_plc_matricula_conc
  , r.nr_total_funcionarios
  , r.ie_contribuinte_receita
  , r.cd_estabelecimento
  , r.nr_seq_pessoa_endereco
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
