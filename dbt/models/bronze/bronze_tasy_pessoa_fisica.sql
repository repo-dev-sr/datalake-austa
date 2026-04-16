{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.PESSOA_FISICA/" %}
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
    r.cd_pessoa_fisica
  , r.ie_tipo_pessoa
  , r.nm_pessoa_fisica
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.dt_nascimento as dh_nascimento
  , r.ie_sexo
  , r.ie_estado_civil
  , r.nr_cpf
  , r.nr_identidade
  , r.nr_telefone_celular
  , r.ie_grau_instrucao
  , r.nr_cep_cidade_nasc
  , r.nr_prontuario
  , r.cd_religiao
  , r.nr_pis_pasep
  , r.cd_nacionalidade
  , r.ie_dependencia_sus
  , r.qt_altura_cm
  , r.ie_tipo_sangue
  , r.ie_fator_rh
  , r.dt_obito as dh_obito
  , r.nr_iss
  , r.nr_inss
  , r.nr_cert_nasc
  , r.cd_cargo
  , r.ds_codigo_prof
  , r.ie_funcionario
  , r.nr_seq_cor_pele
  , r.ds_orgao_emissor_ci
  , r.nr_cartao_nac_sus_ant
  , r.cd_cbo_sus
  , r.cd_atividade_sus
  , r.ie_vinculo_sus
  , r.cd_estabelecimento
  , r.cd_sistema_ant
  , r.cd_funcionario
  , r.nr_pager_bip
  , r.nr_transacao_sus
  , r.cd_medico
  , r.nm_pessoa_pesquisa
  , r.dt_emissao_ci as dh_emissao_ci
  , r.nr_seq_conselho
  , r.dt_admissao_hosp as dh_admissao_hosp
  , r.ie_fluencia_portugues
  , r.nr_titulo_eleitor
  , r.nr_zona
  , r.nr_secao
  , r.nr_cartao_estrangeiro
  , r.nr_reg_geral_estrang
  , r.dt_chegada_brasil as dh_chegada_brasil
  , r.nm_usuario_original
  , r.ds_historico
  , r.dt_cadastro_original as dh_cadastro_original
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.ie_tipo_prontuario
  , r.ds_observacao
  , r.qt_dependente
  , r.nr_transplante
  , r.cd_empresa
  , r.sg_emissora_ci
  , r.dt_naturalizacao_pf as dh_naturalizacao_pf
  , r.nr_ctps
  , r.uf_emissora_ctps
  , r.dt_emissao_ctps as dh_emissao_ctps
  , r.nm_pessoa_fisica_sem_acento
  , r.dt_revisao as dh_revisao
  , r.nm_usuario_revisao
  , r.dt_demissao_hosp as dh_demissao_hosp
  , r.nr_contra_ref_sus
  , r.cd_puericultura
  , r.nr_portaria_nat
  , r.ds_senha
  , r.ds_fonetica
  , r.ie_revisar
  , r.cd_cnes
  , r.nr_seq_perfil
  , r.nr_same
  , r.nr_seq_cbo_saude
  , r.dt_integracao_externa as dh_integracao_externa
  , r.ds_fonetica_cns
  , r.dt_geracao_pront as dh_geracao_pront
  , r.ds_apelido
  , r.cd_municipio_ibge
  , r.nr_seq_pais
  , r.nr_cert_casamento
  , r.nr_seq_cartorio_nasc
  , r.nr_seq_cartorio_casamento
  , r.dt_emissao_cert_nasc as dh_emissao_cert_nasc
  , r.dt_emissao_cert_casamento as dh_emissao_cert_casamento
  , r.nr_livro_cert_nasc
  , r.nr_livro_cert_casamento
  , r.nr_folha_cert_nasc
  , r.nr_folha_cert_casamento
  , r.nr_seq_nut_perfil
  , r.nr_registro_pls
  , r.dt_validade_rg as dh_validade_rg
  , r.qt_peso_nasc
  , r.uf_conselho
  , r.ie_status_exportar
  , r.ie_endereco_correspondencia
  , r.nr_pront_dv
  , r.nm_abreviado
  , r.nr_ddd_celular
  , r.nr_ddi_celular
  , r.ie_frequenta_escola
  , r.nr_ccm
  , r.dt_fim_experiencia as dh_fim_experiencia
  , r.dt_validade_conselho as dh_validade_conselho
  , r.ie_nf_correio
  , r.ds_profissao
  , r.ds_empresa_pf
  , r.nr_passaporte
  , r.cd_tipo_pj
  , r.nr_cnh
  , r.nr_cert_militar
  , r.cd_perfil_ativo
  , r.nr_pront_ext
  , r.dt_inicio_ocup_atual as dh_inicio_ocup_atual
  , r.ie_escolaridade_cns
  , r.ie_situacao_conj_cns
  , r.nr_folha_cert_div
  , r.nr_cert_divorcio
  , r.nr_seq_cartorio_divorcio
  , r.dt_emissao_cert_divorcio as dh_emissao_cert_divorcio
  , r.nr_livro_cert_divorcio
  , r.dt_alta_institucional as dh_alta_institucional
  , r.dt_transplante as dh_transplante
  , r.cd_familia
  , r.nr_matricula_nasc
  , r.ie_doador
  , r.dt_vencimento_cnh as dh_vencimento_cnh
  , r.ds_categoria_cnh
  , r.cd_pessoa_mae
  , r.dt_primeira_admissao as dh_primeira_admissao
  , r.dt_fim_prorrogacao as dh_fim_prorrogacao
  , r.nr_certidao_obito
  , r.nr_seq_etnia
  , r.nr_cartao_nac_sus
  , r.qt_peso
  , r.ie_emancipado
  , r.ie_vinculo_profissional
  , r.cd_nit
  , r.ds_email_ccih
  , r.ie_dependente
  , r.cd_cgc_orig_transpl
  , r.dt_afastamento as dh_afastamento
  , r.nr_seq_tipo_beneficio
  , r.nr_seq_tipo_incapacidade
  , r.ie_tratamento_psiquiatrico
  , r.nr_seq_agencia_inss
  , r.qt_dias_ig
  , r.qt_semanas_ig
  , r.ie_regra_ig
  , r.dt_nascimento_ig as dh_nascimento_ig
  , r.ie_status_usuario_event
  , r.cd_cid_direta
  , r.ie_coren
  , r.dt_validade_coren as dh_validade_coren
  , r.nm_social
  , r.nr_seq_cor_olho
  , r.nr_seq_cor_cabelo
  , r.dt_cad_sistema_ant as dh_cad_sistema_ant
  , r.nr_seq_classif_pac_age
  , r.ie_perm_sms_email
  , r.nr_inscricao_estadual
  , r.ie_socio
  , r.cd_ult_profissao
  , r.ie_vegetariano
  , r.ds_orientacao_cobranca
  , r.ie_conselheiro
  , r.ie_fumante
  , r.nr_celular_numeros
  , r.nr_codigo_serv_prest
  , r.dt_adocao as dh_adocao
  , r.ds_laudo_anat_patol
  , r.dt_laudo_anat_patol as dh_laudo_anat_patol
  , r.nr_ric
  , r.ie_consiste_nr_serie_nf
  , r.cd_pessoa_cross
  , r.nr_rga
  , r.cd_barras_pessoa
  , r.nr_seq_funcao_pf
  , r.nm_usuario_princ_ci
  , r.nr_seq_turno_trabalho
  , r.nr_seq_chefia
  , r.cd_declaracao_nasc_vivo
  , r.nr_termo_cert_nasc
  , r.cd_curp
  , r.cd_rfc
  , r.sg_estado_nasc
  , r.ie_fornecedor
  , r.cd_ife
  , r.nm_primeiro_nome
  , r.nm_sobrenome_pai
  , r.nm_sobrenome_mae
  , r.ie_rh_fraco
  , r.ds_municipio_nasc_estrangeiro
  , r.ie_gemelar
  , r.ie_subtipo_sanguineo
  , r.nr_seq_person_name
  , r.ie_tipo_definitivo_provisorio
  , r.nr_inscricao_municipal
  , r.nr_seq_forma_trat
  , r.nr_spss
  , r.nr_seq_lingua_indigena
  , r.nr_seq_nome_solteiro
  , r.nr_serie_ctps
  , r.ie_considera_indio
  , r.ie_ocupacao_habitual
  , r.ie_nasc_estimado
  , r.qt_peso_um
  , r.ie_unid_med_peso
  , r.cd_genero
  , r.dt_atualizacao_senha as dh_atualizacao_senha
  , r.nr_genero
  , r.ie_sit_rua_sus
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
