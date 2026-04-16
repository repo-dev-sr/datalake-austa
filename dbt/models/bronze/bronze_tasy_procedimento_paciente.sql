{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.PROCEDIMENTO_PACIENTE/" %}
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
    nr_sequencia
  , nr_atendimento
  , dt_entrada_unidade as dh_entrada_unidade
  , cd_procedimento
  , dt_procedimento as dh_procedimento
  , qt_procedimento
  , dt_atualizacao as dh_atualizacao
  , nm_usuario
  , cd_medico
  , cd_convenio
  , cd_categoria
  , cd_pessoa_fisica
  , dt_prescricao as dh_prescricao
  , ds_observacao
  , vl_procedimento
  , vl_medico
  , vl_anestesista
  , vl_materiais
  , cd_edicao_amb
  , cd_tabela_servico
  , dt_vigencia_preco as dh_vigencia_preco
  , cd_procedimento_princ
  , dt_procedimento_princ as dh_procedimento_princ
  , dt_acerto_conta as dh_acerto_conta
  , dt_acerto_convenio as dh_acerto_convenio
  , dt_acerto_medico as dh_acerto_medico
  , vl_auxiliares
  , vl_custo_operacional
  , tx_medico
  , tx_anestesia
  , nr_prescricao
  , nr_sequencia_prescricao
  , cd_motivo_exc_conta
  , ds_compl_motivo_excon
  , cd_acao
  , qt_devolvida
  , cd_motivo_devolucao
  , nr_cirurgia
  , nr_doc_convenio
  , cd_medico_executor
  , ie_cobra_pf_pj
  , nr_laudo
  , dt_conta as dh_conta
  , cd_setor_atendimento
  , cd_conta_contabil
  , cd_procedimento_aih
  , ie_origem_proced
  , nr_aih
  , ie_responsavel_credito
  , tx_procedimento
  , cd_equipamento
  , ie_valor_informado
  , cd_estabelecimento_custo
  , cd_tabela_custo
  , cd_situacao_glosa
  , nr_lote_contabil
  , cd_procedimento_convenio
  , nr_seq_autorizacao
  , ie_tipo_servico_sus
  , ie_tipo_ato_sus
  , cd_cgc_prestador
  , nr_nf_prestador
  , cd_atividade_prof_bpa
  , nr_interno_conta
  , nr_seq_proc_princ
  , ie_guia_informada
  , dt_inicio_procedimento as dh_inicio_procedimento
  , ie_emite_conta
  , ie_funcao_medico
  , ie_classif_sus
  , cd_especialidade
  , nm_usuario_original
  , nr_seq_proc_pacote
  , ie_tipo_proc_sus
  , cd_setor_receita
  , vl_adic_plant
  , qt_porte_anestesico
  , tx_hora_extra
  , ie_emite_conta_honor
  , nr_seq_atepacu
  , ie_proc_princ_atend
  , cd_medico_req
  , ie_tipo_guia
  , ie_video
  , nr_doc_interno
  , ie_auditoria
  , nr_seq_grupo_rec
  , cd_medico_convenio
  , cd_motivo_ajuste
  , ie_via_acesso
  , nr_seq_cor_exec
  , nr_seq_exame
  , ie_intercorrencia
  , nr_seq_origem
  , ie_dispersao
  , cd_setor_paciente
  , nr_seq_conta_origem
  , nr_seq_parcial
  , nr_seq_proc_interno
  , cd_senha
  , nr_seq_aih
  , nr_doc_honor_conv
  , dt_conferencia as dh_conferencia
  , ie_tipo_atend_bpa
  , ie_grupo_atend_bpa
  , cd_cgc_prestador_conta
  , cd_medico_exec_conta
  , nr_seq_pq_proc
  , qt_filme
  , nr_seq_proc_crit_repasse
  , cd_senha_autor
  , cd_autor_convenio
  , nr_seq_regra_lanc
  , ie_tiss_tipo_guia
  , nr_seq_tiss_tabela
  , nr_minuto_duracao
  , ie_tiss_tipo_guia_honor
  , nr_seq_regra_preco
  , ie_tiss_tipo_guia_desp
  , ie_tecnica_utilizada
  , ie_tiss_tipo_despesa
  , cd_cgc_prestador_tiss
  , cd_cgc_honorario_tiss
  , ie_integracao
  , nr_seq_proc_acerto
  , vl_original_tabela
  , cd_doenca_cid
  , nr_seq_apac
  , nr_seq_ajuste_proc
  , cd_tab_custo_preco
  , nr_seq_proc_autor
  , ie_doc_executor
  , nr_seq_regra_doc
  , cd_cbo
  , cd_prestador_tiss
  , cd_cgc_prest_solic_tiss
  , cd_setor_resp
  , dt_final_procedimento as dh_final_procedimento
  , ie_tiss_desp_honor
  , nr_seq_proc_est
  , cd_prest_resp
  , cd_medico_exec_tiss
  , ds_proc_tiss
  , nr_seq_tamanho_filme
  , ds_prestador_tiss
  , nr_seq_proc_crit_hor
  , nr_seq_reg_template
  , cd_procedimento_tuss
  , ie_ratear_item
  , ie_complexidade
  , ie_tipo_financiamento
  , cd_funcao
  , cd_perfil
  , nr_seq_regra_qtde_exec
  , cd_centro_custo_receita
  , nr_seq_regra_guia_tiss
  , nr_seq_regra_honor_tiss
  , ie_tipo_atend_tiss
  , vl_desp_tiss
  , ie_resp_cred_manual
  , nr_seq_just_valor_inf
  , cd_medico_solic_tiss
  , cd_medico_prof_solic_tiss
  , vl_repasse_calc
  , tx_custo_oper_qt
  , ie_spect
  , nr_seq_agenda_rxt
  , cd_prestador_solic_tiss
  , ds_prestador_solic_tiss
  , vl_tx_desconto
  , vl_tx_adm
  , cd_medico_honor_tiss
  , nr_controle
  , nr_seq_regra_taxa_cir
  , nr_seq_proc_orig
  , nr_sequencia_gas
  , nr_seq_etapa_checkup
  , nr_seq_checkup_etapa
  , ie_via_hemodinamica
  , cd_regra_repasse
  , ie_trat_conta_rn
  , nr_seq_reg_proced
  , nr_seq_pepo
  , nr_seq_motivo_incl
  , dt_conversao_manual as dh_conversao_manual
  , cd_cgc_prest_honor_tiss
  , nr_seq_transfusao
  , nr_seq_reserva
  , ie_lado
  , nr_seq_hc_equipamento
  , dt_vinc_proced_adic as dh_vinc_proced_adic
  , ds_indicacao
  , cd_tipo_anestesia
  , nr_seq_lanc_acao
  , nr_seq_material
  , ie_carater_cirurgia
  , ds_just_valor_inf
  , nr_fone_integracao
  , dt_ligacao_integracao as dh_ligacao_integracao
  , ds_just_alter_data
  , cd_medico_prev_laudo
  , nr_seq_orig_audit
  , nr_seq_servico
  , nr_seq_servico_classif
  , nr_seq_sus_equipe
  , nr_seq_proc_ditado
  , nr_seq_crit_honorario
  , ie_tx_cir_tempo
  , nr_seq_pe_prescr
  , cd_medico_autenticacao
  , dt_autenticacao as dh_autenticacao
  , ds_biometria
  , nr_seq_prescr_mat
  , nr_seq_solucao
  , nr_seq_proc_desdob
  , cd_medico_residente
  , ie_retorno
  , nr_ato_ipasgo
  , nr_seq_conta_reversao
  , nr_seq_regra_pepo
  , ds_proc_tuss
  , nr_seq_tuss_item
  , cd_sequencia_parametro
  , tx_auxiliar
  , cd_autorizacao_prest
  , nr_presc_mipres
  , cd_id_entrega
  , nr_seq_proc_pacote_origem
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
