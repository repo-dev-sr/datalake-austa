{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.ATENDIMENTO_PACIENTE/" %}
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
  , cd_pessoa_fisica
  , cd_estabelecimento
  , cd_procedencia
  , dt_entrada AS dh_entrada
  , ie_tipo_atendimento
  , dt_atualizacao AS dh_atualizacao
  , nm_usuario
  , cd_medico_resp
  , cd_motivo_alta
  , ds_sintoma_paciente
  , ds_observacao
  , dt_alta AS dh_alta
  , ie_clinica
  , nm_usuario_atend
  , ie_responsavel
  , dt_fim_conta AS dh_fim_conta
  , ie_fim_conta
  , nr_cat
  , ds_causa_externa
  , cd_cgc_seguradora
  , nr_bilhete
  , nr_serie_bilhete
  , ie_carater_inter_sus
  , ie_vinculo_sus
  , ie_tipo_convenio
  , ie_tipo_atend_bpa
  , ie_grupo_atend_bpa
  , cd_medico_atendimento
  , dt_alta_interno AS dh_alta_interno
  , nr_seq_unid_atual
  , nr_seq_unid_int
  , nr_atend_original
  , qt_dia_longa_perm
  , dt_inicio_atendimento AS dh_inicio_atendimento
  , ie_permite_visita
  , ie_status_atendimento
  , dt_previsto_alta AS dh_previsto_alta
  , nm_usuario_alta
  , cd_pessoa_responsavel
  , dt_atend_medico AS dh_atend_medico
  , dt_fim_consulta AS dh_fim_consulta
  , dt_medicacao AS dh_medicacao
  , dt_saida_real AS dh_saida_real
  , ie_clinica_alta
  , dt_lib_medico AS dh_lib_medico
  , nr_seq_regra_funcao
  , nr_seq_local_pa
  , nr_seq_tipo_acidente
  , dt_ocorrencia AS dh_ocorrencia
  , ds_pend_autorizacao
  , nr_seq_check_list
  , dt_fim_triagem AS dh_fim_triagem
  , nr_reserva_leito
  , ie_paciente_isolado
  , ie_permite_visita_rel
  , ds_senha
  , ie_probabilidade_alta
  , nr_seq_forma_chegada
  , nr_seq_indicacao
  , ds_obs_alta
  , cd_pessoa_indic
  , nm_medico_externo
  , nr_gestante_pre_natal
  , nr_seq_forma_laudo
  , dt_alta_medico AS dh_alta_medico
  , cd_motivo_alta_medica
  , nr_seq_pq_protocolo
  , ie_extra_teto
  , cd_psicologo
  , nr_seq_classificacao
  , ds_senha_qmatic
  , nr_seq_queixa
  , nr_seq_triagem
  , cd_medico_referido
  , ie_necropsia
  , nr_seq_classif_medico
  , ie_tipo_consulta
  , ie_tipo_saida_consulta
  , ie_tipo_atend_tiss
  , dt_impressao AS dh_impressao
  , cd_pessoa_juridica_indic
  , nr_atendimento_mae
  , ie_trat_conta_rn
  , qt_dias_prev_inter
  , nr_seq_grau_parentesco
  , ie_boletim_inform
  , dt_chamada_paciente AS dh_chamada_paciente
  , ie_chamado
  , ds_obs_prev_alta
  , ie_avisar_medico_referido
  , cd_medico_chamado
  , nr_atend_alta
  , dt_saida_prev_loc_pa AS dh_saida_prev_loc_pa
  , ie_status_pa
  , dt_liberacao_enfermagem AS dh_liberacao_enfermagem
  , dt_reavaliacao_medica AS dh_reavaliacao_medica
  , ds_justif_saida_real
  , nm_usuario_triagem
  , nr_seq_ficha
  , dt_recebimento_senha AS dh_recebimento_senha
  , ie_prm
  , dt_ver_prev_alta AS dh_ver_prev_alta
  , nr_seq_cat
  , dt_atend_original AS dh_atend_original
  , dt_chamada_enfermagem AS dh_chamada_enfermagem
  , nr_seq_local_destino
  , ie_divulgar_obito
  , nr_seq_topografia
  , nr_seq_tipo_lesao
  , ie_clinica_ant
  , cd_perfil_ativo
  , nm_usuario_intern
  , ds_obs_alta_medic
  , ie_modo_internacao
  , cd_municipio_ocorrencia
  , dt_usuario_intern AS dh_usuario_intern
  , dt_chamada_reavaliacao AS dh_chamada_reavaliacao
  , dt_inicio_reavaliacao AS dh_inicio_reavaliacao
  , nr_vinculo_censo
  , dt_chegada_paciente AS dh_chegada_paciente
  , ds_vinculo_censo
  , nr_seq_triagem_prioridade
  , nm_usuario_saida
  , nr_seq_triagem_old
  , crm_medico_externo
  , dt_fim_reavaliacao AS dh_fim_reavaliacao
  , cd_setor_obito
  , nm_usuario_alta_medica
  , nr_seq_segurado
  , dt_alta_tesouraria AS dh_alta_tesouraria
  , nr_atend_origem_pa
  , cd_cgc_indicacao
  , dt_inicio_observacao AS dh_inicio_observacao
  , dt_fim_observacao AS dh_fim_observacao
  , nr_seq_pac_senha_fila
  , dt_cancelamento AS dh_cancelamento
  , nm_usuario_cancelamento
  , nm_usuario_prob_alta
  , ie_permite_acomp
  , nr_seq_informacao
  , ds_informacao
  , nr_seq_tipo_midia
  , nm_inicio_atendimento
  , nm_fim_triagem
  , nr_dias_prev_alta
  , ie_declaracao_obito
  , dt_chamada_medic AS dh_chamada_medic
  , dt_chegada_medic AS dh_chegada_medic
  , nr_seq_atend_pls
  , nr_seq_evento_atend
  , dt_descolonizar_paciente AS dh_descolonizar_paciente
  , nm_descolonizar_paciente
  , dt_checagem_adep AS dh_checagem_adep
  , ie_laudo_preenchido
  , dt_impressao_alta AS dh_impressao_alta
  , nr_seq_classif_esp
  , cd_medico_preferencia
  , ds_obs_prior
  , vl_consulta
  , ds_obs_pa
  , nr_conv_interno
  , dt_inicio_prescr_pa AS dh_inicio_prescr_pa
  , dt_fim_prescr_pa AS dh_fim_prescr_pa
  , dt_inicio_esp_exame AS dh_inicio_esp_exame
  , dt_fim_esp_exame AS dh_fim_esp_exame
  , nr_seq_pa_status
  , nr_seq_registro
  , ie_assinou_termo_biobanco
  , ds_observacao_biobanco
  , nr_seq_motivo_biobanco
  , nm_usuario_biobanco
  , dt_atualizacao_biobanco AS dh_atualizacao_biobanco
  , dt_liberacao_financeiro AS dh_liberacao_financeiro
  , nr_seq_ficha_lote
  , ie_tipo_endereco_entrega
  , cd_setor_usuario_atend
  , dt_ultima_menstruacao AS dh_ultima_menstruacao
  , qt_ig_semana
  , qt_ig_dia
  , cd_funcao_alta_medica
  , nr_submotivo_alta
  , ie_tipo_status_alta
  , ie_tipo_alta
  , ie_tipo_vaga
  , cd_setor_desejado
  , cd_tipo_acomod_desej
  , nr_int_cross
  , dt_classif_risco AS dh_classif_risco
  , nr_seq_ficha_lote_ant
  , ie_classif_lote_ent
  , cd_medico_infect
  , nr_seq_tipo_obs_alta
  , nr_atend_origem_bpa
  , ie_liberado_checkout
  , nr_seq_oftalmo
  , ds_senha_internet
  , dt_atualizacao_nrec AS dh_atualizacao_nrec
  , nm_usuario_nrec
  , ie_paciente_gravida
  , ds_utc
  , ie_horario_verao
  , ie_nivel_atencao
  , ds_utc_atualizacao
  , nr_seq_episodio
  , cd_lugar_acc
  , ie_tipo_serv_mx
  , cd_serv_entrada_mx
  , cd_serv_sec_mx
  , cd_serv_ter_mx
  , cd_serv_alta_mx
  , nr_seq_tipo_admissao_fat
  , ie_inform_incompletas
  , btn_alterar_dados_resp
  , ie_tipo_complemento_resp
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r

