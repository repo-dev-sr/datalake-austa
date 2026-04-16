{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.AUTORIZACAO_CONVENIO/" %}
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
  SELECT *
  FROM avro.`{{ raw_path }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
)
SELECT
    nr_atendimento
  , nr_seq_autorizacao
  , cd_convenio
  , cd_autorizacao
  , dt_autorizacao as dh_autorizacao
  , dt_inicio_vigencia as dh_inicio_vigencia
  , dt_atualizacao as dh_atualizacao
  , nm_usuario
  , dt_fim_vigencia as dh_fim_vigencia
  , nm_responsavel
  , ds_observacao
  , cd_senha
  , cd_procedimento_principal
  , ie_origem_proced
  , dt_pedido_medico as dh_pedido_medico
  , cd_medico_solicitante
  , ie_tipo_guia
  , qt_dia_autorizado
  , nr_prescricao
  , dt_envio as dh_envio
  , dt_retorno as dh_retorno
  , nr_seq_estagio
  , ie_tipo_autorizacao
  , ie_tipo_dia
  , cd_tipo_acomodacao
  , nr_seq_autor_cirurgia
  , ds_indicacao
  , dt_geracao_cih as dh_geracao_cih
  , nr_sequencia
  , nr_seq_agenda
  , qt_dia_solicitado
  , nr_seq_proc_interno
  , cd_procedimento_convenio
  , dt_entrada_prevista as dh_entrada_prevista
  , nr_seq_gestao
  , ie_carater_int_tiss
  , nr_seq_autor_origem
  , ie_resp_autor
  , nr_seq_agenda_consulta
  , nr_seq_autor_desdob
  , cd_cgc_prestador
  , nr_seq_paciente_setor
  , nr_ciclo
  , nr_seq_age_integ
  , nr_seq_paciente
  , cd_medico_solic_tiss
  , nr_seq_classif
  , nr_interno_conta_ref
  , nr_seq_agenda_proc
  , qt_dias_prazo
  , cd_pessoa_fisica
  , ds_dia_ciclo
  , cd_setor_origem
  , cd_senha_provisoria
  , dt_referencia as dh_referencia
  , nr_seq_auditoria
  , ds_tarja_cartao
  , nr_seq_apres
  , nr_seq_regra_autor
  , nr_seq_guia_plano
  , cd_estabelecimento
  , cd_empresa_pac
  , ds_prestador_tiss
  , cd_prestador_tiss
  , dt_agenda as dh_agenda
  , dt_agenda_cons as dh_agenda_cons
  , dt_agenda_integ as dh_agenda_integ
  , nr_ordem_compra
  , nm_usuario_resp
  , ie_tiss_tipo_anexo_autor
  , cd_setor_resp
  , nr_seq_rxt_tratamento
  , dt_desdobramento_conta as dh_desdobramento_conta
  , ie_tipo_internacao_tiss
  , ie_regime_internacao
  , ie_tiss_tipo_acidente
  , ie_previsao_uso_quimio
  , ie_previsao_uso_opme
  , dt_geracao_pacote as dh_geracao_pacote
  , dt_validade_guia as dh_validade_guia
  , ds_cbo_tiss
  , cd_tipo_acomod_desej
  , cd_autorizacao_prest
  , cd_interno_origem_tiss
  , cd_interno_contrat_exec
  , ds_contratado_exec_tiss
  , cd_interno_contrat_solic
  , cd_tipo_procedimento
  , cd_medico_exec_agenda
  , dt_atualizacao_nrec as dh_atualizacao_nrec
  , nm_usuario_nrec
  , ie_tiss_tipo_etapa_autor
  , cd_validacao_tiss
  , cd_ausencia_cod_valid
  , ie_eclipse_status
  , cd_proc_principal_loc
  , ie_tipo_tramite
  , ie_tiss_cobertura_especial
  , ds_motivo_cancelamento
  , nr_seq_episodio
  , nr_contract_partner
  , nr_seq_regra_autor_qtd
  , nr_seq_dados_mod_remun
  , qt_bonus
  , qt_bonus_apres
  , ds_token
  , nr_seq_exam_ext
  , nr_seq_categoria_iva
  , cd_plano_convenio
  , cd_categoria
  , vl_copago
  , cd_cnpj_fabric_opme
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
