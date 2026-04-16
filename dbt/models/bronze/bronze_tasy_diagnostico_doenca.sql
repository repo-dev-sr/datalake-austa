{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.DIAGNOSTICO_DOENCA/" %}
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
  , dt_diagnostico as dh_diagnostico
  , cd_doenca
  , dt_atualizacao as dh_atualizacao
  , nm_usuario
  , ds_diagnostico
  , ie_classificacao_doenca
  , ie_tipo_doenca
  , ie_unidade_tempo
  , qt_tempo
  , ie_lado
  , dt_manifestacao as dh_manifestacao
  , nr_seq_diag_interno
  , nr_seq_grupo_diag
  , dt_inicio as dh_inicio
  , dt_fim as dh_fim
  , dt_liberacao as dh_liberacao
  , cd_perfil_ativo
  , ie_tipo_diagnostico
  , cd_medico
  , ie_tipo_atendimento
  , ie_situacao
  , dt_inativacao as dh_inativacao
  , nm_usuario_inativacao
  , ds_justificativa
  , nr_seq_assinatura
  , nr_seq_interno
  , nr_cirurgia
  , nr_seq_pepo
  , nr_seq_etiologia
  , nr_seq_assinat_inativacao
  , ie_rn
  , nr_recem_nato
  , dt_cid as dh_cid
  , nr_seq_classif_adic
  , nr_seq_reg_elemento
  , ie_tipo_diag_classif
  , ie_nivel_atencao
  , ds_utc
  , ie_horario_verao
  , ie_reported
  , dt_reported as dh_reported
  , ds_utc_atualizacao
  , cd_doenca_superior
  , ie_tipo_afeicao
  , nr_seq_versao_cid
  , ie_sintomatico
  , nr_seq_consulta_form
  , nr_seq_loco_reg
  , nr_seq_pend_pac_acao
  , nr_seq_rotina
  , cd_setor_atendimento
  , ie_diag_princ_episodio
  , ie_diag_princ_depart
  , ie_diag_obito
  , ie_diag_alta
  , ie_diag_cirurgia
  , ie_diag_pre_cir
  , ie_diag_trat_cert
  , ie_diag_admissao
  , ie_diag_tratamento
  , ie_diag_referencia
  , nr_rqe
  , ie_diag_cronico
  , ie_diag_trat_especial
  , ie_status_diag
  , nr_seq_atend_cons_pepa
  , ie_acao
  , ie_relevante_drg
  , cd_cid_autocomplete
  , nr_seq_atepacu
  , ie_sist_ext_origem
  , cd_disease_medis
  , nr_seq_formulario
  , ie_main_enc_probl
  , ie_status_problema
  , ie_convenio
  , cd_evolucao
  , cd_effective_months
  , dt_effective_date as dh_effective_date
  , ie_side_modifier
  , ie_main_hospitalization
  , nr_seq_jap_pref_1
  , nr_seq_jap_pref_2
  , nr_seq_jap_pref_3
  , nr_seq_jap_sufi_1
  , nr_seq_jap_sufi_2
  , nr_seq_jap_sufi_3
  , nr_seq_nais_insurance
  , nr_seq_diagnosis_pref_suf
  , cd_especialidade_med
  , cd_departamento_med
  , nr_seq_interno_pai
  , ie_origem_infeccao
  , cd_cid_vinc_pai
  , nr_seq_disease_number
  , ds_regra_just
  , nm_usuario_nrec
  , dt_reg_diag as dh_reg_diag
  , dt_atualizacao_nrec as dh_atualizacao_nrec
  , ds_justificativa_retro
  , ie_contagio_mech
  , cd_unidade_basica
  , cd_unidade_compl
  , cd_poa
  , ds_causa_externa
  , ie_primeira_vez
  , ie_avaliador_aux
  , dt_liberacao_aux as dh_liberacao_aux
  , cd_avaliador_aux
  , nr_seq_etiologia_renal
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
