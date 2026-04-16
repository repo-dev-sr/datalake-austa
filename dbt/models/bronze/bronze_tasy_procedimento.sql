{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.PROCEDIMENTO/" %}
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
    r.cd_procedimento
  , r.ds_procedimento
  , r.ds_complemento
  , r.ie_situacao
  , r.cd_grupo_proc
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.cd_tipo_procedimento
  , r.ie_classificacao
  , r.cd_laudo_padrao
  , r.cd_setor_exclusivo
  , r.ie_origem_proced
  , r.qt_dia_internacao_sus
  , r.qt_idade_minima_sus
  , r.qt_idade_maxima_sus
  , r.ie_sexo_sus
  , r.ie_inreal_sus
  , r.ie_inatom_sus
  , r.cd_grupo_sus
  , r.cd_doenca_cid
  , r.cd_cid_secundario
  , r.ds_proc_interno
  , r.nr_proc_interno
  , r.ie_util_prescricao
  , r.cd_kit_material
  , r.ds_orientacao
  , r.ie_valor_especial
  , r.dt_carga as dh_carga
  , r.qt_max_procedimento
  , r.ie_exige_laudo
  , r.ie_forma_apresentacao
  , r.ie_apuracao_custo
  , r.qt_hora_baixar_prescr
  , r.nr_seq_grupo_rec
  , r.ds_prescricao
  , r.ie_exige_autor_sus
  , r.qt_exec_barra
  , r.ds_procedimento_pesquisa
  , r.ie_especialidade_aih
  , r.ie_ativ_prof_bpa
  , r.cd_atividade_prof_bpa
  , r.ie_alta_complexidade
  , r.cd_grupo_proc_aih
  , r.ie_ignora_origem
  , r.ie_estadio
  , r.ie_exige_lado
  , r.ie_classif_custo
  , r.cd_intervalo
  , r.cd_subgrupo_bpa
  , r.ie_credenciamento_sus
  , r.nr_seq_forma_org
  , r.ie_assoc_adep
  , r.ie_tipo_despesa_tiss
  , r.ie_localizador
  , r.ie_porte_cirurgia
  , r.cd_proc_cih
  , r.ds_orientacao_sms
  , r.ie_cobra_adep
  , r.ie_cobrar_horario
  , r.ie_exige_peso_agenda
  , r.nr_seq_item_serv
  , r.ds_complemento_sus
  , r.ie_gera_associado
  , r.ie_dupla_checagem
  , r.ie_protocolo_tev
  , r.ie_alto_custo_ipasgo
  , r.ie_odontologico
  , r.cd_procedimento_loc
  , r.cd_procimento_mx
  , r.ds_tipo_vasectomia
  , r.cd_sistema_ant
  , r.nr_seq_tuss_serv
  , r.ie_proced_type
  , r.ie_patient_class
  , r.nr_seq_catalogo
  , r.ie_tipo_rel
  , r.ie_conselho_medico
  , r.ie_servico_complementar
  , r.cd_mipres
  , r.ie_nopbs
  , r.ie_tipo_servico
  , r.cd_grupo_espec
  , r.ie_grupo_espec
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
