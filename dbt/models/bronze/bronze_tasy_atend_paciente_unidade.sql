{{
  config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.ATEND_PACIENTE_UNIDADE/" %}
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
    nr_seq_interno
  , nr_atendimento
  , nr_sequencia
  , cd_setor_atendimento
  , cd_unidade_basica
  , cd_unidade_compl
  , dt_entrada_unidade AS dh_entrada_unidade
  , dt_atualizacao AS dh_atualizacao
  , nm_usuario
  , cd_tipo_acomodacao
  , dt_saida_unidade AS dh_saida_unidade
  , nr_atend_dia
  , ds_observacao
  , nm_usuario_original
  , dt_saida_interno AS dh_saida_interno
  , ie_passagem_setor
  , nr_acompanhante
  , ie_calcular_dif_diaria
  , nr_seq_motivo_transf
  , dt_entrada_real AS dh_entrada_real
  , nm_usuario_real
  , ie_radiacao
  , nr_seq_motivo_dif
  , nr_seq_mot_dif_diaria
  , cd_unidade_externa
  , dt_alta_medico_setor AS dh_alta_medico_setor
  , cd_motivo_alta_setor
  , cd_procedencia_setor
  , nr_seq_motivo_int
  , nr_seq_motivo_int_sub
  , nr_seq_motivo_perm
  , dt_atualizacao_nrec AS dh_atualizacao_nrec
  , nm_usuario_nrec
  , nr_cirurgia
  , nr_seq_pepo
  , nr_seq_agrupamento
  , qt_tempo_prev
  , nr_seq_unid_ant
  , dt_saida_temporaria AS dh_saida_temporaria
  , dt_retorno_saida_temporaria AS dh_retorno_saida_temporaria
  , id_leito_temp_cross
  , cd_departamento
  , cd_procedencia_atend
  , nr_seq_classif_esp
  , ie_anzics_generated
  , cd_evolucao
  , dt_est_return AS dh_est_return
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
