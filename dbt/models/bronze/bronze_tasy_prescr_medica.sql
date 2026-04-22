{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.PRESCR_MEDICA/" %}
{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}
{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}

WITH target_watermark AS (
  {% if is_incremental() %}
    SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms FROM {{ this }}
  {% else %}
    SELECT CAST(0 AS BIGINT) AS max_ts_ms
  {% endif %}
)
, params AS (
  SELECT
      max_ts_ms
    , GREATEST(0, max_ts_ms - ({{ cdc_lookback_hours }} * 3600 * 1000)) AS wm_start_ms
  FROM target_watermark
)
, raw_incremental AS (
  SELECT *
  FROM avro.`{{ raw_path }}`
  WHERE CAST(__ts_ms AS BIGINT) >= (SELECT wm_start_ms FROM params)
)

SELECT
    -- PK
    r.nr_prescricao

    -- FKs
  , r.nr_atendimento
  , r.cd_medico
  , r.cd_setor_atendimento
  , r.cd_pessoa_fisica
  , r.cd_estabelecimento
  , r.nr_seq_agrupador
  , r.nr_prescricao_original
  , r.cd_funcao_origem

    -- Flags / categoricos
  , r.ie_origem_inf
  , r.ie_adep
  , r.ie_prescr_emergencia
  , r.ie_recem_nato
  , r.ie_lib_farm
  , r.ie_hemodialise
  , r.ie_tipo_prescricao
  , r.ie_situacao
  , r.ie_retroativa

    -- Timestamps (DT_* -> dh_*)
  , CAST(r.dt_prescricao AS TIMESTAMP)             AS dh_prescricao
  , CAST(r.dt_liberacao AS TIMESTAMP)              AS dh_liberacao
  , CAST(r.dt_liberacao_medico AS TIMESTAMP)       AS dh_liberacao_medico
  , CAST(r.dt_liberacao_farmacia AS TIMESTAMP)     AS dh_liberacao_farmacia
  , CAST(r.dt_suspensao AS TIMESTAMP)              AS dh_suspensao
  , CAST(r.dt_validade_prescr AS TIMESTAMP)        AS dh_validade_prescr
  , CAST(r.dt_entrega AS TIMESTAMP)                AS dh_entrega
  , CAST(r.dt_primeiro_horario AS TIMESTAMP)       AS dh_primeiro_horario

    -- Observacoes
  , r.ds_observacao
  , r.ds_justificativa

    -- Auditoria TASY
  , CAST(r.dt_atualizacao AS TIMESTAMP)            AS dh_atualizacao
  , r.nm_usuario
  , CAST(r.dt_atualizacao_nrec AS TIMESTAMP)       AS dh_atualizacao_nrec
  , r.nm_usuario_nrec

    -- Auditoria CDC bronze (macro gera 8 colunas)
  {{ bronze_audit_columns(raw_path) }}

    -- Preservar __source_txid para desempate no ROW_NUMBER da Silver
  , r.__source_txid
FROM raw_incremental r
