{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.AP_LOTE/" %}
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
    r.nr_sequencia

    -- FKs
  , r.nr_prescricao
  , r.nr_atendimento
  , r.cd_setor_atendimento
  , r.cd_setor_ant
  , r.nr_seq_classif
  , r.nr_seq_turno
  , r.nr_seq_lote_sup
  , r.nr_seq_embalagem
  , r.nr_seq_motivo_parcial
  , r.nr_seq_mot_desdobrar
  , r.cd_estabelecimento

    -- Atributos categoricos / flags
  , r.ie_status_lote
  , r.ie_urgente
  , r.ie_controlado
  , r.ie_dispensacao
  , r.ie_tipo_lote

    -- Timestamps do ciclo SLA (TASY DT_* -> dh_*)
  , CAST(r.dt_geracao_lote AS TIMESTAMP)       AS dh_geracao_lote
  , CAST(r.dt_inicio_dispensacao AS TIMESTAMP) AS dh_inicio_dispensacao
  , CAST(r.dt_atend_farmacia AS TIMESTAMP)     AS dh_atend_farmacia
  , CAST(r.dt_disp_farmacia AS TIMESTAMP)      AS dh_disp_farmacia
  , CAST(r.dt_entrega_setor AS TIMESTAMP)      AS dh_entrega_setor
  , CAST(r.dt_recebimento_setor AS TIMESTAMP)  AS dh_recebimento_setor
  , CAST(r.dt_impressao AS TIMESTAMP)          AS dh_impressao
  , CAST(r.dt_conferencia AS TIMESTAMP)        AS dh_conferencia
  , CAST(r.dt_cancelamento AS TIMESTAMP)       AS dh_cancelamento
  , CAST(r.dt_suspensao AS TIMESTAMP)          AS dh_suspensao

    -- Usuarios / observacoes
  , r.nm_usuario_atendimento
  , r.nm_usuario_disp
  , r.nm_usuario_entrega
  , r.nm_usuario_receb
  , r.ds_observacao
  , r.ds_justificativa

    -- Auditoria TASY
  , CAST(r.dt_atualizacao AS TIMESTAMP)        AS dh_atualizacao
  , r.nm_usuario
  , CAST(r.dt_atualizacao_nrec AS TIMESTAMP)   AS dh_atualizacao_nrec
  , r.nm_usuario_nrec

    -- Auditoria CDC bronze (macro gera 8 colunas)
  {{ bronze_audit_columns(raw_path) }}

    -- Preservar __source_txid para desempate no ROW_NUMBER da Silver
  , r.__source_txid
FROM raw_incremental r
