{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.MATERIAL_ATEND_PACIENTE/" %}
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
  , r.nr_atendimento
  , r.cd_material
  , r.cd_tab_preco_material
  , r.nr_seq_regra_ajuste_mat
  , r.cd_unidade_medida
  , r.cd_convenio
  , r.cd_categoria
  , r.nr_prescricao
  , r.nr_sequencia_prescricao
  , r.cd_setor_atendimento
  , r.cd_estabelecimento
  , r.cd_medico

    -- Quantidades e valores (monetario)
  , r.qt_material
  , r.vl_material
  , r.vl_unitario
  , r.vl_custo
  , r.vl_custo_medio
  , r.vl_convenio
  , r.pr_desconto

    -- Flags / situacao
  , r.ie_situacao
  , r.ie_valor_informado
  , r.ie_tipo_material
  , r.ie_cobranca
  , r.ie_suspenso

    -- Timestamps (DT_* -> dh_*)
  , CAST(r.dt_atendimento AS TIMESTAMP)         AS dh_atendimento
  , CAST(r.dt_conta AS TIMESTAMP)               AS dh_conta
  , CAST(r.dt_validade AS TIMESTAMP)            AS dh_validade

    -- Observacoes
  , r.ds_observacao
  , r.ds_complemento

    -- Auditoria TASY
  , CAST(r.dt_atualizacao AS TIMESTAMP)         AS dh_atualizacao
  , r.nm_usuario
  , CAST(r.dt_atualizacao_nrec AS TIMESTAMP)    AS dh_atualizacao_nrec
  , r.nm_usuario_nrec

    -- Auditoria CDC bronze (macro gera 8 colunas)
  {{ bronze_audit_columns(raw_path) }}

    -- Preservar __source_txid para desempate no ROW_NUMBER da Silver
  , r.__source_txid
FROM raw_incremental r
