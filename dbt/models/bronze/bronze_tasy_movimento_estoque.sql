{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.MOVIMENTO_ESTOQUE/" %}
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
    r.nr_movimento_estoque

    -- FKs
  , r.cd_material
  , r.cd_operacao_estoque
  , r.nr_prescricao
  , r.nr_atendimento
  , r.cd_local_estoque
  , r.cd_local_estoque_dest
  , r.cd_setor_atendimento
  , r.cd_estabelecimento
  , r.cd_unidade_medida
  , r.cd_unidade_medida_cons
  , r.nr_ordem_compra
  , r.nr_lote_fabric

    -- Categoricos
  , r.ie_origem_documento
  , r.ie_tipo_movimento
  , r.ie_entrada_saida
  , r.ie_situacao
  , r.ie_consignado

    -- Quantidades e valores (valor pode ser NULL na bronze — valorizacao em MOVIMENTO_ESTOQUE_VALOR)
  , r.qt_movimento
  , r.qt_estoque
  , r.vl_movimento
  , r.vl_unitario
  , r.pr_desconto

    -- Timestamps (DT_* -> dh_*)
  , CAST(r.dt_movimento_estoque AS TIMESTAMP)   AS dh_movimento_estoque
  , CAST(r.dt_mesano_referencia AS TIMESTAMP)   AS dh_mesano_referencia
  , CAST(r.dt_validade AS TIMESTAMP)            AS dh_validade

    -- Observacoes
  , r.ds_observacao
  , r.ds_justificativa

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
