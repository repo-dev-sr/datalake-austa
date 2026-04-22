{{ config(
    materialized='incremental',
    schema='bronze',
    file_format='iceberg',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    tags=['bronze','tasy','farmacia']
) }}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.MATERIAL/" %}
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
    r.cd_material

    -- FKs
  , r.cd_grupo_material
  , r.cd_subgrupo_material
  , r.cd_classe_material
  , r.cd_unidade_medida_estoque
  , r.cd_unidade_medida_compra
  , r.cd_unidade_medida_consumo
  , r.cd_material_generico

    -- Descricoes
  , r.ds_material
  , r.ds_material_direto
  , r.ds_reduzida
  , r.ds_marca
  , r.ds_fabricante
  , r.ds_principio_ativo
  , r.cd_barras

    -- Categoricos / flags
  , r.ie_tipo_material
  , r.ie_situacao
  , r.ie_padronizado
  , r.ie_consignado
  , r.ie_curva_abc
  , r.ie_classif_xyz
  , r.ie_alto_risco
  , r.ie_controlado
  , r.ie_controle_medico
  , r.ie_restrito
  , r.ie_manipulado
  , r.ie_medicamento
  , r.ie_antibiotico
  , r.ie_quimioterapico
  , r.ie_prescricao

    -- Atributos ANVISA / regulatorio
  , r.nr_registro_anvisa
  , r.cd_anvisa
  , r.cd_ean
  , r.cd_tuss

    -- Timestamps (DT_* -> dh_*)
  , CAST(r.dt_validade AS TIMESTAMP)            AS dh_validade
  , CAST(r.dt_inicio_vigencia AS TIMESTAMP)     AS dh_inicio_vigencia
  , CAST(r.dt_fim_vigencia AS TIMESTAMP)        AS dh_fim_vigencia

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
