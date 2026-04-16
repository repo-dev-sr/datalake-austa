{#
  UNION: ramo tasy.TASY.* (flat + __ts_ms no raiz) + ramo austa.TASY.* (envelope Debezium).

  O ramo austa expande coalesce(after, before) e replica metadados no mesmo estilo do flat
  (__deleted, __op, __ts_ms, __source_table, __source_txid, kafka_ingestion_source NULL).

  envelope_null_pad: o Avro flat do tasy pode ter mais colunas de metadado no fim do que
  d.* + metadados sintéticos; acrescente tantos CAST(NULL AS STRING) quanto o Spark exigir
  (erro NUM_COLUMNS_MISMATCH: N vs M → envelope_null_pad = N - M).

  Uso:
    {{ bronze_raw_incremental_union_tasy_austa(raw_path_tasy, raw_path_austa) }}
    {{ bronze_raw_incremental_union_tasy_austa(raw_path_tasy, raw_path_austa, 1) }}
#}
{% macro bronze_raw_incremental_union_tasy_austa(raw_path_tasy, raw_path_austa, envelope_null_pad=0) %}
, raw_incremental AS (
  SELECT *
  FROM avro.`{{ raw_path_tasy }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
  UNION
  SELECT
      d.*
    , IF(op = 'd', 'true', 'false') AS __deleted
    , op AS __op
    , CAST(ts_ms AS BIGINT) AS __ts_ms
    , CAST(source.`table` AS STRING) AS __source_table
    , CAST(source.txId AS STRING) AS __source_txid
    , CAST(NULL AS STRING) AS kafka_ingestion_source
    {%- if envelope_null_pad | int > 0 -%}
    {%- for i in range(envelope_null_pad | int) -%}
    , CAST(NULL AS STRING) AS __union_envelope_pad_{{ i }}
    {%- endfor -%}
    {%- endif %}
  FROM (
    SELECT
        COALESCE(`after`, `before`) AS d
      , op
      , source
      , ts_ms
    FROM avro.`{{ raw_path_austa }}`
    WHERE CAST(COALESCE(ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
  ) e
)
{% endmacro %}
