{#
  Leitura Avro no S3 para tópicos austa.TASY.* em formato Debezium **envelope**:
  raiz = before, after, source, op, ts_ms, … (sem __ts_ms no raiz).

  Expande `coalesce(after, before)` para colunas de negócio e alinha metadados ao padrão
  flat (como tasy.TASY.*): __deleted, __op, __ts_ms, __source_table, __source_txid.

  Uso (após CTE `params`, antes do SELECT final):
    {{ bronze_raw_incremental_austa_envelope(raw_path) }}

  Referência: inspeção S3 — tasy.TASY.* = flat + __ts_ms; austa.TASY.* = envelope + ts_ms.
#}
{% macro bronze_raw_incremental_austa_envelope(raw_path) %}
, raw_incremental AS (
  SELECT
      d.*
    , IF(op = 'd', 'true', 'false') AS __deleted
    , op AS __op
    , CAST(ts_ms AS BIGINT) AS __ts_ms
    , CAST(source.`table` AS STRING) AS __source_table
    , CAST(source.txId AS STRING) AS __source_txid
  FROM (
    SELECT
        COALESCE(`after`, `before`) AS d
      , op
      , source
      , ts_ms
    FROM avro.`{{ raw_path }}`
    WHERE CAST(COALESCE(ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
  ) e
)
{% endmacro %}
