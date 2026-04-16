{#
  Colunas de auditoria bronze (CDC Debezium → Avro → Iceberg).

  Uso:
    {{ bronze_audit_columns(raw_path) }}
    {{ bronze_audit_columns(raw_path, raw_alias='r', cdc_ts_col='__ts_ms') }}

  Convenção:
    - CTE `raw_incremental` com leitura Avro filtrada por watermark.
    - `FROM raw_incremental r` (ou outro alias via raw_alias).
    - cdc_ts_col: nome físico do timestamp CDC no Avro — default `__ts_ms` (Debezium).

  _row_hash: MD5(TO_JSON(STRUCT(<alias>.*))) sobre a linha atual de raw_incremental.
  Sem subconsulta correlacionada — o Spark não permite outer refs em agregados
  escalares nesse contexto (UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY).

  Nomes físicos: usa __source_txid (minúsculas), padrão do SQL neste repositório.
#}
{% macro bronze_audit_columns(raw_path, raw_alias='r', cdc_ts_col='__ts_ms') %}
  {% set ra = raw_alias | trim %}
  {% set ts = cdc_ts_col | trim %}
  , CAST(({{ ra }}.__deleted = 'true') AS BOOLEAN) AS _is_deleted
  , {{ ra }}.__op AS _cdc_op
  , CAST(COALESCE({{ ra }}.{{ ts }}, 0) AS BIGINT) AS _cdc_ts_ms
  , CAST(FROM_UNIXTIME(CAST(COALESCE({{ ra }}.{{ ts }}, 0) AS DOUBLE) / 1000.0) AS TIMESTAMP) AS _cdc_event_at
  , {{ ra }}.__source_table AS _cdc_source_table
  , '{{ raw_path | replace("'", "''") }}' AS _source_path
  , MD5(TO_JSON(STRUCT({{ ra }}.*))) AS _row_hash
  , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _bronze_loaded_at
{% endmacro %}
