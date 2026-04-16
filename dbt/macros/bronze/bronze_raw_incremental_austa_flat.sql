{#
  Leitura Avro para tópicos austa.TASY.* em formato **flat** (igual a tasy.TASY.*):
  raiz com __ts_ms, __op, … — sem envelope before/after/ts_ms.

  Uso (após CTE `params`):
    {{ bronze_raw_incremental_austa_flat(raw_path) }}
#}
{% macro bronze_raw_incremental_austa_flat(raw_path) %}
, raw_incremental AS (
  SELECT *
  FROM avro.`{{ raw_path }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
)
{% endmacro %}
