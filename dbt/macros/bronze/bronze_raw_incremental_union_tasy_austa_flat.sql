{#
  UNION: ramo tasy.TASY.* (flat + __ts_ms) + ramo austa.TASY.* também **flat** (__ts_ms).

  tasy_null_pad: quando o Avro do tasy tem **menos** colunas que o do austa, acrescente NULLs
  no fim do primeiro ramo (erro: first N, second M com M > N → tasy_null_pad = M - N).

  Uso:
    {{ bronze_raw_incremental_union_tasy_austa_flat(raw_path_tasy, raw_path_austa) }}
    {{ bronze_raw_incremental_union_tasy_austa_flat(raw_path_tasy, raw_path_austa, 1) }}
#}
{% macro bronze_raw_incremental_union_tasy_austa_flat(raw_path_tasy, raw_path_austa, tasy_null_pad=0) %}
, raw_incremental AS (
  SELECT
    *
    {%- if tasy_null_pad | int > 0 -%}
    {%- for i in range(tasy_null_pad | int) -%}
    , CAST(NULL AS STRING) AS __union_flat_pad_tasy_{{ i }}
    {%- endfor -%}
    {%- endif %}
  FROM avro.`{{ raw_path_tasy }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
  UNION
  SELECT *
  FROM avro.`{{ raw_path_austa }}`
  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)
)
{% endmacro %}
