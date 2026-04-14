"""Expande macros dbt dos modelos silver para Spark SQL (fragmentos)."""

from __future__ import annotations

import re


def _std_date(expr: str) -> str:
    return f"""CASE
  WHEN {expr} IS NULL THEN DATE '1900-01-01'
  WHEN TRY_CAST({expr} AS DATE) IS NULL THEN DATE '1900-01-01'
  WHEN YEAR(TRY_CAST({expr} AS DATE)) >= 2900 THEN DATE '1900-01-01'
  ELSE TRY_CAST({expr} AS DATE)
END"""


def _std_ts(expr: str) -> str:
    return f"""CASE
  WHEN {expr} IS NULL THEN TIMESTAMP '1900-01-01 00:00:00'
  WHEN TRY_CAST({expr} AS TIMESTAMP) IS NULL THEN TIMESTAMP '1900-01-01 00:00:00'
  WHEN YEAR(TRY_CAST({expr} AS TIMESTAMP)) >= 2900 THEN TIMESTAMP '1900-01-01 00:00:00'
  ELSE TRY_CAST({expr} AS TIMESTAMP)
END"""


def _fill_bigint(expr: str, sentinel: int = -1) -> str:
    return f"COALESCE({expr}, CAST({sentinel} AS BIGINT))"


def _fill_string(expr: str, lit: str) -> str:
    return f"COALESCE(NULLIF(TRIM(CAST({expr} AS STRING)), ''), {lit})"


def _std_text_initcap(expr: str, lit: str = "'indefinido'") -> str:
    return f"INITCAP({_fill_string(expr, lit)})"


def _std_enum(expr: str, lit: str = "'i'") -> str:
    return f"LOWER(COALESCE(NULLIF(TRIM(CAST({expr} AS STRING)), ''), {lit}))"


def _norm_dec(expr: str, scale: int, sentinel: float) -> str:
    return f"""COALESCE(
  ROUND(CAST({expr} AS DECIMAL(38, 6)), {scale}),
  CAST({sentinel} AS DECIMAL(38, 2))
)"""


def _std_doc(expr: str, length: int) -> str:
    return f"""CASE
  WHEN {expr} IS NULL THEN CAST(NULL AS STRING)
  WHEN LENGTH(REGEXP_REPLACE(TRIM(CAST({expr} AS STRING)), '[^0-9]', '')) = {length} THEN
    REGEXP_REPLACE(TRIM(CAST({expr} AS STRING)), '[^0-9]', '')
  WHEN LENGTH(REGEXP_REPLACE(TRIM(CAST({expr} AS STRING)), '[^0-9]', '')) BETWEEN 1 AND ({length} - 1) THEN
    LPAD(REGEXP_REPLACE(TRIM(CAST({expr} AS STRING)), '[^0-9]', ''), {length}, '0')
  ELSE REGEXP_REPLACE(TRIM(CAST({expr} AS STRING)), '[^0-9]', '')
END"""


def expand_silver_context_audit() -> str:
    return """
    , from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS _context_processed_at
    , CAST(uuid() AS STRING) AS _dbt_invocation_id"""


def expand_silver_audit() -> str:
    return """
    , d._is_deleted AS _is_deleted
    , d._cdc_op AS _cdc_op
    , d._cdc_event_at AS _cdc_event_at
    , d._cdc_ts_ms AS _cdc_ts_ms
    , d._row_hash AS _bronze_row_hash
    , d._bronze_loaded_at AS _bronze_loaded_at
    , from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS _silver_processed_at
    , CAST(uuid() AS STRING) AS _dbt_invocation_id"""


def expand_macros_in_sql(sql: str) -> str:
    """Substitui chamadas {{ macro(...) }} no texto SQL."""

    def repl_std_date(m: re.Match) -> str:
        return _std_date(m.group(1))

    def repl_std_ts(m: re.Match) -> str:
        return _std_ts(m.group(1))

    def repl_fb(m: re.Match) -> str:
        return _fill_bigint(m.group(1), int(m.group(2)))

    def repl_fs(m: re.Match) -> str:
        return _fill_string(m.group(1), m.group(2))

    def repl_sti(m: re.Match) -> str:
        return _std_text_initcap(m.group(1), m.group(2))

    def repl_se(m: re.Match) -> str:
        return _std_enum(m.group(1), m.group(2))

    def repl_nd(m: re.Match) -> str:
        return _norm_dec(m.group(1), int(m.group(2)), float(m.group(3)))

    def repl_sd(m: re.Match) -> str:
        return _std_doc(m.group(1), int(m.group(2)))

    out = sql
    out = re.sub(
        r"\{\{\s*standardize_date\('([^']+)'\)\s*\}\}",
        repl_std_date,
        out,
    )
    out = re.sub(
        r"\{\{\s*standardize_timestamp\('([^']+)'\)\s*\}\}",
        repl_std_ts,
        out,
    )
    out = re.sub(
        r"\{\{\s*fill_null_bigint\('([^']+)',\s*(-?\d+)\)\s*\}\}",
        repl_fb,
        out,
    )
    out = re.sub(
        r"\{\{\s*fill_null_string\('([^']+)',\s*\"'([^']*)'\"\)\s*\}\}",
        lambda m: _fill_string(m.group(1), f"'{m.group(2)}'"),
        out,
    )
    out = re.sub(
        r"\{\{\s*standardize_text_initcap\('([^']+)',\s*\"'([^']*)'\"\)\s*\}\}",
        lambda m: _std_text_initcap(m.group(1), f"'{m.group(2)}'"),
        out,
    )
    out = re.sub(
        r"\{\{\s*standardize_enum\('([^']+)',\s*\"'([^']*)'\"\)\s*\}\}",
        lambda m: _std_enum(m.group(1), f"'{m.group(2)}'"),
        out,
    )
    out = re.sub(
        r"\{\{\s*normalize_decimal\('([^']+)',\s*(\d+),\s*(-?[\d.]+)\)\s*\}\}",
        repl_nd,
        out,
    )
    out = re.sub(
        r"\{\{\s*standardize_documents\('([^']+)',\s*(\d+)\)\s*\}\}",
        repl_sd,
        out,
    )
    out = re.sub(r"\{\{\s*silver_audit_columns\(\)\s*\}\}", expand_silver_audit(), out)
    out = re.sub(r"\{\{\s*silver_context_audit_columns\(\)\s*\}\}", expand_silver_context_audit(), out)
    return out


def strip_jinja_config(sql: str) -> str:
    return re.sub(r"\{\{[\s\S]*?^\}\}", "", sql, count=1, flags=re.MULTILINE)


def dbt_to_spark_sql(
    raw_sql: str,
    *,
    bronze_table: str,
    silver_table: str,
) -> str:
    """Remove config, expand incremental, ref/this, macros."""
    # remove first {{ config ... }}
    sql = re.sub(r"\{\{[\s\S]*?config\([\s\S]*?\)\s*\}\}", "", raw_sql, count=1)
    # remove {#- ... -#}
    sql = re.sub(r"\{#-[\s\S]*?-#\}", "", sql)
    sql = sql.replace("{{ ref('bronze_tasy_proc_paciente_convenio') }}", bronze_table)
    # generic ref
    sql = re.sub(
        r"\{\{\s*ref\('([^']+)'\)\s*\}\}",
        lambda m: f"{bronze_table.split('.')[0]}.bronze.{m.group(1)}"
        if m.group(1).startswith("bronze_")
        else f"{bronze_table.split('.')[0]}.silver.{m.group(1)}",
        sql,
    )
    # {{ this }} -> silver_table
    sql = re.sub(r"\{\{\s*this\s*\}\}", silver_table, sql)
    # incremental if
    sql = re.sub(
        r"\{%\s*if\s+is_incremental\(\)\s*%\}([\s\S]*?)\{%\s*endif\s*%\}",
        r"\1",
        sql,
    )
    sql = expand_macros_in_sql(sql)
    return sql.strip()
