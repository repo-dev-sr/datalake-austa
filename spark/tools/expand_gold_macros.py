"""Expande macros Gold (dbt_utils, ref, var, config)."""

from __future__ import annotations

import re

CATALOG = "glue_catalog"


def expand_generate_surrogate_key(sql: str) -> str:
    def repl(m: re.Match) -> str:
        inner = m.group(1)
        cols = re.findall(r"'([^']+)'", inner)
        parts = [f"coalesce(cast({c} as string), '_dbt_utils_surrogate_key_null_')" for c in cols]
        return f"md5(concat_ws('||', {', '.join(parts)}))"

    return re.sub(
        r"\{\{\s*dbt_utils\.generate_surrogate_key\(\[([^\]]+)\]\)\s*\}\}",
        repl,
        sql,
    )


def replace_vars(sql: str) -> str:
    sql = re.sub(
        r"\{\{\s*var\(\s*\"dim_tempo_start_date\"\s*,\s*\"([^\"]+)\"\s*\)\s*\}\}",
        r"\1",
        sql,
    )
    sql = re.sub(
        r"\{\{\s*var\(\s*\"dim_tempo_end_date\"\s*,\s*\"([^\"]+)\"\s*\)\s*\}\}",
        r"\1",
        sql,
    )
    return sql


def replace_refs(sql: str) -> str:
    def repl(m: re.Match) -> str:
        ref = m.group(1)
        if ref == "procedimento":
            return f"{CATALOG}.silver_context.procedimento"
        return f"{CATALOG}.gold.{ref}"

    return re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", repl, sql)


def expand_gold_sql(raw: str) -> str:
    raw = re.sub(r"\{\{#-[\s\S]*?-#\}\}", "", raw)
    raw = re.sub(r"\{\{\s*config\([\s\S]*?\)\s*\}\}", "", raw, count=1)
    raw = replace_vars(raw)
    raw = replace_refs(raw)
    raw = re.sub(
        r"\{%\s*if\s+is_incremental\(\)\s*%\}([\s\S]*?)\{%\s*endif\s*%\}",
        r"\1",
        raw,
    )
    raw = raw.replace("{{ this }}", "__THIS__")
    raw = expand_generate_surrogate_key(raw)
    return raw


def patch_this_table(sql: str, gold_table_fq: str) -> str:
    return sql.replace("__THIS__", gold_table_fq)
