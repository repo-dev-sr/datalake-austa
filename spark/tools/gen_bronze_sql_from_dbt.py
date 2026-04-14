"""
Gera spark/sql/bronze/<stem>.sql a partir de dbt/models/bronze/<file>.sql.
Uso: python spark/tools/gen_bronze_sql_from_dbt.py bronze_tasy_conta_paciente.sql
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def extract_business_cols(text: str) -> list[str]:
    m = re.search(
        r"{%\s*set\s+\w+_business_cols\s*=\s*\[(.*?)\]\s*%}",
        text,
        re.DOTALL,
    )
    if not m:
        raise ValueError("business_cols não encontrado")
    body = m.group(1)
    cols = re.findall(r"'([^']+)'", body)
    return cols


def extract_select_sql(text: str) -> str:
    i = text.upper().find("\nSELECT")
    if i == -1:
        i = text.upper().find("SELECT\n")
    if i == -1:
        raise ValueError("SELECT não encontrado")
    chunk = text[i:].strip()
    j = chunk.upper().rfind("FROM RAW_INCREMENTAL")
    if j == -1:
        raise ValueError("FROM raw_incremental não encontrado")
    return chunk[: j + len("FROM raw_incremental")].strip()


def expand_audit(raw_path: str, business_cols: list[str]) -> str:
    struct_inner = ", ".join(business_cols)
    esc = raw_path.replace("'", "''")
    return f"""  , CAST((__deleted = 'true') AS BOOLEAN) AS _is_deleted
  , __op AS _cdc_op
  , CAST(COALESCE(__ts_ms, 0) AS BIGINT) AS _cdc_ts_ms
  , CAST(from_unixtime(CAST(COALESCE(__ts_ms, 0) AS DOUBLE) / 1000.0) AS TIMESTAMP) AS _cdc_event_at
  , __source_table AS _cdc_source_table
  , '{esc}' AS _source_path
  , md5(to_json(struct({struct_inner}))) AS _row_hash
  , from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') AS _bronze_loaded_at
  , __source_txid"""


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: gen_bronze_sql_from_dbt.py <arquivo_dbt.sql>")
        sys.exit(1)
    dbt_name = sys.argv[1]
    src = ROOT / "dbt" / "models" / "bronze" / dbt_name
    text = src.read_text(encoding="utf-8")
    mpath = re.search(r'{%\s*set\s+raw_path\s*=\s*"([^"]+)"\s*%}', text)
    if not mpath:
        mpath = re.search(r"{%\s*set\s+raw_path\s*=\s*'([^']+)'\s*%}", text)
    raw_path = mpath.group(1) if mpath else ""
    cols = extract_business_cols(text)
    sel = extract_select_sql(text)
    sel = re.sub(
        r"\{\{\s*bronze_audit_columns\([^)]*\)\s*\}\}",
        expand_audit(raw_path, cols),
        sel,
        flags=re.DOTALL,
    )
    stem = dbt_name.replace("bronze_tasy_", "").replace(".sql", "")
    out = ROOT / "spark" / "sql" / "bronze" / f"{stem}.sql"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(sel + "\n", encoding="utf-8")
    print(f"Escrito {out} ({len(cols)} cols negócio)")


if __name__ == "__main__":
    main()
