"""
Gera spark/sql/silver_context/<nome>.sql a partir de dbt/models/silver_context/*.sql
Uso: python spark/tools/gen_silver_context_sql_from_dbt.py atendimento.sql
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from expand_silver_macros import expand_macros_in_sql  # noqa: E402

CATALOG = "glue_catalog"


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: gen_silver_context_sql_from_dbt.py <arquivo.sql>")
        sys.exit(1)
    fname = sys.argv[1]
    src = ROOT / "dbt" / "models" / "silver_context" / fname
    text = src.read_text(encoding="utf-8")
    text = re.sub(r"\{\{#-[\s\S]*?-#\}\}", "", text)
    text = re.sub(r"\{\{\s*config\([\s\S]*?\)\s*\}\}", "", text, count=1)

    def repl_ref(m: re.Match) -> str:
        ref = m.group(1)
        return f"{CATALOG}.silver.{ref}"

    text = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", repl_ref, text)
    text = expand_macros_in_sql(text)
    out = ROOT / "spark" / "sql" / "silver_context" / fname
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text.strip() + "\n", encoding="utf-8")
    print(f"Escrito {out}")


if __name__ == "__main__":
    main()
