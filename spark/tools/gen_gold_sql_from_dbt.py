"""
Gera spark/sql/gold/<stem>.sql a partir de dbt/models/gold/...
Uso: python spark/tools/gen_gold_sql_from_dbt.py dimensions/shared/dim_tempo.sql
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from expand_gold_macros import expand_gold_sql, patch_this_table  # noqa: E402


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: gen_gold_sql_from_dbt.py <caminho relativo em models/gold/>")
        sys.exit(1)
    rel = sys.argv[1]
    src = ROOT / "dbt" / "models" / "gold" / rel
    text = src.read_text(encoding="utf-8")
    stem = src.stem
    gold_fq = f"glue_catalog.gold.{stem}"
    text = expand_gold_sql(text)
    text = patch_this_table(text, gold_fq)
    out_dir = ROOT / "spark" / "sql" / "gold"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{stem}.sql"
    out.write_text(text.strip() + "\n", encoding="utf-8")
    print(f"Escrito {out}")


if __name__ == "__main__":
    main()
