"""Copia `spark/sql/**/*.sql` → `spark/embedded/**/*.py` com PIPELINE_SQL = repr(texto).

Requer a pasta `spark/sql/` (ex.: restaurada a partir do git). Após gerar, pode remover `sql/` de novo.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SQL_ROOT = ROOT / "sql"
OUT_ROOT = ROOT / "embedded"


def main() -> None:
    if not SQL_ROOT.is_dir():
        print("Nada a fazer: spark/sql/ não existe.", file=sys.stderr)
        sys.exit(0)
    for p in sorted(SQL_ROOT.rglob("*.sql")):
        rel = p.relative_to(SQL_ROOT)
        text = p.read_text(encoding="utf-8")
        out = OUT_ROOT / rel.with_suffix(".py")
        out.parent.mkdir(parents=True, exist_ok=True)
        header = (
            "# Gerado a partir de spark/sql/ (pasta sql removida; conteúdo embutido para paridade com dbt).\n\n"
        )
        out.write_text(header + "PIPELINE_SQL = " + repr(text) + "\n", encoding="utf-8")
        print(out)


if __name__ == "__main__":
    main()
