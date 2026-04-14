"""Garante PYTHONPATH apontando para o diretório `spark/` (pai de `common/`)."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_spark_root() -> Path:
    root = Path(__file__).resolve().parent.parent
    s = str(root)
    if s not in sys.path:
        sys.path.insert(0, s)
    return root
