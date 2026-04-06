"""
Bronze acionada por dataset (stream): um único `dbt run --select <modelo>`.

Sem Cosmos → evita `dbt ls` no parse da DAG, operadores `.test` e competição pelo pool
`spark_dbt` reservado às camadas silver / silver_context / gold.

Roda na fila Celery **default** (worker com maior concorrência). Pool **bronze_stream**
limita quantas bronzes em paralelo (protege Kyuubi e RAM do worker default).
"""
from __future__ import annotations

import os
from pathlib import Path

from common.config import DBT_PROFILE_NAME, DBT_PROJECT_DIR, DBT_TARGET

# Alinhar slots no Airflow (airflow-init / UI) ao volume de eventos e RAM do airflow-worker.
BRONZE_STREAM_POOL = "bronze_stream"


def bronze_dbt_run_env() -> dict[str, str]:
    merged = os.environ.copy()
    plugins = str(Path(DBT_PROJECT_DIR).resolve() / "plugins")
    prev = merged.get("PYTHONPATH", "")
    merged["PYTHONPATH"] = f"{plugins}{os.pathsep}{prev}" if prev else plugins
    return merged


def bash_dbt_run_select(model: str) -> str:
    return (
        f"cd {DBT_PROJECT_DIR} && dbt run --select {model} "
        f"--profile {DBT_PROFILE_NAME} --target {DBT_TARGET}"
    )
