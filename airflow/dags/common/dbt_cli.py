"""
dbt em BashOperator no worker Celery.

BashOperator(env=...) substitui o ambiente do worker (append_env=False por padrão) e o dict
costuma ser resolvido no parse — sem SPARK_THRIFT_* do container. Usar export no bash.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from common.config import DBT_PROFILE_NAME, DBT_PROFILES_DIR, DBT_PROJECT_DIR, DBT_TARGET

_AIRFLOW_LOCAL_BIN = "/home/airflow/.local/bin"


def dbt_executable_path() -> str:
    override = (os.environ.get("DBT_EXECUTABLE") or "").strip()
    if override:
        return override
    found = shutil.which("dbt")
    if found:
        return found
    return f"{_AIRFLOW_LOCAL_BIN}/dbt"


def proj_quote(path: str) -> str:
    p = str(Path(path).resolve())
    if "'" in p:
        return f'"{p}"'
    return f"'{p}'"


def dbt_shell_preamble() -> str:
    prof = str(Path(DBT_PROFILES_DIR).resolve())
    plug = str(Path(DBT_PROJECT_DIR).resolve() / "plugins")
    return (
        f'export DBT_PROFILES_DIR="{prof}"; '
        f'export PYTHONPATH="{plug}${{PYTHONPATH:+:$PYTHONPATH}}"; '
        f'export PATH="{_AIRFLOW_LOCAL_BIN}${{PATH:+:$PATH}}"; '
    )


def dbt_fail_tail() -> str:
    return (
        '|| { echo "=== dbt.log (tail) ==="; '
        'tail -n 200 "$_L/dbt.log" 2>/dev/null || true; exit 1; }'
    )


def dbt_run_command(*, select: str, extra_args: str = "") -> str:
    profiles = str(Path(DBT_PROFILES_DIR).resolve())
    exe = dbt_executable_path()
    tail = f" {extra_args}" if extra_args.strip() else ""
    return (
        f"{dbt_shell_preamble()}"
        f"_L=/tmp/dbt_af_$$; mkdir -p \"$_L\"; "
        f"cd {proj_quote(DBT_PROJECT_DIR)} && "
        f"{exe} run --no-use-colors --log-path \"$_L\" --select {select} "
        f"--profiles-dir {profiles} --profile {DBT_PROFILE_NAME} --target {DBT_TARGET}{tail} "
        f"{dbt_fail_tail()}"
    )


def dbt_deps_then_run_command(*, select: str, extra_args: str = "") -> str:
    profiles = str(Path(DBT_PROFILES_DIR).resolve())
    exe = dbt_executable_path()
    tail = f" {extra_args}" if extra_args.strip() else ""
    return (
        f"{dbt_shell_preamble()}"
        f"_L=/tmp/dbt_af_$$; mkdir -p \"$_L\"; "
        f"cd {proj_quote(DBT_PROJECT_DIR)} && "
        f"{exe} deps --profiles-dir {profiles} && "
        f"{exe} run --no-use-colors --log-path \"$_L\" --select {select} "
        f"--profiles-dir {profiles} --profile {DBT_PROFILE_NAME} --target {DBT_TARGET}{tail} "
        f"{dbt_fail_tail()}"
    )
