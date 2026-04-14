"""
Bronze por stream: um `dbt run` via Bash (sem env= no operador — ver common/dbt_cli.py).
"""
from common.dbt_cli import dbt_run_command

BRONZE_STREAM_POOL = "bronze_stream"


def bash_dbt_run_select(model: str) -> str:
    return dbt_run_command(select=model)
