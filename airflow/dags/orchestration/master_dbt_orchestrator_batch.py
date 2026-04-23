"""Orquestrador: bronze → silver → silver_context (agendado ou manual). Opcional CLI com --vars."""
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from common.config import DBT_PROFILE_NAME, DBT_PROFILES_DIR, DBT_PROJECT_DIR, DBT_TARGET
from common.dbt_cli import dbt_deps_then_run_command, dbt_executable_path, dbt_run_command, dbt_subprocess_env
from common.default_args import DEFAULT_ARGS


def _truthy_run_cli_first(conf: dict, params: dict) -> bool:
    for src in (conf, params):
        v = src.get("run_cli_first") if isinstance(src, dict) else None
        if v is True:
            return True
        if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes", "on"):
            return True
    return False


def _pick_cli_branch(**context):
    conf = context["dag_run"].conf
    if conf is None or not isinstance(conf, dict):
        conf = {}
    params = context.get("params") or {}
    if _truthy_run_cli_first(conf, params):
        return "dbt_run_with_vars"
    return "skip_cli_before_triggers"


@dag(
    dag_id="master_dbt_orchestrator_batch",
    description="Compactacao raw AVRO (in-place) → dbt bronze → silver → silver_context",
    schedule=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_cli_first": Param(
            False,
            type="boolean",
            title="dbt CLI + --vars antes do bronze",
            description="Reprocesso: dbt run com vars CDC antes das três camadas.",
        ),
        "dbt_select": Param(
            "path:models/bronze",
            type="string",
            title="--select (só com CLI opcional)",
        ),
        "cdc_lookback_hours": Param(2, type="integer", title="cdc_lookback_hours"),
        "cdc_reprocess_hours": Param(0, type="integer", title="cdc_reprocess_hours"),
    },
    tags=["orchestrator", "batch", "lakehouse", "dbt", "hourly", "scheduled"],
)
def master_dbt_orchestrator_batch_dag():
    _env = dbt_subprocess_env()
    _exe = dbt_executable_path()
    _profiles = str(Path(DBT_PROFILES_DIR).resolve())

    # trigger_raw_compaction = TriggerDagRunOperator(
    #     task_id="trigger_raw_tasy_avro_compactor",
    #     trigger_dag_id="raw_tasy_avro_compactor",
    #     wait_for_completion=True,
    #     poke_interval=30,
    #     reset_dag_run=False,
    # )

    branch = BranchPythonOperator(
        task_id="branch_cli_or_skip",
        python_callable=_pick_cli_branch,
    )
    skip_cli = EmptyOperator(task_id="skip_cli_before_triggers")
    dbt_run_with_vars = BashOperator(
        task_id="dbt_run_with_vars",
        pool="spark_dbt",
        queue="dbt",
        env=_env,
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && {_exe} deps --profiles-dir {_profiles} && {_exe} run "
            f"--profiles-dir {_profiles} --profile {DBT_PROFILE_NAME} --target {DBT_TARGET} "
            "--select '{{ params.dbt_select or \"path:models/bronze\" }}' "
            "--vars '{{ dict(cdc_lookback_hours=params.cdc_lookback_hours, "
            "cdc_reprocess_hours=params.cdc_reprocess_hours) | tojson }}'"
        ),
    )

    dbt_bronze_layer = BashOperator(
        task_id="dbt_bronze_layer",
        pool="spark_dbt",
        queue="dbt",
        env=_env,
        bash_command=dbt_deps_then_run_command(select="path:models/bronze"),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    dbt_silver_layer = BashOperator(
        task_id="dbt_silver_layer",
        pool="spark_dbt",
        queue="dbt",
        env=_env,
        bash_command=dbt_run_command(select="path:models/silver"),
    )
    dbt_silver_context_layer = BashOperator(
        task_id="dbt_silver_context_layer",
        pool="spark_dbt",
        queue="dbt",
        env=_env,
        bash_command=dbt_run_command(select="path:models/silver_context"),
    )

    # trigger_raw_compaction >> branch >> [skip_cli, dbt_run_with_vars]
    branch >> [skip_cli, dbt_run_with_vars]
    skip_cli >> dbt_bronze_layer
    dbt_run_with_vars >> dbt_bronze_layer
    dbt_bronze_layer >> dbt_silver_layer >> dbt_silver_context_layer


master_dbt_orchestrator_batch_dag()
