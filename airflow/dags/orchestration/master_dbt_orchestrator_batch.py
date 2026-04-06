"""
Orquestrador operacional (agendado): a cada hora processa o lakehouse na ordem canônica.

Fluxo: bronze (todos os modelos) → silver → silver_context [→ gold quando ativado].
Serve para incorporar dados que chegaram no raw desde a última janela CDC — não é só manutenção/backfill.

- `schedule`: 1 hora.
- Opcional: `run_cli_first=true` no conf/param para um `dbt run` CLI com --vars antes dos triggers (ex.: pré-aquecer bronze com janela CDC).
"""
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from common.config import DBT_PROFILE_NAME, DBT_PROJECT_DIR, DBT_TARGET
from common.default_args import DEFAULT_ARGS


def _pick_cli_branch(**context):
    conf = context["dag_run"].conf or {}
    if conf.get("run_cli_first") or context["params"].get("run_cli_first"):
        return "dbt_run_with_vars"
    return "skip_cli_before_triggers"


@dag(
    dag_id="master_dbt_orchestrator_batch",
    description="Lakehouse horário: bronze all → silver → silver_context; dados novos a cada 1h",
    schedule=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_cli_first": False,
        "dbt_project_dir": DBT_PROJECT_DIR,
        "dbt_plugins": f"{DBT_PROJECT_DIR}/plugins",
        "dbt_profile_name": DBT_PROFILE_NAME,
        "dbt_target": DBT_TARGET,
        "dbt_select": "path:models/bronze",
        "cdc_lookback_hours": 2,
        "cdc_reprocess_hours": 0,
    },
    tags=["orchestrator", "batch", "lakehouse", "dbt", "hourly", "scheduled"],
)
def master_dbt_orchestrator_batch_dag():
    branch = BranchPythonOperator(
        task_id="branch_cli_or_skip",
        python_callable=_pick_cli_branch,
    )
    skip_cli = EmptyOperator(task_id="skip_cli_before_triggers")
    dbt_run_with_vars = BashOperator(
        task_id="dbt_run_with_vars",
        pool="spark_dbt",
        queue="dbt",
        bash_command=(
            "cd {{ params.dbt_project_dir }} && PYTHONPATH={{ params.dbt_plugins }} "
            "dbt run --select '{{ params.dbt_select or \"path:models/bronze\" }}' "
            "--profile {{ params.dbt_profile_name }} --target {{ params.dbt_target }} "
            "--vars '{{ dict(cdc_lookback_hours=params.cdc_lookback_hours, "
            "cdc_reprocess_hours=params.cdc_reprocess_hours) | tojson }}'"
        ),
    )

    trigger_bronze_all = TriggerDagRunOperator(
        task_id="trigger_bronze_dbt_task_group_all",
        trigger_dag_id="bronze_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
    )
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dbt_task_group_all",
        trigger_dag_id="silver_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
    )
    trigger_silver_context = TriggerDagRunOperator(
        task_id="trigger_silver_context_dbt_task_group_all",
        trigger_dag_id="silver_context_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
    )

    branch >> [skip_cli, dbt_run_with_vars]
    skip_cli >> trigger_bronze_all
    dbt_run_with_vars >> trigger_bronze_all
    trigger_bronze_all >> trigger_silver >> trigger_silver_context

    # GOLD_LAYER_TODO — descomente quando gold_dbt_task_group_all.py estiver ativo:
    # trigger_gold = TriggerDagRunOperator(
    #     task_id="trigger_gold_dbt_task_group_all",
    #     trigger_dag_id="gold_dbt_task_group_all",
    #     wait_for_completion=True,
    #     poke_interval=120,
    #     reset_dag_run=False,
    # )
    # trigger_silver_context >> trigger_gold


master_dbt_orchestrator_batch_dag()
