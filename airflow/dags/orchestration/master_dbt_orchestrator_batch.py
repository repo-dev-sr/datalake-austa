"""
Orquestrador batch: backfill / reprocessamento (bronze all → silver → silver_context [→ gold]).

- Cadeia principal: triggers com wait_for_completion.
- Opcional: `run_cli_first=true` no conf do DagRun (ou param) para rodar `dbt run` com --vars antes dos triggers.
"""
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from common.config import DBT_PROJECT_DIR
from common.default_args import DEFAULT_ARGS


def _pick_cli_branch(**context):
    conf = context["dag_run"].conf or {}
    if conf.get("run_cli_first") or context["params"].get("run_cli_first"):
        return "dbt_run_with_vars"
    return "skip_cli_before_triggers"


@dag(
    dag_id="master_dbt_orchestrator_batch",
    description="Orquestrador batch: bronze all → silver → silver_context [→ gold]; opcional dbt CLI com vars",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    params={
        "run_cli_first": False,
        "dbt_project_dir": DBT_PROJECT_DIR,
        "dbt_plugins": f"{DBT_PROJECT_DIR}/plugins",
        "dbt_select": "path:models/bronze",
        "cdc_lookback_hours": 2,
        "cdc_reprocess_hours": 0,
    },
    tags=["orchestrator", "batch", "lakehouse", "dbt"],
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
            "dbt run --select {{ params.dbt_select }} "
            '--vars \'{"cdc_lookback_hours": {{ params.cdc_lookback_hours }}, '
            '"cdc_reprocess_hours": {{ params.cdc_reprocess_hours }}}}\''
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
