"""
Orquestrador stream: agenda silver → silver_context (e gold quando existir).

Não dispara bronze por tópico nem bronze_dbt_task_group_all (bronze segue event-driven + batch).
"""
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from common.default_args import DEFAULT_ARGS


@dag(
    dag_id="master_dbt_orchestrator_stream",
    description="Orquestrador stream: silver → silver_context [→ gold]",
    schedule=timedelta(minutes=30),
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["orchestrator", "stream", "lakehouse", "dbt"],
)
def master_dbt_orchestrator_stream_dag():
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dbt_task_group_all",
        trigger_dag_id="silver_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=15,
        reset_dag_run=False,
    )
    trigger_silver_context = TriggerDagRunOperator(
        task_id="trigger_silver_context_dbt_task_group_all",
        trigger_dag_id="silver_context_dbt_task_group_all",
        wait_for_completion=True,
        poke_interval=15,
        reset_dag_run=False,
    )
    trigger_silver >> trigger_silver_context

    # GOLD_LAYER_TODO — descomente quando gold_dbt_task_group_all.py estiver ativo:
    # trigger_gold = TriggerDagRunOperator(
    #     task_id="trigger_gold_dbt_task_group_all",
    #     trigger_dag_id="gold_dbt_task_group_all",
    #     wait_for_completion=True,
    #     poke_interval=60,
    #     reset_dag_run=False,
    # )
    # trigger_silver_context >> trigger_gold


master_dbt_orchestrator_stream_dag()
