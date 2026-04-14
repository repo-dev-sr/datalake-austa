"""Orquestrador: dispara DAGs PySpark por camada (bronze → silver → silver_context → gold)."""

from __future__ import annotations

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from common.default_args import DEFAULT_ARGS


@dag(
    dag_id="master_pyspark_orchestrator_batch",
    description="Trigger: pyspark_bronze_batch → silver → silver_context → gold",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["pyspark", "orchestrator", "batch", "teste", "lakehouse"],
)
def master_pyspark_orchestrator_batch_dag():
    b = TriggerDagRunOperator(
        task_id="trigger_pyspark_bronze_batch",
        trigger_dag_id="pyspark_bronze_batch",
        wait_for_completion=True,
    )
    s = TriggerDagRunOperator(
        task_id="trigger_pyspark_silver_batch",
        trigger_dag_id="pyspark_silver_batch",
        wait_for_completion=True,
    )
    sc = TriggerDagRunOperator(
        task_id="trigger_pyspark_silver_context_batch",
        trigger_dag_id="pyspark_silver_context_batch",
        wait_for_completion=True,
    )
    g = TriggerDagRunOperator(
        task_id="trigger_pyspark_gold_batch",
        trigger_dag_id="pyspark_gold_batch",
        wait_for_completion=True,
    )
    b >> s >> sc >> g


master_pyspark_orchestrator_batch_dag()
