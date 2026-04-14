"""Teste: Silver-Context PySpark (3 entidades)."""

from __future__ import annotations

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from common.default_args import DEFAULT_ARGS
from common.spark_submit import spark_submit_ssh_bash

_SC = [
    ("spark/silver_context/silver_context_atendimento.py", "silver_context_atendimento"),
    ("spark/silver_context/silver_context_movimentacao_paciente.py", "silver_context_movimentacao_paciente"),
    ("spark/silver_context/silver_context_procedimento.py", "silver_context_procedimento"),
]


@dag(
    dag_id="pyspark_silver_context_batch",
    description="PySpark silver_context — manual, teste",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["pyspark", "batch", "teste", "silver_context", "lakehouse"],
)
def pyspark_silver_context_batch_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    tasks = []
    for rel, app in _SC:
        tid = app.replace(".", "_")
        tasks.append(
            BashOperator(
                task_id=f"submit_{tid}",
                pool="spark_pyspark",
                queue="default",
                bash_command=spark_submit_ssh_bash(script_relative=rel, app_name=app),
            )
        )
    start >> tasks >> end


pyspark_silver_context_batch_dag()
