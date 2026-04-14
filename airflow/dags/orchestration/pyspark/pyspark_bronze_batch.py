"""Teste: executa todos os jobs PySpark da camada Bronze (spark-submit via SSH)."""

from __future__ import annotations

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from common.default_args import DEFAULT_ARGS
from common.spark_submit import spark_submit_ssh_bash

_BRONZE = [
    ("spark/bronze/bronze_tasy_atend_paciente_unidade.py", "bronze_tasy_atend_paciente_unidade"),
    ("spark/bronze/bronze_tasy_atendimento_paciente.py", "bronze_tasy_atendimento_paciente"),
    ("spark/bronze/bronze_tasy_conta_paciente.py", "bronze_tasy_conta_paciente"),
    ("spark/bronze/bronze_tasy_proc_paciente_convenio.py", "bronze_tasy_proc_paciente_convenio"),
    ("spark/bronze/bronze_tasy_proc_paciente_valor.py", "bronze_tasy_proc_paciente_valor"),
    ("spark/bronze/bronze_tasy_procedimento_paciente.py", "bronze_tasy_procedimento_paciente"),
]


@dag(
    dag_id="pyspark_bronze_batch",
    description="PySpark Bronze (6 entidades) — manual, teste",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["pyspark", "batch", "teste", "bronze", "lakehouse"],
)
def pyspark_bronze_batch_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    tasks = []
    for rel, app in _BRONZE:
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


pyspark_bronze_batch_dag()
