"""Teste: executa todos os jobs PySpark da camada Silver (sequencial)."""

from __future__ import annotations

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.default_args import DEFAULT_ARGS
from common.spark_submit import spark_submit_ssh_bash

_SILVER = [
    ("spark/silver/silver_tasy_atend_paciente_unidade.py", "silver_tasy_atend_paciente_unidade"),
    ("spark/silver/silver_tasy_atendimento_paciente.py", "silver_tasy_atendimento_paciente"),
    ("spark/silver/silver_tasy_conta_paciente.py", "silver_tasy_conta_paciente"),
    ("spark/silver/silver_tasy_proc_paciente_convenio.py", "silver_tasy_proc_paciente_convenio"),
    ("spark/silver/silver_tasy_proc_paciente_valor.py", "silver_tasy_proc_paciente_valor"),
    ("spark/silver/silver_tasy_procedimento_paciente.py", "silver_tasy_procedimento_paciente"),
]


@dag(
    dag_id="pyspark_silver_batch",
    description="PySpark Silver (6 entidades) — manual, teste, sequencial",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["pyspark", "batch", "teste", "silver", "lakehouse"],
)
def pyspark_silver_batch_dag():
    ops = []
    for rel, app in _SILVER:
        tid = app.replace(".", "_")
        ops.append(
            BashOperator(
                task_id=f"submit_{tid}",
                pool="spark_pyspark",
                queue="default",
                bash_command=spark_submit_ssh_bash(script_relative=rel, app_name=app),
            )
        )
    chain(*ops)


pyspark_silver_batch_dag()
