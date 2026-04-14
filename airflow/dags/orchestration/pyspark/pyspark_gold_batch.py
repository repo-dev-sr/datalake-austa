"""Teste: Gold PySpark — dim_tempo → dims → fato."""

from __future__ import annotations

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.default_args import DEFAULT_ARGS
from common.spark_submit import spark_submit_ssh_bash


@dag(
    dag_id="pyspark_gold_batch",
    description="PySpark Gold — manual, teste",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
    tags=["pyspark", "batch", "teste", "gold", "lakehouse"],
)
def pyspark_gold_batch_dag():
    dim_tempo = BashOperator(
        task_id="submit_dim_tempo",
        pool="spark_pyspark",
        queue="default",
        bash_command=spark_submit_ssh_bash(
            script_relative="spark/gold/dim_tempo.py",
            app_name="gold_dim_tempo",
        ),
    )
    dims = [
        BashOperator(
            task_id=f"submit_{n}",
            pool="spark_pyspark",
            queue="default",
            bash_command=spark_submit_ssh_bash(
                script_relative=f"spark/gold/{n}.py",
                app_name=f"gold_{n}",
            ),
        )
        for n in ("dim_cid", "dim_convenio", "dim_medico", "dim_procedimento")
    ]
    fct = BashOperator(
        task_id="submit_fct_producao_medica",
        pool="spark_pyspark",
        queue="default",
        bash_command=spark_submit_ssh_bash(
            script_relative="spark/gold/fct_producao_medica.py",
            app_name="gold_fct_producao_medica",
        ),
    )
    dim_tempo >> dims >> fct


pyspark_gold_batch_dag()
