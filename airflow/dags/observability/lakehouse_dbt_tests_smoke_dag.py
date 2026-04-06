"""
Smoke: executa um subconjunto de testes dbt (camada silver) no projeto lakehouse.

Falhas disparam e-mail (OBSERVABILITY_DEFAULT_ARGS) se SMTP estiver configurado no Airflow.
"""
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.config import DBT_PROJECT_DIR
from common.default_args import OBSERVABILITY_DEFAULT_ARGS

DBT_PLUGINS = f"{DBT_PROJECT_DIR}/plugins"


@dag(
    dag_id="lakehouse_dbt_tests_smoke",
    description="Smoke: dbt test em modelos tag:silver (lakehouse Tasy)",
    schedule="0 7 * * *",
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=OBSERVABILITY_DEFAULT_ARGS,
    tags=["observability", "lakehouse", "dbt"],
)
def lakehouse_dbt_tests_smoke_dag():
    BashOperator(
        task_id="dbt_test_silver_tag",
        pool="spark_dbt",
        queue="dbt",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && PYTHONPATH={DBT_PLUGINS} "
            "dbt test --select tag:silver"
        ),
    )


lakehouse_dbt_tests_smoke_dag()
