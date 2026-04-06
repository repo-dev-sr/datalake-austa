"""
DAG bronze ATEND_PACIENTE_UNIDADE: acionada pelo dataset tasy.ATEND_PACIENTE_UNIDADE.
Quando o stream_tasy_producer emite o dataset (novo Avro em raw/raw-tasy/stream/),
esta DAG executa `dbt run` do modelo bronze_tasy_atend_paciente_unidade (Bash, pool
`bronze_stream`; sem Cosmos).
"""
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.bronze_stream_dbt import (
    BRONZE_STREAM_POOL,
    bash_dbt_run_select,
    bronze_dbt_run_env,
)
from common.constants import get_dataset_for_topic
from common.default_args import DEFAULT_ARGS

TOPIC = "tasy.TASY.ATEND_PACIENTE_UNIDADE"
DATASET = get_dataset_for_topic(TOPIC)

MODEL = "bronze_tasy_atend_paciente_unidade"


@dag(
    dag_id="bronze_tasy_atend_paciente_unidade",
    description="Bronze ATEND_PACIENTE_UNIDADE (dataset → Iceberg)",
    schedule=[DATASET],
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "dbt", "tasy", "dataset", "atend_paciente_unidade", "stream"],
)
def bronze_tasy_atend_paciente_unidade_dag():
    BashOperator(
        task_id="run_bronze_tasy_atend_paciente_unidade",
        pool=BRONZE_STREAM_POOL,
        queue="default",
        env=bronze_dbt_run_env(),
        bash_command=bash_dbt_run_select(MODEL),
    )


bronze_tasy_atend_paciente_unidade_dag()
