"""
DAG de compactacao dos AVROs raw por topico.

Agendada a cada 30 minutos (e disparavel pelo master batch):

1) ranking dos topicos pelo AVRO mais recente;
2) compactacao in-place da ultima hora (~100 MB por ficheiro) no mesmo prefixo S3 que a bronze consome.
"""
from datetime import timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from common.config import (
    AWS_REGION,
    DATALAKE_BUCKET,
    SPARK_HOST,
    SPARK_MASTER_URL,
    SPARK_REMOTE_COMPACTION_SCRIPT,
    SPARK_SSH_KEY_PATH,
    SPARK_SSH_USER,
)
from common.default_args import DEFAULT_ARGS

BUCKET = DATALAKE_BUCKET
INPUT_PREFIX = "raw/raw-tasy/stream/"
TARGET_SIZE_MB = 100

_SSH_OPTS = "-o BatchMode=yes -o StrictHostKeyChecking=no"
_SSH_BASE = (
    f'ssh -i {SPARK_SSH_KEY_PATH} {_SSH_OPTS} {SPARK_SSH_USER}@{SPARK_HOST} '
)
_SPARK_SUBMIT_REMOTE = "/opt/spark/bin/spark-submit"


@dag(
    dag_id="raw_tasy_avro_compactor",
    description="Ranking + compactacao in-place AVRO raw (ultima hora) para leitura bronze",
    schedule=timedelta(minutes=30),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    tags=["streaming", "raw", "avro", "compactacao", "lakehouse"],
)
def raw_tasy_avro_compactor_dag():
    # Ranking S3: roda na maquina Spark (python3 + boto3) via SSH — mesmo script que o job Spark.
    report_latest_files = BashOperator(
        task_id="report_latest_files_by_topic",
        bash_command=(
            _SSH_BASE
            + f'"python3 {SPARK_REMOTE_COMPACTION_SCRIPT} '
            + "--mode report "
            + f"--bucket {BUCKET} "
            + f"--input-prefix {INPUT_PREFIX} "
            + f'--region {AWS_REGION}"'
        ),
        queue="default",
    )

    # Compactacao: obrigatoriamente spark-submit na maquina Spark (nao no container Airflow).
    compact_last_hour = BashOperator(
        task_id="compact_last_hour",
        bash_command=(
            _SSH_BASE
            + f'"{_SPARK_SUBMIT_REMOTE} --master {SPARK_MASTER_URL} {SPARK_REMOTE_COMPACTION_SCRIPT} '
            + "--mode compact "
            + f"--bucket {BUCKET} "
            + f"--input-prefix {INPUT_PREFIX} "
            + "--in-place "
            + f"--target-size-mb {TARGET_SIZE_MB} "
            + "--execution-ts '{{ ts }}' "
            + f'--region {AWS_REGION}"'
        ),
        queue="default",
        pool="spark_pyspark",
    )

    report_latest_files >> compact_last_hour


raw_tasy_avro_compactor_dag()
