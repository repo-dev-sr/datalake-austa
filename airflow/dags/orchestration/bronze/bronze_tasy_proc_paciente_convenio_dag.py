"""
DAG bronze PROC_PACIENTE_CONVENIO — desativada no Airflow.

Substituída por `bronze_dbt_task_group_all` (orquestrada por `master_dbt_orchestrator_batch`).
Reative o bloco `if False` abaixo para voltar ao modo dataset/stream.
"""
if False:
    from airflow.decorators import dag
    from airflow.operators.bash import BashOperator
    from airflow.utils.dates import days_ago

    from common.bronze_stream_dbt import BRONZE_STREAM_POOL, bash_dbt_run_select
    from common.constants import get_dataset_for_topic
    from common.default_args import DEFAULT_ARGS

    TOPIC = "tasy.TASY.PROC_PACIENTE_CONVENIO"
    DATASET = get_dataset_for_topic(TOPIC)

    MODEL = "bronze_tasy_proc_paciente_convenio"

    @dag(
        dag_id="bronze_tasy_proc_paciente_convenio",
        description="Bronze PROC_PACIENTE_CONVENIO (dataset → Iceberg)",
        schedule=[DATASET],
        start_date=days_ago(1),
        catchup=False,
        is_paused_upon_creation=False,
        default_args=DEFAULT_ARGS,
        tags=["bronze", "dbt", "tasy", "dataset", "proc_paciente_convenio", "stream"],
    )
    def bronze_tasy_proc_paciente_convenio_dag():
        BashOperator(
            task_id="run_bronze_tasy_proc_paciente_convenio",
            pool=BRONZE_STREAM_POOL,
            queue="default",
            bash_command=bash_dbt_run_select(MODEL),
        )

    bronze_tasy_proc_paciente_convenio_dag()
