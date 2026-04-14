"""
DAG bronze PROCEDIMENTO_PACIENTE — desativada no Airflow.

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

    TOPIC = "tasy.TASY.PROCEDIMENTO_PACIENTE"
    DATASET = get_dataset_for_topic(TOPIC)

    MODEL = "bronze_tasy_procedimento_paciente"

    @dag(
        dag_id="bronze_tasy_procedimento_paciente",
        description="Bronze PROCEDIMENTO_PACIENTE (dataset → Iceberg)",
        schedule=[DATASET],
        start_date=days_ago(1),
        catchup=False,
        is_paused_upon_creation=False,
        default_args=DEFAULT_ARGS,
        tags=["bronze", "dbt", "tasy", "dataset", "procedimento_paciente", "stream"],
    )
    def bronze_tasy_procedimento_paciente_dag():
        BashOperator(
            task_id="run_bronze_tasy_procedimento_paciente",
            pool=BRONZE_STREAM_POOL,
            queue="default",
            bash_command=bash_dbt_run_select(MODEL),
        )

    bronze_tasy_procedimento_paciente_dag()
