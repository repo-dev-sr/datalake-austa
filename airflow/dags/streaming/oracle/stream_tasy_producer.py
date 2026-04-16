"""
DAG contínua: escuta eventos S3 (via SQS) em raw/raw_tasy/stream/ e roteia por tópico.

Desativada no Airflow — bronze passou a ser orquestrada por `master_dbt_orchestrator_batch`
+ `bronze_dbt_task_group_all`. Reative o bloco `if False` abaixo para voltar ao producer
dataset-driven.

Variable: s3_raw_tasy_sqs_queue_url
"""
if False:
    import json
    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.python import BranchPythonOperator, PythonOperator
    from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
    from datetime import datetime

    from common.constants import (
        TOPIC_DATASET_MAPPING,
        get_dataset_for_topic,
    )
    from common.default_args import DEFAULT_ARGS

    SQS_QUEUE_URL_VAR = "s3_raw_tasy_sqs_queue_url"

    def route_by_topic(**context):
        """Lê mensagens do XCom e retorna o task_id do branch conforme o path S3."""
        ti = context["ti"]
        messages = ti.xcom_pull(task_ids="wait_s3_events", key="messages") or []

        for msg in messages:
            body = msg.get("Body", "{}")
            if isinstance(body, str):
                body = json.loads(body) if body else {}
            records = body.get("Records", [])
            for record in records:
                key = record.get("s3", {}).get("object", {}).get("key", "")
                for topic in TOPIC_DATASET_MAPPING:
                    if topic in key:
                        return f"produce_dataset_{topic.replace('.', '_').replace(' ', '_')}"

        return "skip"

    def emit_dataset(topic: str):
        """Factory: retorna callable que emite o dataset do tópico."""

        def _emit(**context):
            print(f"Dataset emitido para tópico: {topic}")

        return _emit

    def create_produce_task(dag, topic: str):
        """Cria task de produce_dataset para o tópico."""
        dataset = get_dataset_for_topic(topic)
        task_id = f"produce_dataset_{topic.replace('.', '_').replace(' ', '_')}"
        return PythonOperator(
            task_id=task_id,
            python_callable=emit_dataset(topic),
            outlets=[dataset],
            dag=dag,
        )

    with DAG(
        dag_id="stream_tasy_producer",
        description="Escuta SQS (eventos S3 raw_tasy/stream) e produz datasets por tópico",
        schedule="@continuous",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={**DEFAULT_ARGS, "poke_interval": 30},
        tags=["continuous", "stream", "tasy", "s3", "sqs"],
    ) as dag:
        sqs_queue_url = Variable.get(SQS_QUEUE_URL_VAR, default_var="")

        wait_sqs = SqsSensor(
            task_id="wait_s3_events",
            sqs_queue=sqs_queue_url,
            max_messages=10,
            wait_time_seconds=20,
            delete_message_on_reception=True,
        )

        route = BranchPythonOperator(
            task_id="route_by_topic",
            python_callable=route_by_topic,
        )

        skip = PythonOperator(
            task_id="skip",
            python_callable=lambda: print("Tópico não reconhecido, ignorando."),
            dag=dag,
        )

        produce_tasks = {
            topic: create_produce_task(dag, topic)
            for topic in TOPIC_DATASET_MAPPING
        }

        wait_sqs >> route

        for topic, task in produce_tasks.items():
            route >> task

        route >> skip
