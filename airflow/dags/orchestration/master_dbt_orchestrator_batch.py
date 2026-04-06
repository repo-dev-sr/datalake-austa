"""
Orquestrador operacional (agendado): a cada hora processa o lakehouse na ordem canônica.

Fluxo padrão (sem config): bronze (todos) → silver → silver_context [→ gold quando ativado].
Executa `dbt run` por camada nesta própria DAG (sem TriggerDagRun), evitando DagNotFound se as
DAGs Cosmos (`*_dbt_task_group_all`) estiverem quebradas ou ausentes no parse.

Opcional: passo extra de `dbt run` com --vars antes do bronze (reprocesso / janela CDC).
"""
from datetime import timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from common.bronze_stream_dbt import bronze_dbt_run_env
from common.config import DBT_PROFILE_NAME, DBT_PROJECT_DIR, DBT_TARGET
from common.default_args import DEFAULT_ARGS


def _truthy_run_cli_first(conf: dict, params: dict) -> bool:
    """True só se run_cli_first vier explicitamente como verdadeiro (conf ou params)."""
    for src in (conf, params):
        v = src.get("run_cli_first") if isinstance(src, dict) else None
        if v is True:
            return True
        if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes", "on"):
            return True
    return False


def _pick_cli_branch(**context):
    conf = context["dag_run"].conf
    if conf is None:
        conf = {}
    if not isinstance(conf, dict):
        conf = {}
    params = context.get("params") or {}
    if _truthy_run_cli_first(conf, params):
        return "dbt_run_with_vars"
    return "skip_cli_before_triggers"


_DBT_CLI_PREFIX = (
    f"cd {DBT_PROJECT_DIR} && dbt run "
    f"--profile {DBT_PROFILE_NAME} --target {DBT_TARGET} "
)


def _dbt_run_layer_bash(select_path: str) -> str:
    """Comando único dbt run para uma camada (path:models/...)."""
    return (
        f"cd {DBT_PROJECT_DIR} && dbt run --select {select_path} "
        f"--profile {DBT_PROFILE_NAME} --target {DBT_TARGET}"
    )


_MASTER_DOC_MD = """
## Execução normal (rotina)

- **Trigger manual sem JSON** ou com `{}` → **bronze → silver → silver_context** via `dbt run` nesta DAG (sem depender de outras DAGs).
- Não é obrigatório preencher nada no formulário de parâmetros.
- A task `skip_cli_before_triggers` **só pula o passo opcional** `dbt run` com `--vars`; **não** pula as camadas principais.

## Reprocessamento / janela CDC (opcional)

1. Marque **`run_cli_first`** = `True` (ou no JSON de conf).
2. Ajuste **`cdc_lookback_hours`** / **`cdc_reprocess_hours`** e, se precisar, **`dbt_select`** (default: `path:models/bronze`).

```json
{
  "run_cli_first": true,
  "cdc_lookback_hours": 48,
  "cdc_reprocess_hours": 24,
  "dbt_select": "path:models/bronze"
}
```

## DAGs Cosmos (`bronze_dbt_task_group_all`, etc.)

Continuam disponíveis para execução manual com **uma task por modelo** na UI, quando o parse (Cosmos/dbt) estiver saudável. O master **não** as dispara.
"""


@dag(
    dag_id="master_dbt_orchestrator_batch",
    description="Lakehouse: dbt bronze → silver → silver_context (1h ou manual); sem trigger de DAGs filhas",
    schedule=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    doc_md=_MASTER_DOC_MD,
    params={
        "run_cli_first": Param(
            False,
            type="boolean",
            title="Rodar dbt CLI antes do bronze",
            description=(
                "Desligado = vai direto ao `dbt run` da camada bronze (e depois silver/silver_context). "
                "Ligado = um `dbt run` extra com --vars (CDC) antes do bronze."
            ),
        ),
        "dbt_select": Param(
            "path:models/bronze",
            type="string",
            title="dbt --select (só com CLI opcional)",
            description="Usado apenas quando 'Rodar dbt CLI antes do bronze' está ligado.",
        ),
        "cdc_lookback_hours": Param(
            2,
            type="integer",
            title="cdc_lookback_hours",
            description="Só afeta o passo CLI opcional com --vars.",
        ),
        "cdc_reprocess_hours": Param(
            0,
            type="integer",
            title="cdc_reprocess_hours",
            description="Só afeta o passo CLI opcional com --vars.",
        ),
    },
    tags=["orchestrator", "batch", "lakehouse", "dbt", "hourly", "scheduled"],
)
def master_dbt_orchestrator_batch_dag():
    branch = BranchPythonOperator(
        task_id="branch_cli_or_skip",
        python_callable=_pick_cli_branch,
    )
    skip_cli = EmptyOperator(task_id="skip_cli_before_triggers")
    dbt_run_with_vars = BashOperator(
        task_id="dbt_run_with_vars",
        pool="spark_dbt",
        queue="dbt",
        env=bronze_dbt_run_env(),
        bash_command=(
            _DBT_CLI_PREFIX
            + "--select '{{ params.dbt_select or \"path:models/bronze\" }}' "
            + "--vars '{{ dict(cdc_lookback_hours=params.cdc_lookback_hours, "
            + "cdc_reprocess_hours=params.cdc_reprocess_hours) | tojson }}'"
        ),
    )

    dbt_bronze_layer = BashOperator(
        task_id="dbt_bronze_layer",
        pool="spark_dbt",
        queue="dbt",
        env=bronze_dbt_run_env(),
        bash_command=_dbt_run_layer_bash("path:models/bronze"),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    dbt_silver_layer = BashOperator(
        task_id="dbt_silver_layer",
        pool="spark_dbt",
        queue="dbt",
        env=bronze_dbt_run_env(),
        bash_command=_dbt_run_layer_bash("path:models/silver"),
    )
    dbt_silver_context_layer = BashOperator(
        task_id="dbt_silver_context_layer",
        pool="spark_dbt",
        queue="dbt",
        env=bronze_dbt_run_env(),
        bash_command=_dbt_run_layer_bash("path:models/silver_context"),
    )

    branch >> [skip_cli, dbt_run_with_vars]
    skip_cli >> dbt_bronze_layer
    dbt_run_with_vars >> dbt_bronze_layer
    dbt_bronze_layer >> dbt_silver_layer >> dbt_silver_context_layer

    # GOLD_LAYER_TODO — quando gold existir:
    # dbt_gold_layer = BashOperator(
    #     task_id="dbt_gold_layer",
    #     pool="spark_dbt",
    #     queue="dbt",
    #     env=bronze_dbt_run_env(),
    #     bash_command=_dbt_run_layer_bash("path:models/gold"),
    # )
    # dbt_silver_context_layer >> dbt_gold_layer


master_dbt_orchestrator_batch_dag()
