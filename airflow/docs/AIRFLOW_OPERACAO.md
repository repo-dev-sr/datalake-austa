# Airflow — Operação e infraestrutura (lakehouse Austa)

Documento para **quem mantém** o ambiente: pools, filas Celery, workers Docker e onde alterar cada coisa. Índice geral das DAGs: `README.md` na pasta `dags/`.

## 1. Visão geral da arquitetura (EC2)

- **Um host EC2** executa **Docker Compose** com: Postgres (metadados Airflow), Redis (broker Celery), Webserver, Scheduler, Triggerer, e **dois workers Celery**.
- **Spark / Kyuubi** rodam **fora** deste host; o Airflow apenas orquestra **dbt-spark** (cliente + envio de SQL).

```
Scheduler / Webserver / Triggerer
        │
        ▼
    Redis (filas Celery)
        │
        ├── fila `default` + `stream`  → container `airflow-worker`
        └── fila `dbt`                   → container `airflow-worker-dbt` (mais RAM)
```

## 2. Filas Celery e workers

| Container | Comando (resumo) | Filas escutadas | Memória (referência compose) |
|-----------|------------------|-----------------|------------------------------|
| `airflow-worker` | `celery worker -q default,stream` | `default`, `stream` | ~1,4 GiB |
| `airflow-worker-dbt` | `celery worker -q dbt` | `dbt` | ~4 GiB |

**Regra geral:** Cosmos / camadas (`silver`, `silver_context`, `bronze_dbt_task_group_all`) → **`queue="dbt"`** no worker com mais RAM.

**Exceção (latência):** bronzes **por evento** (`bronze_tasy_*` com dataset) usam **`BashOperator`** + **`queue="default"`** + pool **`bronze_stream`** — um só `dbt run` por DAG, sem Cosmos (sem `dbt ls` no parse nem task `.test`), até **3 em paralelo** (slots do pool), sem competir com **`spark_dbt`**.

**Onde está no código**

- Cosmos: `common/cosmos_dbt.py` → `pool` + `queue` para camadas.
- Bronze stream: `common/bronze_stream_dbt.py` + DAGs `bronze_tasy_*`.
- Smoke dbt: `observability/lakehouse_dbt_tests_smoke_dag.py`.
- Batch opcional CLI: `orchestration/master_dbt_orchestrator_batch.py` → `dbt_run_with_vars`.

Tasks leves (triggers, Python, sensors, branch) usam a fila **default** (implícita).

## 3. Pools

Pools limitam **quantas tasks com o mesmo pool** podem estar **rodando ao mesmo tempo** no cluster (além da concorrência Celery).

| Pool | Slots (padrão init) | Uso |
|------|----------------------|-----|
| `default_pool` | 128 (padrão Airflow) | Tasks sem `pool` explícito ou explícito `default_pool`. |
| `bronze_stream` | **3** | Bronzes **por dataset** (`bronze_tasy_*`): `dbt run` Bash na fila **default**; paralelismo limitado para não martelar Kyuubi/RAM do `airflow-worker`. |
| `spark_dbt` | **1** | Cosmos (silver, silver_context, `bronze_dbt_task_group_all`) + smoke + bash batch opcional. Não partilha slots com `bronze_stream`. |

**Removido / obsoleto**

- `dbt_bronze_pool`: existia no metadado mas **não era referenciada** no código. O `airflow-init` do Compose passa a executar `airflow pools delete dbt_bronze_pool` (ignora erro se já não existir).

**Ajustar slots** (ex.: subir para 2 após aumentar RAM do `airflow-worker-dbt`):

```bash
docker exec airflow-scheduler airflow pools set spark_dbt 2 'dbt-spark/Cosmos'
```

Atualize também o comando em `docker-compose.yaml` (serviço `airflow-init`) para novos ambientes.

## 4. Onde fica cada ficheiro importante

| Ficheiro | Conteúdo relevante |
|----------|-------------------|
| `/opt/airflow/docker-compose.yaml` | Limites de memória, workers, **comando `airflow-init`** (migrate, user, pools). |
| `dags/common/cosmos_dbt.py` | `pool` + `queue` dos operadores Cosmos (camadas). |
| `dags/common/bronze_stream_dbt.py` | Constantes/env para bronzes por evento (Bash). |
| `dags/README.md` | Mapa de DAGs e fluxo lakehouse. |

**Repositório Git:** cópia canónica também em **`airflow/docs/AIRFLOW_OPERACAO.md`**. Este ficheiro em **`dags/docs/`** é incluído no rsync de DAGs para o EC2.

## 5. DAGs e consumo de pool / fila (resumo)

| Área | DAGs (exemplos) | Pool / fila |
|------|-----------------|-------------|
| Bronze por evento | `bronze_tasy_*` | **`bronze_stream`** + fila **`default`** (Bash `dbt run`) |
| Bronze/silver camadas (Cosmos) | `bronze_dbt_task_group_all`, `silver_*`, `gold` (ativo) | `spark_dbt` + `dbt` (`cosmos_dbt`) |
| Smoke dbt | `lakehouse_dbt_tests_smoke` | `spark_dbt` + `dbt` |
| Batch CLI dbt | `master_dbt_orchestrator_batch` → `dbt_run_with_vars` | `spark_dbt` + `dbt` |
| Orquestradores (só triggers) | `master_dbt_orchestrator_stream`, resto do batch | `default` |
| Streaming | `stream_tasy_producer` | `default` |

## 6. Comandos úteis (manutenção)

```bash
docker exec airflow-scheduler airflow pools list
docker exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
```

## 7. Decisões de desenho (histórico curto)

- **`spark_dbt` com 1 slot:** após **SIGKILL (-9)** com vários Cosmos em paralelo no worker dbt.
- **Fila `dbt` dedicada:** Cosmos / batch pesado no `airflow-worker-dbt`.
- **Bronze stream fora do Cosmos:** evita fila única com silver e parse `dbt ls` por DAG; trade-off: vários `dbt run` no **`airflow-worker`** — se houver OOM, baixar slots de **`bronze_stream`** (ex.: 2) ou subir RAM do worker default.

Para escalar: **RAM** dos workers, depois ajustar slots **`bronze_stream`** e **`spark_dbt`** com monitorização Kyuubi.
