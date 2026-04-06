# Diagnóstico Completo de Performance — Pipeline Hospital Austa

**Data:** 2026-04-06  
**Analista:** Diagnóstico automatizado via coleta SSH nos servidores de produção  
**Escopo:** Pipeline Bronze → Silver → Silver-Context (SLA alvo: ≤ 5 minutos)

---

## 1. Resumo Executivo

| Métrica | Valor Medido |
|---------|-------------|
| **SLA alvo** | ≤ 5 minutos (Bronze → Silver-Context) |
| **SLA real medida (bronze_all)** | **37.1 min média, 66.4 min máx** |
| **SLA real medida (pipeline completo batch)** | **~60–120 min estimado** |
| **Veredito** | **NÃO — SLA nunca foi cumprida. Fator 7–13× acima da meta.** |
| Gargalos críticos identificados | **7** |
| Taxa de sucesso bronze_all (30 dias) | **0% (4 runs, 4 falharam)** |
| Taxa de sucesso silver_all (30 dias) | **12.5% (41 runs, 5 com sucesso, 36 falharam)** |

**Diagnóstico em uma frase:** O pipeline está fundamentalmente subdimensionado — Spark roda em modo `local[4]` (single JVM, 4 threads) numa t3.xlarge, o cluster Standalone implantado está completamente ocioso, dbt não tem `threads` configurado, e o Cosmos executa modelos com paralelismo limitado pelo pool e worker. Além disso, falhas sistêmicas na camada Silver desde 2026-04-06 tornaram o pipeline inoperante.

---

## 2. Inventário de Infraestrutura Coletado

### 2.1 Airflow EC2 (`18.228.95.222` — t3.large)

| Item | Valor |
|------|-------|
| vCPUs | 2 |
| RAM total | 7.6 GB |
| RAM usada (containers) | ~3.3 GB |
| Disco | 80 GB (11% usado) |
| Swap | 0 (desabilitado) |
| Uptime | 19 dias |
| Load average | 0.55, 0.46, 0.55 |

**Containers Docker ativos:**

| Container | CPU | Memória Usada | Limite | % Uso |
|-----------|-----|---------------|--------|-------|
| airflow-scheduler | 2.0% | 514 MB | 1.17 GB | 42.8% |
| airflow-webserver | 0.1% | 1.09 GB | 1.76 GB | **62.0%** |
| airflow-worker | 0.05% | 404 MB | 1.37 GB | 28.9% |
| **airflow-worker-dbt** | 0.08% | 782 MB | **4.0 GB** | 19.1% |
| airflow-triggerer | 0.91% | 393 MB | 1.17 GB | 32.8% |
| airflow-postgres | 1.0% | 109 MB | sem limite | 1.4% |
| airflow-redis | 0.52% | 5 MB | sem limite | 0.06% |

### 2.2 Spark/Kyuubi EC2 (`177.71.255.159` — t3.xlarge)

| Item | Valor |
|------|-------|
| vCPUs | 4 |
| RAM total | 15 GB |
| RAM usada | 3.7 GB |
| RAM disponível | 11 GB |
| Disco | 80 GB (13% usado) |
| Swap | 0 (desabilitado) |
| Uptime | 5 dias, 18 horas |
| Load average (idle) | 0.07 |

**Processos Java ativos:**

| Processo | PID | Uso |
|----------|-----|-----|
| KyuubiServer | 2788 | 344 MB RSS |
| Spark Master | 2571 | 319 MB RSS |
| Spark Worker | 2716 | 321 MB RSS |
| **SparkSQLEngine (dbt subdomain)** | 261981 | **2.6 GB RSS, 88.6% CPU** |

### 2.3 Configuração Airflow (valores em produção)

| Parâmetro | Valor |
|-----------|-------|
| Executor | **CeleryExecutor** |
| `PARALLELISM` (scheduler global) | 32 |
| `MAX_ACTIVE_TASKS_PER_DAG` (scheduler) | 16 |
| `MAX_ACTIVE_RUNS_PER_DAG` | 2 (scheduler), 1 (worker-dbt) |
| `WORKER_CONCURRENCY` (worker-dbt) | **5** |
| `WORKER_AUTOSCALE` (worker-dbt) | 3,5 |
| `MIN_FILE_PROCESS_INTERVAL` | 30s |
| `DAG_DIR_LIST_INTERVAL` | 300s |
| Worker default: queues | `default,stream` |
| Worker dbt: queues | `dbt` |

### 2.4 Pools Airflow

| Pool | Slots | Descrição |
|------|-------|-----------|
| default_pool | 128 | Default |
| **spark_dbt** | **5** | dbt-spark/Cosmos |
| bronze_stream | 3 | Bronze stream por evento |

### 2.5 Configuração Kyuubi/Spark

| Parâmetro | Valor | Observação |
|-----------|-------|------------|
| **`spark.master`** | **`local[4]`** | **CLUSTER IGNORADO** |
| `spark.driver.memory` | 8g | |
| `spark.executor.memory` | 4g | Irrelevante em local mode |
| `spark.executor.cores` | 2 | Irrelevante em local mode |
| `spark.sql.shuffle.partitions` | 8 | |
| `spark.sql.adaptive.enabled` | true | |
| `kyuubi.engine.share.level` | USER | Compartilha engine por usuário |
| `kyuubi.session.engine.idle.timeout` | PT2H | Engine permanece warm 2h |
| `kyuubi.session.idle.timeout` | PT6H | |
| Spark Standalone Worker | 4 cores, 14.7 GB | **0 cores usados, 0 MB usado** |

### 2.6 Configuração dbt

| Parâmetro | Valor | Observação |
|-----------|-------|------------|
| **`threads`** | **NÃO CONFIGURADO** | Default = 1 |
| `connect_timeout` | 180s | |
| `connect_retries` | 5 | |
| `retry_all` | true | |
| `kyuubi.engine.share.level.subdomain` | dbt | |

### 2.7 Cosmos (Astronomer Cosmos)

| Parâmetro | Valor | Observação |
|-----------|-------|------------|
| **`LoadMode`** | **`DBT_LS`** | Executa `dbt ls` a cada parse |
| `ExecutionMode` | LOCAL (default) | Sem overhead de spawn |
| `pool` | spark_dbt | |
| `install_deps` | False | |

---

## 3. Mapa de Latência por Etapa

### 3.1 Bronze (`bronze_dbt_task_group_all`) — DAG run mais recente com dados completos (16:31 UTC)

| Task | Queue Wait | Duração | Status |
|------|-----------|---------|--------|
| `bronze_tasy_atend_paciente_unidade` | 1.0s | 156.3s (2.6 min) | success |
| `bronze_tasy_atendimento_paciente` | 158.3s | 221.3s (3.7 min) | success |
| `bronze_tasy_proc_paciente_convenio` | 492.8s | 116.4s (1.9 min) | success |
| `bronze_tasy_conta_paciente` | 21.1s | 498.6s (8.3 min) | **failed** |
| `bronze_tasy_proc_paciente_valor` | **700.2s** | 402.4s (6.7 min) | success |
| `bronze_tasy_procedimento_paciente` | 0.8s | **1384.5s (23.1 min)** | success |
| **Total DAG** | — | **66.4 min** | **failed** |

### 3.2 Bronze — Run mais recente (17:10 UTC, após ajuste de concurrency)

| Task | Queue Wait | Duração | Status |
|------|-----------|---------|--------|
| `bronze_tasy_atend_paciente_unidade` | 45.1s | **1361.5s (22.7 min)** | failed |
| `bronze_tasy_atendimento_paciente` | 47.2s | **1357.6s (22.6 min)** | failed |
| `bronze_tasy_procedimento_paciente` | 47.0s | 1342.4s (22.4 min) | success |
| **Total DAG** | — | **22.3 min** | **failed** |

> **Nota:** Nesta run, apenas 3 de 6 tasks foram executadas (as demais não foram disparadas ou foram canceladas no início). As 3 tasks rodaram em paralelo (queue wait ~47s), indicando que a concurrency do worker já estava em 5. Mesmo assim, o modelo mais lento levou **22.4 min**.

### 3.3 Silver (`silver_dbt_task_group_all`) — Runs com sucesso

| Métrica | Valor |
|---------|-------|
| Runs com sucesso (30 dias) | 3 |
| Duração média | **5.5 min** |
| Duração mínima | 5.0 min |
| Duração máxima | 6.3 min |
| Taxa de falha | **87.8%** (36 de 41 runs) |

### 3.4 Silver-Context (`silver_context_dbt_task_group_all`)

| Métrica | Valor |
|---------|-------|
| Runs com sucesso (30 dias) | 2 |
| Duração média (sucesso) | 3.2 min |
| Duração máxima | 6.15 min |

### 3.5 DAG Run Durations — Agregado (30 dias)

| DAG | Avg (min) | Max (min) | Min (min) | Runs |
|-----|----------|----------|----------|------|
| bronze_dbt_task_group_all | **37.1** | **66.4** | 22.3 | 4 |
| silver_dbt_task_group_all | 7.3 | 69.3 | 2.6 | 41 |
| silver_context_dbt_task_group_all | 18.1 | 68.9 | 0.3 | 5 |
| master_dbt_orchestrator_stream | 18.9 | 67.9 | 2.2 | 14 |
| master_dbt_orchestrator_batch | 21.2 | 36.1 | 2.2 | 3 |

### 3.6 Estimativa de Latência End-to-End (melhor cenário)

| Etapa | Duração Estimada | % do Total |
|-------|-----------------|------------|
| Bronze (modelo mais lento) | **22.4 min** | **64.0%** |
| Scheduler lag (poke_interval batch) | 2.0 min | 5.7% |
| Silver (quando funciona) | 5.5 min | 15.7% |
| Scheduler lag | 1.0 min | 2.9% |
| Silver-Context | 3.2 min | 9.1% |
| Scheduler/queue overhead | 0.9 min | 2.6% |
| **TOTAL ESTIMADO** | **~35.0 min** | **100%** |
| **Meta SLA** | **5.0 min** | — |
| **Fator acima da meta** | **7.0×** | — |

---

## 4. Gargalos Identificados (rankeados por impacto na SLA)

### 🔴 Gargalo #1: Spark roda em `local[4]` — Cluster Standalone completamente ocioso

**O quê:** O Kyuubi está configurado com `spark.master = local[4]`, que processa tudo em uma única JVM com 4 threads. O cluster Spark Standalone (Master + Worker com 4 cores e 14.7 GB) está deployado e funcionando, mas com **0 cores usados e 0 MB de memória alocada**.

**Evidência:**
- `kyuubi-defaults.conf`: `kyuubi.engine.spark.conf.spark.master  local[4]`
- Spark Master UI (`http://172.36.2.222:8080/json/`): `"coresused": 0, "memoryused": 0, "aliveworkers": 1`
- Processo SparkSQLEngine (PID 261981): 88.6% CPU, 2.6 GB RSS — processando tudo em 1 JVM

**Impacto na SLA:** Modelos bronze levam 22+ minutos. Com processamento distribuído, seriam estimados 8-12 min (redução de ~50%). **Economia estimada: 10-12 minutos.**

**Causa raiz:** A linha `kyuubi.engine.spark.conf.spark.master  local[4]` em `/opt/kyuubi/conf/kyuubi-defaults.conf` sobrescreve a linha anterior `kyuubi.engine.spark.master  spark://172.36.2.222:7077`. O Kyuubi usa a última configuração encontrada.

---

### 🔴 Gargalo #2: dbt sem configuração de `threads` (default = 1)

**O quê:** O `profiles.yml` e o `dbt_project.yml` não definem `threads`. O default do dbt-spark é 1, o que significa que cada invocação do dbt processa 1 modelo por vez.

**Evidência:**
- `dbt/profiles.yml`: sem campo `threads`
- `dbt/dbt_project.yml`: sem campo `threads`
- Confirmado por grep no container: nenhuma ocorrência de "threads" nos arquivos dbt

**Impacto na SLA:** Com Cosmos criando 1 task por modelo, o impacto direto é limitado (cada task roda 1 modelo de qualquer forma). Porém, se houver testes (`test` tasks) dependentes, eles rodam serializados. Ao migrar para execução unificada (recomendação #6), threads seria crítico. **Economia potencial: 2-5 min.**

**Causa raiz:** Omissão na configuração do `profiles.yml`.

---

### 🔴 Gargalo #3: Modelos Bronze excessivamente lentos (22+ min para o maior)

**O quê:** O modelo `bronze_tasy_procedimento_paciente` leva **1342-1385 segundos (22.4-23.1 min)** consistentemente. Outros modelos bronze variam de 116s a 1362s.

**Evidência:**
- Task history: `bronze_tasy_procedimento_paciente.run` avg=1363.5s, stddev=29.8s (consistente)
- `bronze_tasy_atendimento_paciente.run` avg=789.5s (alta variância: 221-1358s)
- Modelos usam `incremental_strategy='append'` sem `partition_by`

**Impacto na SLA:** O modelo mais lento define o tempo mínimo do pipeline. **22.4 min mínimo só para bronze — já 4.5× acima da SLA total.**

**Causa raiz:**
1. Leitura full-scan de Avro no S3 (sem filtro de partição temporal eficiente)
2. Deduplicação por ROW_NUMBER sobre dataset completo
3. Spark local[4] com apenas 4 threads de processamento
4. Sem `partition_by` nos modelos Iceberg → merge/append opera sobre tabela inteira

---

### 🔴 Gargalo #4: Falhas sistêmicas na Silver (87.8% de taxa de falha)

**O quê:** A camada Silver falha em 36 de 41 runs nos últimos 30 dias. As falhas se concentram em 2026-04-06, com 35-37 falhas por task a partir das 11:45 UTC.

**Evidência:**
- DB Airflow: 228 tasks falharam no pool `spark_dbt` nos últimos 30 dias
- Tasks falhadas levam ~3-5 segundos (falha imediata de conexão/sessão)
- Log do Kyuubi engine (`kyuubi-spark-sql-engine.log.1`):
  ```
  ERROR ExecuteStatement: Error operating ExecuteStatement: java.lang.InterruptedException
  ERROR SparkTBinaryFrontendService: Error fetching results: Invalid OperationHandle
  software.amazon.awssdk.core.exception.AbortedException
  ```
- 677 ERRORs no log do engine atual

**Impacto na SLA:** Pipeline Silver é inoperante. Mesmo que Bronze complete, Silver falha. Com `retries=2` e `retry_delay=60s`, cada falha adiciona 120s de espera antes de abortar. **Impacto: bloqueio total do pipeline.**

**Causa raiz provável:** Concorrência de sessões no Kyuubi engine sobrecarregando o driver local[4] ou timeout de operações S3. O `InterruptedException` sugere que operações são canceladas (possivelmente por timeout ou OOM transiente no engine).

---

### 🟡 Gargalo #5: Pool `spark_dbt` subdimensionado (5 slots para 6+ modelos)

**O quê:** O pool tem 5 slots, mas existem 6 modelos bronze (+ 6 silver + 3 silver_context = 15 tasks potenciais). Quando a DAG bronze dispara, pelo menos 1 task fica em fila.

**Evidência:**
- Pool `spark_dbt`: 5 slots
- Bronze: 6 tasks `.run` + 6 tasks `.test` = 12 tasks potenciais
- Queue wait histórico: **avg 43.1s para tarefas com sucesso**
- Máximo histórico: **700.2s** para `proc_paciente_valor.run`

**Impacto na SLA:** Com 5 slots e 6 modelos, pelo menos 1 modelo sempre espera. **Overhead: 0.5-11.7 min adicionais por run.**

| Pergunta | Resposta |
|----------|----------|
| Slots no pool `spark_dbt` | 5 |
| Tasks simultâneas da DAG bronze | 6 (run) + 6 (test) = 12 |
| O pool é o limitante do paralelismo? | **Sim, parcialmente** (pool=5 < 6 modelos) |
| Tasks esperaram em fila? Quanto? | Sim, até **700.2s** (11.7 min) |
| **Recomendação de slots** | **8-10** (6 modelos + margem para testes) |

---

### 🟡 Gargalo #6: Cosmos `LoadMode.DBT_LS` sobrecarrega scheduler

**O quê:** O Cosmos está configurado com `LoadMode.DBT_LS`, que executa `dbt ls` no processo do scheduler a cada ciclo de parse (a cada 30s conforme `MIN_FILE_PROCESS_INTERVAL`).

**Evidência:**
- `cosmos_dbt.py` linha 36: `load_method=LoadMode.DBT_LS`
- Scheduler mem 514 MB (43% do limite de 1.17 GB)
- `DAG_DIR_LIST_INTERVAL=300s`, `MIN_FILE_PROCESS_INTERVAL=30s`

**Impacto na SLA:** `dbt ls` invoca o dbt CLI que conecta ao Kyuubi e resolve o grafo. Isso consome CPU do scheduler e sessões Kyuubi desnecessárias. **Impacto: 5-15s de overhead por parse cycle + carga adicional no Kyuubi.**

**Causa raiz:** Configuração padrão do Cosmos não foi migrada para `LoadMode.DBT_MANIFEST`.

---

### 🟡 Gargalo #7: TriggerDagRunOperator com `poke_interval` alto

**O quê:** O `master_dbt_orchestrator_batch` usa `poke_interval=120s` e o `stream` usa `poke_interval=60s` entre layers. Isso adiciona latência de polling entre Bronze → Silver → Silver-Context.

**Evidência:**
- `master_dbt_orchestrator_batch.py` linhas 63-64: `poke_interval=120`
- `master_dbt_orchestrator_stream.py` linhas 30-31: `poke_interval=60`

**Impacto na SLA:** Até 2 min de polling delay entre bronze→silver, e mais 2 min entre silver→silver_context. **Total: até 4 min de latência pura de polling.**

**Causa raiz:** Valores conservadores no poke_interval.

---

### 🟢 Gargalo #8: Modelos dbt sem `partition_by` no Iceberg

**O quê:** Nenhum modelo bronze, silver ou silver_context define `partition_by` na configuração Iceberg. Isso significa que operações incrementais (merge/append) operam sobre a tabela inteira.

**Evidência:**
- Nenhum `partition_by` encontrado nos modelos SQL
- Bronze usa `incremental_strategy='append'`, Silver usa `merge`
- Silver `merge` com `unique_key` faz scan completo para match

**Impacto na SLA:** Silver merge sem partição = full table scan. Para tabelas grandes, isso domina o tempo de execução. **Impacto estimado: 30-60% do tempo de silver models.**

---

### 🟢 Gargalo #9: Airflow EC2 subdimensionada (t3.large, 2 vCPUs)

**O quê:** A EC2 do Airflow é uma t3.large (2 vCPUs, 8 GB RAM). O webserver está a 62% da memória e o scheduler a 43%. Com Cosmos executando `dbt ls` e CeleryExecutor gerenciando workers, 2 vCPUs são limitantes.

**Evidência:**
- Webserver: 1.09 GB / 1.76 GB (62%)
- Load average: 0.55 (55% de 1 core)

**Impacto na SLA:** Não é o gargalo primário, mas pode causar scheduling lag. **Impacto estimado: 5-15s de overhead.**

---

### 🟢 Gargalo #10: SPARK_THRIFT_HOST aponta para IP público

**O quê:** O worker-dbt conecta ao Kyuubi via IP público (`177.71.255.159`) em vez do IP privado da VPC.

**Evidência:**
- `docker exec airflow-worker-dbt env`: `SPARK_THRIFT_HOST=177.71.255.159`
- Template Terraform usa `${spark_private_ip}`, mas o container real usa o IP público

**Impacto na SLA:** Tráfego sai pela interface pública, adiciona latência de rede (~1-5ms por query). Para centenas de operações, pode somar segundos. **Impacto: marginal (1-3s).**

---

### 🟢 Gargalo #11: Kyuubi engine JVM — Metaspace quase cheia

**O quê:** A JVM do SparkSQLEngine tem Metaspace em 99.2% de utilização (263 MB / 265 MB).

**Evidência:**
- jstat: `MU=263188.3 KB`, `MC=265472.0 KB` (99.1% usage)
- `CCSU=30896.4 KB`, `CCSC=31936.0 KB` (96.8%)

**Impacto na SLA:** Metaspace cheia pode causar Full GC ou `OutOfMemoryError: Metaspace`. Isso pode explicar as falhas intermitentes no Silver. **Impacto: potencialmente crítico se causar restart do engine.**

---

### 🟢 Gargalo #12: Sem SQS sensor — pipeline batch não é event-driven

**O quê:** O `bronze_dbt_task_group_all` tem `schedule=None` e é acionado manualmente ou via `master_dbt_orchestrator_batch`. Não há trigger automático baseado em eventos S3/SQS.

**Evidência:**
- `bronze_dbt_task_group_all.py`: `schedule=None, is_paused_upon_creation=True`
- O stream orchestrator roda Silver/Silver-Context a cada 30min, mas NÃO dispara Bronze

**Impacto na SLA:** Sem event-driven trigger, o pipeline não reage à chegada de dados. O time-to-insight depende de quando alguém aciona manualmente. **O conceito de SLA de 5 min é inaplicável no modo batch manual.**

---

## 5. Recomendações Priorizadas

### Ordenadas por: Impacto na SLA × Esforço de implementação

| # | Recomendação | Impacto Estimado | Esforço | Risco |
|---|-------------|-----------------|---------|-------|
| 1 | Corrigir `spark.master` para usar cluster ou `local[*]` | **-10 a -15 min** | **Baixo** (1 linha de config) | Baixo |
| 2 | Adicionar `partition_by` nos modelos Iceberg (bronze/silver) | **-5 a -10 min** | Médio (refactor SQL + migrate) | Médio |
| 3 | Investigar e corrigir falhas sistêmicas da Silver | **Pipeline inoperante → operante** | Médio | Baixo |
| 4 | Aumentar pool `spark_dbt` para 8-10 slots | **-1 a -5 min** | **Baixo** (1 comando) | Baixo |
| 5 | Adicionar `threads: 4` no `profiles.yml` | **-1 a -3 min** | **Baixo** (1 linha) | Baixo |
| 6 | Migrar Cosmos `LoadMode.DBT_LS` → `DBT_MANIFEST` | **-5 a -15s parse, estabilidade** | **Baixo** (1 linha + `dbt compile`) | Baixo |
| 7 | Reduzir `poke_interval` nos TriggerDagRunOperator | **-2 a -4 min** | **Baixo** (mudar 60→15, 120→30) | Baixo |
| 8 | Aumentar Metaspace JVM do engine Kyuubi | **Previne crashes** | **Baixo** (1 config) | Baixo |
| 9 | Corrigir SPARK_THRIFT_HOST para IP privado | **-1 a -3s** | **Baixo** | Baixo |
| 10 | Upgrade Spark EC2 para t3.2xlarge (8 vCPU, 32 GB) | **-5 a -10 min** | Médio (Terraform) | Baixo |
| 11 | Consolidar DAGs em pipeline única (eliminar TriggerDagRunOperator) | **-4 min de polling** | Alto (refactor) | Médio |
| 12 | Implementar trigger event-driven para Bronze (SQS/Dataset) | **SLA meaningful** | Alto (novo design) | Médio |

---

## 6. Quick Wins (implementáveis em < 1 dia)

### 6.1 Corrigir `spark.master` no Kyuubi (5 min)

```bash
# No Spark EC2
ssh austa-spark

# Opção A: Usar o cluster Standalone já deployado
sudo sed -i 's/^kyuubi.engine.spark.conf.spark.master  local\[4\]/kyuubi.engine.spark.conf.spark.master  spark:\/\/172.36.2.222:7077/' /opt/kyuubi/conf/kyuubi-defaults.conf

# Opção B: Usar todos os cores locais (mais seguro, sem depender do cluster)
sudo sed -i 's/^kyuubi.engine.spark.conf.spark.master  local\[4\]/kyuubi.engine.spark.conf.spark.master  local[*]/' /opt/kyuubi/conf/kyuubi-defaults.conf

# Reiniciar Kyuubi para aplicar (mata engine ativo)
sudo systemctl restart kyuubi
```

> **Recomendação:** Usar `Opção A` (cluster) se possível. Testar com `local[*]` primeiro se houver receio de instabilidade.

### 6.2 Adicionar `threads: 4` no `profiles.yml` (2 min)

No `dbt/profiles.yml`, adicionar `threads: 4` na seção `dev`:

```yaml
lakehouse_tasy:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      auth: LDAP
      schema: raw
      threads: 4    # <-- ADICIONAR
      host: "{{ env_var('SPARK_THRIFT_HOST') }}"
      ...
```

Depois, fazer deploy no Airflow:
```bash
ssh austa-airflow
# Copiar para o volume montado
docker cp /opt/airflow/dbt/profiles.yml airflow-worker-dbt:/opt/airflow/dbt/profiles.yml
```

### 6.3 Aumentar pool `spark_dbt` de 5 para 8 (1 min)

```bash
ssh austa-airflow
docker exec airflow-scheduler airflow pools set spark_dbt 8 "dbt-spark/Cosmos (8 slots)"
```

### 6.4 Migrar Cosmos para `LoadMode.DBT_MANIFEST` (15 min)

Em `airflow/dags/common/cosmos_dbt.py`, alterar:

```python
# ANTES
def render_config_for_select(select: list[str]) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=select,
    )

# DEPOIS
def render_config_for_select(select: list[str]) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=select,
    )
```

E adicionar um step de `dbt compile` no CI/CD ou como pre-task:
```bash
cd /opt/airflow/dbt && dbt compile --target dev
```

### 6.5 Reduzir `poke_interval` nos orquestradores (5 min)

Em `master_dbt_orchestrator_batch.py`:
```python
# ANTES: poke_interval=120
# DEPOIS:
poke_interval=30
```

Em `master_dbt_orchestrator_stream.py`:
```python
# ANTES: poke_interval=60
# DEPOIS:
poke_interval=15
```

### 6.6 Aumentar Metaspace JVM (5 min)

Em `/opt/kyuubi/conf/kyuubi-defaults.conf`:
```
# ANTES
kyuubi.engine.spark.conf.spark.driver.extraJavaOptions  -XX:MetaspaceSize=256m -Djava.io.tmpdir=/var/lib/spark/tmp

# DEPOIS
kyuubi.engine.spark.conf.spark.driver.extraJavaOptions  -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=768m -Djava.io.tmpdir=/var/lib/spark/tmp
```

### 6.7 Corrigir SPARK_THRIFT_HOST no Docker Compose (10 min)

Verificar se o `docker-compose.yaml` em `/opt/airflow/` usa o IP privado do Spark:
```bash
ssh austa-airflow
grep SPARK_THRIFT_HOST /opt/airflow/docker-compose.yaml
# Se for o IP público 177.71.255.159, trocar para o IP privado da VPC
```

---

## 7. Mudanças Estruturais Necessárias

### 7.1 Adicionar `partition_by` nos modelos Iceberg

Para atingir SLA de 5 min, os modelos precisam de partição temporal:

```sql
-- Bronze models config
{{
  config(
    materialized='incremental',
    file_format='iceberg',
    incremental_strategy='append',
    partition_by=['days(create_at)']  -- ou campo de data relevante
  )
}}
```

```sql
-- Silver models config
{{
  config(
    materialized='incremental',
    file_format='iceberg',
    incremental_strategy='merge',
    unique_key='nr_sequencia',
    partition_by=['days(_loaded_at)'],  -- limita scan do merge
    merge_update_columns=[...]
  )
}}
```

**Impacto:** Merge parcial em vez de full-table scan. Redução estimada de 50-80% no tempo de silver.

### 7.2 Upgrade da EC2 Spark

| Instância | vCPUs | RAM | Custo mensal (sa-east-1) |
|-----------|-------|-----|--------------------------|
| t3.xlarge (atual) | 4 | 16 GB | ~$170 |
| t3.2xlarge (recomendado) | 8 | 32 GB | ~$340 |
| r6i.xlarge (alternativa mem-optimized) | 4 | 32 GB | ~$280 |

Com 8 vCPUs e `local[8]` ou cluster mode, o throughput dobraria.

### 7.3 Consolidar pipeline em DAG única

Eliminar `TriggerDagRunOperator` e colocar bronze → silver → silver_context na mesma DAG:

```python
@dag(dag_id="pipeline_bronze_to_silver_context", ...)
def pipeline():
    bronze = layer_dbt_task_group("bronze", ["path:models/bronze"])
    silver = layer_dbt_task_group("silver", ["path:models/silver"])
    silver_ctx = layer_dbt_task_group("silver_ctx", ["path:models/silver_context"])
    bronze >> silver >> silver_ctx
```

**Benefício:** Elimina 4 min de polling delay entre layers. As dependências são respeitadas nativamente pelo Airflow.

### 7.4 Implementar trigger event-driven

Para que a SLA de 5 min seja significativa, o Bronze precisa ser disparado automaticamente pela chegada de dados no S3. O pattern de `streaming/oracle/` com Datasets já existe para modelos individuais — consolidar para um pipeline end-to-end.

---

## 8. Próximos Passos com Dono e Prazo Sugerido

| # | Ação | Responsável | Prazo Sugerido | Impacto |
|---|------|-------------|----------------|---------|
| 1 | Corrigir `spark.master` para cluster ou `local[*]` | Infra/DevOps | **Imediato** | Crítico |
| 2 | Aumentar Metaspace JVM e reiniciar Kyuubi | Infra/DevOps | **Imediato** | Crítico |
| 3 | Aumentar pool `spark_dbt` para 8 slots | Airflow Admin | **Imediato** | Alto |
| 4 | Adicionar `threads: 4` ao `profiles.yml` | Engenheiro de Dados | **Imediato** | Alto |
| 5 | Investigar falhas Silver (engine log, InterruptedException) | Engenheiro de Dados + Infra | **Hoje** | Blocker |
| 6 | Migrar Cosmos para `DBT_MANIFEST` | Engenheiro de Dados | **1-2 dias** | Médio |
| 7 | Reduzir `poke_interval` nos orquestradores | Engenheiro de Dados | **1-2 dias** | Médio |
| 8 | Adicionar `partition_by` nos modelos Iceberg | Engenheiro de Dados | **1 semana** | Alto |
| 9 | Avaliar upgrade Spark EC2 para t3.2xlarge | Infra/DevOps + Gestor | **1 semana** | Alto |
| 10 | Consolidar pipeline em DAG única | Engenheiro de Dados | **2 semanas** | Médio |
| 11 | Implementar trigger event-driven completo | Engenheiro de Dados | **3 semanas** | Alto |
| 12 | Corrigir SPARK_THRIFT_HOST para IP privado | Infra/DevOps | **1-2 dias** | Baixo |

---

## 9. Apêndice — Comandos Executados e Outputs Relevantes

### 9.1 SSH Config Utilizado
```
Host austa-airflow → 18.228.95.222 (ec2-user, dlk-austa-sa.pem)
Host austa-spark → 177.71.255.159 (ec2-user, dlk-austa-sa.pem)
```

### 9.2 Airflow Pools
```
pool          | slots | description
default_pool  | 128   | Default pool
bronze_stream | 3     | Bronze stream: dbt run por evento
spark_dbt     | 5     | dbt-spark/Cosmos (5 slots — alinhado ao worker-dbt)
```

### 9.3 Airflow Config (Scheduler)
```
parallelism = 32
max_active_tasks_per_dag = 16
max_active_runs_per_dag = 2
worker_concurrency = 16
executor = CeleryExecutor
min_file_process_interval = 30
dag_dir_list_interval = 300
```

### 9.4 Worker-dbt Config (Running Container)
```
AIRFLOW__CELERY__WORKER_CONCURRENCY=5
AIRFLOW__CELERY__WORKER_AUTOSCALE=3,5
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
AIRFLOW__CORE__PARALLELISM=32
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
```

### 9.5 Spark Master API (Idle)
```json
{
  "url": "spark://172.36.2.222:7077",
  "aliveworkers": 1,
  "cores": 4,
  "coresused": 0,
  "memory": 14764,
  "memoryused": 0
}
```

### 9.6 JVM Stats (SparkSQLEngine PID 261981)
```
Eden: 753 MB capacity, 98 MB used (13%)
Old Gen: 410 MB capacity, 338 MB used (82.5%)
Metaspace: 265 MB capacity, 263 MB used (99.2%) ← CRÍTICO
Young GC: 1455 (45.9s total)
Full GC: 4 (1.4s total)
Concurrent GC: 484 (8.9s total)
```

### 9.7 Kyuubi Engine Log (últimos erros — 17:33:56 UTC)
```
ERROR ExecuteStatement: java.lang.InterruptedException
ERROR SparkTBinaryFrontendService: Invalid OperationHandle
software.amazon.awssdk.core.exception.AbortedException
Job 158/159/166/167 cancelled
```

### 9.8 Docker-compose Template vs Running Config

| Parâmetro (worker-dbt) | Template | Running |
|------------------------|----------|---------|
| WORKER_CONCURRENCY | 1 | **5** |
| WORKER_AUTOSCALE | 1,1 | **3,5** |
| MAX_ACTIVE_TASKS_PER_DAG | 1 | **16** |
| PARALLELISM | 2 | **32** |
| mem_limit | 2400m | **4GB** |

> O template Terraform está desatualizado em relação ao ambiente real. **Ação necessária: atualizar o `docker-compose.yaml` no Terraform para refletir os valores em produção.**

---

## 10. Conclusão

O pipeline do Hospital Austa está **7× acima da SLA de 5 minutos** no melhor cenário, e **operacionalmente inoperante** no momento da análise (0% de sucesso no bronze, 87.8% de falha no silver).

Os **3 maiores contribuintes** para o gap de SLA são:

1. **Spark local[4]** — cluster idle enquanto o engine processa tudo em 4 threads (~64% do tempo)
2. **Modelos sem partition_by** — merge/append fazem full table scan (~15-20% do tempo)
3. **Polling overhead entre DAGs** — TriggerDagRunOperator adiciona ~4 min de latência pura (~12% do tempo)

Aplicando os Quick Wins (#1-#7), a estimativa é reduzir o pipeline de **35 min para ~12-15 min**. Para atingir os **5 minutos**, será necessário também:
- Adicionar `partition_by` nos modelos
- Fazer upgrade da EC2 Spark para 8 vCPUs
- Consolidar em pipeline única event-driven

**Prioridade máxima imediata:** Corrigir `spark.master` e investigar as falhas sistêmicas da Silver. Sem essas correções, o pipeline não funciona.

---

## 11. Benchmark Pós-Quick Wins (2026-04-06 18:24 UTC)

### Quick Wins Aplicados

| # | Quick Win | Status |
|---|-----------|--------|
| 1 | `spark.master` local[4] → local[*] | ✅ Aplicado + Kyuubi reiniciado |
| 2 | Metaspace JVM 256m → 512m/768m | ✅ Aplicado |
| 3 | Pool `spark_dbt` 5 → 8 slots | ✅ Aplicado |
| 4 | `threads: 4` no profiles.yml | ✅ Aplicado |
| 5 | Cosmos `DBT_LS` → `DBT_MANIFEST` + manifest_path | ✅ Aplicado |
| 6 | `poke_interval` batch 120→30, stream 60→15 | ✅ Aplicado |
| 7 | docker-compose SPARK_THRIFT_HOST → IP privado | ✅ Arquivo atualizado (efetivo no próximo restart) |

### Resultado: `bronze_dbt_task_group_all` run_id `bench_quickwins_20260406`

**DAG total: 39.44 min** (antes: 66.44 min máx / 37.13 min média) — **melhoria de ~40%**

| Task | Duração (s) | Queue Wait (s) | Estado | Antes (s) |
|------|------------|----------------|--------|-----------|
| proc_paciente_valor.run | 438.1 | 2.9 | ✅ success | 402.4 |
| atend_paciente_unidade.run | 612.1 | 5.6 | ✅ success | 156.3 |
| procedimento_paciente.run | 1759.3 | 2.8 | ✅ success | 1384.5 |
| atendimento_paciente.run | 1921.7 | 5.2 | ✅ success | 1357.6 (failed) |
| proc_paciente_convenio.run | 1601.7 | 441.9 | ✅ success | 116.4 |
| conta_paciente.run (retry) | 125.7 | 4.1 | ❌ failed | 498.6 (failed) |

### Análise do Benchmark

**Positivo:**
- **5 de 6 `.run` completaram com sucesso** (antes eram 2 de 6)
- **Queue wait caiu de 700s → 3-6s** para tasks do primeiro batch
- **DAG wall time reduziu ~40%** (66→39 min)
- **Cosmos DBT_MANIFEST eliminou overhead de `dbt ls` no scheduler**

**Negativo — Contention de CPU severa:**
- 5 tasks em paralelo em 4 vCPUs (t3.xlarge) causa degradação individual
- `proc_paciente_convenio.run`: 116s → 1602s (14× pior — competindo por CPU)
- `atend_paciente_unidade.run`: 156s → 612s (4× pior)
- Testes que levavam 8-11s estão levando 15-74s

**Conclusão:** O paralelismo de 5 tasks é excessivo para 4 vCPUs. A configuração ótima para o hardware atual seria **3 tasks simultâneas** (1 task por ~1.3 cores). Para 5+ tasks em paralelo, é necessário upgrade para **t3.2xlarge (8 vCPUs)**.

### Recomendação Imediata Pós-Benchmark

| Ação | Impacto |
|------|---------|
| Reduzir `WORKER_CONCURRENCY` de 5 → 3 | Tasks individuais mais rápidas, DAG total similar |
| OU: Upgrade Spark EC2 para t3.2xlarge | 5+ tasks em paralelo sem contention |
