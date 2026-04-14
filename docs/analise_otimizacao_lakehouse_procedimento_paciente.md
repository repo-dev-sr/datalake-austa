# Análise de Otimização: Pipeline procedimento_paciente no mesmo EC2

Data: 2026-04-09
Autor: Análise automatizada do lakehouse
Escopo: bronze → silver → silver_context (procedimento_paciente)
Máquina: EC2 `t3.xlarge` (4 vCPU, 16 GiB RAM, Intel Xeon 8259CL 2.50GHz, 80 GB NVMe)

---

## 1. Inventário do Problema (Estado Atual)

### 1.1 Dados Raw

| Métrica | Valor |
|---|---|
| Arquivos Avro no S3 | **4.957** |
| Tamanho total | **7,34 GB** |
| Tamanho médio por arquivo | **~1,5 MB** |
| Particionamento | `year/month/day/hour` |
| Colunas na tabela Tasy (PROCEDIMENTO_PACIENTE) | **~230** |

### 1.2 Configuração Spark/Kyuubi Atual

| Parâmetro | Kyuubi (dbt) | spark-defaults (PySpark) |
|---|---|---|
| `spark.master` | `local[*]` (4 cores) | `local[4]` |
| `spark.driver.memory` | 8g | 8g |
| `spark.executor.memory` | 4g | 4g |
| `spark.sql.shuffle.partitions` | 8 | 8 |
| `spark.sql.adaptive.enabled` | true | true |
| `spark.sql.adaptive.skewJoin.enabled` | true | - |
| `spark.sql.parquet.enableVectorizedReader` | **false** | **false** |
| Metaspace | 512m init / 768m max | 256m |

### 1.3 Tempos Observados no Benchmark

| Pipeline | Bronze | Silver | Silver Context | Total |
|---|---:|---:|---:|---:|
| dbt + Kyuubi | >= 296s | n/d | n/d | >= 296s |
| PySpark (local[4]) | >= 2540s | n/d | n/d | >= 2540s |
| PySpark (2 workers × 2 cores, bronze-only otimizado) | ~1170s | n/d | n/d | ~1170s |

**SLA alvo: 300s (5 min) para bronze → silver_context completo.**

---

## 2. Diagnóstico: Onde Está o Tempo?

### 🔴 Gargalo #1 — Small Files Problem (CRÍTICO)

**4.957 arquivos Avro com média de 1,5 MB cada.**

O Spark cria (no mínimo) 1 task por arquivo. Com 4 cores, são ~1.240 "rodadas" de tasks, cada uma com overhead de:
- HTTP HEAD + GET no S3 (~50-150ms por arquivo)
- Deserialização do schema Avro por arquivo
- Scheduling overhead no driver

**Impacto estimado:** só o overhead de I/O S3 já consome entre **4-12 minutos** (4957 × 80ms = ~6,6 min). Antes de qualquer transformação.

**Comparação:** Se fossem ~50 arquivos de 150 MB, o overhead cairia para ~4s.

### 🔴 Gargalo #2 — `MD5(TO_JSON(STRUCT(230 colunas)))` na Bronze (CRÍTICO)

A macro `bronze_audit_columns` computa:

```sql
MD5(TO_JSON(STRUCT(nr_atendimento, dt_entrada_unidade, cd_procedimento, ...230 colunas...)))
```

Isso significa que **para cada linha** do CDC:
1. Monta um STRUCT com 230 campos
2. Serializa para JSON (~2-5 KB de string por linha)
3. Calcula MD5 sobre a string

**Para um batch incremental de 50K linhas**, são ~125-250 MB de strings JSON geradas e hashadas. Para um full scan (~milhões de linhas), isso explode em CPU e GC pressure.

### 🟠 Gargalo #3 — Tripla Aplicação de Macros (ALTO)

Os macros de padronização (`fill_null_bigint`, `standardize_date`, `normalize_decimal`, `standardize_text_initcap`, `standardize_enum`, `standardize_documents`) são aplicados **3 vezes** nas mesmas colunas:

| Camada | Onde aplica | Colunas afetadas |
|---|---|---|
| Bronze | `MD5(TO_JSON(STRUCT(...)))` computa implicitamente sobre os raw values | ~230 colunas (no hash) |
| Silver | CTE `shaped` aplica todos os macros explicitamente | ~230 colunas |
| Silver Context | SELECT final reaaplica os macros nas colunas que JÁ estão limpas | ~230 colunas |

**A silver_context está fazendo `COALESCE(valor_que_já_é_not_null, -1)` e `CASE WHEN dt IS NULL THEN '1900-01-01' ELSE dt END` em datas que já são `'1900-01-01'`.** Isso é 100% de CPU desperdiçada nessa camada.

### 🟠 Gargalo #4 — 230 Colunas Carregadas Quando Só ~30 São Usadas (ALTO)

A `fct_producao_medica` (Gold) consome do `silver_context.procedimento` apenas:

```
nr_sequencia, nr_atendimento, cd_procedimento, dt_procedimento, qt_procedimento,
vl_procedimento, vl_medico, vl_anestesista, vl_materiais, vl_auxiliares,
vl_custo_operacional, vl_repasse_calc, cd_medico, cd_medico_executor, cd_convenio,
cd_pessoa_fisica, cd_categoria, cd_especialidade, cd_setor_atendimento, cd_doenca_cid,
nr_minuto_duracao, atend_ie_tipo_atendimento, atend_ie_clinica, ie_funcao_medico,
ie_origem_proced
```

São **~25 colunas** de ~230 totais. O lakehouse carrega, transforma e escreve **205 colunas que ninguém consome**.

**Impacto:**
- Avro é row-based → full row read obrigatório (não há column pruning)
- O MERGE da Silver gera um UPDATE SET com 230+ colunas
- A escrita Iceberg/Parquet das 230 colunas consome I/O e memória

### 🟡 Gargalo #5 — Silver Context `materialized='table'` (MÉDIO)

O modelo `procedimento.sql` na silver_context faz full rebuild (`table`) a cada execução:
- Lê **todos** os registros da silver
- Faz dois LEFT JOINs (proc_paciente_convenio + atendimento_paciente)
- Escreve tudo de novo

Para uma base crescente, isso escala linearmente.

### 🟡 Gargalo #6 — `enableVectorizedReader = false` (MÉDIO)

O vectorized reader do Parquet está **desabilitado** tanto no Kyuubi quanto no spark-defaults. Isso afeta a leitura de tabelas Iceberg (silver, silver_context) que internamente usam Parquet.

O vectorized reader pode ser **2-5x mais rápido** em scans de colunas numéricas/dates.

### 🟡 Gargalo #7 — Silver MERGE com `post_hook_delete_source_tombstones` (MÉDIO)

Após o MERGE na silver (que já é custoso com 230 colunas), roda:

```sql
DELETE FROM silver_tasy_procedimento_paciente
WHERE nr_sequencia IN (
  SELECT nr_sequencia FROM bronze_tasy_procedimento_paciente WHERE _is_deleted = true
)
```

É um segundo scan da bronze + delete na silver, em separado do MERGE.

### 🟢 Gargalo #8 — Kyuubi Engine Idle Timeout (POSITIVO)

O Kyuubi mantém o engine Spark ativo por **2h** (`session.engine.idle.timeout: PT2H`), o que é bom: o dbt não paga o custo de startup do Spark (~20-30s) em execuções sequenciais. Já o PySpark via `spark-submit` paga o startup a cada chamada.

---

## 3. Plano de Otimização (mesmo EC2, mesmo custo)

### Nível 1 — Impacto ALTO, Esforço BAIXO (pode implementar hoje)

#### 1A. Habilitar Vectorized Reader para Iceberg

```
spark.sql.parquet.enableVectorizedReader  true
```

Aplicar em `kyuubi-defaults.conf` e `spark-defaults.conf`.

**Risco:** Algumas versões antigas Iceberg/Spark têm bugs com vectorized + complex types. Testar primeiro.

**Ganho esperado:** 2-3x mais rápido na leitura de tabelas Iceberg (silver, silver_context, bronze Iceberg).

#### 1B. Configurar `spark.sql.files.openCostInBytes` e `maxPartitionBytes`

Adicionar ao Kyuubi e spark-defaults:

```
spark.sql.files.openCostInBytes          1048576
spark.sql.files.maxPartitionBytes        268435456
```

- `openCostInBytes = 1MB` (default 4MB): faz o Spark coalescer mais small files numa mesma partition
- `maxPartitionBytes = 256MB`: permite partitions maiores, reduzindo o número de tasks

**Ganho esperado para a Bronze (Avro):** de ~4957 tasks para ~60-100 tasks. Redução drástica de overhead S3.

#### 1C. Ajustar S3A connection pool e prefetch

```
spark.hadoop.fs.s3a.connection.maximum     200
spark.hadoop.fs.s3a.threads.max            200
spark.hadoop.fs.s3a.readahead.range        6291456
spark.hadoop.fs.s3a.fast.upload            true
spark.hadoop.fs.s3a.multipart.size         67108864
spark.hadoop.fs.s3a.block.size             67108864
spark.hadoop.fs.s3a.experimental.input.fadvise  random
```

**Ganho:** paralelismo de download S3, prefetch de dados, upload assíncrono.

### Nível 2 — Impacto ALTO, Esforço MÉDIO (1-2 dias de refactoring)

#### 2A. Remover/Simplificar o `_row_hash` na Bronze

**Opção A (recomendada):** Remover completamente. O pipeline já tem `_cdc_ts_ms` + `__source_txid` para ordering e `nr_sequencia` como PK natural. O hash sobre 230 colunas não é usado em nenhum downstream (nem na silver nem na gold).

**Opção B:** Se for necessário manter change detection, computar sobre apenas as ~5 colunas que definem mudança de negócio:

```sql
MD5(CONCAT_WS('|',
  CAST(nr_sequencia AS STRING),
  CAST(vl_procedimento AS STRING),
  CAST(cd_procedimento AS STRING),
  CAST(qt_procedimento AS STRING)
))
```

**Ganho:** elimina a operação mais CPU-intensiva da bronze.

#### 2B. Remover macros redundantes no Silver Context

O `procedimento.sql` (silver_context) deveria **passar colunas direto** da silver sem reaplicar os macros:

**Antes (atual):**
```sql
, {{ fill_null_bigint('proc.nr_atendimento', -1) }} AS nr_atendimento
, {{ standardize_date('proc.dt_procedimento') }}    AS dt_procedimento
```

**Depois (correto):**
```sql
, proc.nr_atendimento
, proc.dt_procedimento
```

As colunas vindas da silver **já passaram** por `fill_null_bigint` e `standardize_date`. Reaplicar é um desperdício puro.

**Somente as colunas de JOIN** (vindas de `conv` e `atend`) precisam dos macros.

**Ganho:** reduz ~230 expressões CASE/WHEN/COALESCE para ~10 (apenas as colunas enriquecidas dos joins).

#### 2C. Silver Context → Incremental

Alterar de `materialized='table'` para:

```sql
{{
  config(
    materialized='incremental',
    schema='silver_context',
    file_format='iceberg',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    tags=["silver_context", "tasy", "procedimento"]
  )
}}
-- com filtro incremental:
{% if is_incremental() %}
WHERE proc._silver_processed_at > (
  SELECT COALESCE(MAX(_context_processed_at), TIMESTAMP '1900-01-01')
  FROM {{ this }}
)
{% endif %}
```

**Ganho:** processa apenas os deltas, não a tabela inteira.

### Nível 3 — Impacto MUITO ALTO, Esforço MÉDIO-ALTO (3-5 dias)

#### 3A. Column Pruning: Carregar Apenas Colunas Necessárias

Em vez de `SELECT * FROM avro...` com 230 colunas na bronze, criar um modelo "slim" ou usar `SELECT` explícito com apenas as ~30-40 colunas efetivamente consumidas downstream.

**Problema:** O Avro é row-based, então o Spark lê a row completa de qualquer forma.
**Solução real:** Apenas não projetar as 200+ colunas inúteis evita:
- A serialização/hash sobre elas
- O MERGE UPDATE SET de 230 colunas na silver
- A escrita de 230 colunas em Parquet/Iceberg

**Ganho potencial:** 60-70% menos I/O de escrita, MERGE significativamente mais rápido.

#### 3B. Compactação dos Arquivos Avro no S3 (Upstream)

Configurar o S3 Sink Connector do Kafka Connect para gerar arquivos maiores:

```properties
# connector config
flush.size=100000          # mais registros por arquivo (default geralmente baixo)
rotate.interval.ms=600000  # 10 min de rotação (em vez de 5 min)
```

**Alvo:** de 4.957 arquivos × 1,5 MB → ~50 arquivos × 150 MB.

**Ganho:** elimina completamente o small files problem, reduz overhead S3 em 99%.

#### 3C. Consolidar MERGE + DELETE na Silver

A silver faz MERGE e depois um DELETE separado para tombstones. Isso pode ser unificado:

O MERGE já pode tratar deletes com `WHEN MATCHED AND source._is_deleted = true THEN DELETE`.
Elimina o `post_hook_delete_source_tombstones` (que faz um segundo scan da bronze).

---

## 4. Comparativo: dbt+Kyuubi vs PySpark — Diferenças Estruturais

| Aspecto | dbt+Kyuubi | PySpark (spark-submit) |
|---|---|---|
| **Startup** | ~0s (engine já ativo via idle timeout 2h) | ~20-30s por spark-submit |
| **Modo de execução** | `local[*]` (driver-only, sem serialização entre processos) | `spark://master:7077` (executor separado, serialização necessária) |
| **Overhead por query** | Baixo (submit SQL via Thrift, planificação no engine existente) | Alto (novo SparkContext ou conexão ao cluster, DAG compilation) |
| **Memoria efetiva** | 8g no driver (tudo in-process) | 6g driver + 2g executor = overhead de 2 JVMs |
| **Metaspace** | 512/768MB (permite mais classes compiladas) | 256MB (limita reflection/code-gen) |
| **Adaptividade** | AQE + coalesce + skewJoin | AQE + coalesce (sem skewJoin explícito) |
| **Cache Glue** | Ativo (5min TTL) | Ativo (via config) |
| **Melhor para** | SQL declarativo, pipelines ELT modulares | Jobs complexos com lógica Python, ML, streaming |

**Conclusão:** Para o workload ELT atual (bronze→silver→silver_context), **Kyuubi em `local[*]` é estruturalmente superior** ao PySpark com cluster standalone, porque:
1. Zero overhead de startup (engine persistente)
2. Sem serialização driver↔executor (tudo roda no mesmo JVM)
3. Num `t3.xlarge` com 4 cores, a separação em processos diferentes só adiciona overhead

O PySpark só ganharia com:
- Cluster multi-node (mais cores que os 4 do EC2)
- Workloads com lógica Python heavy (ML, pandas UDFs)
- Streaming com checkpoints

---

## 5. Estimativa de Ganho com Otimizações

### Cenário Conservador (Nível 1 apenas)

| Otimização | Ganho estimado |
|---|---|
| Vectorized Reader ON | -25% no tempo de leitura Iceberg |
| maxPartitionBytes + openCostInBytes | -40% no tempo de scan Avro (menos tasks) |
| S3A tuning | -15% no I/O S3 |
| **Total estimado (Bronze dbt)** | **de ~296s para ~150-180s** |

### Cenário Agressivo (Nível 1 + 2 + 3)

| Otimização | Ganho estimado |
|---|---|
| Todas do Nível 1 | -50% no tempo base |
| Remover _row_hash (MD5 230 cols) | -20% CPU na Bronze |
| Remover macros redundantes silver_context | -40% CPU na Silver Context |
| Silver Context incremental | -70% no tempo da Silver Context |
| Column pruning (só colunas usadas) | -50% no I/O de escrita |
| **Total estimado (pipeline completo dbt)** | **de >296s (só bronze) para ~60-120s (bronze + silver + silver_context)** |

### Cenário Ideal (+ compactação Avro upstream)

Com compactação dos Avros no S3 Sink Connector:
- **50 arquivos de 150 MB** em vez de 4957 × 1,5 MB
- Elimina ~95% do overhead S3
- **Estimativa total pipeline: 30-60s** (dentro do SLA de 5 min com folga)

---

## 6. Plano de Ação Priorizado

| # | Ação | Impacto | Esforço | Risco |
|---|---|---|---|---|
| 1 | Habilitar `enableVectorizedReader = true` | Alto | 5 min | Baixo (testar) |
| 2 | Ajustar `openCostInBytes` + `maxPartitionBytes` | Alto | 5 min | Nenhum |
| 3 | Tunar S3A (connections, prefetch) | Médio | 5 min | Nenhum |
| 4 | Remover `_row_hash` da bronze | Alto | 30 min | Baixo (não usado downstream) |
| 5 | Remover macros redundantes em `procedimento.sql` | Alto | 1h | Nenhum |
| 6 | Silver Context → incremental | Médio | 1h | Baixo |
| 7 | Aumentar Metaspace do PySpark | Baixo | 5 min | Nenhum |
| 8 | Column pruning na bronze | Muito Alto | 2-3h | Médio (requer validar schema) |
| 9 | Compactar Avros no S3 Sink | Muito Alto | 2-4h | Médio (altera upstream) |
| 10 | Unificar MERGE+DELETE na silver | Médio | 1-2h | Baixo |

---

## 7. Resposta Final: Vale Trocar de Abordagem?

**Não.** Com o mesmo `t3.xlarge`, a arquitetura **dbt+Kyuubi** é a escolha correta porque:

1. O Kyuubi mantém um engine Spark persistente — zero custo de startup
2. `local[*]` num nó de 4 cores é mais eficiente que cluster standalone (sem overhead inter-processo)
3. O dbt oferece modularidade, testes, lineage e documentação que PySpark puro não tem
4. O problema de performance **não está na engine** (Kyuubi vs PySpark) — está nos **modelos e na configuração**

As otimizações de Nível 1 + 2 + 3 podem levar o pipeline de **>5 min (só bronze)** para **<2 min (completo)**, sem trocar EC2, sem trocar engine, sem reescrever em PySpark.

**O maior ROI está em:**
1. Corrigir o small files problem (config Spark + compactação Avro)
2. Eliminar o MD5 de 230 colunas
3. Parar de reaplicar macros no silver_context
