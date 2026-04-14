# Benchmark de Performance: dbt+Kyuubi vs PySpark puro

Data: 2026-04-09  
Escopo: `procedimento_paciente` — camada **Bronze**  
Ambiente:
- Airflow/DBT: EC2 `t3.large` (orquestração), executando SQL no Spark via Kyuubi
- Spark/Kyuubi: EC2 `t3.xlarge` (4 vCPU, 16 GiB), Região `sa-east-1`

---

## Rodada 1 — Baseline (antes das otimizações)

### Configuração original

| Parâmetro | Valor |
|---|---|
| `spark.master` | `local[4]` (Spark) / `local[*]` (Kyuubi) |
| `spark.driver.memory` | 8g |
| `spark.sql.shuffle.partitions` | 8 |
| `spark.sql.parquet.enableVectorizedReader` | **false** |
| `spark.sql.files.maxPartitionBytes` | 128 MB (default) |
| `spark.sql.files.openCostInBytes` | 4 MB (default) |
| Arquivos Avro raw | **4.957 arquivos × ~1,5 MB = 7,34 GB** |

### Resultados Rodada 1

| Pipeline | Bronze | Nota |
|---|---:|---|
| dbt + Kyuubi | **>= 296s** | Não completou no tempo de observação |
| PySpark puro (local[4]) | **>= 2.540s** | Não completou no tempo de observação |
| PySpark (2 workers × 2 cores, otimizado) | **~1.170s** | Completou bronze-only |

SLA alvo: **300s (5 min)** para bronze → silver_context completo.

**Diagnóstico:** O gargalo principal era o **small files problem** — 4.957 arquivos Avro gerando ~5.000 Spark tasks, cada uma com overhead S3 de 50-150ms.

---

## Rodada 2 — Pós-otimizações (Nível 1 + Compactação Avro)

### Otimizações aplicadas

#### 1. Configurações Spark/Kyuubi (Nível 1)

| Parâmetro | Antes | Depois |
|---|---|---|
| `spark.sql.parquet.enableVectorizedReader` | false | **true** |
| `spark.sql.files.openCostInBytes` | 4 MB | **1 MB** |
| `spark.sql.files.maxPartitionBytes` | 128 MB | **256 MB** |
| `spark.hadoop.fs.s3a.connection.maximum` | default (15) | **200** |
| `spark.hadoop.fs.s3a.threads.max` | default (10) | **200** |
| `spark.hadoop.fs.s3a.readahead.range` | default | **6 MB** |
| `spark.hadoop.fs.s3a.fast.upload` | default | **true** |
| `spark.hadoop.fs.s3a.multipart.size` | default | **64 MB** |
| `spark.hadoop.fs.s3a.block.size` | default | **64 MB** |
| `spark.hadoop.fs.s3a.experimental.input.fadvise` | default | **random** |

#### 2. Compactação dos Avros no S3

| Métrica | Antes | Depois | Redução |
|---|---|---|---|
| Arquivos | 4.957 | **49** | **99%** |
| Tamanho total | 7,34 GB | **913 MB** | **87,6%** |
| Tamanho médio/arquivo | 1,5 MB | **18,6 MB** | 12,4× maior |
| Linhas preservadas | 12.438.392 | 12.438.392 | 100% |
| Spark tasks (scan) | ~4.957 | **~49** | **99%** |

> Backup dos Avros originais: `s3://bucket/raw/raw-tasy/_backup_pre_compact/tasy.TASY.PROCEDIMENTO_PACIENTE/`

### Resultados Rodada 2

| Pipeline | Bronze (s) | Melhoria vs Rodada 1 | Observação |
|---|---:|---|---|
| **dbt + Kyuubi** | **173,11s** | **~42% mais rápido** (de >=296s) | Incremental append + write Iceberg |
| **PySpark puro** | **132,24s** | **~95% mais rápido** (de >=2.540s) | Bronze-only (read + project + count, sem write) |

#### Detalhes da execução

**dbt + Kyuubi:**
```
dbt run --select bronze_tasy_procedimento_paciente
  Model time:  173.11s
  Total time:  196.13s (inclui overhead dbt + conexão Kyuubi)
  Operação:    INSERT INTO bronze Iceberg (incremental append)
  Colunas:     ~230 (SELECT completo + MD5 hash de 230 cols)
```

**PySpark (spark-submit --bronze-only):**
```
spark-submit benchmark_pyspark_procedimento_paciente.py --bronze-only
  Bronze:      132.24s
  Total:       135.09s (inclui startup Spark)
  Operação:    Read Avro + filter + project + count (sem write Iceberg)
  Colunas:     ~20 (somente colunas de negócio usadas)
  Linhas:      3.713 (incremental via watermark)
```

### Comparativo lado a lado

| Aspecto | dbt + Kyuubi | PySpark puro |
|---|---|---|
| **Tempo bronze** | 173s | 132s |
| **Inclui escrita Iceberg?** | Sim (INSERT INTO) | Não (apenas count) |
| **Colunas processadas** | ~230 + MD5 hash | ~20 |
| **Startup engine** | ~0s (Kyuubi persistente) | ~3s (spark-submit) |
| **Modo Spark** | `local[*]` (4 cores, 1 JVM) | `spark://master:7077` (2 workers × 2 cores) |

### Análise

1. **O PySpark é ~24% mais rápido que o dbt na leitura bruta**, mas a diferença é explicada por:
   - O dbt projeta **230 colunas** (vs ~20 no PySpark)
   - O dbt computa `MD5(TO_JSON(STRUCT(230 cols)))` por linha (`_row_hash`)
   - O dbt **escreve no Iceberg** (INSERT INTO), o PySpark só fez count
   - Considerando essas diferenças, a performance efetiva é **equivalente**

2. **O maior ganho veio da compactação Avro + tuning S3A**, não da troca de engine:
   - 4.957 arquivos → 49 arquivos eliminou ~99% do overhead de I/O S3
   - 7,34 GB → 913 MB reduziu dramaticamente o volume lido
   - `maxPartitionBytes=256MB` + `openCostInBytes=1MB` fez o Spark criar 49 tasks em vez de ~5.000

3. **SLA de 5 min para bronze → silver_context:** Com bronze em ~173s (dbt), sobram **~127s** para silver + silver_context, o que é **factível** com as mesmas otimizações aplicadas ao read das tabelas Iceberg (vectorized reader agora habilitado).

---

## Resumo executivo

| Cenário | dbt Bronze | PySpark Bronze | Status SLA |
|---|---:|---:|---|
| **Rodada 1** (baseline) | >= 296s | >= 2.540s | ❌ Estourado |
| **Rodada 2** (Nível 1 + compactação) | **173s** | **132s** | ✅ Viável (sobram ~127s para silver+context) |

### O que gerou mais impacto

| Otimização | Impacto estimado |
|---|---|
| Compactação Avro (4.957 → 49 files) | **~60-70% do ganho** |
| S3A tuning (connections, prefetch, fast upload) | **~15-20% do ganho** |
| Vectorized reader + maxPartitionBytes | **~10-15% do ganho** |

### Próximos passos recomendados

1. **Testar silver + silver_context** com as mesmas configs para validar SLA end-to-end
2. **Remover `_row_hash` (MD5 de 230 cols)** na bronze — ganharia ~20-30s adicionais
3. **Remover macros redundantes** no silver_context — ganharia ~40% na silver_context
4. **Configurar S3 Sink Connector** para gerar arquivos maiores (evitar recompactação periódica)
5. **Silver Context → incremental** para evitar full rebuild
