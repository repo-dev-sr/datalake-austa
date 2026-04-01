# dbt — Convenções por camada (guia para novos modelos)

Guia prático para **analytics engineers** e desenvolvedores criarem modelos alinhados ao que já existe em `dbt/models/`. Regras globais do repositório continuam em `CLAUDE.md` (raiz). Detalhes CDC da bronze: [bronze.md](bronze.md). Arquitetura e Airflow: [dbt/docs/ARCHITECTURE.md](../dbt/docs/ARCHITECTURE.md).

---

## Inventário de modelos no repositório

Confira se o nome do novo modelo segue o padrão e se a pasta está correta.

### Bronze (`dbt/models/bronze/`)

| Modelo | Arquivo |
|--------|---------|
| `bronze_tasy_atendimento_paciente` | `bronze_tasy_atendimento_paciente.sql` |
| `bronze_tasy_atend_paciente_unidade` | `bronze_tasy_atend_paciente_unidade.sql` |
| `bronze_tasy_conta_paciente` | `bronze_tasy_conta_paciente.sql` |
| `bronze_tasy_proc_paciente_convenio` | `bronze_tasy_proc_paciente_convenio.sql` |
| `bronze_tasy_proc_paciente_valor` | `bronze_tasy_proc_paciente_valor.sql` |
| `bronze_tasy_procedimento_paciente` | `bronze_tasy_procedimento_paciente.sql` |

Testes e colunas: `bronze/schema.yml`.

### Silver (`dbt/models/silver/`)

| Modelo | Depende de (ref) |
|--------|------------------|
| `silver_tasy_atendimento_paciente` | `bronze_tasy_atendimento_paciente` |
| `silver_tasy_atend_paciente_unidade` | `bronze_tasy_atend_paciente_unidade` |
| `silver_tasy_conta_paciente` | `bronze_tasy_conta_paciente` |
| `silver_tasy_proc_paciente_convenio` | `bronze_tasy_proc_paciente_convenio` |
| `silver_tasy_proc_paciente_valor` | `bronze_tasy_proc_paciente_valor` |
| `silver_tasy_procedimento_paciente` | `bronze_tasy_procedimento_paciente` |

Testes: `silver/schema.yml`.

### Silver-Context (`dbt/models/silver_context/`)

| Modelo | Ideia |
|--------|--------|
| `atendimento` | Join atendimento + conta (grão: `nr_atendimento`). |
| `movimentacao_paciente` | Visão de movimentação (ver SQL e `schema.yml`). |
| `procedimento` | Visão de procedimento (ver SQL e `schema.yml`). |

### Gold (`dbt/models/gold/`)

Hoje há apenas **`schema.yml`** com documentação de exemplo (`gold_dim_paciente`). Ao criar `.sql`, usar prefixos `gold_dim_*` / `gold_fct_*`.

---

## Resumo dos objetivos por camada

| Camada | Objetivo em uma frase |
|--------|------------------------|
| **Bronze** | Trazir CDC do S3 (Avro) para Iceberg com auditoria e janela temporal; **sem** negócio analítico e **sem** joins. |
| **Silver** | **Deduplicar** por chave de negócio e **limpar** tipos/valores com macros reutilizáveis. |
| **Silver-Context** | **Juntar** silvers para perguntas de negócio que precisam de mais de uma entidade. |
| **Gold** | Entregar **dimensões e fatos** para BI (surrogate keys, integridade). |

---

## Bronze — esqueleto mental (pseudo-código)

Alinhado aos arquivos `bronze_tasy_*.sql` atuais (`incremental` + **append**).

```text
CONFIG:
  materialized = incremental
  incremental_strategy = append
  file_format = iceberg
  schema = bronze

VARS:
  raw_path = s3a://<bucket>/raw/raw-tasy/stream/<tópico>/
  cdc_lookback_hours, cdc_reprocess_hours (dbt_project.yml ou --vars)

CTE target_watermark:
  SE incremental ENTÃO MAX(_cdc_ts_ms) DA PRÓPRIA TABELA BRONZE
  SENÃO 0

CTE params:
  wm_start_ms = reprocess OU (watermark - lookback)

CTE raw_incremental:
  SELECT * FROM avro.`raw_path`
  WHERE __ts_ms >= wm_start_ms

SELECT:
  colunas negócio (dt_* AS dh_* onde aplicável)
  + bronze_audit_columns(raw_path, lista_colunas_negócio)
  + __source_txid (para ordenação na silver)
```

**Checklist junior**

1. Nome do arquivo = nome do modelo = `bronze_tasy_{entidade}`.
2. `raw_path` igual ao prefixo S3 usado pelo pipeline (ver `airflow/dags/common/constants.py`).
3. Lista `business_columns` da macro **deve** bater com colunas usadas no hash (mesma ordem/conjunto que o `STRUCT` na bronze).
4. Documentar no `bronze/schema.yml` com os **mesmos** nomes do `SELECT` final.

---

## Silver — esqueleto mental (pseudo-código)

Padrão observado em `silver_tasy_atendimento_paciente` e equivalentes.

```text
CONFIG:
  materialized = incremental
  incremental_strategy = merge
  unique_key = '<PK_negócio>'   # ex.: nr_atendimento
  merge_update_columns = [ ... todas menos _silver_processed_at ... ]
  file_format = iceberg
  post_hook = post_hook_delete_source_tombstones(
    'bronze_tasy_<entidade>',
    '<PK_negócio>',
    '_is_deleted'
  )

CTE base:
  SELECT * FROM ref('bronze_tasy_<entidade>') b
  SE incremental ENTÃO
    WHERE b._bronze_loaded_at > (SELECT MAX(_silver_processed_at) FROM this)

CTE latest_by_pk:
  ROW_NUMBER() OVER (
    PARTITION BY b.<PK>
    ORDER BY b._cdc_ts_ms DESC, b.__source_txid DESC
  ) = 1

CTE shaped:
  SELECT
    PK e colunas negócio tratadas com macros (standardize_date, fill_null_bigint, ...)
    + silver_audit_columns()   -- exige alias "d" no FROM = latest_by_pk d

SELECT * FROM shaped
```

**Checklist junior**

1. Nome `silver_tasy_{entidade}` e `ref()` só para a bronze **homônima**.
2. Definir `unique_key` = mesma PK usada no `PARTITION BY`.
3. Não incluir `_silver_processed_at` em `merge_update_columns`.
4. Ajustar `post_hook_delete_source_tombstones` com o nome exato do modelo bronze e a coluna de delete (`_is_deleted` na bronze atual).
5. Colunas de data na silver costumam sair como `dt_*` após `standardize_date(d.dh_*)`.

---

## Silver-Context — esqueleto mental (pseudo-código)

Padrão observado em `atendimento.sql`.

```text
CONFIG:
  materialized = table   # (ou incremental, se o time padronizar)
  file_format = iceberg
  schema = silver_context

CTE entidades:
  SELECT * FROM ref('silver_tasy_...')

SELECT
  colunas de negócio (joins, COALESCE, macros de limpeza se necessário)
  + silver_context_audit_columns()
```

**Checklist junior**

1. **Nunca** `ref()` na bronze — apenas `silver_tasy_*`.
2. Documentar no `silver_context/schema.yml` o **grão** (ex.: 1 linha por `nr_atendimento`).
3. Nomes de modelo **sem** prefixo `silver_tasy_` (ex.: `atendimento`, `procedimento`).

---

## Gold — exemplo futuro (pseudo-código)

Quando existir `gold_dim_paciente.sql` (ou fato):

```text
CONFIG:
  materialized = table
  file_format = iceberg
  schema = gold

SELECT
  {{ dbt_utils.generate_surrogate_key(['cd_paciente']) }} AS sk_paciente,
  ...
FROM {{ ref('silver_tasy_paciente') }}   -- ou context, conforme desenho
```

Usar pacote `dbt_utils` conforme `CLAUDE.md` e testes `unique` / `not_null` / `relationships`.

---

## Onde pedir ajuda no repo

| Dúvida | Onde ler |
|--------|----------|
| Watermark, full-refresh, reprocess | `dbt/RUNBOOK_CDC_BRONZE.md` |
| Macros disponíveis | `dbt/docs/MACROS.md` |
| Ambiente local e comandos | `dbt/docs/FLUXO_USO_E_DICAS.md` |
| Patch Spark / `.env` | `dbt/docs/PLUGINS.md` |
