# Macros dbt — referência rápida (Austa)

Todas as macros abaixo estão em `dbt/macros/`. Uso no **Spark SQL** compilado pelo dbt. Para instalação do pacote que carrega `sitecustomize`, ver [PLUGINS.md](PLUGINS.md).

---

## Auditoria

### `bronze_audit_columns(raw_path [, raw_alias])`

**Arquivo:** `macros/audit/bronze_audit_columns.sql`

| Argumento | Tipo | Descrição |
|-----------|------|-----------|
| `raw_path` | string | Caminho S3 do Avro (ex.: `s3a://bucket/.../`). |
| `raw_alias` | string (opcional) | Alias do `FROM raw_incremental` (default `r`). |

**Convenção:** CTE `raw_incremental` + `FROM raw_incremental r` (ou o alias informado).

**Gera (fragmento com vírgulas à esquerda):**

- `_is_deleted` — de `__deleted`
- `_cdc_op`, `_cdc_ts_ms`, `_cdc_event_at`, `_cdc_source_table`
- `_source_path` — literal do `raw_path`
- `_row_hash` — MD5 do JSON do `STRUCT(*)` sobre a linha Avro com `SELECT * EXCEPT (__deleted, __source_txid, kafka_ingestion_source, year, month, day, hour)` (subconsulta correlacionada a `__ts_ms` + `__source_txid`)
- `_bronze_loaded_at` — timestamp de carga (America/Sao_Paulo)

**Exemplo**

```sql
SELECT
    nr_atendimento
  , dt_entrada AS dh_entrada
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
```

---

### `silver_audit_columns()`

**Arquivo:** `macros/audit/silver_audit_columns.sql`

Sem argumentos. Exige que o `FROM` da subquery use alias **`d`** (colunas vindas da bronze via `latest_by_pk d`).

**Gera:** `_is_deleted`, `_cdc_op`, `_cdc_event_at`, `_cdc_ts_ms`, `_bronze_row_hash` (de `d._row_hash`), `_bronze_loaded_at`, `_silver_processed_at`, `_dbt_invocation_id`.

**Exemplo**

```sql
, shaped AS (
  SELECT
      d.nr_atendimento AS nr_atendimento
    , {{ standardize_date('d.dh_entrada') }} AS dt_entrada
    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)
```

**Importante:** omitir `_silver_processed_at` de `merge_update_columns` no `config` do modelo silver.

---

### `silver_context_audit_columns()`

**Arquivo:** `macros/audit/silver_context_audit_columns.sql`

Sem argumentos. Gera `_context_processed_at` e `_dbt_invocation_id`.

**Exemplo**

```sql
SELECT
    atend.nr_atendimento
  , ...
  {{ silver_context_audit_columns() }}
FROM ...
```

---

## Hooks

### `post_hook_delete_source_tombstones(source_model, key_column, deleted_column='__deleted')`

**Arquivo:** `macros/hooks/post_hook_delete_source_tombstones.sql`

Após merge incremental na **silver**, remove linhas cuja PK ainda existe na **bronze** com flag de exclusão.

| Argumento | Descrição |
|-----------|-----------|
| `source_model` | Nome do modelo bronze (string), ex.: `'bronze_tasy_atendimento_paciente'` |
| `key_column` | PK de negócio, ex.: `'nr_atendimento'` |
| `deleted_column` | Coluna booleana de tombstone; na bronze atual use **`'_is_deleted'`** |

**Exemplo (config)**

```jinja
post_hook=post_hook_delete_source_tombstones(
  'bronze_tasy_atendimento_paciente',
  'nr_atendimento',
  '_is_deleted'
)
```

---

## Tempo

### `standardize_timestamp(expression)`

**Arquivo:** `macros/tempo/standardize_timestamp.sql`

NULL, cast inválido ou ano ≥ 2900 → `TIMESTAMP '1900-01-01 00:00:00'`.

```sql
{{ standardize_timestamp('d.dh_algum_instante') }} AS ts_algum_instante
```

---

### `standardize_date(expression)`

**Arquivo:** `macros/tempo/standardize_date.sql`

NULL, cast inválido ou ano ≥ 2900 → `DATE '1900-01-01'`.

```sql
{{ standardize_date('d.dh_entrada') }} AS dt_entrada
```

---

## Numérico

### `normalize_decimal(expression, scale, sentinel)`

**Arquivo:** `macros/numerico/normalize_decimal.sql`

`ROUND(CAST(... AS DECIMAL(38,6)), scale)` com `COALESCE` para um **sentinela** decimal.

```sql
{{ normalize_decimal('d.qt_dia_longa_perm', 2, -1) }} AS qt_dia_longa_perm
```

---

### `fill_null_bigint(expression, sentinel)`

**Arquivo:** `macros/numerico/fill_null_bigint.sql`

```sql
{{ fill_null_bigint('d.cd_pessoa_fisica', -1) }} AS cd_pessoa_fisica
```

---

## Texto

### `fill_null_string(expression, literal_sql)`

**Arquivo:** `macros/texto/fill_null_string.sql`

`literal_sql` já entre aspas SQL, ex.: `"'indefinido'"`.

```sql
{{ fill_null_string('d.nm_obs', "'indefinido'") }} AS nm_obs
```

---

### `standardize_text_initcap(expression, literal_sql)`

**Arquivo:** `macros/texto/standardize_text_initcap.sql`

Aplica `fill_null_string` depois `INITCAP`.

```sql
{{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }} AS nm_usuario
```

---

### `standardize_enum(expression, literal_sql)`

**Arquivo:** `macros/texto/standardize_enum.sql`

`LOWER(TRIM(...))` com fallback para o literal (ex.: `"'i'"` para indefinido).

```sql
{{ standardize_enum('d.ie_tipo_atendimento', "'i'") }} AS ie_tipo_atendimento
```

---

## Documentos (CPF/CNPJ)

### `standardize_documents(expression, expected_length)`

**Arquivo:** `macros/documentos/standardize_documents.sql`

Remove não-dígitos; se comprimento = `expected_length`, retorna só dígitos; se entre 1 e `expected_length-1`, faz `LPAD` com zeros; senão retorna apenas a limpeza.

```sql
{{ standardize_documents('d.cd_cgc_seguradora', 14) }} AS cd_cgc_seguradora
```

---

## Schema (dbt Core)

### `generate_schema_name(custom_schema_name, node)`

**Arquivo:** `macros/generate_schema_name.sql`

Se `custom_schema_name` for definido no config, usa esse nome; caso contrário usa `target.schema` do `profiles.yml`. Garante schemas `bronze`, `silver`, etc., como definidos no `dbt_project.yml`.

---

## Mapa de pastas

| Pasta | Macros |
|-------|--------|
| `macros/audit/` | `bronze_audit_columns`, `silver_audit_columns`, `silver_context_audit_columns` |
| `macros/hooks/` | `post_hook_delete_source_tombstones` |
| `macros/tempo/` | `standardize_timestamp`, `standardize_date` |
| `macros/numerico/` | `normalize_decimal`, `fill_null_bigint` |
| `macros/texto/` | `fill_null_string`, `standardize_text_initcap`, `standardize_enum` |
| `macros/documentos/` | `standardize_documents` |
| `macros/` (raiz) | `generate_schema_name` |
