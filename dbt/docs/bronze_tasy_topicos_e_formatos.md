# Bronze Tasy — dois tópicos e formatos Avro (flat vs envelope Debezium)

Este documento lista os modelos bronze que leem **mais de um prefixo Kafka/S3** (`tasy.TASY.*` e `austa.TASY.*`) e classifica os tópicos `austa.TASY.*` segundo o **layout Avro** no raw: **flat** (habitual, alinhado a `tasy.TASY.*`) ou **envelope Debezium** (`before` / `after` / `source` / `op` / `ts_ms` no raiz).

---

## 1. Modelos alimentados por dois tópicos diferentes

Hoje **dois** modelos fazem `UNION` entre o stream **flat** `tasy.TASY.*` e o stream `austa.TASY.*`:

| Modelo dbt | Tópico `tasy` | Tópico `austa` | Macro / notas |
|------------|----------------|------------------|-----------------|
| `bronze_tasy_convenio` | `tasy.TASY.CONVENIO` | `austa.TASY.CONVENIO` | `bronze_raw_incremental_union_tasy_austa(..., envelope_null_pad=1)` — ramo `austa` em **envelope**; padding para igualar número de colunas ao `SELECT *` do `tasy`. |
| `bronze_tasy_compl_pessoa_fisica` | `tasy.TASY.COMPL_PESSOA_FISICA` | `austa.TASY.COMPL_PESSOA_FISICA` | `bronze_raw_incremental_union_tasy_austa_flat(..., tasy_null_pad=1)` — **ambos** os ramos são **flat** (`__ts_ms`); padding no ramo `tasy` para alinhar contagem de colunas. |

### Modelos que antes uniam dois tópicos, mas hoje leem só `tasy.TASY.*`

O `UNION` com o ramo **envelope** `austa` gerava **incompatibilidade de tipos** nas mesmas posições (por exemplo `TIMESTAMP` no flat vs `BIGINT` no payload `after`/`before`). Até existir normalização explícita (casts) ou bronze separado por fonte, estes modelos usam **apenas** o tópico `tasy`:

| Modelo dbt | Tópico utilizado | Observação |
|------------|------------------|------------|
| `bronze_tasy_autorizacao_convenio` | `tasy.TASY.AUTORIZACAO_CONVENIO` | Eventos só em `austa.TASY.AUTORIZACAO_CONVENIO` não entram nesta tabela bronze. |
| `bronze_tasy_diagnostico_doenca` | `tasy.TASY.DIAGNOSTICO_DOENCA` | Idem para `austa.TASY.DIAGNOSTICO_DOENCA`. |

---

## 2. Tópicos `austa.TASY.*` com padronização **envelope** Debezium

Características no S3: raiz com `before`, `after`, `source`, `op`, `ts_ms` (sem `__ts_ms` no topo); negócio em `COALESCE(after, before)`.

**Macro:** `bronze_raw_incremental_austa_envelope`

| Modelo dbt | Tópico / prefixo S3 |
|------------|----------------------|
| `bronze_tasy_area_procedimento` | `austa.TASY.AREA_PROCEDIMENTO` |
| `bronze_tasy_cbo_saude` | `austa.TASY.CBO_SAUDE` |
| `bronze_tasy_especialidade_proc` | `austa.TASY.ESPECIALIDADE_PROC` |
| `bronze_tasy_grau_parentesco` | `austa.TASY.GRAU_PARENTESCO` |
| `bronze_tasy_grupo_proc` | `austa.TASY.GRUPO_PROC` |
| `bronze_tasy_pessoa_juridica` | `austa.TASY.PESSOA_JURIDICA` |
| `bronze_tasy_pls_lote_mensalidade` | `austa.TASY.PLS_LOTE_MENSALIDADE` |
| `bronze_tasy_pls_rol_grupo_proc` | `austa.TASY.PLS_ROL_GRUPO_PROC` |
| `bronze_tasy_pls_rol_procedimento` | `austa.TASY.PLS_ROL_PROCEDIMENTO` |
| `bronze_tasy_tipo_pessoa_juridica` | `austa.TASY.TIPO_PESSOA_JURIDICA` |
| `bronze_tasy_tiss_motivo_glosa` | `austa.TASY.TISS_MOTIVO_GLOSA` |

No **UNION** de `bronze_tasy_convenio`, o ramo `austa` segue este mesmo padrão envelope (macro `bronze_raw_incremental_union_tasy_austa`).

---

## 3. Tópicos `austa.TASY.*` com padronização **flat** (igual ao habitual `tasy.TASY.*`)

Características: `__ts_ms` e demais metadados no mesmo estilo do conector “flat”; leitura com `SELECT *` filtrado por watermark.

**Macro:** `bronze_raw_incremental_austa_flat`

| Modelo dbt | Tópico / prefixo S3 |
|------------|----------------------|
| `bronze_tasy_austa_beneficiario` | `austa.TASY.AUSTA_BENEFICIARIO` |
| `bronze_tasy_austa_conta` | `austa.TASY.AUSTA_CONTA` |
| `bronze_tasy_austa_mensalidade` | `austa.TASY.AUSTA_MENSALIDADE` |
| `bronze_tasy_austa_prestador` | `austa.TASY.AUSTA_PRESTADOR` |
| `bronze_tasy_austa_proc_e_mat` | `austa.TASY.AUSTA_PROC_E_MAT` |
| `bronze_tasy_austa_requisicao` | `austa.TASY.AUSTA_REQUISICAO` |
| `bronze_tasy_pessoa_fisica` | `austa.TASY.PESSOA_FISICA` |
| `bronze_tasy_pessoa_juridica_compl` | `austa.TASY.PESSOA_JURIDICA_COMPL` |
| `bronze_tasy_pls_mensalidade` | `austa.TASY.PLS_MENSALIDADE` |
| `bronze_tasy_pls_mensalidade_segurado` | `austa.TASY.PLS_MENSALIDADE_SEGURADO` |
| `bronze_tasy_procedimento` | `austa.TASY.PROCEDIMENTO` |
| `bronze_tasy_sus_municipio` | `austa.TASY.SUS_MUNICIPIO` |

No **UNION** de `bronze_tasy_compl_pessoa_fisica`, o ramo `austa` é tratado como **flat** (macro `bronze_raw_incremental_union_tasy_austa_flat`).

---

## 4. Referência rápida das macros

| Macro | Uso |
|-------|-----|
| `bronze_raw_incremental_austa_envelope` | Só `austa` em envelope Debezium. |
| `bronze_raw_incremental_austa_flat` | Só `austa` em layout flat (`__ts_ms`). |
| `bronze_raw_incremental_union_tasy_austa` | `tasy` flat + `austa` envelope; parâmetro opcional `envelope_null_pad` para alinhar colunas ao `UNION`. |
| `bronze_raw_incremental_union_tasy_austa_flat` | `tasy` flat + `austa` flat; parâmetro opcional `tasy_null_pad` quando o `austa` tem mais colunas que o `tasy`. |

Os modelos que leem **somente** `tasy.TASY.*` (sem `austa`) seguem o padrão já documentado em `docs/bronze.md`: `SELECT *` do Avro com filtro por `__ts_ms`.
