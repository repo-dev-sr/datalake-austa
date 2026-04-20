"""
Gera modelos bronze dbt a partir de amostras Avro no S3 (payload Debezium já achatado no lake: __ts_ms no raiz).
Requer: aws CLI, fastavro, Python 3.10+

Uso (na raiz do repo):
  python dbt/tools/generate_bronze_austa_models.py
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import fastavro

BUCKET = "austa-lakehouse-prod-data-lake-169446931765"
PREFIX_BASE = "raw/raw-tasy/stream"
S3A_BASE = f"s3a://{BUCKET}/{PREFIX_BASE}"

# 22 tópicos exclusivos austa (sem modelo ainda)
AUSTA_ONLY = [
    "austa.TASY.AREA_PROCEDIMENTO",
    "austa.TASY.AUSTA_BENEFICIARIO",
    "austa.TASY.AUSTA_CONTA",
    "austa.TASY.AUSTA_MENSALIDADE",
    "austa.TASY.AUSTA_PRESTADOR",
    "austa.TASY.AUSTA_PROC_E_MAT",
    "austa.TASY.AUSTA_REQUISICAO",
    "austa.TASY.CBO_SAUDE",
    "austa.TASY.ESPECIALIDADE_PROC",
    "austa.TASY.GRAU_PARENTESCO",
    "austa.TASY.GRUPO_PROC",
    "austa.TASY.PESSOA_FISICA",
    "austa.TASY.PESSOA_JURIDICA",
    "austa.TASY.PESSOA_JURIDICA_COMPL",
    "austa.TASY.PLS_LOTE_MENSALIDADE",
    "austa.TASY.PLS_MENSALIDADE",
    "austa.TASY.PLS_MENSALIDADE_SEGURADO",
    "austa.TASY.PLS_ROL_GRUPO_PROC",
    "austa.TASY.PLS_ROL_PROCEDIMENTO",
    "austa.TASY.PROCEDIMENTO",
    "austa.TASY.SUS_MUNICIPIO",
    "austa.TASY.TIPO_PESSOA_JURIDICA",
    "austa.TASY.TISS_MOTIVO_GLOSA",
]

# Sobreposição tasy + austa (UNION ALL)
OVERLAP = [
    ("tasy.TASY.AUTORIZACAO_CONVENIO", "austa.TASY.AUTORIZACAO_CONVENIO"),
    ("tasy.TASY.COMPL_PESSOA_FISICA", "austa.TASY.COMPL_PESSOA_FISICA"),
    ("tasy.TASY.CONVENIO", "austa.TASY.CONVENIO"),
    ("tasy.TASY.DIAGNOSTICO_DOENCA", "austa.TASY.DIAGNOSTICO_DOENCA"),
]


def run_aws_ls_latest_key(topic_prefix: str) -> str | None:
    """Retorna key do .avro com LastModified mais recente (paginação básica)."""
    prefix = f"{PREFIX_BASE}/{topic_prefix}/"
    token = None
    best: tuple[str, str] | None = None  # (LastModified, Key)
    while True:
        cmd = [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            BUCKET,
            "--prefix",
            prefix,
        ]
        if token:
            cmd.extend(["--continuation-token", token])
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print(f"WARN list {prefix}: {e.output}", file=sys.stderr)
            return None
        data = json.loads(out)
        for c in data.get("Contents") or []:
            key = c["Key"]
            if not key.endswith(".avro"):
                continue
            lm = c.get("LastModified", "")
            if best is None or lm > best[0]:
                best = (lm, key)
        token = data.get("NextContinuationToken")
        if not token:
            break
    return best[1] if best else None


def download_s3_key(key: str, dest: Path) -> None:
    uri = f"s3://{BUCKET}/{key}"
    subprocess.check_call(["aws", "s3", "cp", uri, str(dest)], stdout=subprocess.DEVNULL)


def _record_fields_from_union(type_node) -> list[str] | None:
    """Encontra o primeiro record não-nulo num union Debezium (ex.: registo interno `after`)."""
    if isinstance(type_node, list):
        for opt in type_node:
            if isinstance(opt, dict) and opt.get("type") == "record" and opt.get("fields"):
                return [x["name"] for x in opt["fields"]]
        return None
    if isinstance(type_node, dict) and type_node.get("type") == "record":
        return [x["name"] for x in type_node.get("fields", [])]
    return None


def extract_value_field_names(schema: dict, avro_path: Path | None = None) -> list[str]:
    """Lista nomes de campos de negócio (a partir do record `after` no schema Avro ou da primeira linha)."""
    fields = schema.get("fields") or []
    for f in fields:
        if f.get("name") != "after":
            continue
        names = _record_fields_from_union(f.get("type"))
        if names:
            return names
    # Unions com referência nomeada (ex.: ['null', 'austa.TASY.X.Value']) — inspecionar linhas
    if avro_path and avro_path.is_file():
        with avro_path.open("rb") as fh:
            reader = fastavro.reader(fh)
            for _ in range(5000):
                try:
                    row = next(reader)
                except StopIteration:
                    break
                a = row.get("after")
                if a is not None and isinstance(a, dict):
                    return list(a.keys())
    raise ValueError("Schema after/payload não encontrado")


def to_spark_field(name: str) -> str:
    """Nome Oracle → identificador Spark (minúsculo)."""
    return name.lower()


def business_alias(field: str) -> str:
    """Nome de saída: dt_* → dh_*."""
    f = field.lower()
    if f.startswith("dt_"):
        return "dh_" + f[3:]
    return f


def final_select_conta_style_lines(field_names: list[str]) -> list[str]:
    """SELECT final a partir de linha já achatada (ex.: UNION): `dt_* AS dh_*`, demais sem alias."""
    lines = ["SELECT"]
    for i, fn in enumerate(field_names):
        sf = to_spark_field(fn)
        if fn.lower().startswith("dt_"):
            col = f"{sf} AS {business_alias(fn)}"
        else:
            col = sf
        if i == 0:
            lines.append(f"    {col}")
        else:
            lines.append(f"  , {col}")
    return lines


def final_select_explicit_flat_r(field_names: list[str]) -> list[str]:
    """SELECT final com alias `r` (padrão bronze_tasy_austa_conta.sql)."""
    lines = ["SELECT"]
    for i, fn in enumerate(field_names):
        sf = to_spark_field(fn)
        src = f"r.{sf}"
        if fn.lower().startswith("dt_"):
            col = f"{src} AS {business_alias(fn)}"
        else:
            col = f"{src} AS {sf}"
        if i == 0:
            lines.append(f"    {col}")
        else:
            lines.append(f"  , {col}")
    return lines


def render_flat_austa_only(topic: str, field_names: list[str], model_name: str) -> str:
    """Um path Avro: raw_incremental = SELECT * + filtro __ts_ms; sem envelope no SQL."""
    raw_path = f"{S3A_BASE}/{topic}/"
    lines = [
        "{{",
        "  config(",
        "    materialized='incremental'",
        "    , schema='bronze'",
        "    , file_format='iceberg'",
        "    , incremental_strategy='append'",
        "    , on_schema_change='append_new_columns'",
        "  )",
        "}}",
        "",
        f'{{% set raw_path = "{raw_path}" %}}',
        "{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}",
        "{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}",
        "",
        "WITH target_watermark AS (",
        "  {% if is_incremental() %}",
        "    SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms",
        "    FROM {{ this }}",
        "  {% else %}",
        "    SELECT CAST(0 AS BIGINT) AS max_ts_ms",
        "  {% endif %}",
        ")",
        ", params AS (",
        "  SELECT",
        "    CASE",
        "      WHEN {{ cdc_reprocess_hours }} > 0 THEN",
        "        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - ({{ cdc_reprocess_hours }} * 3600)) * 1000",
        "      ELSE",
        "        GREATEST(",
        "          0",
        "          , (SELECT max_ts_ms FROM target_watermark) - ({{ cdc_lookback_hours }} * 3600 * 1000)",
        "        )",
        "    end  AS wm_start_ms",
        ")",
        ", raw_incremental AS (",
        "  SELECT   *",
        f"  FROM avro.`{{{{ raw_path }}}}`",
        "  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)",
        ")",
    ]
    lines.extend(final_select_explicit_flat_r(field_names))
    lines.extend(
        [
            "  {{ bronze_audit_columns(raw_path) }}",
            "  , r.__source_txid",
            "FROM raw_incremental r",
            "",
        ]
    )
    return "\n".join(lines)


def topic_to_model(topic: str) -> str:
    """austa.TASY.FOO -> bronze_tasy_foo (minúsculo)."""
    m = re.match(r"^(?:austa|tasy)\.TASY\.(.+)$", topic)
    if not m:
        raise ValueError(topic)
    return "bronze_tasy_" + m.group(1).lower()


def main() -> None:
    out_dir = Path(__file__).resolve().parents[1] / "models" / "bronze"
    tmp = Path(tempfile.mkdtemp())

    for topic in AUSTA_ONLY:
        key = run_aws_ls_latest_key(topic)
        if not key:
            print(f"SKIP sem avro: {topic}", file=sys.stderr)
            continue
        local = tmp / (topic.replace(".", "_") + ".avro")
        download_s3_key(key, local)
        with local.open("rb") as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema
        fields = extract_value_field_names(schema, local)
        model = topic_to_model(topic)
        sql = render_flat_austa_only(topic, fields, model)
        path = out_dir / f"{model}.sql"
        path.write_text(sql, encoding="utf-8")
        print(f"Wrote {path.name} ({len(fields)} cols)")

    for tasy_t, austa_t in OVERLAP:
        key = run_aws_ls_latest_key(austa_t)
        if not key:
            print(f"SKIP overlap sem avro austa: {austa_t}", file=sys.stderr)
            continue
        local = tmp / "overlap.avro"
        download_s3_key(key, local)
        with local.open("rb") as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema
        fields = extract_value_field_names(schema, local)
        model = topic_to_model(austa_t)
        sql = render_union_overlap(tasy_t, austa_t, fields)
        path = out_dir / f"{model}.sql"
        path.write_text(sql, encoding="utf-8")
        print(f"Wrote UNION {path.name} ({len(fields)} cols)")


def render_union_overlap(tasy_topic: str, austa_topic: str, field_names: list[str]) -> str:
    """UNION: dois SELECT * com filtro __ts_ms (padrão bronze_tasy_convenio.sql)."""
    raw_tasy = f"{S3A_BASE}/{tasy_topic}/"
    raw_austa = f"{S3A_BASE}/{austa_topic}/"
    canon = raw_austa

    lines = [
        "{{",
        "  config(",
        "    materialized='incremental'",
        "    , schema='bronze'",
        "    , file_format='iceberg'",
        "    , incremental_strategy='append'",
        "    , on_schema_change='append_new_columns'",
        "  )",
        "}}",
        "",
        f'{{% set raw_path = "{canon}" %}}',
        f'{{% set raw_path_tasy = "{raw_tasy}" %}}',
        f'{{% set raw_path_austa = "{raw_austa}" %}}',
        "{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}",
        "{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}",
        "",
        "WITH target_watermark AS (",
        "  {% if is_incremental() %}",
        "    SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms",
        "    FROM {{ this }}",
        "  {% else %}",
        "    SELECT CAST(0 AS BIGINT) AS max_ts_ms",
        "  {% endif %}",
        ")",
        ", params AS (",
        "  SELECT",
        "    CASE",
        "      WHEN {{ cdc_reprocess_hours }} > 0 THEN",
        "        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - ({{ cdc_reprocess_hours }} * 3600)) * 1000",
        "      ELSE",
        "        GREATEST(",
        "          0",
        "          , (SELECT max_ts_ms FROM target_watermark) - ({{ cdc_lookback_hours }} * 3600 * 1000)",
        "        )",
        "    end  AS wm_start_ms",
        ")",
        ", raw_incremental AS (",
        "  SELECT *",
        f"  FROM avro.`{{{{ raw_path_tasy }}}}`",
        "  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)",
        "  UNION ",
        "  SELECT *",
        f"  FROM avro.`{{{{ raw_path_austa }}}}`",
        "  WHERE CAST(COALESCE(__ts_ms, 0) AS BIGINT) >= (SELECT wm_start_ms FROM params)",
        ")",
    ]
    lines.extend(final_select_conta_style_lines(field_names))
    lines.extend(
        [
            "  {{ bronze_audit_columns(raw_path) }}",
            "  , r.__source_txid",
            "FROM raw_incremental r",
            "",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    main()
