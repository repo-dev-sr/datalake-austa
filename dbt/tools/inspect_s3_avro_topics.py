"""Inspeciona schema Avro no S3 (raiz do record) por tópico — uso one-off."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import fastavro

BUCKET = "austa-lakehouse-prod-data-lake-169446931765"
PREFIX_BASE = "raw/raw-tasy/stream"

TOPICS = [
    "austa.TASY.AREA_PROCEDIMENTO",
    "austa.TASY.AUSTA_BENEFICIARIO",
    "austa.TASY.AUSTA_CONTA",
    "austa.TASY.AUSTA_MENSALIDADE",
    "austa.TASY.AUSTA_PRESTADOR",
    "austa.TASY.AUSTA_PROC_E_MAT",
    "austa.TASY.AUSTA_REQUISICAO",
    "austa.TASY.AUTORIZACAO_CONVENIO",
    "austa.TASY.CBO_SAUDE",
    "austa.TASY.COMPL_PESSOA_FISICA",
    "austa.TASY.CONVENIO",
    "austa.TASY.DIAGNOSTICO_DOENCA",
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


def latest_avro_key(topic: str) -> str | None:
    prefix = f"{PREFIX_BASE}/{topic}/"
    token = None
    best: tuple[str, str] | None = None
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
            print(f"ERR list {prefix}: {e.output}", file=sys.stderr)
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


def root_field_names(schema: dict) -> list[str]:
    return [f["name"] for f in (schema.get("fields") or [])]


def main() -> None:
    tmp = Path(tempfile.mkdtemp())
    for topic in TOPICS:
        key = latest_avro_key(topic)
        if not key:
            print(f"{topic}\tNO_FILE")
            continue
        dest = tmp / "x.avro"
        subprocess.check_call(
            ["aws", "s3", "cp", f"s3://{BUCKET}/{key}", str(dest)],
            stdout=subprocess.DEVNULL,
        )
        with dest.open("rb") as fh:
            reader = fastavro.reader(fh)
            sch = reader.writer_schema
        names = root_field_names(sch)
        kind = "ENVELOPE" if {"before", "after"}.issubset(set(names)) else "FLAT"
        ts = "__ts_ms" if "__ts_ms" in names else ("ts_ms" if "ts_ms" in names else "?")
        print(f"{topic}\t{kind}\tcdc_col={ts}\troot_fields={','.join(names[:20])}{'...' if len(names)>20 else ''}")


if __name__ == "__main__":
    main()
