"""Emite bloco YAML mínimo para models bronze (auditoria CDC).

Saída por defeito: stdout. O canónico do projeto é `models/bronze/schema.yml`
(fundido com stubs gerados). Copie o resultado para lá em vez de usar um
ficheiro paralelo, para não duplicar testes.
"""
from __future__ import annotations

import sys

MODELS = [
    ("bronze_tasy_area_procedimento", "AREA_PROCEDIMENTO (austa)"),
    ("bronze_tasy_austa_beneficiario", "AUSTA_BENEFICIARIO"),
    ("bronze_tasy_austa_conta", "AUSTA_CONTA"),
    ("bronze_tasy_austa_mensalidade", "AUSTA_MENSALIDADE"),
    ("bronze_tasy_austa_prestador", "AUSTA_PRESTADOR"),
    ("bronze_tasy_austa_proc_e_mat", "AUSTA_PROC_E_MAT"),
    ("bronze_tasy_austa_requisicao", "AUSTA_REQUISICAO"),
    ("bronze_tasy_cbo_saude", "CBO_SAUDE"),
    ("bronze_tasy_especialidade_proc", "ESPECIALIDADE_PROC"),
    ("bronze_tasy_grau_parentesco", "GRAU_PARENTESCO"),
    ("bronze_tasy_grupo_proc", "GRUPO_PROC"),
    ("bronze_tasy_pessoa_fisica", "PESSOA_FISICA"),
    ("bronze_tasy_pessoa_juridica", "PESSOA_JURIDICA"),
    ("bronze_tasy_pessoa_juridica_compl", "PESSOA_JURIDICA_COMPL"),
    ("bronze_tasy_pls_lote_mensalidade", "PLS_LOTE_MENSALIDADE"),
    ("bronze_tasy_pls_mensalidade", "PLS_MENSALIDADE"),
    ("bronze_tasy_pls_mensalidade_segurado", "PLS_MENSALIDADE_SEGURADO"),
    ("bronze_tasy_pls_rol_grupo_proc", "PLS_ROL_GRUPO_PROC"),
    ("bronze_tasy_pls_rol_procedimento", "PLS_ROL_PROCEDIMENTO"),
    ("bronze_tasy_procedimento", "PROCEDIMENTO (cadastro)"),
    ("bronze_tasy_sus_municipio", "SUS_MUNICIPIO"),
    ("bronze_tasy_tipo_pessoa_juridica", "TIPO_PESSOA_JURIDICA"),
    ("bronze_tasy_tiss_motivo_glosa", "TISS_MOTIVO_GLOSA"),
    ("bronze_tasy_autorizacao_convenio", "AUTORIZACAO_CONVENIO (tasy UNION austa)"),
    ("bronze_tasy_compl_pessoa_fisica", "COMPL_PESSOA_FISICA (tasy UNION austa)"),
    ("bronze_tasy_convenio", "CONVENIO (tasy UNION austa)"),
    ("bronze_tasy_diagnostico_doenca", "DIAGNOSTICO_DOENCA (tasy UNION austa)"),
]

TEMPLATE = """
  - name: {name}
    description: "Bronze TASY {label} - CDC Oracle via Kafka (stream Avro para Iceberg)."
    columns:
      - name: _cdc_op
        description: "Operação CDC (c=create, u=update, d=delete, r=read)"
        tests:
          - accepted_values:
              values: ["c", "u", "d", "r"]
      - name: _cdc_ts_ms
        description: "Timestamp CDC em ms (campo físico __ts_ms no Avro)"
        tests:
          - not_null
      - name: _is_deleted
        description: "Tombstone CDC (boolean)"
      - name: _source_path
        description: "Caminho S3 raw do stream"
"""


def main() -> None:
    out = [
        "# Gerado por dbt/tools/emit_bronze_schema_yml.py — fundir em models/bronze/schema.yml",
        "version: 2",
        "",
        "models:",
    ]
    for name, label in MODELS:
        out.append(TEMPLATE.format(name=name, label=label).rstrip())
    text = "\n".join(out) + "\n"
    dest = sys.stdout
    dest.write(text)


if __name__ == "__main__":
    main()
