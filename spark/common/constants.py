"""Constantes do lakehouse (bucket, catálogo Glue, paths raw)."""

# Glue + Iceberg
CATALOG = "glue_catalog"

DATALAKE_BUCKET = "austa-lakehouse-prod-data-lake-169446931765"
RAW_PREFIX = f"s3a://{DATALAKE_BUCKET}/raw/raw-tasy/stream"

TIMEZONE_BR = "America/Sao_Paulo"

# Vars CDC (alinhado ao dbt_project.yml)
CDC_LOOKBACK_HOURS_DEFAULT = 2
CDC_REPROCESS_HOURS_DEFAULT = 0

# Surrogate key — texto nulo padrão do dbt_utils
SURROGATE_KEY_NULL = "_dbt_utils_surrogate_key_null_"

# Paths Avro por tópico (subpasta após stream/)
RAW_SUBPATHS = {
    "atend_paciente_unidade": "tasy.TASY.ATEND_PACIENTE_UNIDADE",
    "atendimento_paciente": "tasy.TASY.ATENDIMENTO_PACIENTE",
    "conta_paciente": "tasy.TASY.CONTA_PACIENTE",
    "proc_paciente_convenio": "tasy.TASY.PROC_PACIENTE_CONVENIO",
    "proc_paciente_valor": "tasy.TASY.PROC_PACIENTE_VALOR",
    "procedimento_paciente": "tasy.TASY.PROCEDIMENTO_PACIENTE",
}


def raw_avro_path(entity_key: str) -> str:
    """Retorna s3a://.../stream/<tópico>/"""
    sub = RAW_SUBPATHS[entity_key]
    return f"{RAW_PREFIX}/{sub}/"


def fq_table(schema: str, table: str) -> str:
    """Retorna glue_catalog.schema.table"""
    return f"{CATALOG}.{schema}.{table}"
