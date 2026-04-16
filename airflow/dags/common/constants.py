"""
Constants for DAGs: schemas, buckets, layer names, table prefixes.
Data lake layers: raw, bronze, silver, silver_context, gold.
"""
from airflow.datasets import Dataset

# Schemas (Glue / Spark)
RAW_SCHEMA = "raw"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
SILVER_CONTEXT_SCHEMA = "silver_context"
GOLD_SCHEMA = "gold"

# Paths S3
RAW_TASY_STREAM_PREFIX = "raw/raw_tasy/stream/"
RAW_TASY_BATCH_PREFIX = "raw/raw_tasy/batch/"
RAW_TASY_STREAM_TOPIC_PREFIX = "raw/raw-tasy/stream/"
RAW_TASY_STREAM_FILE_EXTENSION = ".avro"

# Bucket
DATA_LAKE_BUCKET = "austa-lakehouse-prod-data-lake-169446931765"

# Airflow Datasets
RAW_TASY_STREAM_DATASET = Dataset("dataset://raw_tasy_stream")
RAW_TASY_BATCH_DATASET = Dataset("dataset://raw_tasy_batch")

# Mapeamento tópico Kafka/S3 → rótulo (valor não usado em get_dataset_for_topic; chave deve existir)
TOPIC_DATASET_MAPPING = {
    # tasy — operacional / transacional
    "tasy.TASY.ATENDIMENTO_PACIENTE": "tasy.ATENDIMENTO_PACIENTE",
    "tasy.TASY.ATEND_PACIENTE_UNIDADE": "tasy.ATEND_PACIENTE_UNIDADE",
    "tasy.TASY.AUTORIZACAO_CONVENIO": "tasy.AUTORIZACAO_CONVENIO",
    "tasy.TASY.COMPL_PESSOA_FISICA": "tasy.COMPL_PESSOA_FISICA",
    "tasy.TASY.CONTA_PACIENTE": "tasy.CONTA_PACIENTE",
    "tasy.TASY.CONVENIO": "tasy.CONVENIO",
    "tasy.TASY.DIAGNOSTICO_DOENCA": "tasy.DIAGNOSTICO_DOENCA",
    "tasy.TASY.PROC_PACIENTE_CONVENIO": "tasy.PROC_PACIENTE_CONVENIO",
    "tasy.TASY.PROC_PACIENTE_VALOR": "tasy.PROC_PACIENTE_VALOR",
    "tasy.TASY.PROCEDIMENTO_PACIENTE": "tasy.PROCEDIMENTO_PACIENTE",
    # austa — cadastros / dimensões / PLS
    "austa.TASY.AREA_PROCEDIMENTO": "austa.AREA_PROCEDIMENTO",
    "austa.TASY.AUSTA_BENEFICIARIO": "austa.AUSTA_BENEFICIARIO",
    "austa.TASY.AUSTA_CONTA": "austa.AUSTA_CONTA",
    "austa.TASY.AUSTA_MENSALIDADE": "austa.AUSTA_MENSALIDADE",
    "austa.TASY.AUSTA_PRESTADOR": "austa.AUSTA_PRESTADOR",
    "austa.TASY.AUSTA_PROC_E_MAT": "austa.AUSTA_PROC_E_MAT",
    "austa.TASY.AUSTA_REQUISICAO": "austa.AUSTA_REQUISICAO",
    "austa.TASY.AUTORIZACAO_CONVENIO": "austa.AUTORIZACAO_CONVENIO",
    "austa.TASY.CBO_SAUDE": "austa.CBO_SAUDE",
    "austa.TASY.COMPL_PESSOA_FISICA": "austa.COMPL_PESSOA_FISICA",
    "austa.TASY.CONVENIO": "austa.CONVENIO",
    "austa.TASY.DIAGNOSTICO_DOENCA": "austa.DIAGNOSTICO_DOENCA",
    "austa.TASY.ESPECIALIDADE_PROC": "austa.ESPECIALIDADE_PROC",
    "austa.TASY.GRAU_PARENTESCO": "austa.GRAU_PARENTESCO",
    "austa.TASY.GRUPO_PROC": "austa.GRUPO_PROC",
    "austa.TASY.PESSOA_FISICA": "austa.PESSOA_FISICA",
    "austa.TASY.PESSOA_JURIDICA": "austa.PESSOA_JURIDICA",
    "austa.TASY.PESSOA_JURIDICA_COMPL": "austa.PESSOA_JURIDICA_COMPL",
    "austa.TASY.PLS_LOTE_MENSALIDADE": "austa.PLS_LOTE_MENSALIDADE",
    "austa.TASY.PLS_MENSALIDADE": "austa.PLS_MENSALIDADE",
    "austa.TASY.PLS_MENSALIDADE_SEGURADO": "austa.PLS_MENSALIDADE_SEGURADO",
    "austa.TASY.PLS_ROL_GRUPO_PROC": "austa.PLS_ROL_GRUPO_PROC",
    "austa.TASY.PLS_ROL_PROCEDIMENTO": "austa.PLS_ROL_PROCEDIMENTO",
    "austa.TASY.PROCEDIMENTO": "austa.PROCEDIMENTO",
    "austa.TASY.SUS_MUNICIPIO": "austa.SUS_MUNICIPIO",
    "austa.TASY.TIPO_PESSOA_JURIDICA": "austa.TIPO_PESSOA_JURIDICA",
    "austa.TASY.TISS_MOTIVO_GLOSA": "austa.TISS_MOTIVO_GLOSA",
}


def get_dataset_for_topic(topic: str) -> Dataset:
    """Retorna o Dataset Airflow para o tópico dado."""
    dataset_name = TOPIC_DATASET_MAPPING.get(topic)
    if not dataset_name:
        raise ValueError(f"Tópico '{topic}' não está no TOPIC_DATASET_MAPPING")
    path_prefix = f"{RAW_TASY_STREAM_TOPIC_PREFIX}{topic}/"
    return Dataset(f"s3://{DATA_LAKE_BUCKET}/{path_prefix}")
