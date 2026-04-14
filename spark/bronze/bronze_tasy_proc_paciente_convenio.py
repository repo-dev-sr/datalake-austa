"""Bronze TASY PROC_PACIENTE_CONVENIO — Avro → Iceberg (PySpark)."""

from __future__ import annotations

from common.paths import ensure_spark_root

ensure_spark_root()

from common.bronze_job import run_bronze_append  # noqa: E402
from common.constants import DATALAKE_BUCKET, fq_table  # noqa: E402
from common.macros import add_bronze_audit_columns  # noqa: E402
from common.run_params import get_cdc_execution_params  # noqa: E402
from common.session import create_spark_session  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as f  # noqa: E402

# Path Avro explícito (stream Debezium)
RAW_AVRO_PATH = (
    f"s3a://{DATALAKE_BUCKET}/raw/raw-tasy/stream/tasy.TASY.PROC_PACIENTE_CONVENIO/"
)

TARGET_TABLE = fq_table("bronze", "bronze_tasy_proc_paciente_convenio")

BUSINESS_COLS = (
    "dt_atualizacao",
    "nm_usuario",
    "cd_procedimento",
    "ds_procedimento",
    "cd_unidade_medida",
    "tx_conversao_qtde",
    "cd_grupo",
    "nr_proc_interno",
)


def build_bronze(_spark: SparkSession, raw: DataFrame) -> DataFrame:
    core = raw.select(
        f.col("nr_seq_procedimento"),
        f.col("dt_atualizacao").alias("dh_atualizacao"),
        f.col("nm_usuario"),
        f.col("cd_procedimento"),
        f.col("ds_procedimento"),
        f.col("cd_unidade_medida"),
        f.col("tx_conversao_qtde"),
        f.col("cd_grupo"),
        f.col("nr_proc_interno"),
        f.col("__deleted"),
        f.col("__op"),
        f.col("__ts_ms"),
        f.col("__source_table"),
        f.col("__source_txid"),
    )
    return add_bronze_audit_columns(core, RAW_AVRO_PATH, BUSINESS_COLS)


def main() -> None:
    spark = create_spark_session("bronze_tasy_proc_paciente_convenio")
    run_bronze_append(
        spark,
        raw_path=RAW_AVRO_PATH,
        target_fq=TARGET_TABLE,
        build_df=build_bronze,
        params=get_cdc_execution_params(),
    )
    spark.stop()


if __name__ == "__main__":
    main()
