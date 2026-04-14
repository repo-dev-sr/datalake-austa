"""Bronze TASY ATEND_PACIENTE_UNIDADE — Avro → Iceberg."""

from __future__ import annotations

from common.paths import ensure_spark_root

ensure_spark_root()

from common.bronze_job import run_bronze_append  # noqa: E402
from common.constants import DATALAKE_BUCKET, fq_table  # noqa: E402
from common.run_params import get_cdc_execution_params  # noqa: E402
from common.session import create_spark_session  # noqa: E402
from embedded.bronze.atend_paciente_unidade import PIPELINE_SQL  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402

RAW_AVRO_PATH = (
    f"s3a://{DATALAKE_BUCKET}/raw/raw-tasy/stream/tasy.TASY.ATEND_PACIENTE_UNIDADE/"
)
TARGET_TABLE = fq_table("bronze", "bronze_tasy_atend_paciente_unidade")


def build_bronze(spark: SparkSession, raw: DataFrame) -> DataFrame:
    """Pipeline compilado embutido (paridade dbt); leitura raw explícita via temp view."""
    raw.createOrReplaceTempView("raw_incremental")
    return spark.sql(PIPELINE_SQL)


def main() -> None:
    spark = create_spark_session("bronze_tasy_atend_paciente_unidade")
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
