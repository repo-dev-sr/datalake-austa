"""Silver TASY procedimento_paciente — pipeline embutido."""

from __future__ import annotations

from common.paths import ensure_spark_root

ensure_spark_root()

from common.constants import fq_table  # noqa: E402
from common.session import create_spark_session  # noqa: E402
from common.silver_pipeline import run_silver_pipeline_sql  # noqa: E402
from embedded.silver.procedimento_paciente import PIPELINE_SQL  # noqa: E402

BRONZE_TABLE = fq_table("bronze", "bronze_tasy_procedimento_paciente")
SILVER_TABLE = fq_table("silver", "silver_tasy_procedimento_paciente")


def main() -> None:
    spark = create_spark_session("silver_tasy_procedimento_paciente")
    run_silver_pipeline_sql(
        spark,
        pipeline_sql=PIPELINE_SQL,
        target_fq=SILVER_TABLE,
        bronze_fq=BRONZE_TABLE,
        pk="nr_sequencia",
    )
    spark.stop()


if __name__ == "__main__":
    main()
