"""Silver-Context procedimento — overwrite Iceberg."""

from __future__ import annotations

from common.paths import ensure_spark_root

ensure_spark_root()

from common.constants import fq_table  # noqa: E402
from common.session import create_spark_session  # noqa: E402
from common.silver_pipeline import run_silver_context_sql  # noqa: E402
from embedded.silver_context.procedimento import PIPELINE_SQL  # noqa: E402

TARGET = fq_table("silver_context", "procedimento")


def main() -> None:
    spark = create_spark_session("silver_context_procedimento")
    run_silver_context_sql(spark, pipeline_sql=PIPELINE_SQL, target_fq=TARGET)
    spark.stop()


if __name__ == "__main__":
    main()
