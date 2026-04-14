"""Silver TASY proc_paciente_convenio — merge Iceberg (shape PySpark nativo)."""

from __future__ import annotations

from common.paths import ensure_spark_root

ensure_spark_root()

from common.constants import fq_table  # noqa: E402
from common.session import create_spark_session  # noqa: E402
from common.silver_pipeline import run_silver_dataframe  # noqa: E402
from common.silver_shapes.proc_paciente_convenio import shape_silver_proc_paciente_convenio  # noqa: E402

BRONZE_TABLE = fq_table("bronze", "bronze_tasy_proc_paciente_convenio")
SILVER_TABLE = fq_table("silver", "silver_tasy_proc_paciente_convenio")


def main() -> None:
    spark = create_spark_session("silver_tasy_proc_paciente_convenio")
    run_silver_dataframe(
        spark,
        bronze_fq=BRONZE_TABLE,
        silver_fq=SILVER_TABLE,
        pk="nr_seq_procedimento",
        shape_fn=shape_silver_proc_paciente_convenio,
    )
    spark.stop()


if __name__ == "__main__":
    main()
