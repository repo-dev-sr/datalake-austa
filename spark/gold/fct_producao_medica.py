"""Gold fct_producao_medica — merge incremental."""

from __future__ import annotations

from common.paths import ensure_spark_root

ensure_spark_root()

from common.constants import fq_table  # noqa: E402
from common.gold_job import run_gold_merge_sql  # noqa: E402
from common.session import create_spark_session  # noqa: E402
from embedded.gold.fct_producao_medica import PIPELINE_SQL  # noqa: E402


def main() -> None:
    spark = create_spark_session("gold_fct_producao_medica")
    run_gold_merge_sql(
        spark,
        pipeline_sql=PIPELINE_SQL,
        target_fq=fq_table("gold", "fct_producao_medica"),
        pk="sk_producao",
    )
    spark.stop()


if __name__ == "__main__":
    main()
