"""Pipeline Bronze: Avro S3 → transformação DataFrame → append Iceberg."""

from __future__ import annotations

import time
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException

from common.iceberg_ops import max_long_or_zero
from common.run_params import CdcExecutionParams


def wm_start_ms(spark: SparkSession, fq_table: str, params: CdcExecutionParams) -> int:
    """wm_start_ms alinhado ao dbt (params CTE)."""
    if params.reprocess_hours and params.reprocess_hours > 0:
        return int((time.time() - params.reprocess_hours * 3600) * 1000)
    mx = max_long_or_zero(spark, fq_table, "_cdc_ts_ms")
    return max(0, mx - params.lookback_hours * 3600 * 1000)


def read_avro_incremental_df(spark: SparkSession, raw_path: str, wm_start: int) -> DataFrame:
    """Lê Avro no path (diretório) com filtro CDC __ts_ms >= wm_start."""
    return spark.read.format("avro").load(raw_path).where(
        f.coalesce(f.col("__ts_ms").cast("long"), f.lit(0)) >= f.lit(wm_start)
    )


def run_bronze_append(
    spark: SparkSession,
    *,
    raw_path: str,
    target_fq: str,
    build_df: Callable[[SparkSession, DataFrame], DataFrame],
    params: CdcExecutionParams,
) -> None:
    """build_df: raw incremental → linhas finais bronze (PySpark; tabelas grandes podem usar SQL embutido)."""
    wm = wm_start_ms(spark, target_fq, params)
    raw = read_avro_incremental_df(spark, raw_path, wm)
    out = build_df(spark, raw)
    out.writeTo(target_fq).append()
