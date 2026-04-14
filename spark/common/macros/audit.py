"""Colunas de auditoria bronze / silver / silver_context (ex-dbt macros)."""

from __future__ import annotations

import os
import uuid
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from common.constants import TIMEZONE_BR


def add_bronze_audit_columns(
    df: DataFrame,
    raw_path_escaped: str,
    business_cols: Sequence[str],
) -> DataFrame:
    """Auditoria bronze + _row_hash (struct dos business_cols no estado RAW)."""
    biz = [f.col(c) for c in business_cols]
    struct_json = f.to_json(f.struct(*biz))
    row_hash = f.md5(struct_json)
    return (
        df.withColumn("_is_deleted", f.col("__deleted") == f.lit("true"))
        .withColumn("_cdc_op", f.col("__op"))
        .withColumn("_cdc_ts_ms", f.coalesce(f.col("__ts_ms").cast("bigint"), f.lit(0)))
        .withColumn(
            "_cdc_event_at",
            f.to_timestamp(
                f.from_unixtime(f.coalesce(f.col("__ts_ms").cast("double"), f.lit(0.0)) / f.lit(1000.0))
            ),
        )
        .withColumn("_cdc_source_table", f.col("__source_table"))
        .withColumn("_source_path", f.lit(raw_path_escaped))
        .withColumn("_row_hash", row_hash)
        .withColumn("_bronze_loaded_at", f.from_utc_timestamp(f.current_timestamp(), TIMEZONE_BR))
    )


def add_silver_audit_columns(df: DataFrame, invocation_id: str | None = None) -> DataFrame:
    inv = invocation_id or os.environ.get("SPARK_INVOCATION_ID") or str(uuid.uuid4())
    return (
        df.withColumn("_silver_processed_at", f.from_utc_timestamp(f.current_timestamp(), TIMEZONE_BR))
        .withColumn("_dbt_invocation_id", f.lit(inv))
    )


def add_context_audit_columns(df: DataFrame, invocation_id: str | None = None) -> DataFrame:
    inv = invocation_id or os.environ.get("SPARK_INVOCATION_ID") or str(uuid.uuid4())
    return (
        df.withColumn("_context_processed_at", f.from_utc_timestamp(f.current_timestamp(), TIMEZONE_BR))
        .withColumn("_dbt_invocation_id", f.lit(inv))
    )


def rename_audit_for_silver(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("_row_hash", "_bronze_row_hash")
