"""Operações Iceberg: merge incremental e limpeza de tombstones (MERGE/DELETE em SQL mínimo)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException


def merge_into_table(spark: SparkSession, target_fq: str, source_view: str, pk: str) -> None:
    """MERGE INTO target usando view temporária (mesmas colunas que o target)."""
    cols = spark.table(source_view).columns
    if pk not in cols:
        raise ValueError(f"PK {pk} não está em {source_view}")
    update_cols = [c for c in cols if c != pk]
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{c}" for c in cols])
    spark.sql(
        f"""
        MERGE INTO {target_fq} t
        USING {source_view} s
        ON t.{pk} = s.{pk}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def delete_silver_tombstones(spark: SparkSession, silver_fq: str, bronze_fq: str, pk: str) -> None:
    spark.sql(
        f"""
        DELETE FROM {silver_fq}
        WHERE {pk} IN (SELECT {pk} FROM {bronze_fq} WHERE _is_deleted = true)
        """
    )


def max_long_or_zero(spark: SparkSession, fq_table: str, col: str = "_cdc_ts_ms") -> int:
    """MAX(col) numérico (watermark bronze) via DataFrame API."""
    try:
        r = spark.read.table(fq_table).select(f.max(f.col(col)).alias("m")).first()
        v = r["m"] if r else None
        return int(v) if v is not None else 0
    except (AnalysisException, ValueError, TypeError):
        return 0


def silver_incremental_cutoff_datetime(spark: SparkSession, silver_fq: str) -> datetime:
    """MAX(_silver_processed_at) ou 1900-01-01 para primeiro load incremental."""
    try:
        r = spark.read.table(silver_fq).agg(f.max("_silver_processed_at").alias("m")).first()
        v = r["m"] if r else None
        return v if v is not None else datetime(1900, 1, 1)
    except Exception:
        return datetime(1900, 1, 1)


def gold_incremental_cutoff_datetime(spark: SparkSession, gold_fq: str) -> datetime:
    try:
        r = spark.read.table(gold_fq).agg(f.max("_gold_loaded_at").alias("m")).first()
        v = r["m"] if r else None
        return v if v is not None else datetime(1900, 1, 1)
    except Exception:
        return datetime(1900, 1, 1)
