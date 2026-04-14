"""Pipeline Silver comum: leitura Iceberg, incremental, dedupe, merge/tombstones."""

from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from common.iceberg_ops import delete_silver_tombstones, merge_into_table, silver_incremental_cutoff_datetime


def dedupe_latest_by_pk(df: DataFrame, pk: str) -> DataFrame:
    """Uma linha por PK: último evento CDC por _cdc_ts_ms / __source_txid."""
    w = Window.partitionBy(f.col(pk)).orderBy(
        f.coalesce(f.col("_cdc_ts_ms"), f.lit(0)).cast("long").desc(),
        f.coalesce(f.col("__source_txid"), f.lit(0)).cast("long").desc(),
    )
    return df.withColumn("_rn", f.row_number().over(w)).filter(f.col("_rn") == 1).drop("_rn")


def run_silver_dataframe(
    spark: SparkSession,
    *,
    bronze_fq: str,
    silver_fq: str,
    pk: str,
    shape_fn: Callable[[DataFrame], DataFrame],
) -> None:
    """shape_fn: latest_by_pk → colunas finais silver (auditoria + filtros já aplicados no SQL legado)."""
    cutoff = silver_incremental_cutoff_datetime(spark, silver_fq)
    base = spark.read.table(bronze_fq).filter(f.col("_bronze_loaded_at") > f.lit(cutoff))
    latest = dedupe_latest_by_pk(base, pk)
    shaped = shape_fn(latest).filter(~f.col("_is_deleted"))
    shaped.createOrReplaceTempView("_silver_merge_src")
    merge_into_table(spark, silver_fq, "_silver_merge_src", pk)
    delete_silver_tombstones(spark, silver_fq, bronze_fq, pk)


def run_silver_pipeline_sql(
    spark: SparkSession,
    *,
    pipeline_sql: str,
    target_fq: str,
    bronze_fq: str,
    pk: str,
) -> None:
    """Pipeline completo embutido em string (paridade 1:1 com SQL gerado; MERGE via Iceberg)."""
    spark.sql(pipeline_sql).createOrReplaceTempView("_silver_merge_src")
    merge_into_table(spark, target_fq, "_silver_merge_src", pk)
    delete_silver_tombstones(spark, target_fq, bronze_fq, pk)


def run_silver_context_dataframe(spark: SparkSession, *, df: DataFrame, target_fq: str) -> None:
    """Overwrite tabela silver_context."""
    df.writeTo(target_fq).createOrReplace()


def run_silver_context_sql(spark: SparkSession, *, pipeline_sql: str, target_fq: str) -> None:
    spark.sql(pipeline_sql).writeTo(target_fq).createOrReplace()


def run_gold_overwrite_dataframe(spark: SparkSession, *, df: DataFrame, target_fq: str) -> None:
    df.writeTo(target_fq).createOrReplace()


def run_gold_merge_dataframe(
    spark: SparkSession,
    *,
    df: DataFrame,
    target_fq: str,
    pk: str,
) -> None:
    df.createOrReplaceTempView("_gold_merge_src")
    merge_into_table(spark, target_fq, "_gold_merge_src", pk)


def run_gold_overwrite_sql(spark: SparkSession, *, pipeline_sql: str, target_fq: str) -> None:
    """Overwrite dimensão Gold a partir de SQL embutido (dim_tempo / dims)."""
    spark.sql(pipeline_sql).writeTo(target_fq).createOrReplace()


def run_gold_merge_sql(spark: SparkSession, *, pipeline_sql: str, target_fq: str, pk: str) -> None:
    """MERGE fato incremental (SQL embutido → view → MERGE Iceberg)."""
    spark.sql(pipeline_sql).createOrReplaceTempView("_gold_merge_src")
    merge_into_table(spark, target_fq, "_gold_merge_src", pk)
