"""Macros numéricas (ex-dbt) — apenas Column API."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f


def fill_null_bigint(col_name: str, sentinel: int = -1) -> Column:
    return f.coalesce(f.col(col_name).cast("bigint"), f.lit(sentinel))


def normalize_decimal(col_name: str, scale: int = 2, sentinel: float = -1.0) -> Column:
    return f.coalesce(
        f.round(f.col(col_name).cast("decimal(38, 6)"), scale),
        f.lit(sentinel).cast("decimal(38, 2)"),
    )
