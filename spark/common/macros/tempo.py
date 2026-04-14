"""Macros de data/timestamp (ex-dbt)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f
def standardize_date(col_name: str) -> Column:
    c = f.col(col_name)
    d = f.try_cast(c, "date")
    return (
        f.when(c.isNull(), f.to_date(f.lit("1900-01-01")))
        .when(d.isNull(), f.to_date(f.lit("1900-01-01")))
        .when(f.year(d) >= f.lit(2900), f.to_date(f.lit("1900-01-01")))
        .otherwise(d)
    )


def standardize_timestamp(col_name: str) -> Column:
    c = f.col(col_name)
    t = f.try_cast(c, "timestamp")
    return (
        f.when(c.isNull(), f.to_timestamp(f.lit("1900-01-01 00:00:00")))
        .when(t.isNull(), f.to_timestamp(f.lit("1900-01-01 00:00:00")))
        .when(f.year(t) >= f.lit(2900), f.to_timestamp(f.lit("1900-01-01 00:00:00")))
        .otherwise(t)
    )
