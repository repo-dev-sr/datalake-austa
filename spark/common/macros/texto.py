"""Macros de texto / enum (ex-dbt)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.types import StringType


def fill_null_string(col_name: str, literal: str) -> Column:
    trimmed = f.trim(f.col(col_name).cast(StringType()))
    return f.coalesce(f.when(trimmed == f.lit(""), f.lit(None)).otherwise(trimmed), f.lit(literal))


def standardize_text_initcap(col_name: str, literal_sql_default: str = "indefinido") -> Column:
    lit = literal_sql_default.strip("'")
    base = fill_null_string(col_name, lit)
    return f.initcap(base)


def standardize_enum(col_name: str, literal_sql_default: str = "i") -> Column:
    lit = literal_sql_default.strip("'")
    trimmed = f.trim(f.col(col_name).cast(StringType()))
    return f.when(trimmed.isNull(), f.lit(lit)).otherwise(f.lower(trimmed))
