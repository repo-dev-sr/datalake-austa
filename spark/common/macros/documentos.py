"""Documentos (ex-dbt standardize_documents)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.types import StringType


def standardize_documents(col_name: str, expected_length: int) -> Column:
    c = f.col(col_name)
    digits = f.regexp_replace(f.trim(c.cast(StringType())), "[^0-9]", "")
    ln = f.length(digits)
    return (
        f.when(c.isNull(), f.lit(None).cast(StringType()))
        .when(ln == f.lit(expected_length), digits)
        .when((ln >= f.lit(1)) & (ln < f.lit(expected_length)), f.lpad(digits, expected_length, "0"))
        .otherwise(digits)
    )
