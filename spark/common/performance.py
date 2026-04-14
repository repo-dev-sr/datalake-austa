"""Boas práticas de performance PySpark no lakehouse (uso opcional nas camadas)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def repartition_by_pk(df: DataFrame, pk: str, n: int | None = None) -> DataFrame:
    """Redistribui por PK antes de MERGE/joins pesados (n default = shuffle partitions da sessão)."""
    if n is not None:
        return df.repartition(n, f.col(pk))
    return df.repartition(f.col(pk))


def cache_if_reused(df: DataFrame, do_cache: bool) -> DataFrame:
    """Persiste em memória se o mesmo DF for consumido mais de uma vez no driver."""
    return df.cache() if do_cache else df
