"""Surrogate key (ex-dbt_utils.generate_surrogate_key)."""

from __future__ import annotations

from typing import List, Sequence

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from common.constants import SURROGATE_KEY_NULL


def generate_surrogate_key(col_names: Sequence[str]) -> Column:
    parts: List[Column] = []
    for name in col_names:
        parts.append(f.coalesce(f.col(name).cast(StringType()), f.lit(SURROGATE_KEY_NULL)))
    return f.md5(f.concat_ws(f.lit("||"), *parts))
