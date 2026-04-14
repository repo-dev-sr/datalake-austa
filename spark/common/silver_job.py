"""Compat — use `common.silver_pipeline` (run_silver_dataframe / run_silver_pipeline_sql)."""

from common.silver_pipeline import (  # noqa: F401
    dedupe_latest_by_pk,
    run_silver_dataframe,
    run_silver_pipeline_sql,
)
