"""Compat — use `common.silver_pipeline` (run_gold_*)."""

from common.silver_pipeline import (  # noqa: F401
    run_gold_merge_dataframe,
    run_gold_merge_sql,
    run_gold_overwrite_dataframe,
    run_gold_overwrite_sql,
)
