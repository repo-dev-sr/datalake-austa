"""Compat: reexporta `common.macros` (macros dbt → PySpark). Prefer `from common.macros import ...`."""

from common.macros import *  # noqa: F401,F403
