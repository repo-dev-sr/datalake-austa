"""Parâmetros padrão de execução CDC / reprocessamento (env + defaults alinhados ao dbt)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from common.constants import CDC_LOOKBACK_HOURS_DEFAULT, CDC_REPROCESS_HOURS_DEFAULT


@dataclass(frozen=True)
class CdcExecutionParams:
    """lookback: janela em ms a partir do MAX(_cdc_ts_ms); reprocess: full desde N horas atrás."""

    lookback_hours: int
    reprocess_hours: int


def get_cdc_execution_params() -> CdcExecutionParams:
    """Lê CDC_LOOKBACK_HOURS e CDC_REPROCESS_HOURS (mesmos nomes do dbt_project / Airflow)."""
    return CdcExecutionParams(
        lookback_hours=int(os.environ.get("CDC_LOOKBACK_HOURS", str(CDC_LOOKBACK_HOURS_DEFAULT))),
        reprocess_hours=int(os.environ.get("CDC_REPROCESS_HOURS", str(CDC_REPROCESS_HOURS_DEFAULT))),
    )
