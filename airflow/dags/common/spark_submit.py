"""
Monta comando bash: SSH na EC2 Spark + spark-submit do script PySpark.

Usa SPARK_MASTER_URL (docker-compose na EC2 Airflow → IP privado da EC2 Spark).
"""

from __future__ import annotations

import shlex

from common.config import (
    SPARK_HOST,
    SPARK_MASTER_URL,
    SPARK_SSH_KEY,
    SPARK_SSH_USER,
    SPARK_SCRIPTS_DIR,
    SPARK_SUBMIT_BIN,
)


def spark_submit_ssh_bash(*, script_relative: str, app_name: str) -> str:
    """script_relative: ex. spark/bronze/bronze_tasy_conta_paciente.py (sob SPARK_SCRIPTS_DIR)."""
    remote_py = f"{SPARK_SCRIPTS_DIR.rstrip('/')}/{script_relative.lstrip('/')}"
    remote_cmd = (
        f"cd {SPARK_SCRIPTS_DIR} && "
        f"export PYTHONPATH={SPARK_SCRIPTS_DIR} && "
        f"{SPARK_SUBMIT_BIN} "
        f"--name {shlex.quote(app_name)} "
        f"--master {shlex.quote(SPARK_MASTER_URL)} "
        f"--deploy-mode client "
        f"{shlex.quote(remote_py)}"
    )
    return (
        f"ssh -o StrictHostKeyChecking=no -i {shlex.quote(SPARK_SSH_KEY)} "
        f"{shlex.quote(SPARK_SSH_USER + '@' + SPARK_HOST)} {shlex.quote(remote_cmd)}"
    )
