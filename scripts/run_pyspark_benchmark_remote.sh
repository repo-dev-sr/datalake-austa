#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1

echo "RUN_START=$(date '+%Y-%m-%d %H:%M:%S')"
/usr/bin/time -f 'ELAPSED_REAL=%e' /opt/spark/bin/spark-submit /home/ec2-user/benchmark_pyspark_procedimento_paciente.py "$@"
echo "RUN_END=$(date '+%Y-%m-%d %H:%M:%S')"
