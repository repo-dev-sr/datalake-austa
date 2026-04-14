#!/usr/bin/env bash
set -euo pipefail
aws s3 ls s3://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/tasy.TASY.PROCEDIMENTO_PACIENTE/ --recursive 2>/dev/null \
  | awk '{sum+=$3; cnt++} END{printf "files=%d avg_kb=%.1f total_gb=%.2f\n", cnt, sum/cnt/1024, sum/1024/1024/1024}'
