#!/usr/bin/env bash
# Local (non-Docker) launch used by the README quick start:
#   ./run_local.sh            -> http://localhost:8000/docs, MLLP 2575
#   ./run_local.sh 8000 2575
set -euo pipefail
HTTP_PORT="${1:-8000}"
MLLP_PORT="${2:-2575}"
export LIS_DB_PATH="${LIS_DB_PATH:-./data/lis.db}"
export LIS_MLLP_HOST="${LIS_MLLP_HOST:-0.0.0.0}"
export LIS_MLLP_PORT="$MLLP_PORT"
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port "$HTTP_PORT"
