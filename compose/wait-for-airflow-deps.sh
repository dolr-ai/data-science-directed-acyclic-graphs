#!/usr/bin/env bash
set -euo pipefail

timeout_seconds="${AIRFLOW_DEP_WAIT_TIMEOUT:-3600}"
sleep_seconds="${AIRFLOW_DEP_WAIT_SLEEP:-5}"
deadline=$((SECONDS + timeout_seconds))

python_check='
import os
import sys
import psycopg2
from urllib.parse import urlparse

url = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
if url.startswith("postgresql+psycopg2://"):
    url = "postgresql://" + url[len("postgresql+psycopg2://"):]

parsed = urlparse(url)
conn = psycopg2.connect(
    dbname=parsed.path.lstrip("/"),
    user=parsed.username,
    password=parsed.password,
    host=parsed.hostname,
    port=parsed.port or 5432,
    connect_timeout=5,
)
conn.close()
sys.exit(0)
'

until python -c "${python_check}" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for Airflow metadata DB after ${timeout_seconds}s" >&2
    exit 1
  fi
  echo "Waiting for Airflow metadata DB..." >&2
  sleep "${sleep_seconds}"
done
