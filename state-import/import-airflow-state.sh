#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

tmp_files=()

cleanup() {
  local path

  for path in "${tmp_files[@]:-}"; do
    cleanup_file_in_airflow "${path}"
  done
}

trap cleanup EXIT

variables_file=""
if [[ -f "${STATE_DIR}/variables-export.json" ]]; then
  variables_file="${STATE_DIR}/variables-export.json"
elif [[ -f "${STATE_DIR}/variables.json" ]]; then
  variables_file="${STATE_DIR}/variables.json"
fi

if [[ -n "${variables_file}" ]]; then
  staged_variables_file="$(stage_file_in_airflow "${variables_file}")"
  tmp_files+=("${staged_variables_file}")
  echo "Importing Airflow Variables from ${variables_file}"
  airflow_cli variables import "${staged_variables_file}"
else
  echo "Skipping Variables import: ${STATE_DIR}/variables-export.json not found"
  echo "The Composer variables list is inventory only; rerun collection with EXPORT_AIRFLOW_VARIABLE_VALUES=1 when ready to import values."
fi

if [[ -f "${STATE_DIR}/pools.json" ]]; then
  echo "Importing Airflow Pools from ${STATE_DIR}/pools.json"
  python3 - "${STATE_DIR}/pools.json" <<'PY' | while IFS=$'\t' read -r pool_name slots description include_deferred; do
import json
import pathlib
import sys

pools = json.loads(pathlib.Path(sys.argv[1]).read_text())
if isinstance(pools, dict):
    iterable = []
    for pool_name, values in pools.items():
        row = {"pool": pool_name}
        if isinstance(values, dict):
            row.update(values)
        else:
            row["slots"] = values
        iterable.append(row)
elif isinstance(pools, list):
    iterable = pools
else:
    raise SystemExit(f"Unsupported pools payload type: {type(pools).__name__}")

for row in iterable:
    print(
        "\t".join(
            [
                str(row.get("pool", "")).strip(),
                str(row.get("slots", "")).strip(),
                str(row.get("description", "")).replace("\t", " ").replace("\n", " ").strip(),
                str(row.get("include_deferred", "")).strip(),
            ]
        )
    )
PY
    [[ -n "${pool_name}" ]] || continue

    airflow_cli_no_stdin pools set "${pool_name}" "${slots}" "${description}"

    if [[ "${include_deferred,,}" == "true" ]]; then
      echo "Warning: pool ${pool_name} exported with include_deferred=True; the helper currently recreates the pool without toggling that flag."
    fi
  done
else
  echo "Skipping Pools import: ${STATE_DIR}/pools.json not found"
fi

cat <<'EOF'
Connections and users are intentionally handled as explicit follow-up steps.

See:
  state-import/README.md

Minimum required connections:
  - clickhouse_yral_prod
  - hetzner_s3_logs

Follow-up commands:
  ./state-import/recreate-core-connections.sh
  ./state-import/prepare-airflow-users-csv.sh
  ./state-import/create-airflow-users.sh
  ./state-import/verify-dags-paused.sh
EOF
