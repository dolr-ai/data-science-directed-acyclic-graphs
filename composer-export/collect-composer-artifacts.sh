#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ENV_NAME="${1:-data-pipeline-orchestrator}"
LOCATION="${2:-us-central1}"
OUT_DIR="${3:-${SCRIPT_DIR}}"

mkdir -p \
  "${OUT_DIR}/dags" \
  "${OUT_DIR}/plugins" \
  "${OUT_DIR}/package-inventory" \
  "${OUT_DIR}/state"

clean_json_after_first_char() {
  local raw_file="$1"
  local clean_file="$2"
  local start_char="$3"
  python3 -c "import json,pathlib,sys; raw=pathlib.Path(sys.argv[1]); clean=pathlib.Path(sys.argv[2]); start=sys.argv[3]; s=raw.read_text(); dec=json.JSONDecoder(); found=False
for i, ch in enumerate(s):
    if ch != start:
        continue
    try:
        data, end = dec.raw_decode(s[i:])
    except json.JSONDecodeError:
        continue
    if s[i + end:].strip():
        continue
    clean.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')
    found=True
    break
if not found:
    raise SystemExit(f'No clean JSON document starting with {start!r} found in {raw}')" \
    "${raw_file}" "${clean_file}" "${start_char}"
}

echo "Exporting Composer DAGs from ${ENV_NAME} (${LOCATION})"
gcloud composer environments storage dags export \
  --environment "${ENV_NAME}" \
  --location "${LOCATION}" \
  --destination "${OUT_DIR}/dags"

echo "Exporting Composer plugins from ${ENV_NAME} (${LOCATION})"
gcloud composer environments storage plugins export \
  --environment "${ENV_NAME}" \
  --location "${LOCATION}" \
  --destination "${OUT_DIR}/plugins"

echo "Saving environment description and custom PyPI package config"
gcloud composer environments describe "${ENV_NAME}" \
  --location "${LOCATION}" \
  --format=json \
  > "${OUT_DIR}/package-inventory/environment-describe.json"

echo "Saving Airflow DAG inventory"
gcloud composer environments run "${ENV_NAME}" \
  --location "${LOCATION}" \
  dags list -- --output=json \
  > "${OUT_DIR}/state/dags-list.raw" 2>&1
clean_json_after_first_char \
  "${OUT_DIR}/state/dags-list.raw" \
  "${OUT_DIR}/state/dags-list.json" \
  "["

echo "Saving Airflow Variable names"
gcloud composer environments run "${ENV_NAME}" \
  --location "${LOCATION}" \
  variables list -- --output=json \
  > "${OUT_DIR}/state/variables-list.raw" 2>&1
clean_json_after_first_char \
  "${OUT_DIR}/state/variables-list.raw" \
  "${OUT_DIR}/state/variables-list.json" \
  "["

echo "Saving Airflow Pools"
gcloud composer environments run "${ENV_NAME}" \
  --location "${LOCATION}" \
  pools list -- --output=json \
  > "${OUT_DIR}/state/pools.raw" 2>&1
clean_json_after_first_char \
  "${OUT_DIR}/state/pools.raw" \
  "${OUT_DIR}/state/pools.json" \
  "["

echo "Saving Airflow users"
gcloud composer environments run "${ENV_NAME}" \
  --location "${LOCATION}" \
  users list -- --output=json \
  > "${OUT_DIR}/state/users.raw" 2>&1
clean_json_after_first_char \
  "${OUT_DIR}/state/users.raw" \
  "${OUT_DIR}/state/users.json" \
  "["

if [[ "${EXPORT_AIRFLOW_VARIABLE_VALUES:-0}" == "1" ]]; then
  echo "Saving Airflow Variables export with values"
  variables_export_file="selfhosted-variables-export.json"
  gcloud composer environments run "${ENV_NAME}" \
    --location "${LOCATION}" \
    variables export -- "/home/airflow/gcs/data/${variables_export_file}" \
    > "${OUT_DIR}/state/variables-export.raw" 2>&1
  gcloud composer environments storage data export \
    --environment "${ENV_NAME}" \
    --location "${LOCATION}" \
    --source "${variables_export_file}" \
    --destination "${OUT_DIR}/state"
  mv "${OUT_DIR}/state/${variables_export_file}" "${OUT_DIR}/state/variables-export.json"
  python3 -m json.tool "${OUT_DIR}/state/variables-export.json" >/dev/null
  gcloud composer environments storage data delete "${variables_export_file}" \
    --environment "${ENV_NAME}" \
    --location "${LOCATION}" \
    --quiet \
    >/dev/null || true
else
  echo "Skipping Airflow Variables value export. Set EXPORT_AIRFLOW_VARIABLE_VALUES=1 to opt in."
fi

if [[ "${EXPORT_CONNECTION_SECRETS:-0}" == "1" ]]; then
  echo "Saving Airflow connections export with secrets"
  connections_export_file="selfhosted-connections-export.json"
  gcloud composer environments run "${ENV_NAME}" \
    --location "${LOCATION}" \
    connections export -- "/home/airflow/gcs/data/${connections_export_file}" \
    > "${OUT_DIR}/state/connections-export.raw" 2>&1
  gcloud composer environments storage data export \
    --environment "${ENV_NAME}" \
    --location "${LOCATION}" \
    --source "${connections_export_file}" \
    --destination "${OUT_DIR}/state"
  mv "${OUT_DIR}/state/${connections_export_file}" "${OUT_DIR}/state/connections-export.json"
  python3 -m json.tool "${OUT_DIR}/state/connections-export.json" >/dev/null
  gcloud composer environments storage data delete "${connections_export_file}" \
    --environment "${ENV_NAME}" \
    --location "${LOCATION}" \
    --quiet \
    >/dev/null || true
else
  echo "Skipping Airflow connections secret export. Set EXPORT_CONNECTION_SECRETS=1 to opt in."
fi

cat <<EOF
Composer artifacts collected under:
  ${OUT_DIR}

Important:
  - If you set EXPORT_AIRFLOW_VARIABLE_VALUES=1, ${OUT_DIR}/state/variables-export.json may contain secrets.
  - If you set EXPORT_CONNECTION_SECRETS=1, ${OUT_DIR}/state/connections-export.json contains connection secrets.
  - Do not paste variable or connection export output/files into chat.
  - After this finishes, tell me only that the export is complete and I will read the staged files from the workspace.
EOF
