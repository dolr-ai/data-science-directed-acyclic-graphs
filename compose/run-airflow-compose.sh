#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHARED_ENV_FILE="${SCRIPT_DIR}/shared.env"
NODE_ENV_FILE="${SCRIPT_DIR}/node.env"

if [[ ! -f "${SHARED_ENV_FILE}" ]]; then
  echo "Missing ${SHARED_ENV_FILE}"
  exit 1
fi

if [[ ! -f "${NODE_ENV_FILE}" ]]; then
  echo "Missing ${NODE_ENV_FILE}"
  exit 1
fi

exec docker compose \
  --env-file "${SHARED_ENV_FILE}" \
  --env-file "${NODE_ENV_FILE}" \
  -f "${SCRIPT_DIR}/docker-compose.airflow-ha.yml" \
  "$@"
