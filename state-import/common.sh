#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-${SCRIPT_DIR}/../composer-export/state}"
COMPOSE_WRAPPER="${COMPOSE_WRAPPER:-${SCRIPT_DIR}/../compose/run-airflow-compose.sh}"
AIRFLOW_SERVICE="${AIRFLOW_SERVICE:-airflow-scheduler}"
AIRFLOW_TMP_DIR="${AIRFLOW_TMP_DIR:-/tmp/airflow-state-import}"

die() {
  echo "Error: $*" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "${path}" ]] || die "Missing ${path}"
}

require_compose_wrapper() {
  [[ -x "${COMPOSE_WRAPPER}" ]] || die "Missing executable ${COMPOSE_WRAPPER}"
}

compose_exec() {
  require_compose_wrapper
  "${COMPOSE_WRAPPER}" exec -T "${AIRFLOW_SERVICE}" "$@"
}

compose_exec_no_stdin() {
  require_compose_wrapper
  "${COMPOSE_WRAPPER}" exec -T "${AIRFLOW_SERVICE}" "$@" < /dev/null
}

airflow_cli() {
  compose_exec airflow "$@"
}

airflow_cli_no_stdin() {
  compose_exec_no_stdin airflow "$@"
}

stage_file_in_airflow() {
  local src="$1"
  local dst="${AIRFLOW_TMP_DIR}/$(basename "${src}")"

  require_file "${src}"
  compose_exec bash -lc "mkdir -p '${AIRFLOW_TMP_DIR}' && cat > '${dst}'" < "${src}"
  printf '%s\n' "${dst}"
}

cleanup_file_in_airflow() {
  local path="$1"
  compose_exec bash -lc "rm -f '${path}'" >/dev/null 2>&1 || true
}
