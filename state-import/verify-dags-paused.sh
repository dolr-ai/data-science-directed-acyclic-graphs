#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

EXPORT_DAGS_JSON="${EXPORT_DAGS_JSON:-${STATE_DIR}/dags-list.json}"
LIVE_DAGS_JSON="$(mktemp)"
ENFORCE=0

cleanup() {
  rm -f "${LIVE_DAGS_JSON}"
}

trap cleanup EXIT

if [[ "${1:-}" == "--enforce" ]]; then
  ENFORCE=1
  shift
fi

if [[ $# -gt 0 ]]; then
  die "Usage: $0 [--enforce]"
fi

if [[ "${ENFORCE}" -eq 1 ]]; then
  echo "Pausing all DAGs via airflow dags pause --treat-dag-id-as-regex '.*'"
  airflow_cli_no_stdin dags pause --treat-dag-id-as-regex --yes '.*' >/dev/null
fi

compose_exec python - <<'PY' > "${LIVE_DAGS_JSON}"
import json

from airflow.models.dag import DagModel
from airflow.settings import Session

with Session() as session:
    rows = (
        session.query(DagModel.dag_id, DagModel.is_paused)
        .order_by(DagModel.dag_id.asc())
        .all()
    )

print(json.dumps([{"dag_id": dag_id, "is_paused": is_paused} for dag_id, is_paused in rows]))
PY

python3 - "${EXPORT_DAGS_JSON}" "${LIVE_DAGS_JSON}" <<'PY'
import json
import pathlib
import sys

export_path = pathlib.Path(sys.argv[1])
live_path = pathlib.Path(sys.argv[2])
live_rows = json.loads(live_path.read_text())

def normalize_paused(row):
    if "is_paused" in row:
        return bool(row["is_paused"])
    if "paused" in row:
        return bool(row["paused"])
    raise SystemExit(f"Missing pause column in live DAG row: {row}")

live_by_id = {}
for row in live_rows:
    dag_id = row.get("dag_id")
    if not dag_id:
        raise SystemExit(f"Missing dag_id in live DAG row: {row}")
    live_by_id[dag_id] = normalize_paused(row)

missing = []
extra = []
if export_path.exists():
    export_rows = json.loads(export_path.read_text())
    expected_ids = {row["dag_id"] for row in export_rows}
    live_ids = set(live_by_id)
    missing = sorted(expected_ids - live_ids)
    extra = sorted(live_ids - expected_ids)

unpaused = sorted(dag_id for dag_id, paused in live_by_id.items() if not paused)

print(f"Live DAG count: {len(live_by_id)}")
if export_path.exists():
    print(f"Export DAG count: {len(expected_ids)}")

if missing:
    print("Missing live DAGs relative to export:")
    for dag_id in missing:
        print(f"  - {dag_id}")

if extra:
    print("Unexpected live DAGs not present in export:")
    for dag_id in extra:
        print(f"  - {dag_id}")

if unpaused:
    print("Unpaused DAGs:")
    for dag_id in unpaused:
        print(f"  - {dag_id}")

if missing or extra or unpaused:
    raise SystemExit(1)

print("All live DAGs are present and paused.")
PY
