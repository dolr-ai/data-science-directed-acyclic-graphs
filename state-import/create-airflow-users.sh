#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

USERS_CSV="${USERS_CSV:-${SCRIPT_DIR}/users.csv}"
RENDERED_ROWS="$(mktemp)"

cleanup() {
  rm -f "${RENDERED_ROWS}"
}

trap cleanup EXIT

require_file "${USERS_CSV}"

python3 - "${USERS_CSV}" "${RENDERED_ROWS}" <<'PY'
import csv
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
required = ["username", "firstname", "lastname", "role", "email", "password"]

with src.open(newline="") as handle:
    reader = csv.DictReader(handle)
    if reader.fieldnames != required:
        raise SystemExit(f"Expected CSV header {required}, got {reader.fieldnames}")

    rows = []
    for row in reader:
        values = {key: (row.get(key) or "").strip() for key in required}
        username = values["username"]

        if not username:
            raise SystemExit("Encountered a user row with an empty username")

        password = values["password"]
        if not password or password == "replace-me":
            raise SystemExit(f"Password is missing or still placeholder for {username}")

        for key, value in values.items():
            if "\t" in value or "\n" in value:
                raise SystemExit(f"{key} for {username} contains a tab or newline")

        rows.append(values)

with dst.open("w", newline="") as handle:
    for row in rows:
        handle.write(
            "\t".join(
                row[key]
                for key in ["username", "firstname", "lastname", "role", "email", "password"]
            )
            + "\n"
        )
PY

while IFS=$'\t' read -r username firstname lastname role email password; do
  [[ -n "${username}" ]] || continue

  airflow_cli_no_stdin users delete --username "${username}" >/dev/null 2>&1 || true
  airflow_cli_no_stdin users create \
    --username "${username}" \
    --firstname "${firstname}" \
    --lastname "${lastname}" \
    --role "${role}" \
    --email "${email}" \
    --password "${password}"
done < "${RENDERED_ROWS}"

echo "Created users from ${USERS_CSV}"
