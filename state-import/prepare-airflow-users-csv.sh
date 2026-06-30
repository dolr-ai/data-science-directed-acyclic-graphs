#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

USERS_JSON="${USERS_JSON:-${STATE_DIR}/users.json}"
USERS_CSV="${USERS_CSV:-${SCRIPT_DIR}/users.csv}"
FORCE=0

if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
elif [[ $# -gt 0 ]]; then
  die "Usage: $0 [--force]"
fi

require_file "${USERS_JSON}"

if [[ -f "${USERS_CSV}" && "${FORCE}" -ne 1 ]]; then
  die "${USERS_CSV} already exists; rerun with --force to overwrite it"
fi

python3 - "${USERS_JSON}" "${USERS_CSV}" <<'PY'
import csv
import json
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
users = json.loads(src.read_text())

with dst.open("w", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=["username", "firstname", "lastname", "role", "email", "password"],
    )
    writer.writeheader()

    for user in users:
        roles = user.get("roles") or ["Op"]
        raw_username = user.get("username", "")
        email = user.get("email", "")
        username = raw_username

        # Composer's Google-auth backend exports usernames like accounts.google.com:<id>.
        # For self-hosted Airflow with local auth, use the operator email as the login name.
        if raw_username.startswith("accounts.google.com:") and email:
            username = email
        elif not username and email:
            username = email

        first_name = user.get("first_name", "")
        if first_name == email and email:
            first_name = email.split("@", 1)[0]

        writer.writerow(
            {
                "username": username,
                "firstname": first_name,
                "lastname": user.get("last_name", ""),
                "role": roles[0],
                "email": email,
                "password": "replace-me",
            }
        )
PY

echo "Wrote ${USERS_CSV}"
echo "Fill real passwords before running ./state-import/create-airflow-users.sh"
