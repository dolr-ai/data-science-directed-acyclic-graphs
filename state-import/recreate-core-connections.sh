#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

: "${CLICKHOUSE_PASSWORD:?Set CLICKHOUSE_PASSWORD}"
: "${HETZNER_S3_ACCESS_KEY:?Set HETZNER_S3_ACCESS_KEY}"
: "${HETZNER_S3_SECRET_KEY:?Set HETZNER_S3_SECRET_KEY}"
: "${HETZNER_S3_ENDPOINT_URL:?Set HETZNER_S3_ENDPOINT_URL}"

GCP_KEY_PATH="${GCP_KEY_PATH:-/opt/airflow/secrets/gcp-service-account.json}"
GCP_PROJECT="${GCP_PROJECT:-hot-or-not-feed-intelligence}"
GCP_CONNECTION_IDS="${GCP_CONNECTION_IDS:-google_cloud_default google_cloud_storage_default google_cloud_datastore_default bigquery_default}"

airflow_cli connections delete clickhouse_yral_prod >/dev/null 2>&1 || true
airflow_cli connections add clickhouse_yral_prod \
  --conn-type generic \
  --conn-host 127.0.0.1 \
  --conn-port 8443 \
  --conn-schema yral \
  --conn-login airflow_writer \
  --conn-password "${CLICKHOUSE_PASSWORD}" \
  --conn-extra '{"secure": true, "verify": false}'

airflow_cli connections delete hetzner_s3_logs >/dev/null 2>&1 || true
airflow_cli connections add hetzner_s3_logs \
  --conn-type aws \
  --conn-login "${HETZNER_S3_ACCESS_KEY}" \
  --conn-password "${HETZNER_S3_SECRET_KEY}" \
  --conn-extra "{\"endpoint_url\": \"${HETZNER_S3_ENDPOINT_URL}\", \"region_name\": \"fsn1\"}"

for conn_id in ${GCP_CONNECTION_IDS}; do
  airflow_cli connections delete "${conn_id}" >/dev/null 2>&1 || true
  airflow_cli connections add "${conn_id}" \
    --conn-type google_cloud_platform \
    --conn-extra "{\"extra__google_cloud_platform__project\": \"${GCP_PROJECT}\", \"extra__google_cloud_platform__key_path\": \"${GCP_KEY_PATH}\", \"extra__google_cloud_platform__scope\": \"https://www.googleapis.com/auth/cloud-platform\"}"
done
