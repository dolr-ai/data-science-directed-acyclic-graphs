#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HAPROXY_CFG="${HAPROXY_CFG:-/etc/haproxy/haproxy.cfg}"
HOST_MAP="${HOST_MAP:-/etc/haproxy/maps/host2backend.map}"
SNIPPET_TEMPLATE="${SCRIPT_DIR}/airflow-ingress-only.cfg"
MAP_ENTRY="airflow.ansuman.yral.com be_airflow_web"
BEGIN_MARKER="# BEGIN YRAL AIRFLOW INGRESS"
END_MARKER="# END YRAL AIRFLOW INGRESS"

die() {
  echo "Error: $*" >&2
  exit 1
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    die "Run this script as root, for example: sudo $0"
  fi
}

require_file() {
  local path="$1"
  [[ -f "${path}" ]] || die "Missing ${path}"
}

detect_tailscale_ip() {
  ip -o -4 addr show tailscale0 | awk '{print $4}' | cut -d/ -f1 | head -n1
}

require_root
require_file "${HAPROXY_CFG}"
require_file "${HOST_MAP}"
require_file "${SNIPPET_TEMPLATE}"

TAILSCALE_NODE_IP="${TAILSCALE_NODE_IP:-$(detect_tailscale_ip)}"
[[ -n "${TAILSCALE_NODE_IP}" ]] || die "Could not detect tailscale0 IPv4 address; set TAILSCALE_NODE_IP explicitly"

TMP_SNIPPET="$(mktemp)"
TMP_CFG="$(mktemp)"
cleanup() {
  rm -f "${TMP_SNIPPET}" "${TMP_CFG}"
}
trap cleanup EXIT

sed "s/\${TAILSCALE_NODE_IP}/${TAILSCALE_NODE_IP}/g" "${SNIPPET_TEMPLATE}" > "${TMP_SNIPPET}"

cp "${HAPROXY_CFG}" "${TMP_CFG}"

if ! grep -Fq "${BEGIN_MARKER}" "${TMP_CFG}"; then
  {
    echo
    echo "${BEGIN_MARKER}"
    cat "${TMP_SNIPPET}"
    echo "${END_MARKER}"
  } >> "${TMP_CFG}"
fi

if ! grep -Fxq "${MAP_ENTRY}" "${HOST_MAP}"; then
  echo "${MAP_ENTRY}" >> "${HOST_MAP}"
fi

haproxy -c -f "${TMP_CFG}"
cp "${TMP_CFG}" "${HAPROXY_CFG}"
systemctl reload haproxy

echo "Airflow ingress config applied for ${TAILSCALE_NODE_IP}"
echo "Validating local bridge health:"
curl -sf http://127.0.0.1:18081/health
echo
