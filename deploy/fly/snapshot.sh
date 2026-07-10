#!/usr/bin/env bash
set -euo pipefail

app="${1:?usage: snapshot.sh APP VOLUME_ID [LOCAL_PRIVATE_PORT]}"
volume_id="${2:?usage: snapshot.sh APP VOLUME_ID [LOCAL_PRIVATE_PORT]}"
local_port="${3:-18081}"
admin_token="${DD_PRIVATE_TOKEN:-${PRIVATE_BEARER_TOKEN:-}}"

if [ -z "$admin_token" ]; then
  echo "DD_PRIVATE_TOKEN (or PRIVATE_BEARER_TOKEN) is required" >&2
  exit 1
fi

if [ -n "${FLYCTL_BIN:-}" ]; then
  flyctl="$FLYCTL_BIN"
elif command -v flyctl >/dev/null 2>&1; then
  flyctl="$(command -v flyctl)"
elif command -v fly >/dev/null 2>&1; then
  flyctl="$(command -v fly)"
else
  echo "flyctl was not found; set FLYCTL_BIN" >&2
  exit 1
fi

proxy_pid=""
draining=0
ready=0
admin_url="http://127.0.0.1:${local_port}"

admin_post() {
  local path="$1"
  curl --fail --silent --show-error \
    --header "authorization: Bearer ${admin_token}" \
    --request POST \
    "${admin_url}${path}"
}

cleanup() {
  local exit_status=$?
  if [ "$draining" = 1 ]; then
    if ! admin_post /v1/admin/resume >/dev/null; then
      echo "warning: failed to resume writes after snapshot attempt" >&2
      exit_status=1
    fi
  fi
  if [ -n "$proxy_pid" ]; then
    kill "$proxy_pid" >/dev/null 2>&1 || true
    wait "$proxy_pid" >/dev/null 2>&1 || true
  fi
  exit "$exit_status"
}
trap cleanup EXIT INT TERM

"$flyctl" proxy "${local_port}:8081" --app "$app" >/tmp/dd-fly-snapshot-proxy.log 2>&1 &
proxy_pid=$!

for _ in $(seq 1 60); do
  if curl --fail --silent --show-error "${admin_url}/healthz" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "$proxy_pid" >/dev/null 2>&1; then
    cat /tmp/dd-fly-snapshot-proxy.log >&2
    echo "fly proxy stopped before the admin endpoint became available" >&2
    exit 1
  fi
  sleep 1
done

if [ "$ready" != 1 ]; then
  cat /tmp/dd-fly-snapshot-proxy.log >&2
  echo "admin endpoint did not become available" >&2
  exit 1
fi

draining=1
admin_post /v1/admin/drain
admin_post /v1/admin/checkpoint
"$flyctl" volumes snapshots create "$volume_id"
admin_post /v1/admin/resume
draining=0

echo "snapshot scheduled for ${volume_id}; service writes resumed"
