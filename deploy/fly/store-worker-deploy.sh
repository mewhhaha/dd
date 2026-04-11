#!/usr/bin/env bash
set -euo pipefail

APP="${1:-dd-private-8956e096}"
NAME="${2:-}"
FILE="${3:-}"
shift 3 || true

if [ -z "$NAME" ] || [ -z "$FILE" ]; then
  echo "usage: store-worker-deploy.sh <app> <name> <file> [deploy flags...]" >&2
  exit 1
fi

if [ ! -f "$FILE" ]; then
  echo "worker file not found: $FILE" >&2
  exit 1
fi

FLYCTL_BIN="${FLYCTL_BIN:-flyctl}"
if ! command -v "$FLYCTL_BIN" >/dev/null 2>&1; then
  if [ -x "/home/mewhhaha/.fly/bin/flyctl" ]; then
    FLYCTL_BIN="/home/mewhhaha/.fly/bin/flyctl"
  else
    echo "flyctl not found (set FLYCTL_BIN or add flyctl to PATH)" >&2
    exit 1
  fi
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for store-worker-deploy.sh" >&2
  exit 1
fi

if ! command -v uuidgen >/dev/null 2>&1; then
  echo "uuidgen is required for store-worker-deploy.sh" >&2
  exit 1
fi

run_dd_cli() {
  if [ -n "${DD_CLI_BIN:-}" ]; then
    "$DD_CLI_BIN" "$@"
  else
    cargo run -p cli -- "$@"
  fi
}

packaged_payload="$(run_dd_cli package-deploy "$NAME" "$FILE" "$@")"

machine_json="$("$FLYCTL_BIN" machines list --app "$APP" --json)"
machine_id="$(printf '%s' "$machine_json" | jq -r '.[] | select(.state == "started") | .id' | head -n1)"
if [ -z "$machine_id" ] || [ "$machine_id" = "null" ]; then
  echo "no started machine found for app: $APP" >&2
  exit 1
fi

encoded_name="$(printf '%s' "$NAME" | od -An -tx1 -v | tr -d ' \n')"
if [ -z "$encoded_name" ]; then
  encoded_name='00'
fi

temp_file="$(mktemp /tmp/dd-worker-store.XXXXXX.json)"
cleanup() {
  rm -f "$temp_file"
}
trap cleanup EXIT

updated_at_ms="$(date +%s%3N)"
deployment_id="$(uuidgen)"

jq -n \
  --argjson payload "$packaged_payload" \
  --arg deployment_id "$deployment_id" \
  --argjson updated_at_ms "$updated_at_ms" \
  '$payload + {
    deployment_id: $deployment_id,
    updated_at_ms: $updated_at_ms
  }' > "$temp_file"

remote_path="/app/store/workers/${encoded_name}.${updated_at_ms}.${deployment_id}.json"
echo "Uploading ${FILE} to ${APP}:${remote_path}"
"$FLYCTL_BIN" ssh sftp put "$temp_file" "$remote_path" --app "$APP" --machine "$machine_id"

echo "Restarting machine ${machine_id} so dd reloads workers from store"
"$FLYCTL_BIN" machines restart "$machine_id" --app "$APP"

echo "Stored worker deployment updated."
echo "worker=${NAME}"
echo "deployment_id=${deployment_id}"
echo "machine_id=${machine_id}"
