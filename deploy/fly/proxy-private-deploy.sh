#!/usr/bin/env bash
set -euo pipefail

APP="${1:-dd-private-8956e096}"
LOCAL_PORT="${2:-18081}"
REMOTE_PORT="${3:-8081}"
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
  echo "jq is required for proxy-private-deploy.sh" >&2
  exit 1
fi

machine_json="$("$FLYCTL_BIN" machines list --app "$APP" --json)"
machine_ip="$(printf "%s" "$machine_json" | jq -r '.[] | select(.state == "started") | .private_ip' | head -n1)"

if [ -z "$machine_ip" ] || [ "$machine_ip" = "null" ]; then
  echo "no started machine private_ip found for app: $APP" >&2
  exit 1
fi

echo "Proxying localhost:${LOCAL_PORT} to [${machine_ip}]:${REMOTE_PORT} for app ${APP}"
exec "$FLYCTL_BIN" proxy "${LOCAL_PORT}:${REMOTE_PORT}" "$machine_ip" --app "$APP"
