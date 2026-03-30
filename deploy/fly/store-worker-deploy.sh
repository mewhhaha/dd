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

config_json='{"public":false,"bindings":[],"internal":{}}'
trace_worker=''
trace_path='/ingest'

while [ "$#" -gt 0 ]; do
  case "$1" in
    --public)
      config_json="$(printf '%s' "$config_json" | jq '.public = true')"
      shift
      ;;
    --kv-binding)
      if [ "$#" -lt 2 ]; then
        echo "--kv-binding requires a value" >&2
        exit 1
      fi
      config_json="$(
        printf '%s' "$config_json" | jq --arg binding "$2" '.bindings += [{type: "kv", binding: $binding}]'
      )"
      shift 2
      ;;
    --actor-binding)
      if [ "$#" -lt 2 ]; then
        echo "--actor-binding requires a value" >&2
        exit 1
      fi
      binding_value="$2"
      if [[ "$binding_value" != *=* ]]; then
        echo "invalid actor binding: $binding_value (expected BINDING=ClassName)" >&2
        exit 1
      fi
      binding_name="${binding_value%%=*}"
      class_name="${binding_value#*=}"
      if [ -z "$binding_name" ] || [ -z "$class_name" ]; then
        echo "invalid actor binding: $binding_value (expected BINDING=ClassName)" >&2
        exit 1
      fi
      config_json="$(
        printf '%s' "$config_json" \
          | jq --arg binding "$binding_name" --arg class "$class_name" \
            '.bindings += [{type: "actor", binding: $binding, class: $class}]'
      )"
      shift 2
      ;;
    --dynamic-binding)
      if [ "$#" -lt 2 ]; then
        echo "--dynamic-binding requires a value" >&2
        exit 1
      fi
      config_json="$(
        printf '%s' "$config_json" | jq --arg binding "$2" '.bindings += [{type: "dynamic", binding: $binding}]'
      )"
      shift 2
      ;;
    --trace-worker)
      if [ "$#" -lt 2 ]; then
        echo "--trace-worker requires a value" >&2
        exit 1
      fi
      trace_worker="$2"
      shift 2
      ;;
    --trace-path)
      if [ "$#" -lt 2 ]; then
        echo "--trace-path requires a value" >&2
        exit 1
      fi
      trace_path="$2"
      shift 2
      ;;
    *)
      echo "unsupported deploy flag for store-worker-deploy.sh: $1" >&2
      exit 1
      ;;
  esac
done

if [ -n "$trace_worker" ]; then
  config_json="$(
    printf '%s' "$config_json" \
      | jq --arg worker "$trace_worker" --arg path "$trace_path" \
        '.internal.trace = {worker: $worker, path: $path}'
  )"
fi

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
  --arg name "$NAME" \
  --rawfile source "$FILE" \
  --argjson config "$config_json" \
  --arg deployment_id "$deployment_id" \
  --argjson updated_at_ms "$updated_at_ms" \
  '{
    name: $name,
    source: $source,
    config: $config,
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
