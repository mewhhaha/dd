#!/usr/bin/env bash
set -euo pipefail

SERVER="${1:-http://127.0.0.1:18081}"
NAME="${2:-}"
FILE="${3:-}"
shift 3 || true

if [ -z "$NAME" ] || [ -z "$FILE" ]; then
  echo "usage: post-worker-deploy.sh <server> <name> <file> [deploy flags...]" >&2
  exit 1
fi

if [ ! -f "$FILE" ]; then
  echo "worker file not found: $FILE" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for post-worker-deploy.sh" >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required for post-worker-deploy.sh" >&2
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
      config_json="$(
        printf '%s' "$config_json" | jq --arg binding "${2:-}" '.bindings += [{type: "kv", binding: $binding}]'
      )"
      shift 2
      ;;
    --actor-binding)
      binding_value="${2:-}"
      if [ -z "$binding_value" ] || [[ "$binding_value" == *=* ]]; then
        echo "invalid actor binding: $binding_value (expected BINDING)" >&2
        exit 1
      fi
      config_json="$(
        printf '%s' "$config_json" \
          | jq --arg binding "$binding_value" \
            '.bindings += [{type: "actor", binding: $binding}]'
      )"
      shift 2
      ;;
    --dynamic-binding)
      config_json="$(
        printf '%s' "$config_json" | jq --arg binding "${2:-}" '.bindings += [{type: "dynamic", binding: $binding}]'
      )"
      shift 2
      ;;
    --trace-worker)
      trace_worker="${2:-}"
      shift 2
      ;;
    --trace-path)
      trace_path="${2:-}"
      shift 2
      ;;
    *)
      echo "unsupported deploy flag for post-worker-deploy.sh: $1" >&2
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

temp_payload="$(mktemp /tmp/dd-worker-deploy.XXXXXX.json)"
cleanup() {
  rm -f "$temp_payload"
}
trap cleanup EXIT

jq -n \
  --arg name "$NAME" \
  --rawfile source "$FILE" \
  --argjson config "$config_json" \
  '{
    name: $name,
    source: $source,
    config: $config
  }' > "$temp_payload"

curl --fail-with-body -sS \
  -H 'content-type: application/json' \
  --data-binary @"$temp_payload" \
  "${SERVER%/}/v1/deploy"
echo
