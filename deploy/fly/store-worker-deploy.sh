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

base64_file() {
  local path="$1"
  if base64 --help 2>&1 | grep -q -- '-w'; then
    base64 -w0 "$path"
  else
    base64 < "$path" | tr -d '\n'
  fi
}

package_assets_dir() {
  local dir="$1"
  local assets_tmp
  local headers_tmp
  declare -A seen_paths=()

  if [ ! -d "$dir" ]; then
    echo "assets dir not found: $dir" >&2
    exit 1
  fi

  if ! command -v base64 >/dev/null 2>&1; then
    echo "base64 is required when using --assets-dir" >&2
    exit 1
  fi

  assets_tmp="$(mktemp /tmp/dd-worker-assets.XXXXXX.jsonl)"
  headers_tmp="$(mktemp /tmp/dd-worker-headers.XXXXXX.txt)"

  while IFS= read -r -d '' path; do
    local relative_path
    local request_path
    local encoded_content

    if [ -L "$path" ]; then
      echo "assets dir must not contain symlinks: $path" >&2
      rm -f "$assets_tmp" "$headers_tmp"
      exit 1
    fi

    relative_path="${path#$dir/}"
    if [ -z "$relative_path" ] || [ "$relative_path" = "$path" ]; then
      echo "failed to normalize asset path: $path" >&2
      rm -f "$assets_tmp" "$headers_tmp"
      exit 1
    fi

    if [[ "$relative_path" == ../* ]] || [[ "$relative_path" == *"/../"* ]] || [[ "$relative_path" == *"/.." ]] || [ "$relative_path" = ".." ]; then
      echo "assets dir must not contain traversal-like paths: $relative_path" >&2
      rm -f "$assets_tmp" "$headers_tmp"
      exit 1
    fi

    if [ "$relative_path" = "_headers" ]; then
      cat "$path" > "$headers_tmp"
      continue
    fi

    request_path="/$relative_path"
    if [ -n "${seen_paths[$request_path]+x}" ]; then
      echo "duplicate asset path after normalization: $request_path" >&2
      rm -f "$assets_tmp" "$headers_tmp"
      exit 1
    fi
    seen_paths["$request_path"]=1
    encoded_content="$(base64_file "$path")"
    jq -cn \
      --arg path "$request_path" \
      --arg content_base64 "$encoded_content" \
      '{path: $path, content_base64: $content_base64}' >> "$assets_tmp"
  done < <(find "$dir" -mindepth 1 \( -type f -o -type l \) -print0 | sort -z)

  if [ -s "$assets_tmp" ]; then
    assets_json="$(jq -sc '.' "$assets_tmp")"
  else
    assets_json='[]'
  fi

  if [ -s "$headers_tmp" ]; then
    asset_headers_json="$(jq -Rs . < "$headers_tmp")"
  else
    asset_headers_json='null'
  fi

  rm -f "$assets_tmp" "$headers_tmp"
}

config_json='{"public":false,"bindings":[],"internal":{}}'
assets_json='[]'
asset_headers_json='null'
assets_dir=''
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
    --memory-binding|--actor-binding)
      if [ "$#" -lt 2 ]; then
        echo "--memory-binding requires a value" >&2
        exit 1
      fi
      binding_value="$2"
      if [ -z "$binding_value" ] || [[ "$binding_value" == *=* ]]; then
        echo "invalid memory binding: $binding_value (expected BINDING)" >&2
        exit 1
      fi
      config_json="$(
        printf '%s' "$config_json" \
          | jq --arg binding "$binding_value" \
            '.bindings += [{type: "memory", binding: $binding}]'
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
    --assets-dir)
      if [ "$#" -lt 2 ]; then
        echo "--assets-dir requires a value" >&2
        exit 1
      fi
      assets_dir="$2"
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

if [ -n "$assets_dir" ]; then
  package_assets_dir "$assets_dir"
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
  --argjson assets "$assets_json" \
  --argjson asset_headers "$asset_headers_json" \
  --arg deployment_id "$deployment_id" \
  --argjson updated_at_ms "$updated_at_ms" \
  '{
    name: $name,
    source: $source,
    config: $config,
    assets: $assets,
    asset_headers: $asset_headers,
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
