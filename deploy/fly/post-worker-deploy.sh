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
      config_json="$(
        printf '%s' "$config_json" | jq --arg binding "${2:-}" '.bindings += [{type: "kv", binding: $binding}]'
      )"
      shift 2
      ;;
    --memory-binding|--actor-binding)
      binding_value="${2:-}"
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
      config_json="$(
        printf '%s' "$config_json" | jq --arg binding "${2:-}" '.bindings += [{type: "dynamic", binding: $binding}]'
      )"
      shift 2
      ;;
    --assets-dir)
      assets_dir="${2:-}"
      if [ -z "$assets_dir" ]; then
        echo "--assets-dir requires a value" >&2
        exit 1
      fi
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

if [ -n "$assets_dir" ]; then
  package_assets_dir "$assets_dir"
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
  --argjson assets "$assets_json" \
  --argjson asset_headers "$asset_headers_json" \
  '{
    name: $name,
    source: $source,
    config: $config,
    assets: $assets,
    asset_headers: $asset_headers
  }' > "$temp_payload"

curl --fail-with-body -sS \
  -H 'content-type: application/json' \
  --data-binary @"$temp_payload" \
  "${SERVER%/}/v1/deploy"
echo
