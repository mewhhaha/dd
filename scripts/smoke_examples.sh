#!/usr/bin/env bash
set -euo pipefail

scheme="${SCHEME:-https}"
base_domain="${BASE_DOMAIN:-wdyt.chat}"

fetch_text() {
  local method="$1"
  local url="$2"
  local data="$3"
  shift 3
  if [[ -n "$data" ]]; then
    curl --silent --show-error --fail --max-time 10 -X "$method" "$url" "$@" --data "$data"
  else
    curl --silent --show-error --fail --max-time 10 -X "$method" "$url" "$@"
  fi
}

check_contains() {
  local name="$1"
  local body="$2"
  local needle="$3"
  if [[ "$body" == *"$needle"* ]]; then
    echo "PASS $name"
  else
    echo "FAIL $name"
    echo "  expected substring: $needle"
    echo "  body: $body"
    return 1
  fi
}

check_regex() {
  local name="$1"
  local body="$2"
  local regex="$3"
  if [[ "$body" =~ $regex ]]; then
    echo "PASS $name"
  else
    echo "FAIL $name"
    echo "  expected regex: $regex"
    echo "  body: $body"
    return 1
  fi
}

url() {
  local worker="$1"
  local path="$2"
  printf '%s://%s.%s%s' "$scheme" "$worker" "$base_domain" "$path"
}

check_contains "hello root" "$(fetch_text GET "$(url hello /)" "")" "hello from worker"
check_contains "router health" "$(fetch_text GET "$(url router /health)" "")" "/health"
check_contains "cache root" "$(fetch_text GET "$(url cache /)" "")" "fresh: /"
check_contains "named-cache root" "$(fetch_text GET "$(url named-cache /)" "")" "named cache miss: /"
check_contains "cache-vary greet" "$(fetch_text GET "$(url cache-vary /greet)" "" -H 'accept-language: fr')" "salut"
check_contains "cache-delete root" "$(fetch_text GET "$(url cache-delete /)" "")" "deleted=true"
check_contains "stream root" "$(fetch_text GET "$(url stream /)" "")" "hello world"
check_regex "kv root" "$(fetch_text GET "$(url kv /)" "")" '^hits=[0-9]+$'
check_contains "kv-counter root docs" "$(fetch_text GET "$(url kv-counter /)" "")" ""routes""
check_regex "kv-counter value" "$(fetch_text GET "$(url kv-counter /value)" "")" '^[0-9]+$'
check_contains "bg root" "$(fetch_text GET "$(url bg /)" "")" "response sent first"
check_contains "bg-kv root docs" "$(fetch_text GET "$(url bg-kv /)" "")" ""wait-until-kv""
check_contains "bg-kv request" "$(fetch_text GET "$(url bg-kv /)" "" -H 'x-request-id: smoke-script')" "queued:smoke-script"
check_contains "memory root docs" "$(fetch_text GET "$(url memory /)" "")" ""worker":"memory-namespace""
check_contains "memory ping" "$(fetch_text GET "$(url memory '/ping?user=smoke')" "")" ""ok":true"
check_contains "receipts root" "$(fetch_text GET "$(url receipts /)" "")" ""receipts worker""
check_contains "trace-hub root" "$(fetch_text GET "$(url trace-hub /)" "")" ""routes""
check_contains "trace-sink root" "$(fetch_text GET "$(url trace-sink /)" "")" "worker traces receiver"
check_contains "hello-traced root" "$(fetch_text GET "$(url hello-traced /)" "")" "hello-traced"
check_contains "dynamic root" "$(fetch_text GET "$(url dynamic /)" "")" "dynamic namespace demo"
check_contains "dynamic run" "$(fetch_text GET "$(url dynamic '/run?path=/hello')" "")" ""ok":true"
check_contains "llm root" "$(fetch_text GET "$(url llm-dynamic /)" "")" "pretend-llm dynamic executor"
check_contains "llm run" "$(fetch_text GET "$(url llm-dynamic '/run?prompt=echo&input=hello')" "")" ""output":"HELLO""
check_contains "preview root" "$(fetch_text GET "$(url preview-dynamic /)" "")" "Dynamic Preview Manager"
check_contains "preview health" "$(fetch_text GET "$(url preview-dynamic '/preview/pr-123/api/health')" "")" ""ok":true"
check_contains "swr root" "$(fetch_text GET "$(url swr-site /)" "")" "dd SWR demo"
check_contains "bundled-router root" "$(fetch_text GET "$(url bundled-router /)" "")" ""service":"bundled-router""
