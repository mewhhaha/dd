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

run_dd_cli() {
  if [ -n "${DD_CLI_BIN:-}" ]; then
    "$DD_CLI_BIN" "$@"
  else
    cargo run -p cli -- "$@"
  fi
}

run_dd_cli --server "${SERVER%/}" deploy "$NAME" "$FILE" "$@"
