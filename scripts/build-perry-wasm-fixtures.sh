#!/usr/bin/env bash
# Regenerates the Perry-compiled wasm fixtures used by crates/wasm-host tests.
# Requires the Perry compiler: npm install -g @perryts/perry (or use npx).
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fixtures="$repo_root/crates/wasm-host/fixtures"
perry="${PERRY:-perry}"

"$perry" compile "$repo_root/examples/perry-wasm-worker/worker.ts" \
  -o "$fixtures/hello_worker.wasm" --target wasm

"$perry" compile "$fixtures/features_worker.ts" \
  -o "$fixtures/features_worker.wasm" --target wasm

echo "fixtures rebuilt with $("$perry" --version)"
