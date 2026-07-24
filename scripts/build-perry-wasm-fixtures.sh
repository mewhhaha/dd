#!/usr/bin/env bash
# Regenerates the Perry-compiled wasm fixtures used by crates/wasm-host tests.
# Requires the Perry compiler: npm install -g @perryts/perry (or use npx).
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fixtures="$repo_root/crates/wasm-host/fixtures"
perry="${PERRY:-perry}"

"$perry" compile "$repo_root/examples/perry-wasm-worker/worker.ts" \
  -o "$fixtures/hello_worker.wasm" --target wasm

for name in features stateful edge auth chat; do
  "$perry" compile "$fixtures/${name}_worker.ts" \
    -o "$fixtures/${name}_worker.wasm" --target wasm
done

echo "fixtures rebuilt with $("$perry" --version)"
