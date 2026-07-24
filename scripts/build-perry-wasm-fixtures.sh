#!/usr/bin/env bash
# Regenerates the Perry-compiled wasm fixtures used by crates/runtime tests.
# Requires the Perry compiler: npm install -g @perryts/perry (or use npx).
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fixtures="$repo_root/crates/runtime/fixtures"
perry="${PERRY:-perry}"

"$perry" compile "$repo_root/examples/hello.ts" \
  -o "$fixtures/hello_worker.wasm" --target wasm

for name in features stateful edge auth chat async; do
  "$perry" compile "$fixtures/${name}_worker.ts" \
    -o "$fixtures/${name}_worker.wasm" --target wasm
done

echo "fixtures rebuilt with $("$perry" --version)"
