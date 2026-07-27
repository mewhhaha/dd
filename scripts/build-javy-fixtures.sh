#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
javy="${JAVY_BIN:-$repo_root/target/tools/javy}"
plugin="${JAVY_PLUGIN:-$repo_root/target/tools/dd-javy-plugin.wasm}"
fixtures="$repo_root/crates/javy-host/fixtures"

if [ ! -x "$javy" ]; then
  echo "Javy executable not found at $javy; run scripts/install-javy.sh" >&2
  exit 1
fi

if [ ! -f "$plugin" ]; then
  JAVY_BIN="$javy" "$repo_root/scripts/build-javy-plugin.sh"
fi

JAVY_BIN="$javy" JAVY_PLUGIN="$plugin" pnpm --filter dd-javy-react-example build
cp "$repo_root/examples/javy-react/dist/worker.wasm" "$fixtures/react_worker.wasm"
JAVY_BIN="$javy" JAVY_PLUGIN="$plugin" \
  node "$repo_root/packages/dd-javy/src/cli.js" \
  "$fixtures/instant_worker.js" \
  "$fixtures/instant_worker.wasm"
"$javy" build "$fixtures/infinite_worker.js" \
  -o "$fixtures/infinite_worker.wasm" \
  -C "plugin=$plugin" \
  -C source=compressed

echo "Javy fixtures rebuilt with $("$javy" --version)"
