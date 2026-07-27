#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
javy="${JAVY_BIN:-$repo_root/target/tools/javy}"
plugin_manifest="$repo_root/packages/dd-javy-plugin/Cargo.toml"
compiled_plugin="$repo_root/packages/dd-javy-plugin/target/wasm32-wasip1/release/dd_javy_plugin.wasm"
initialized_plugin="${JAVY_PLUGIN_OUT:-$repo_root/target/tools/dd-javy-plugin.wasm}"

if [ ! -x "$javy" ]; then
  echo "Javy executable not found at $javy; run scripts/install-javy.sh" >&2
  exit 1
fi

cargo build --manifest-path "$plugin_manifest" --target wasm32-wasip1 --release
mkdir -p "$(dirname "$initialized_plugin")"
"$javy" init-plugin "$compiled_plugin" -o "$initialized_plugin"

echo "Built Javy plugin at $initialized_plugin"
