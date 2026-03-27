#!/usr/bin/env bash
set -euo pipefail

crate="${1:-}"
version="${2:-}"

if [[ -z "$crate" ]]; then
  echo "usage: just patch <crate> [version]" >&2
  exit 1
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "$version" ]]; then
  version="$(
    awk -v crate="$crate" '
      /^\[\[package\]\]$/ { in_pkg = 1; name = ""; version = ""; next }
      in_pkg && /^name = / {
        line = $0
        sub(/^name = "/, "", line)
        sub(/"$/, "", line)
        name = line
        next
      }
      in_pkg && /^version = / {
        line = $0
        sub(/^version = "/, "", line)
        sub(/"$/, "", line)
        version = line
        next
      }
      in_pkg && name == crate && version != "" {
        print version
        exit
      }
    ' "$repo_root/Cargo.lock"
  )"
fi

if [[ -z "$version" ]]; then
  echo "could not resolve version for crate '$crate'; pass an explicit version" >&2
  exit 1
fi

cargo_home="${CARGO_HOME:-$HOME/.cargo}"
source_dir="$(
  find "$cargo_home/registry/src" -mindepth 2 -maxdepth 2 -type d -name "${crate}-${version}" -print -quit 2>/dev/null
)"

if [[ -z "$source_dir" ]]; then
  echo "crate source not found for ${crate}@${version} under $cargo_home/registry/src" >&2
  echo "run 'cargo fetch --locked' first, then retry" >&2
  exit 1
fi

dest_dir="$repo_root/vendor/$crate"
if [[ -d "$dest_dir" && "${PATCH_REFRESH:-0}" != "1" ]]; then
  echo "vendor patch already exists at $dest_dir"
  echo "edit it directly, or run 'just patch-refresh $crate ${version}' to replace it from the registry cache"
  exit 0
fi

rm -rf "$dest_dir"
mkdir -p "$repo_root/vendor"
cp -R "$source_dir" "$dest_dir"

echo "patched ${crate}@${version} into $dest_dir"
