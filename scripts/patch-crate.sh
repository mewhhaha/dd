#!/usr/bin/env bash
set -euo pipefail

crate="${1:-}"
version="${2:-}"

if [[ -z "$crate" ]]; then
  echo "usage: just patch <crate> [version]" >&2
  exit 1
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

download_crate_source() {
  local crate="$1"
  local version="$2"
  local output_dir="$3"
  local tmp_dir archive extract_dir

  tmp_dir="$(mktemp -d)"
  archive="$tmp_dir/${crate}-${version}.crate"
  extract_dir="$tmp_dir/extract"
  trap 'rm -rf "$tmp_dir"' RETURN

  mkdir -p "$extract_dir"
  curl -L --fail --show-error --silent \
    "https://crates.io/api/v1/crates/${crate}/${version}/download" \
    -o "$archive"
  tar -xzf "$archive" -C "$extract_dir"
  cp -R "$extract_dir/${crate}-${version}" "$output_dir"
}

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
source_dir=""
if [[ -d "$cargo_home/registry/src" ]]; then
  source_dir="$(
    find "$cargo_home/registry/src" -mindepth 2 -maxdepth 2 -type d -name "${crate}-${version}" -print -quit 2>/dev/null
  )"
fi

dest_dir="$repo_root/patched-crates/$crate"
if [[ -d "$dest_dir" && "${PATCH_REFRESH:-0}" != "1" ]]; then
  echo "patched crate already exists at $dest_dir"
  echo "edit it, then run 'just patch-save $crate ${version}' to refresh patches/${crate}.patch"
  echo "or run 'just patch-refresh $crate ${version}' to replace it from crates.io source and reapply the saved patch"
  exit 0
fi

rm -rf "$dest_dir"
mkdir -p "$repo_root/patched-crates"

if [[ -n "$source_dir" ]]; then
  cp -R "$source_dir" "$dest_dir"
else
  echo "crate source not found under $cargo_home/registry/src; downloading ${crate}@${version}" >&2
  download_crate_source "$crate" "$version" "$dest_dir"
fi

patch_file="$repo_root/patches/${crate}.patch"
if [[ -f "$patch_file" ]]; then
  patch -d "$dest_dir" -p1 < "$patch_file"
  echo "applied $patch_file"
fi

echo "materialized ${crate}@${version} into $dest_dir"
