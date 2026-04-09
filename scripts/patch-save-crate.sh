#!/usr/bin/env bash
set -euo pipefail

crate="${1:-}"
version="${2:-}"

if [[ -z "$crate" ]]; then
  echo "usage: just patch-save <crate> [version]" >&2
  exit 1
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
dest_dir="$repo_root/patched-crates/$crate"

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

if [[ ! -d "$dest_dir" ]]; then
  echo "patched crate not found at $dest_dir" >&2
  echo "run 'just patch $crate ${version}' first" >&2
  exit 1
fi

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

pristine_tmp_dir=""
tmp_file=""
if [[ -z "$source_dir" ]]; then
  pristine_tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_file" "$pristine_tmp_dir"' EXIT
  echo "crate source not found under $cargo_home/registry/src; downloading ${crate}@${version}" >&2
  download_crate_source "$crate" "$version" "$pristine_tmp_dir/${crate}-${version}"
  source_dir="$pristine_tmp_dir/${crate}-${version}"
fi

patch_dir="$repo_root/patches"
patch_file="$patch_dir/${crate}.patch"
tmp_file="$(mktemp)"
if [[ -z "$pristine_tmp_dir" ]]; then
  trap 'rm -f "$tmp_file"' EXIT
fi

mkdir -p "$patch_dir"

mapfile -t files < <(
  {
    cd "$source_dir" && find . -type f | sed 's#^\./##'
    cd "$dest_dir" && find . -type f | sed 's#^\./##'
  } | sort -u
)

for rel in "${files[@]}"; do
  left="$source_dir/$rel"
  right="$dest_dir/$rel"
  if [[ -f "$left" && -f "$right" ]] && cmp -s "$left" "$right"; then
    continue
  fi

  if [[ -f "$left" && -f "$right" ]]; then
    diff -u --label "a/$rel" --label "b/$rel" "$left" "$right" >> "$tmp_file" || true
  elif [[ -f "$left" ]]; then
    diff -u --label "a/$rel" --label "b/$rel" "$left" /dev/null >> "$tmp_file" || true
  else
    diff -u --label "a/$rel" --label "b/$rel" /dev/null "$right" >> "$tmp_file" || true
  fi
done

if [[ ! -s "$tmp_file" ]]; then
  rm -f "$patch_file"
  echo "no patch needed for ${crate}@${version}; removed $patch_file if it existed"
  exit 0
fi

mv "$tmp_file" "$patch_file"
echo "wrote $patch_file"
