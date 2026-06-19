#!/usr/bin/env bash
set -euo pipefail

profile="${1:-dist}"
package="dd_server"
binary_name="dd_server"

root="$(git rev-parse --show-toplevel)"
cd "$root"

git_sha="$(git rev-parse HEAD)"
report_dir="target/size-report/$git_sha/$profile"
mkdir -p "$report_dir"

cargo build --locked --profile "$profile" -p "$package"

binary="target/$profile/$binary_name"
if [ ! -f "$binary" ]; then
  echo "built binary not found: $binary" >&2
  exit 1
fi

copy="$report_dir/$binary_name"
cp "$binary" "$copy"
unstripped_bytes="$(stat -c %s "$copy")"

if command -v llvm-strip >/dev/null 2>&1; then
  strip_tool="llvm-strip"
else
  strip_tool="strip"
fi
"$strip_tool" "$copy"
stripped_bytes="$(stat -c %s "$copy")"
sha256="$(sha256sum "$copy" | awk '{print $1}')"

{
  printf '{\n'
  printf '  "git_sha": "%s",\n' "$git_sha"
  printf '  "profile": "%s",\n' "$profile"
  printf '  "target": "%s",\n' "$(rustc -Vv | awk -F': ' '/^host:/ {print $2}')"
  printf '  "rustc": "%s",\n' "$(rustc -V)"
  printf '  "binary": "%s",\n' "$binary"
  printf '  "strip_tool": "%s",\n' "$strip_tool"
  printf '  "unstripped_bytes": %s,\n' "$unstripped_bytes"
  printf '  "stripped_bytes": %s,\n' "$stripped_bytes"
  printf '  "sha256": "%s"\n' "$sha256"
  printf '}\n'
} > "$report_dir/summary.json"

{
  printf '# Binary Size Report\n\n'
  printf -- '- Git SHA: `%s`\n' "$git_sha"
  printf -- '- Profile: `%s`\n' "$profile"
  printf -- '- Target: `%s`\n' "$(rustc -Vv | awk -F': ' '/^host:/ {print $2}')"
  printf -- '- Rustc: `%s`\n' "$(rustc -V)"
  printf -- '- Binary: `%s`\n' "$binary"
  printf -- '- Strip tool: `%s`\n' "$strip_tool"
  printf -- '- Unstripped bytes: `%s`\n' "$unstripped_bytes"
  printf -- '- Stripped bytes: `%s`\n' "$stripped_bytes"
  printf -- '- SHA-256: `%s`\n' "$sha256"
  printf '\n## Profile Settings\n\n'
  sed -n '/^\[profile\.'"$profile"'\]/,/^\[/p' Cargo.toml | sed '${/^\[/d;}'
} > "$report_dir/summary.md"

if command -v llvm-size >/dev/null 2>&1; then
  llvm-size -A "$copy" > "$report_dir/sections.txt"
else
  size -A "$copy" > "$report_dir/sections.txt"
fi

run_optional() {
  local name="$1"
  shift
  if "$@" > "$report_dir/$name.txt" 2>&1; then
    return 0
  fi
  {
    printf 'command failed or package absent:'
    printf ' %q' "$@"
    printf '\n'
    cat "$report_dir/$name.txt"
  } > "$report_dir/$name.tmp"
  mv "$report_dir/$name.tmp" "$report_dir/$name.txt"
}

if command -v cargo-bloat >/dev/null 2>&1; then
  run_optional cargo-bloat-crates cargo bloat --profile "$profile" -p "$package" --crates
  run_optional cargo-bloat-symbols cargo bloat --profile "$profile" -p "$package" -n 100
else
  printf 'cargo-bloat not installed\n' > "$report_dir/cargo-bloat-crates.txt"
  printf 'cargo-bloat not installed\n' > "$report_dir/cargo-bloat-symbols.txt"
fi

if command -v bloaty >/dev/null 2>&1; then
  run_optional bloaty-compileunits bloaty "$copy" -d compileunits
  run_optional bloaty-symbols bloaty "$copy" -d symbols
else
  printf 'bloaty not installed\n' > "$report_dir/bloaty-compileunits.txt"
  printf 'bloaty not installed\n' > "$report_dir/bloaty-symbols.txt"
fi

run_optional cargo-tree-duplicates cargo tree -p "$package" -d
run_optional cargo-tree-features cargo tree -p "$package" -e features

for crate in ring aws-lc-rs openssl boring quinn tonic tonic@0.12.3 tonic@0.14.5 prost@0.13.5 prost@0.14.3 opentelemetry opentelemetry-otlp native-tls hyper-tls reqwest; do
  safe_name="${crate//[^A-Za-z0-9_]/_}"
  run_optional "cargo-tree-inverse-$safe_name" cargo tree -p "$package" -i "$crate"
done

printf 'Wrote %s\n' "$report_dir"
