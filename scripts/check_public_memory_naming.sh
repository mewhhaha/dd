#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

matches="$(
  rg -n --hidden -S '\b[a]ctor\b|\b[A]ctor\b|\b[A][C][T][O][R]\b|[a]ctor_|_[a]ctor|[A]ctor[A-Z]|[A][C][T][O][R]_' \
    . \
    -g '!target/**' \
    -g '!.git/**' \
    -g '!crates/runtime/js/vendor/**' \
    -g '!crates/runtime/src/generated/memory_rpc_capnp.rs' \
  || true
)"

if [[ -z "$matches" ]]; then
  exit 0
fi

unexpected=()
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  case "$line" in
    ./scripts/check_public_memory_naming.sh:*) continue ;;
    ./crates/cli/src/main.rs:*) continue ;;
    ./crates/common/src/lib.rs:*) continue ;;
  esac
  unexpected+=("$line")
done <<< "$matches"

if ((${#unexpected[@]} > 0)); then
  printf 'unexpected legacy memory naming drift:\n' >&2
  printf '%s\n' "${unexpected[@]}" >&2
  exit 1
fi
