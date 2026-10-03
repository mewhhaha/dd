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
  || true
)"

if [[ -z "$matches" ]]; then
  exit 0
fi

unexpected=()
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  [[ "$line" == ./scripts/check_public_memory_naming.sh:* ]] && continue
  content="${line#*:}"
  content="${content#*:}"
  content="${content#"${content%%[![:space:]]*}"}"
  # Only rejection fixtures and the offline converter may name the retired type.
  # Match entire source lines so other code in those files remains checked.
  case "${line%%:*}:$content" in
    './crates/cli/src/main.rs:"--actor-binding",') continue ;;
    './crates/common/src/lib.rs:{ "type": "actor", "binding": "ROOMS" }') continue ;;
    './crates/storage/src/convert.rs:Some("memory" | "actor")') continue ;;
  esac
  unexpected+=("$line")
done <<< "$matches"

if ((${#unexpected[@]} > 0)); then
  printf 'unexpected legacy memory naming drift:\n' >&2
  printf '%s\n' "${unexpected[@]}" >&2
  exit 1
fi
