#!/usr/bin/env bash
# Fetches the web-platform-tests checkout the WPT runner reads: the commit
# pinned in wpt/WPT_SHA, shallow, blob-less and sparse, so only the
# directories below are downloaded. It lands in .cache/wpt (or $WPT_DIR).
#
# The checkout is untrusted data. Nothing in it is executed here; the runner
# (cargo run -p runtime --bin wpt) only reads its files as test input.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
sha="$(tr -d '[:space:]' < "$repo_root/wpt/WPT_SHA")"
dest="${WPT_DIR:-$repo_root/.cache/wpt}"
remote="https://github.com/web-platform-tests/wpt.git"

# What the runner reads. Changing this list changes test inputs, so CI keys
# its cache on this script as well as on the pin.
sparse_paths=(
  "/resources/*.js"
  "/resources/webidl2/lib/"
  "/common/"
  "/interfaces/"
  "/streams/"
  "/fetch/api/headers/"
  "/fetch/api/request/"
  "/fetch/api/response/"
  "/fetch/api/body/"
  "/fetch/api/resources/"
  "/fetch/data-urls/resources/"
  "/url/"
  "/urlpattern/"
  "/encoding/"
  "/WebCryptoAPI/"
  "/dom/abort/"
  "/dom/events/"
  "/FileAPI/"
  "/html/webappapis/atob/"
  "/html/webappapis/microtask-queuing/"
  "/html/webappapis/structured-clone/"
  "/hr-time/"
  "/console/"
)

if ! [[ "$sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "wpt/WPT_SHA must hold a full commit SHA, got: $sha" >&2
  exit 1
fi

stamp="$sha $(printf '%s\n' "${sparse_paths[@]}" | git hash-object --stdin)"
if [[ -f "$dest/.git/dd-wpt-stamp" && "$(cat "$dest/.git/dd-wpt-stamp")" == "$stamp" ]]; then
  echo "wpt: $dest is already at $sha"
  exit 0
fi

echo "wpt: fetching $sha into $dest"
rm -rf "$dest"
mkdir -p "$dest"
git -C "$dest" init --quiet
git -C "$dest" remote add origin "$remote"
printf '%s\n' "${sparse_paths[@]}" | git -C "$dest" sparse-checkout set --no-cone --stdin
git -C "$dest" fetch --quiet --depth 1 --filter=blob:none origin "$sha"
git -C "$dest" -c advice.detachedHead=false checkout --quiet FETCH_HEAD

head="$(git -C "$dest" rev-parse HEAD)"
if [[ "$head" != "$sha" ]]; then
  echo "wpt: checked out $head, expected $sha" >&2
  exit 1
fi
printf '%s\n' "$stamp" > "$dest/.git/dd-wpt-stamp"
echo "wpt: $dest is at $sha"
