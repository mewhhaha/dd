#!/usr/bin/env bash
set -euo pipefail

package_name="${1:-}"
profile="${DD_RUNTIME_PROFILE:-dev-runtime}"
target="${DD_RUNTIME_TARGET:-}"

if [ -z "$package_name" ]; then
  package_name="$(node -e 'console.log(require("./packages/dd-runtime").runtimePackageName())')"
fi

case "$package_name" in
  @dd/runtime-linux-x64)
    default_target="x86_64-unknown-linux-gnu"
    binary_name="dd_dev_runtime"
    ;;
  @dd/runtime-linux-arm64)
    default_target="aarch64-unknown-linux-gnu"
    binary_name="dd_dev_runtime"
    ;;
  @dd/runtime-darwin-x64)
    default_target="x86_64-apple-darwin"
    binary_name="dd_dev_runtime"
    ;;
  @dd/runtime-darwin-arm64)
    default_target="aarch64-apple-darwin"
    binary_name="dd_dev_runtime"
    ;;
  @dd/runtime-win32-x64)
    default_target="x86_64-pc-windows-msvc"
    binary_name="dd_dev_runtime.exe"
    ;;
  *)
    echo "unsupported runtime package: $package_name" >&2
    exit 1
    ;;
esac

package_dir="packages/dd-${package_name#@dd/}"
if [ ! -d "$package_dir" ]; then
  echo "runtime package directory not found: $package_dir" >&2
  exit 1
fi

cargo_args=(build --profile "$profile" -p runtime --bin dd_dev_runtime)
if [ -n "$target" ]; then
  cargo_args+=(--target "$target")
elif [ "$(rustc -vV | awk '/host:/ { print $2 }')" != "$default_target" ]; then
  target="$default_target"
  cargo_args+=(--target "$target")
fi

cargo "${cargo_args[@]}"

if [ -n "$target" ]; then
  built_binary="target/$target/$profile/$binary_name"
else
  built_binary="target/$profile/$binary_name"
fi

if [ ! -f "$built_binary" ]; then
  echo "built runtime binary not found: $built_binary" >&2
  exit 1
fi

mkdir -p "$package_dir/bin"
cp "$built_binary" "$package_dir/bin/$binary_name"
chmod 755 "$package_dir/bin/$binary_name"

if command -v strip >/dev/null 2>&1 && [ "$binary_name" = "dd_dev_runtime" ]; then
  strip "$package_dir/bin/$binary_name" 2>/dev/null || true
fi

bytes="$(wc -c < "$package_dir/bin/$binary_name" | tr -d '[:space:]')"
echo "Wrote $package_dir/bin/$binary_name ($bytes bytes)"
