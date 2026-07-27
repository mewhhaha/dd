#!/usr/bin/env bash
set -euo pipefail

version="9.0.0"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
destination="${JAVY_INSTALL_PATH:-$repo_root/target/tools/javy}"
platform="$(uname -s)"
architecture="$(uname -m)"

case "$platform:$architecture" in
  Linux:x86_64)
    artifact="javy-x86_64-linux-v$version.gz"
    expected_sha256="51a240468da9ebfebeb4292db635e2fab58ea01b9b81832001f780a05dbb744b"
    ;;
  Linux:aarch64 | Linux:arm64)
    artifact="javy-arm-linux-v$version.gz"
    expected_sha256="1ec90c7ada039cab39e79d1377ad72bd80a20c9c07c714741568090fbb870b1a"
    ;;
  Darwin:x86_64)
    artifact="javy-x86_64-macos-v$version.gz"
    expected_sha256="7bb6a868e0fb9015814be67ed6df90fbbe5b515f1bc657e06412a2a4dd690987"
    ;;
  Darwin:arm64)
    artifact="javy-arm-macos-v$version.gz"
    expected_sha256="86e4490a55f47c3fd76966e32edce1bec5e97c2e9d1627697fa8e840785fdd4c"
    ;;
  *)
    echo "unsupported Javy build platform: $platform $architecture" >&2
    exit 1
    ;;
esac

temporary_directory="$(mktemp -d)"
trap 'rm -rf "$temporary_directory"' EXIT
archive="$temporary_directory/$artifact"
url="https://github.com/bytecodealliance/javy/releases/download/v$version/$artifact"
curl -fsSL "$url" -o "$archive"

if command -v sha256sum >/dev/null 2>&1; then
  actual_sha256="$(sha256sum "$archive" | awk '{ print $1 }')"
else
  actual_sha256="$(shasum -a 256 "$archive" | awk '{ print $1 }')"
fi
if [ "$actual_sha256" != "$expected_sha256" ]; then
  echo "Javy archive checksum mismatch: expected $expected_sha256, got $actual_sha256" >&2
  exit 1
fi

mkdir -p "$(dirname "$destination")"
gzip -dc "$archive" > "$destination"
chmod 755 "$destination"
"$destination" --version
