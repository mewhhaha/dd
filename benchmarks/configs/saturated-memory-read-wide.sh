#!/usr/bin/env bash
set -euo pipefail

DD_BENCH_MODE=direct-read-memory-wide \
DD_BENCH_REQUESTS=20000 \
DD_BENCH_CONCURRENCY=256 \
DD_BENCH_MIN_ISOLATES=16 \
DD_BENCH_MAX_ISOLATES=16 \
DD_BENCH_MAX_INFLIGHT=16 \
DD_BENCH_WIDE_KEY_SPACE=4096 \
DD_BENCH_MEMORY_KEY_MODE=cross-shard \
cargo run -p runtime --bin bench_memory_storage --release
