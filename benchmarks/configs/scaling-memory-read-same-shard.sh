#!/usr/bin/env bash
set -euo pipefail

DD_BENCH_MODE=direct-read-memory-wide \
DD_BENCH_REQUESTS=12000 \
DD_BENCH_CONCURRENCY=192 \
DD_BENCH_MIN_ISOLATES=16 \
DD_BENCH_MAX_ISOLATES=16 \
DD_BENCH_MAX_INFLIGHT=16 \
DD_BENCH_WIDE_KEY_SPACE=2048 \
DD_BENCH_MEMORY_NAMESPACE_SHARDS=16 \
DD_BENCH_MEMORY_KEY_MODE=same-shard \
cargo run -p runtime --bin bench_memory_storage --release
