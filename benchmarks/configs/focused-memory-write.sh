#!/usr/bin/env bash
set -euo pipefail

DD_BENCH_MODE=direct-write-memory \
DD_BENCH_REQUESTS=300 \
DD_BENCH_CONCURRENCY=16 \
DD_BENCH_MAX_ISOLATES=4 \
DD_BENCH_MAX_INFLIGHT=8 \
cargo run -p runtime --bin bench_memory_storage --release
