#!/usr/bin/env bash
set -euo pipefail

DD_BENCH_ONLY=dynamic \
DD_BENCH_DYNAMIC_REQUESTS=100 \
DD_BENCH_DYNAMIC_CONCURRENCY=16 \
DD_BENCH_DYNAMIC_COLD_ROUNDS=10 \
cargo run -p runtime --bin bench --release
