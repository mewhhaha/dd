#!/usr/bin/env bash
set -euo pipefail

DD_BENCH_INTERNAL_CONFIG=sat-16 \
DD_BENCH_INTERNAL_SCENARIO=instant-text \
DD_BENCH_CUSTOM_NAME=sat-16 \
DD_BENCH_MIN_ISOLATES=16 \
DD_BENCH_MAX_ISOLATES=16 \
DD_BENCH_MAX_INFLIGHT=16 \
DD_BENCH_REQUESTS=20000 \
DD_BENCH_CONCURRENCY=256 \
cargo run -p runtime --bin bench_fetch_fast --release
