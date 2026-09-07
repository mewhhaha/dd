#!/usr/bin/env bash
set -euo pipefail

DD_BENCH_REQUESTS="${DD_BENCH_REQUESTS:-4000}"
DD_BENCH_CONCURRENCY="${DD_BENCH_CONCURRENCY:-64}"
DD_BENCH_CPUSET="${DD_BENCH_CPUSET:-first-available}"
DD_BENCH_MIN_ISOLATES="${DD_BENCH_MIN_ISOLATES:-0}"
DD_BENCH_MAX_INFLIGHT="${DD_BENCH_MAX_INFLIGHT:-4}"
DD_BENCH_MAX_ISOLATES="${DD_BENCH_MAX_ISOLATES:-8}"
DD_BENCH_MAX_GLOBAL_ISOLATES="${DD_BENCH_MAX_GLOBAL_ISOLATES:-2}"
DD_BENCH_MEMORY_OUTBOX_MAX_CONCURRENT_SHARDS="${DD_BENCH_MEMORY_OUTBOX_MAX_CONCURRENT_SHARDS:-1}"
DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES="${DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES:-4096}"
DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_BYTES="${DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_BYTES:-67108864}"
DD_BENCH_WIDE_KEY_SPACE="${DD_BENCH_WIDE_KEY_SPACE:-256}"
DD_BENCH_MEMORY_KEY_MODE="${DD_BENCH_MEMORY_KEY_MODE:-cross-shard}"

export DD_BENCH_REQUESTS
export DD_BENCH_CONCURRENCY
export DD_BENCH_MIN_ISOLATES
export DD_BENCH_MAX_INFLIGHT
export DD_BENCH_MAX_ISOLATES
export DD_BENCH_MAX_GLOBAL_ISOLATES
export DD_BENCH_MEMORY_OUTBOX_MAX_CONCURRENT_SHARDS
export DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES
export DD_BENCH_MEMORY_SNAPSHOT_CACHE_MAX_BYTES
export DD_BENCH_WIDE_KEY_SPACE
export DD_BENCH_MEMORY_KEY_MODE

cargo build --locked --release \
  -p dd_server --bin bench_http_server \
  -p runtime --bin bench_memory_storage \
  -p runtime --bin bench

benchmark_target="${CARGO_TARGET_DIR:-target}/release"
cpu_set="$DD_BENCH_CPUSET"
if [ "$cpu_set" = first-available ]; then
  cpu_set="$(awk '/Cpus_allowed_list/ { split($2, cpus, /[-,]/); print cpus[1] }' /proc/self/status)"
fi
run_on_profile_cpu=(taskset -c "$cpu_set")

"${run_on_profile_cpu[@]}" "$benchmark_target/bench_http_server"

for mode in \
  atomic-read-memory-wide \
  atomic-write-only-memory-wide \
  atomic-readwrite-memory-wide \
  atomic-write-effect-memory-wide \
  realworld-rate-limiter \
  realworld-multiworker-auth
do
  DD_BENCH_MODE="$mode" \
    "${run_on_profile_cpu[@]}" "$benchmark_target/bench_memory_storage"
done

DD_BENCH_ONLY=lifecycle \
DD_BENCH_CONFIG=fly-production \
DD_BENCH_COLD_ROUNDS="${DD_BENCH_COLD_ROUNDS:-40}" \
  "${run_on_profile_cpu[@]}" "$benchmark_target/bench"
