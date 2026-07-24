#!/usr/bin/env bash
set -euo pipefail

iterations="${DD_BENCH_STRESS_ITERATIONS:-50}"
out_dir="${DD_BENCH_STRESS_OUT_DIR:-tmp/bench-crash}"
mkdir -p "$out_dir"

echo "bench stress output: $out_dir"
echo "iterations: $iterations"
echo "command: cargo run -p runtime --bin bench --release"

for iteration in $(seq 1 "$iterations"); do
  log="$out_dir/bench-$iteration.log"
  echo "bench stress iteration $iteration/$iterations"
  set +e
  RUST_BACKTRACE="${RUST_BACKTRACE:-1}" \
  DD_BENCH_PROGRESS_MARKERS="${DD_BENCH_PROGRESS_MARKERS:-1}" \
  cargo run -p runtime --bin bench --release >"$log" 2>&1
  status=$?
  set -e
  if [ "$status" -ne 0 ]; then
    {
      echo "iteration=$iteration"
      echo "status=$status"
      echo "log=$log"
      echo "last_markers:"
      grep 'bench-marker' "$log" | tail -20 || true
      echo "last_output:"
      tail -80 "$log" || true
    } >"$out_dir/failure-summary.txt"
    cat "$out_dir/failure-summary.txt" >&2
    exit "$status"
  fi
done

echo "bench stress completed $iterations iterations"
