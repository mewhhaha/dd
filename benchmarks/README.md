# Benchmark configurations

The scripts in [`configs/`](configs/) are the canonical commands for the
workloads we intentionally test. Raw sampler output is generated under
`benchmarks/results/` and is not committed.

The checked-in performance interpretation lives in
[`SCALING.md`](SCALING.md).

## Running benchmarks

Run one configuration from the repository root:

```bash
./benchmarks/configs/saturated-fast-fetch-instant-text.sh
```

Run the reproducible sampler and write machine-readable local output:

```bash
node benchmarks/run.mjs --samples 3 \
  --out benchmarks/results/local.json
```

The default sampler skips benchmark configs marked `DD_BENCH_MATRIX_OPT_IN=1`,
including the full atomic scaling matrix. It prints a note for skipped opt-in
configs so the default command remains a small-suite run.

Pass `--config` more than once to select a subset:

```bash
node benchmarks/run.mjs --samples 5 \
  --config scaling-memory-write-cross-shard.sh \
  --config atomic-memory-readwrite-cross-shard.sh \
  --out benchmarks/results/local-write-check.json
```

Run the real-world memory workloads:

```bash
node benchmarks/run.mjs --samples 3 \
  --config realworld-rate-limiter.sh \
  --config realworld-multiworker-auth.sh \
  --out benchmarks/results/local-realworld.json
```

The runner records git, toolchain, and OS metadata; the parsed benchmark
environment; raw stdout and stderr for every sample; exit status or signal;
parsed result rows; and median summaries.

Inspect the exact plan without spawning child processes or writing a result
file:

```bash
node benchmarks/run.mjs --plan --samples 5 \
  --config scaling-atomic-memory-matrix.sh
```

`--list` remains a compact name-only view. `--plan` expands matrices, shows
environment overrides, prints the variant count, sample count, total child
process runs, and the output path that would be used.

Runs are budgeted before execution. The default budget is 100 child processes
and can be changed with `--max-runs N` or `DD_BENCH_MAX_RUNS=N`. Pass
`--allow-large-run` or set `DD_BENCH_ALLOW_LARGE_RUN=1` only for intentional
large jobs. A child failure stops the run by default after writing partial JSON
with `complete: false`, `failure`, and `failures`; use `--keep-going` for
deliberate batch collection.

All throughput runs measure `RuntimeService` directly unless noted. They do not
include public API or network listener overhead.

## Atomic scaling matrix

[`scaling-atomic-memory-matrix.sh`](configs/scaling-atomic-memory-matrix.sh)
expands into one sampled configuration for every selected matrix combination.

Default dimensions:

| Dimension | Values |
| --- | --- |
| Isolates | `1`, `2`, `4`, `8`, `16`, `32` |
| Memory shards | `1`, `2`, `4`, `8`, `16`, `32`, `64` |
| Key modes | `same-shard`, `cross-shard`, `skewed-hotspot` |
| Workloads | direct write, atomic read + write, atomic write + effect |

The default matrix has `6 * 7 * 3 * 3 = 378` variants. At five samples that is
1,890 child-process runs, so the command below requires either
`--allow-large-run` or a sufficiently high `--max-runs`.

Run the complete matrix from a clean worktree when producing release evidence:

```bash
git diff --quiet
node benchmarks/run.mjs --plan --samples 5 \
  --config scaling-atomic-memory-matrix.sh
node benchmarks/run.mjs --samples 5 \
  --config scaling-atomic-memory-matrix.sh \
  --allow-large-run \
  --out benchmarks/results/local-atomic-memory-scaling-matrix.json
```

Override matrix dimensions for a shorter development check:

```bash
DD_BENCH_MATRIX_ISOLATES="1 2 4 8 16" \
DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS="16" \
DD_BENCH_MATRIX_KEY_MODES="same-shard cross-shard" \
DD_BENCH_MATRIX_MODES="atomic-readwrite-memory-wide" \
node benchmarks/run.mjs --samples 1 \
  --config scaling-atomic-memory-matrix.sh \
  --max-runs 12 \
  --out benchmarks/results/local-atomic-memory-scaling-smoke.json
```

Run the relative smoke regression policy on a small same-host matrix:

```bash
DD_BENCH_MATRIX_ISOLATES="1 8" \
DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS="8" \
DD_BENCH_MATRIX_KEY_MODES="same-shard cross-shard" \
DD_BENCH_MATRIX_MODES="direct-write-memory-wide atomic-readwrite-memory-wide" \
node benchmarks/run.mjs --samples 3 \
  --config scaling-atomic-memory-matrix.sh \
  --out benchmarks/results/local-memory-smoke.json

node benchmarks/check-regression.mjs \
  --results benchmarks/results/local-memory-smoke.json \
  --policy benchmarks/baselines/memory-scaling-policy.json
```

This gate checks structural ratios such as cross-shard atomic advantage and
basic scaling from one isolate. It intentionally avoids machine-specific
absolute throughput thresholds for ordinary local and pull-request use.

Use `--samples 3` for a shorter repeated check. Use `--samples 5` on a clean
worktree before treating medians as release or main-branch evidence.

## Workload notes

- Focused memory configs preserve the historical gate shape: 300 requests, 16
  concurrency, 1-4 isolates, and 8 max inflight.
- Saturated fast fetch prestarts 16 isolates and uses 256 concurrent client
  tasks. It checks peak simple-fetch throughput.
- Saturated wide memory read also prestarts 16 isolates with 256 concurrency,
  but measures keyed-memory routing and hydration.
- Legacy `scaling-memory-*` configs fix the memory shard count at 16 and compare
  same-shard with cross-shard direct reads and writes.
- Storage-only write configs invoke `MemoryStore.apply_batch()` directly. They
  isolate storage behavior from JavaScript and runtime scheduling.
- Atomic read/write configs read the current value and write `payload = "1"`.
- Atomic write/effect configs write `payload = "1"` and emit an
  `audit.bench.write` durable effect.
- Real-world rate limiter checks a memory-backed per-client window, writes the
  request count/reset metadata atomically, and verifies the summed counters
  equal total requests.
- Real-world multi-worker auth deploys a private auth worker with KV-backed
  users plus memory-backed session state, then benchmarks a frontend worker
  calling it through an `AUTH` service binding.
- `DD_BENCH_MODE=realworld-auth-worker-direct` is a diagnostic auth mode that
  hits the auth worker directly with KV and memory bindings, bypassing the
  frontend and service binding layer.
- `DD_BENCH_MEMORY_NAMESPACE_SHARDS` configures both runtime storage sharding
  and the same-shard/cross-shard key generator.
- Direct and atomic wide-write workloads verify all distinct written keys.

## Broad runtime suite diagnostics

The broad runtime benchmark supports stable listing and filtering:

```bash
cargo run -p runtime --bin bench -- --list
DD_BENCH_ONLY=steady-state \
DD_BENCH_CONFIG=single-isolate \
DD_BENCH_SCENARIO=instant-response \
cargo run -p runtime --bin bench --release
```

Enable machine-parseable lifecycle markers when investigating a crash:

```bash
DD_BENCH_PROGRESS_MARKERS=1 RUST_BACKTRACE=1 \
cargo run -p runtime --bin bench --release
```

The suite now explicitly shuts down each `RuntimeService` between sections and
restore-cycle iterations. To stress a suspected lifecycle issue repeatedly and
capture the last markers under ignored `tmp/bench-crash/` output:

```bash
bash scripts/run_bench_until_failure.sh
```

## Result retention

Do not commit raw JSON output. Keep durable conclusions and selected generated
tables in [`SCALING.md`](SCALING.md), including enough environment and workload
context to reproduce them.

Recommended summary workflow:

1. Run selected benchmarks into ignored `benchmarks/results/*.json` files.
2. Inspect child failures, `complete`, `failure`, and metadata before using the
   result as evidence.
3. Regenerate the measured Markdown section:

   ```bash
   just benchmark-summary \
     benchmarks/results/local-atomic-memory-scaling-matrix.json
   ```

4. Review the Markdown diff, especially provenance and dirty-worktree status.
5. Check freshness without modifying files:

   ```bash
   node benchmarks/summarize.mjs \
     --fixed benchmarks/results/local-atomic-memory-scaling-matrix.json \
     --core benchmarks/results/local-atomic-memory-scaling-matrix.json \
     --out benchmarks/SCALING.md \
     --check
   ```

6. Run `just check` and commit only Markdown, scripts, fixtures, or intentional
   baseline metadata.

The summarizer rejects dirty, incomplete, failed, ambiguous, incompatible, or
malformed results by default. Use `--allow-dirty` only when deliberately marking
generated text as non-release evidence.
