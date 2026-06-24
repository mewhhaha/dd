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

Pass `--config` more than once to select a subset:

```bash
node benchmarks/run.mjs --samples 5 \
  --config scaling-memory-write-cross-shard.sh \
  --config atomic-memory-readwrite-cross-shard.sh \
  --out benchmarks/results/local-write-check.json
```

The runner records git, toolchain, and OS metadata; the parsed benchmark
environment; raw stdout and stderr for every sample; exit status or signal;
parsed result rows; and median summaries.

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

Run the complete matrix from a clean worktree when producing release evidence:

```bash
git diff --quiet
node benchmarks/run.mjs --samples 5 \
  --config scaling-atomic-memory-matrix.sh \
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
  --out benchmarks/results/local-atomic-memory-scaling-smoke.json
```

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
- `DD_BENCH_MEMORY_NAMESPACE_SHARDS` configures both runtime storage sharding
  and the same-shard/cross-shard key generator.
- Direct and atomic wide-write workloads verify all distinct written keys.
- The full default `cargo run -p runtime --bin bench --release` suite is not a
  gate because earlier runs documented signal-139 instability.

## Result retention

Do not commit raw JSON output. Keep durable conclusions and selected tables in
[`SCALING.md`](SCALING.md), including enough environment and workload context to
reproduce them.