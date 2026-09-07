# Memory scaling benchmarks

State storage uses 32 fixed shards. Each shard has one durable writer and two
pooled readers. Memory transactions execute synchronous callbacks in the caller
isolate and hold an entity lease through commit. A successful write response
means the transaction committed.

The matrix varies isolate count, key placement, and transaction workload:

| Dimension | Values |
| --- | --- |
| Isolates | 1, 2, 4, 8, 16, 32 |
| Key placement | same shard, cross shard, skewed hotspot |
| Workload | write only, read + write, write + durable effect |

The default matrix contains 54 variants. Five samples require 270 child runs.
Use the plan command to inspect a run before collecting measurements:

```bash
node benchmarks/run.mjs --plan --samples 5 \
  --config scaling-atomic-memory-matrix.sh
```

A shorter scaling check uses two key placements and one workload:

```bash
DD_BENCH_MATRIX_ISOLATES="1 2 4 8 16" \
DD_BENCH_MATRIX_KEY_MODES="same-shard cross-shard" \
DD_BENCH_MATRIX_MODES="atomic-readwrite-memory-wide" \
node benchmarks/run.mjs --samples 3 \
  --config scaling-atomic-memory-matrix.sh \
  --out benchmarks/results/local-memory-scaling.json
```

Measure on the same host with matching CPU affinity, build configuration, and
request/concurrency settings. Keep the raw result JSON, which records hardware,
process limits, source revision, effective configuration, and sample metrics.
Run from a clean worktree for release evidence; a dirty run is exploratory.

The runtime profile reports snapshot hydration, commit time, snapshot cache
activity, WebSocket wake scheduling, and outbox delivery. State writer metrics
report committed groups and commands, rollbacks, busy retries, and pending
command/byte budgets. Benchmark results from earlier storage layouts must be
remeasured before drawing performance conclusions about this architecture.

See [the harness guide](README.md) for regression checks, result comparison,
and the complete matrix command.

<!-- BEGIN GENERATED: scaling-summary -->
Current measurements can be inserted here with `benchmarks/summarize.mjs` after
collecting the fixed and core matrices.
<!-- END GENERATED: scaling-summary -->
