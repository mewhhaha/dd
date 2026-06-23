# Benchmark Configurations

This directory keeps the benchmark configurations we intentionally test and the
latest local results for those configurations. The scripts in
[`configs/`](configs/) are the canonical commands for each workload.

Run a configuration from the repository root:

```bash
./benchmarks/configs/saturated-fast-fetch-instant-text.sh
```

Run the reproducible sampler and write machine-readable results:

```bash
node benchmarks/run.mjs --samples 3 --out benchmarks/results/local.json
```

Pass `--config saturated-fast-fetch-instant-text.sh` to run a subset. The
runner records git/toolchain/OS metadata, the parsed benchmark environment,
raw stdout/stderr for every sample, exit status or signal, parsed result rows,
and median summaries.

All throughput runs measure `RuntimeService` directly unless noted; they do not
include public API or network listener overhead.

## Recorded Direct-Write Environment

The table below is the latest recorded local result, not a clean-main claim.
The source JSON is
[`results/feedback-direct-write-fast-path.json`](results/feedback-direct-write-fast-path.json),
whose metadata reports a dirty worktree. Regenerate on a clean worktree before
using these numbers as release or main-branch evidence.

- Date: `2026-06-23T18:21:56.685Z`
- Runtime code commit: `02d4358`
- Worktree: dirty with direct memory-write fast path, storage-only benchmark
  configs, and `FEEDBACK.md`
- OS: `Linux cachyos-x8664 7.0.9-1-cachyos #1 SMP PREEMPT_DYNAMIC Sun, 17 May 2026 16:56:12 +0000 x86_64 GNU/Linux`
- Logical CPUs: `16`
- Rust: `rustc 1.96.0-nightly (3645249d7 2026-03-16)`
- Cargo: `cargo 1.96.0-nightly (cbb9bb8bd 2026-03-13)`

## Results

Rows with multiple samples use medians; even sample counts average the two
middle values. This table is from:

```bash
node benchmarks/run.mjs --samples 1 \
  --config scaling-memory-write-cross-shard.sh \
  --config scaling-memory-write-same-shard.sh \
  --config storage-memory-write-cross-shard.sh \
  --config storage-memory-write-same-shard.sh \
  --out benchmarks/results/feedback-direct-write-fast-path.json
```

| Config | Workload | Samples | Requests | Concurrency | Isolates | Max inflight | Throughput | Mean | P50 | P95 | P99 |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [`scaling-memory-write-cross-shard.sh`](configs/scaling-memory-write-cross-shard.sh) | keyed memory direct write, cross-shard matrix | 1 | 4,096 | 128 | 16-16 | 16 | 18,405 req/s | 6.37ms | 2.41ms | 22.97ms | 35.69ms |
| [`scaling-memory-write-same-shard.sh`](configs/scaling-memory-write-same-shard.sh) | keyed memory direct write, same-shard matrix | 1 | 4,096 | 128 | 16-16 | 16 | 3,944 req/s | 26.92ms | 4.18ms | 128.76ms | 429.05ms |
| [`storage-memory-write-cross-shard.sh`](configs/storage-memory-write-cross-shard.sh) | storage-only direct write, cross-shard matrix | 1 | 4,096 | 128 | n/a | n/a | 10,494 req/s | 12.07ms | 12.30ms | 15.58ms | 16.55ms |
| [`storage-memory-write-same-shard.sh`](configs/storage-memory-write-same-shard.sh) | storage-only direct write, same-shard matrix | 1 | 4,096 | 128 | n/a | n/a | 4,217 req/s | 21.96ms | 16.40ms | 32.14ms | 94.87ms |

Previous runtime direct-write baseline from
`benchmarks/results/feedback-memory-shards.json`:

| Config | Previous Throughput | Current Throughput |
| --- | ---: | ---: |
| `scaling-memory-write-cross-shard.sh` | 1,040 req/s | 18,405 req/s |
| `scaling-memory-write-same-shard.sh` | 1,955 req/s | 3,944 req/s |

## Atomic Write Matrix

The atomic write configs are intentionally not included in the recorded table
above. They should be sampled with the storage-only and direct-write configs so
the comparison separates storage, direct convenience writes, atomic read+write,
and atomic write+effect behavior:

```bash
git diff --quiet
node benchmarks/run.mjs --samples 5 \
  --config storage-memory-write-cross-shard.sh \
  --config storage-memory-write-same-shard.sh \
  --config scaling-memory-write-cross-shard.sh \
  --config scaling-memory-write-same-shard.sh \
  --config atomic-memory-readwrite-cross-shard.sh \
  --config atomic-memory-readwrite-same-shard.sh \
  --config atomic-memory-write-cross-shard.sh \
  --config atomic-memory-write-same-shard.sh \
  --out benchmarks/results/local-atomic-memory-write-matrix.json
```

Use `--samples 3` for a shorter development check. Use `--samples 5` on a
clean worktree before treating the medians as release or main-branch evidence.

| Config | Workload | Requests | Concurrency | Isolates | Max inflight | Key space | Shards |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: |
| [`atomic-memory-readwrite-cross-shard.sh`](configs/atomic-memory-readwrite-cross-shard.sh) | keyed memory `atomic(...)` read+write, cross-shard matrix | 4,096 | 128 | 16-16 | 16 | 1,024 | 16 |
| [`atomic-memory-readwrite-same-shard.sh`](configs/atomic-memory-readwrite-same-shard.sh) | keyed memory `atomic(...)` read+write, same-shard matrix | 4,096 | 128 | 16-16 | 16 | 1,024 | 16 |
| [`atomic-memory-write-cross-shard.sh`](configs/atomic-memory-write-cross-shard.sh) | keyed memory `atomic(...)` write+effect, cross-shard matrix | 4,096 | 128 | 16-16 | 16 | 1,024 | 16 |
| [`atomic-memory-write-same-shard.sh`](configs/atomic-memory-write-same-shard.sh) | keyed memory `atomic(...)` write+effect, same-shard matrix | 4,096 | 128 | 16-16 | 16 | 1,024 | 16 |

## Atomic Scheduler Implementation Matrix

This clean temporary `main` snapshot run is implementation feedback for the
shard-aware atomic scheduler and background outbox drain changes. The source JSON is
[`results/feedback-atomic-sharded-scheduler.json`](results/feedback-atomic-sharded-scheduler.json).
Regenerate from the real repository worktree before using these numbers as
release evidence.

- Date: `2026-06-23T20:12:24.542Z`
- Runtime code commit: `cc4075f`
- Worktree: clean temporary `main` snapshot with shard-aware atomic scheduler and background outbox drain changes
- Samples: `3`

| Config | Workload | Samples | Requests | Concurrency | Isolates | Max inflight | Throughput | Mean | P95 | P99 |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| [`storage-memory-write-cross-shard.sh`](configs/storage-memory-write-cross-shard.sh) | storage-only direct write, cross-shard matrix | 3 | 4,096 | 128 | n/a | n/a | 11,639 req/s | 10.88ms | 12.95ms | 15.33ms |
| [`storage-memory-write-same-shard.sh`](configs/storage-memory-write-same-shard.sh) | storage-only direct write, same-shard matrix | 3 | 4,096 | 128 | n/a | n/a | 3,042 req/s | 28.10ms | 43.33ms | 277.00ms |
| [`scaling-memory-write-cross-shard.sh`](configs/scaling-memory-write-cross-shard.sh) | keyed memory direct write, cross-shard matrix | 3 | 4,096 | 128 | 16-16 | 16 | 16,821 req/s | 6.67ms | 25.10ms | 39.56ms |
| [`scaling-memory-write-same-shard.sh`](configs/scaling-memory-write-same-shard.sh) | keyed memory direct write, same-shard matrix | 3 | 4,096 | 128 | 16-16 | 16 | 4,271 req/s | 25.49ms | 104.44ms | 428.95ms |
| [`atomic-memory-readwrite-cross-shard.sh`](configs/atomic-memory-readwrite-cross-shard.sh) | keyed memory `atomic(...)` read+write, cross-shard matrix | 3 | 4,096 | 128 | 16-16 | 16 | 7,099 req/s | 17.91ms | 27.92ms | 115.51ms |
| [`atomic-memory-readwrite-same-shard.sh`](configs/atomic-memory-readwrite-same-shard.sh) | keyed memory `atomic(...)` read+write, same-shard matrix | 3 | 4,096 | 128 | 16-16 | 16 | 2,106 req/s | 59.45ms | 122.70ms | 177.18ms |
| [`atomic-memory-write-cross-shard.sh`](configs/atomic-memory-write-cross-shard.sh) | keyed memory `atomic(...)` write+effect, cross-shard matrix | 3 | 4,096 | 128 | 16-16 | 16 | 3,229 req/s | 39.19ms | 59.54ms | 134.74ms |
| [`atomic-memory-write-same-shard.sh`](configs/atomic-memory-write-same-shard.sh) | keyed memory `atomic(...)` write+effect, same-shard matrix | 3 | 4,096 | 128 | 16-16 | 16 | 748 req/s | 154.17ms | 492.73ms | 1070.76ms |

## Current Write Matrix

This one-sample run used the full write matrix on commit `cc3b9a5`. Treat it as
implementation feedback, not release benchmark evidence. Replace it with a
clean-worktree `--samples 3` to `--samples 5` result before making a release
claim.

| Config | Workload | Requests | Concurrency | Isolates | Max inflight | Throughput | Mean | P50 | P95 | P99 |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [`storage-memory-write-cross-shard.sh`](configs/storage-memory-write-cross-shard.sh) | storage-only direct write, cross-shard matrix | 4,096 | 128 | n/a | n/a | 11,735 req/s | 10.81ms | 10.92ms | 13.25ms | 16.24ms |
| [`storage-memory-write-same-shard.sh`](configs/storage-memory-write-same-shard.sh) | storage-only direct write, same-shard matrix | 4,096 | 128 | n/a | n/a | 3,188 req/s | 25.20ms | 13.11ms | 185.46ms | 259.98ms |
| [`scaling-memory-write-cross-shard.sh`](configs/scaling-memory-write-cross-shard.sh) | keyed memory direct write, cross-shard matrix | 4,096 | 128 | 16-16 | 16 | 19,928 req/s | 5.49ms | 1.83ms | 21.15ms | 32.82ms |
| [`scaling-memory-write-same-shard.sh`](configs/scaling-memory-write-same-shard.sh) | keyed memory direct write, same-shard matrix | 4,096 | 128 | 16-16 | 16 | 4,707 req/s | 22.16ms | 3.62ms | 103.94ms | 230.35ms |
| [`atomic-memory-readwrite-cross-shard.sh`](configs/atomic-memory-readwrite-cross-shard.sh) | keyed memory `atomic(...)` read+write, cross-shard matrix | 4,096 | 128 | 16-16 | 16 | 1,188 req/s | 106.78ms | 97.31ms | 166.88ms | 227.20ms |
| [`atomic-memory-readwrite-same-shard.sh`](configs/atomic-memory-readwrite-same-shard.sh) | keyed memory `atomic(...)` read+write, same-shard matrix | 4,096 | 128 | 16-16 | 16 | 1,741 req/s | 72.86ms | 68.99ms | 120.99ms | 164.32ms |
| [`atomic-memory-write-cross-shard.sh`](configs/atomic-memory-write-cross-shard.sh) | keyed memory `atomic(...)` write+effect, cross-shard matrix | 4,096 | 128 | 16-16 | 16 | 499 req/s | 253.24ms | 253.63ms | 407.56ms | 482.83ms |
| [`atomic-memory-write-same-shard.sh`](configs/atomic-memory-write-same-shard.sh) | keyed memory `atomic(...)` write+effect, same-shard matrix | 4,096 | 128 | 16-16 | 16 | 417 req/s | 295.06ms | 227.50ms | 747.65ms | 1482.89ms |

## Config Notes

- Focused memory configs preserve the historical gate shape:
  `300` requests, `16` concurrency, `1-4` isolates, and `8` max inflight.
- Focused dynamic runs `DD_BENCH_ONLY=dynamic`; the sampler currently groups
  rows with the same workload label across the printed dynamic configs.
- Saturated fast fetch prestarts `16` isolates and uses `256` concurrent client
  tasks. This is the configuration to use when checking peak simple-fetch RPS.
- Saturated wide memory read also prestarts `16` isolates with `256`
  concurrency, but it measures keyed memory routing and read hydration rather
  than the plain fetch path.
- The scaling memory matrix fixes `DD_BENCH_MEMORY_NAMESPACE_SHARDS=16` and
  compares `DD_BENCH_MEMORY_KEY_MODE=same-shard` against `cross-shard` for wide
  direct reads, direct writes, and atomic writes.
- The storage-only write configs use `MemoryStore.apply_batch()` directly with
  the same request count, concurrency, keyspace, shard count, and key generator
  as the runtime direct-write matrix. They isolate storage behavior from JS and
  runtime scheduling.
- The atomic read+write configs call `stub.atomic(...)` with a callback that
  reads the current value and writes `payload = "1"`.
- The atomic write configs call `stub.atomic(...)` with a callback that writes
  `payload = "1"` and emits an `audit.bench.write` durable effect.
- `DD_BENCH_MEMORY_NAMESPACE_SHARDS` configures both the benchmark runtime
  storage shard count and the key generator used by same-shard/cross-shard
  modes.
- `direct-write-memory-wide` verifies all distinct written keys with
  `/sum-read`.
- `atomic-readwrite-memory-wide` and `atomic-write-memory-wide` verify all
  distinct written keys with `/sum`.
- The full default `cargo run -p runtime --bin bench --release` suite is not a
  gate here because previous runs documented signal-139 instability.
