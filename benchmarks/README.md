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

All current throughput runs measure `RuntimeService` directly unless noted; they
do not include public API or network listener overhead.

## Current Environment

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
  direct reads and writes.
- The storage-only write configs use `MemoryStore.apply_batch()` directly with
  the same request count, concurrency, keyspace, shard count, and key generator
  as the runtime direct-write matrix. They isolate storage behavior from JS and
  runtime scheduling.
- `DD_BENCH_MEMORY_NAMESPACE_SHARDS` configures both the benchmark runtime
  storage shard count and the key generator used by same-shard/cross-shard
  modes.
- `direct-write-memory-wide` verifies all distinct written keys with
  `/sum-read`.
- The full default `cargo run -p runtime --bin bench --release` suite is not a
  gate here because previous runs documented signal-139 instability.
