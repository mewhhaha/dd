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

- Date: `2026-06-23T14:27:14.772Z`
- Runtime code commit: `60cf94f`
- Worktree: dirty with local benchmark and memory-store changes
- OS: `Linux cachyos-x8664 7.0.9-1-cachyos #1 SMP PREEMPT_DYNAMIC Sun, 17 May 2026 16:56:12 +0000 x86_64 GNU/Linux`
- Logical CPUs: `16`
- Rust: `rustc 1.96.0-nightly (3645249d7 2026-03-16)`
- Cargo: `cargo 1.96.0-nightly (cbb9bb8bd 2026-03-13)`

## Results

Rows with multiple samples use medians; even sample counts average the two
middle values. This table is from `node benchmarks/run.mjs --samples 1 --out
benchmarks/results/feedback-memory-shards.json`.

| Config | Workload | Samples | Requests | Concurrency | Isolates | Max inflight | Throughput | Mean | P50 | P95 | P99 |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [`focused-memory-read.sh`](configs/focused-memory-read.sh) | keyed memory direct read | 1 | 300 | 16 | 1-4 | 8 | 16,141 req/s | 0.95ms | 0.62ms | 2.18ms | 7.75ms |
| [`focused-memory-write.sh`](configs/focused-memory-write.sh) | keyed memory direct write | 1 | 300 | 16 | 1-4 | 8 | 1,444 req/s | 10.83ms | 10.16ms | 16.20ms | 21.62ms |
| [`focused-dynamic.sh`](configs/focused-dynamic.sh) | dynamic baseline, all printed configs | 2 | 100 | 16 | mixed | 4 | 5,205 req/s | 3.02ms | 1.38ms | 11.80ms | 14.20ms |
| [`focused-dynamic.sh`](configs/focused-dynamic.sh) | dynamic hot fetch, all printed configs | 2 | 100 | 16 | mixed | 4 | 8,233 req/s | 2.49ms | 2.27ms | 5.68ms | 6.21ms |
| [`focused-dynamic.sh`](configs/focused-dynamic.sh) | dynamic hot fetch + host RPC, all printed configs | 2 | 100 | 16 | mixed | 4 | 4,993 req/s | 3.88ms | 3.57ms | 7.53ms | 7.86ms |
| [`saturated-fast-fetch-instant-text.sh`](configs/saturated-fast-fetch-instant-text.sh) | fast fetch instant text | 1 | 20,000 | 256 | 16-16 | 16 | 91,817 req/s | 2.72ms | 2.24ms | 5.35ms | 18.30ms |
| [`saturated-memory-read-wide.sh`](configs/saturated-memory-read-wide.sh) | keyed memory direct read, cross-shard wide keyspace | 1 | 20,000 | 256 | 16-16 | 16 | 62,942 req/s | 4.02ms | 0.45ms | 20.36ms | 48.25ms |
| [`scaling-memory-read-cross-shard.sh`](configs/scaling-memory-read-cross-shard.sh) | keyed memory direct read, cross-shard matrix | 1 | 12,000 | 192 | 16-16 | 16 | 59,752 req/s | 3.19ms | 0.57ms | 19.61ms | 27.26ms |
| [`scaling-memory-read-same-shard.sh`](configs/scaling-memory-read-same-shard.sh) | keyed memory direct read, same-shard matrix | 1 | 12,000 | 192 | 16-16 | 16 | 26,660 req/s | 7.13ms | 8.34ms | 17.75ms | 36.81ms |
| [`scaling-memory-write-cross-shard.sh`](configs/scaling-memory-write-cross-shard.sh) | keyed memory direct write, cross-shard matrix | 1 | 4,096 | 128 | 16-16 | 16 | 1,040 req/s | 121.82ms | 120.36ms | 185.85ms | 222.33ms |
| [`scaling-memory-write-same-shard.sh`](configs/scaling-memory-write-same-shard.sh) | keyed memory direct write, same-shard matrix | 1 | 4,096 | 128 | 16-16 | 16 | 1,955 req/s | 64.47ms | 62.07ms | 106.20ms | 159.74ms |

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
- `DD_BENCH_MEMORY_NAMESPACE_SHARDS` configures both the benchmark runtime
  storage shard count and the key generator used by same-shard/cross-shard
  modes.
- `direct-write-memory-wide` verifies all distinct written keys with
  `/sum-read`.
- The full default `cargo run -p runtime --bin bench --release` suite is not a
  gate here because previous runs documented signal-139 instability.
