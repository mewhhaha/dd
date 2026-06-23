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

- Date: `2026-06-23T13:36:27+02:00`
- Runtime code commit: `43c9a89`
- OS: `Linux cachyos-x8664 7.0.9-1-cachyos #1 SMP PREEMPT_DYNAMIC Sun, 17 May 2026 16:56:12 +0000 x86_64 GNU/Linux`
- Logical CPUs: `16`
- Rust: `rustc 1.96.0-nightly (3645249d7 2026-03-16)`
- Cargo: `cargo 1.96.0-nightly (cbb9bb8bd 2026-03-13)`

## Results

Rows with multiple samples use medians; even sample counts average the two
middle values. The wide memory saturation row is a single sample because its
seed phase is much heavier than the timed read phase.

| Config | Workload | Samples | Requests | Concurrency | Isolates | Max inflight | Throughput | Mean | P50 | P95 | P99 |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [`focused-memory-read.sh`](configs/focused-memory-read.sh) | keyed memory direct read | 3 | 300 | 16 | 1-4 | 8 | 18,177 req/s | 0.87ms | 0.51ms | 2.31ms | 6.31ms |
| [`focused-memory-write.sh`](configs/focused-memory-write.sh) | keyed memory direct write | 3 | 300 | 16 | 1-4 | 8 | 1,251 req/s | 12.50ms | 12.11ms | 16.56ms | 22.08ms |
| [`focused-dynamic.sh`](configs/focused-dynamic.sh) | dynamic baseline, autoscaling-8 | 3 | 100 | 16 | 0-8 | 4 | 4,545 req/s | 3.00ms | 0.99ms | 12.26ms | 16.21ms |
| [`focused-dynamic.sh`](configs/focused-dynamic.sh) | dynamic hot fetch, autoscaling-8 | 3 | 100 | 16 | 0-8 | 4 | 11,597 req/s | 1.30ms | 0.82ms | 6.74ms | 7.83ms |
| [`focused-dynamic.sh`](configs/focused-dynamic.sh) | dynamic hot fetch + host RPC, autoscaling-8 | 3 | 100 | 16 | 0-8 | 4 | 6,938 req/s | 2.10ms | 1.40ms | 7.21ms | 8.07ms |
| [`saturated-memory-read-wide.sh`](configs/saturated-memory-read-wide.sh) | keyed memory direct read, cross-shard wide keyspace | 1 | 20,000 | 256 | 16-16 | 16 | 16,158 req/s | 15.81ms | 15.30ms | 37.23ms | 41.34ms |
| [`saturated-fast-fetch-instant-text.sh`](configs/saturated-fast-fetch-instant-text.sh) | fast fetch instant text | 4 | 20,000 | 256 | 16-16 | 16 | 115,398 req/s | 2.18ms | 1.53ms | 5.60ms | 17.33ms |

## Config Notes

- Focused memory configs preserve the historical gate shape:
  `300` requests, `16` concurrency, `1-4` isolates, and `8` max inflight.
- Focused dynamic runs `DD_BENCH_ONLY=dynamic` and records the autoscaling-8
  rows. The harness also prints single-isolate rows.
- Saturated fast fetch prestarts `16` isolates and uses `256` concurrent client
  tasks. This is the configuration to use when checking peak simple-fetch RPS.
- Saturated wide memory read also prestarts `16` isolates with `256`
  concurrency, but it measures keyed memory routing and read hydration rather
  than the plain fetch path.
- The full default `cargo run -p runtime --bin bench --release` suite is not a
  gate here because previous runs documented signal-139 instability.

## Raw Samples

Focused keyed memory direct read:

| Sample | Total | Throughput | Mean | P50 | P95 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16.50ms | 18,177 req/s | 0.87ms | 0.52ms | 2.35ms | 6.82ms |
| 2 | 15.65ms | 19,170 req/s | 0.82ms | 0.51ms | 2.31ms | 6.14ms |
| 3 | 17.15ms | 17,490 req/s | 0.87ms | 0.48ms | 2.19ms | 6.31ms |

Focused keyed memory direct write:

| Sample | Total | Throughput | Mean | P50 | P95 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 236.30ms | 1,270 req/s | 12.33ms | 11.90ms | 16.43ms | 22.08ms |
| 2 | 239.76ms | 1,251 req/s | 12.50ms | 12.11ms | 16.56ms | 21.94ms |
| 3 | 247.24ms | 1,213 req/s | 12.93ms | 12.56ms | 17.11ms | 22.41ms |

Focused dynamic autoscaling-8:

| Sample | Baseline | Hot fetch | Hot fetch + host RPC |
| ---: | ---: | ---: | ---: |
| 1 | 4,482 req/s | 11,597 req/s | 6,859 req/s |
| 2 | 5,622 req/s | 11,602 req/s | 6,938 req/s |
| 3 | 4,545 req/s | 9,974 req/s | 7,516 req/s |

Saturated fast fetch instant text:

| Sample | Throughput | Mean | P50 | P95 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 114,353 req/s | 2.19ms | 1.54ms | 5.16ms | 17.73ms |
| 2 | 116,442 req/s | 2.16ms | 1.52ms | 6.04ms | 17.58ms |
| 3 | 98,772 req/s | 2.58ms | 1.88ms | 6.33ms | 17.07ms |
| 4 | 131,208 req/s | 1.94ms | 1.26ms | 5.06ms | 16.56ms |

Saturated keyed memory wide direct read:

| Sample | Total | Throughput | Mean | P50 | P95 | P99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,237.74ms | 16,158 req/s | 15.81ms | 15.30ms | 37.23ms | 41.34ms |
