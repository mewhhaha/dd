# Keyed memory scaling

This is the durable summary of the keyed-memory performance work. Raw sampler
output is generated locally under `benchmarks/results/` and is intentionally not
versioned.

## Test shape

The recorded runs used `RuntimeService` directly, without public API or network
listener overhead.

- Machine: 16 logical CPUs
- Requests: 4,096 for write workloads
- Client concurrency: 128
- Maximum inflight requests per isolate: 16
- Wide key space: 1,024 keys
- Core sweep: 1, 2, 4, 8, 16, and 32 isolates
- Storage sweep: 1, 2, 4, 8, 16, 32, and 64 memory shards
- Key distributions: same-shard, cross-shard, and skewed-hotspot

Use five samples on a clean worktree for release evidence. The generated
summary below records the full five-sample atomic scaling matrix, with the core
curve shown as the 16-memory-shard slice.

<!-- BEGIN GENERATED: scaling-summary -->
## Generated benchmark evidence

Generated from local benchmark JSON. Raw result files remain ignored; this section stores the selected rows, derived ratios, and provenance needed to reproduce the measurement.

## Five-sample 16-isolate comparison

| Workload | Cross-shard | Same-shard | Cross-shard advantage |
| --- | ---: | ---: | ---: |
| Runtime direct write | 15,085 req/s | 4,729 req/s | 3.2x |
| Atomic read + write | 8,168 req/s | 2,477 req/s | 3.3x |
| Atomic write + durable effect | 3,673 req/s | 810 req/s | 4.5x |

These results show the intended behavior: unrelated memory IDs can progress in parallel, while work concentrated on one storage shard remains contention-bound.

## Cross-shard core scaling

This five-sample matrix slice fixes the memory shard count at 16.

| Isolates | Direct write | Atomic read + write | Atomic write + effect |
| ---: | ---: | ---: | ---: |
| 1 | 5,504 req/s | 2,367 req/s | 1,491 req/s |
| 2 | 10,509 req/s | 4,118 req/s | 2,258 req/s |
| 4 | 14,281 req/s | 6,375 req/s | 2,791 req/s |
| 8 | 15,575 req/s | 8,395 req/s | 3,906 req/s |
| 16 | 15,085 req/s | 8,168 req/s | 3,673 req/s |
| 32 | 17,077 req/s | 9,069 req/s | 4,103 req/s |

The atomic read/write path scales by about 3.5x from one to sixteen isolates. The write/effect path scales by about 2.5x over the same interval. Moving from 16 to 32 isolates improves modestly for atomic read/write throughput on this machine.

Direct writes peak at 32 isolates in this matrix slice. They are cheap enough that storage and coordination overhead can become limiting before isolate execution does.

## Same-shard control

Adding isolates does not improve one hot shard. That is expected: ordering and transactional correctness deliberately serialize conflicting work.

| Isolates | Direct write | Atomic read + write | Atomic write + effect |
| ---: | ---: | ---: | ---: |
| 1 | 5,562 req/s | 2,552 req/s | 1,150 req/s |
| 2 | 5,723 req/s | 2,574 req/s | 939 req/s |
| 4 | 5,024 req/s | 2,346 req/s | 889 req/s |
| 8 | 5,027 req/s | 2,170 req/s | 774 req/s |
| 16 | 4,729 req/s | 2,477 req/s | 810 req/s |
| 32 | 5,020 req/s | 2,518 req/s | 736 req/s |

## Tail latency at 16 isolates

| Workload | Distribution | P95 | P99 |
| --- | --- | ---: | ---: |
| Direct write | Cross-shard | 25.05 ms | 36.26 ms |
| Direct write | Same-shard | 103.50 ms | 329.37 ms |
| Atomic read + write | Cross-shard | 25.28 ms | 108.98 ms |
| Atomic read + write | Same-shard | 113.60 ms | 164.09 ms |
| Atomic write + durable effect | Cross-shard | 47.54 ms | 125.09 ms |
| Atomic write + durable effect | Same-shard | 410.99 ms | 1043.12 ms |

The hot-shard tail is the main remaining risk for skewed production workloads. Use the `skewed-hotspot` matrix mode when evaluating fairness or scheduler changes.

## Provenance

- Inputs: local-atomic-memory-scaling-matrix.json
- Started at: 2026-06-24T06:43:30.876Z
- Git commit: 151b84706b2f0259fcf6044f69951d559b1dd58b (clean)
- Samples: 5
- Logical CPUs: 16
- OS: Linux cachyos-x8664 7.0.9-1-cachyos #1 SMP PREEMPT_DYNAMIC Sun, 17 May 2026 16:56:12 +0000 x86_64 GNU/Linux
- Rust: rustc 1.96.0-nightly (3645249d7 2026-03-16)
- Cargo: cargo 1.96.0-nightly (cbb9bb8bd 2026-03-13)
- Requests: 4096
- Concurrency: 128
- Fixed comparison: isolates=16, shards=16
- Core curve: isolates=1 2 4 8 16 32, shards=16
<!-- END GENERATED: scaling-summary -->

## Regenerating results

Run the full matrix from a clean worktree:

```bash
git diff --quiet
node benchmarks/run.mjs --plan --samples 5 \
  --config scaling-atomic-memory-matrix.sh
node benchmarks/run.mjs --samples 5 \
  --config scaling-atomic-memory-matrix.sh \
  --allow-large-run \
  --out benchmarks/results/local-atomic-memory-scaling-matrix.json
```

For a shorter core-scaling check at 16 shards:

```bash
DD_BENCH_MATRIX_ISOLATES="1 2 4 8 16 32" \
DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS="16" \
DD_BENCH_MATRIX_KEY_MODES="same-shard cross-shard" \
DD_BENCH_MATRIX_MODES="direct-write-memory-wide atomic-readwrite-memory-wide atomic-write-memory-wide" \
node benchmarks/run.mjs --samples 1 \
  --config scaling-atomic-memory-matrix.sh \
  --max-runs 12 \
  --out benchmarks/results/local-core-scaling.json
```

The full default matrix is 378 variants, or 1,890 child-process runs at five
samples. Update this Markdown summary when a clean repeated run materially
changes the performance conclusions. Do not commit the raw JSON output.
