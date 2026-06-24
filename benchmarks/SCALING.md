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

Use five samples on a clean worktree for release evidence. The compact core
curve below is a one-sample diagnostic slice at 16 memory shards; the fixed
16-isolate comparison is a five-sample median.

## Five-sample 16-isolate comparison

| Workload | Cross-shard | Same-shard | Cross-shard advantage |
| --- | ---: | ---: | ---: |
| Storage-only direct write | 11,623 req/s | 2,174 req/s | 5.3x |
| Runtime direct write | 13,206 req/s | 4,237 req/s | 3.1x |
| Atomic read + write | 8,620 req/s | 2,342 req/s | 3.7x |
| Atomic write + durable effect | 3,521 req/s | 715 req/s | 4.9x |

These results show the intended behavior: unrelated memory IDs can progress in
parallel, while work concentrated on one storage shard remains contention-bound.

## Cross-shard core scaling

This diagnostic slice fixes the memory shard count at 16.

| Isolates | Direct write | Atomic read + write | Atomic write + effect |
| ---: | ---: | ---: | ---: |
| 1 | 6,107 req/s | 2,549 req/s | 1,565 req/s |
| 2 | 11,196 req/s | 4,307 req/s | 2,321 req/s |
| 4 | 17,540 req/s | 6,413 req/s | 2,999 req/s |
| 8 | 18,723 req/s | 8,405 req/s | 3,918 req/s |
| 16 | 14,291 req/s | 9,264 req/s | 4,116 req/s |
| 32 | 15,843 req/s | 9,123 req/s | 4,222 req/s |

The atomic read/write path scales by about 3.6x from one to sixteen isolates.
The write/effect path scales by about 2.6x over the same interval. On this
16-logical-CPU machine, moving from 16 to 32 isolates produces little or no
additional atomic throughput, which is consistent with hardware saturation and
runtime oversubscription.

Direct writes peak earlier in this diagnostic run. They are cheap enough that
storage and coordination overhead become limiting before isolate execution does.

## Same-shard control

| Isolates | Direct write | Atomic read + write | Atomic write + effect |
| ---: | ---: | ---: | ---: |
| 1 | 6,194 req/s | 2,828 req/s | 1,256 req/s |
| 2 | 6,049 req/s | 2,581 req/s | 921 req/s |
| 4 | 5,991 req/s | 2,387 req/s | 930 req/s |
| 8 | 4,472 req/s | 2,050 req/s | 782 req/s |
| 16 | 4,728 req/s | 2,546 req/s | 827 req/s |
| 32 | 5,137 req/s | 2,359 req/s | 852 req/s |

Adding isolates does not improve one hot shard. That is expected: ordering and
transactional correctness deliberately serialize conflicting work.

## Tail latency at 16 isolates

| Workload | Distribution | P95 | P99 |
| --- | --- | ---: | ---: |
| Direct write | Cross-shard | 23.02 ms | 34.06 ms |
| Direct write | Same-shard | 104.04 ms | 329.39 ms |
| Atomic read + write | Cross-shard | 20.70 ms | 101.83 ms |
| Atomic read + write | Same-shard | 113.74 ms | 179.16 ms |
| Atomic write + effect | Cross-shard | 41.38 ms | 121.03 ms |
| Atomic write + effect | Same-shard | 563.94 ms | 820.33 ms |

The hot-shard tail is the main remaining risk for skewed production workloads.
Use the `skewed-hotspot` matrix mode when evaluating fairness or scheduler
changes.

## Regenerating results

Run the full matrix from a clean worktree:

```bash
git diff --quiet
node benchmarks/run.mjs --samples 5 \
  --config scaling-atomic-memory-matrix.sh \
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
  --out benchmarks/results/local-core-scaling.json
```

Update this Markdown summary when a clean repeated run materially changes the
performance conclusions. Do not commit the raw JSON output.