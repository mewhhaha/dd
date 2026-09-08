# Memory latency follow-up — 2026-09-08

The snapshot-read implementation was committed and pushed as `4f30ef1` before
this investigation. This follow-up adds measurement controls and makes no
further runtime performance change. The existing service retains its durable
commit, lease, idempotency, and snapshot semantics.

Durable commit latency is the dominant measured write cost. Removing read
leases helps readers, but does not make contended durable writes cheap. A
combined read/write p99 obscures this distinction.

## Equal offered load

Both APIs use the same optimized executable. Reads select `atomic()` or
`read()`; writes always use `atomic()`. Each request calls sixteen memories
concurrently. These are local in-process RuntimeService measurements on eight
physical cores, with 32 callers, 90% read requests, and 10% write requests.
Three alternating pairs use 3s warmup and 12s of scheduled arrivals.

The hot-entity case schedules 200 requests/s across sixteen entities, each
containing one 128-byte value. Both APIs sustain the offered rate. Median
request latencies across the three runs:

| Operation | Mean ms `atomic()` → `read()` | p99 ms `atomic()` → `read()` |
|---|---:|---:|
| Read | 3.55 → 1.64 | 22.56 → 2.58 |
| Write | 17.29 → 18.57 | 34.54 → 37.57 |

Read p99 improved in every pair. **Write latency did not improve**: write p99
ranged from 33.78–38.26 ms with `atomic()` reads and 36.11–96.29 ms with
snapshot reads. The isolated 96.29 ms sample is retained. This result does not
establish write-latency non-regression.

The distributed case schedules 3,000 requests/s across 256 entities, each
containing sixteen varied 4 KiB values. The `atomic()` baseline accumulated
substantial caller backlog in two of three runs. Median read p99 was
5,543.69 → 615.20 ms; write p99 was 5,584.13 → 719.48 ms. The baseline needed
up to 6.97 additional seconds to drain the scheduled requests; snapshot reads
needed at most 0.072 seconds after the arrival window ended.

Those medians do not imply consistently low latency. **The first distributed
pair regressed**: read p99 was 164.68 → 615.20 ms and write p99 was
186.83 → 719.48 ms. Candidate read p99 ranged from 60.24–685.00 ms, with most
of that tail already present in caller dispatch delay. The shared host and
variable storage waits materially limit interpretation. These capped-rate
runs establish neither a new peak-throughput figure nor production capacity.

All twelve fixed-rate processes completed every scheduled request and verified
460,800 memory writes before shutdown and after reopening. The [full results](MEMORY-LATENCY-RESULTS.md)
retain every pair's range, resource use, and effective rate.

## Where writes wait

Four separate saturated profiling runs cover the distributed and hot-entity
workloads, with both APIs. Profiling changes execution cost, so their throughput
is diagnostic. Native durations are elapsed time, including overlapping waits;
they are not CPU percentages.

| Workload / read API | Commands/commit | Mean writer queue µs | Mean SQL µs/group | Mean COMMIT µs/group | Mean entity lease wait µs |
|---|---:|---:|---:|---:|---:|
| Distributed / `atomic()` | 1.67 | 3,436 | 87 | 5,930 | 2,605 |
| Distributed / `read()` | 2.85 | 4,931 | 127 | 6,987 | 7,588 |
| Hot / `atomic()` | 1.00 | 1,136 | 43 | 5,683 | 10,144 |
| Hot / `read()` | 1.00 | 2,504 | 44 | 11,794 | 168,782 |

Snapshot lookup averaged 0.2–0.7 µs. Cache publication averaged 1–33 µs per
commit group. Writer thread samples frequently observed `btrfs_sync_log` and
other filesystem writeback waits. Those samples cover the whole process;
the table's native timings cover the timed phase.

In the distributed profile, snapshot-read request p99 was 4.16 ms while write
p99 was 59.45 ms. In the hot profile they were 2.91 ms and 794.82 ms. Hot writes
hold the entity lease through durable acknowledgement, so later writes wait
behind that commit. Snapshot reads can proceed independently. With a finite
caller pool, enough pending writes can still delay admission of new requests.

This evidence supports keeping the current snapshot/STM design. Optimizing
the tens of microseconds of SQL work would address a small part of the measured
write path. The next write-performance investigation should target durable
commit cost on the deployment's storage. Batching several commands for one hot
entity would require an explicit design for speculative state, failures, and
acknowledgements; the optional functional wrapper alone does not provide it.
No batching delay, reduced durability, or weaker consistency was introduced.

The [profiling diagnostics](MEMORY-LATENCY-DIAGNOSTICS.md) retain the individual
measurements, including the unfavorable hot-entity throughput result.

## Measurement controls and verification

`DD_FANOUT_REQUESTS_PER_SECOND`, exposed by the paired runner as
`--requests-per-second`, schedules request arrivals independently of completion.
Zero retains the saturated caller pool. The concurrency limit bounds active
callers, while scheduled timestamps account for requests waiting for a caller.
Latency starts at scheduled arrival, including timer delay and caller backlog.
All arrivals within the configured window drain before verification, even when
that extends the measured duration. Throughput divides completed requests by
actual elapsed time, including the drain.

`by_operation` separates read and write request counts, mean, p50, p95, p99,
and maximum latency. `dispatch_delay` reports the driver delay separately.
The runner rejects mismatched operation counts or missing scheduled requests.
The benchmark still validates every response, exact counters, all initialized
fields, and state after reopening.

A deliberate overload check scheduled 200 write requests in 200 ms using only
four callers. It completed all 200, took 1.162 seconds, and reported write p99
of 960.18 ms and dispatch-delay p99 of 941.99 ms. This proves that the driver
retains backlog in its latency measurement instead of stopping at the deadline
or discarding scheduled requests. Including warmup, 4,800 writes were verified.

`just check` passes: 407 Rust tests, four existing ignored tests, Clippy with
warnings denied, formatting, JavaScript, TypeScript, naming, and vendored-source
checks. The changed benchmark path was exercised with saturated, fixed-rate,
and deliberately overloaded traffic. Service and storage behavior are unchanged
from the preceding snapshot-read implementation and its SIGKILL recovery check.

## Reproduction and raw evidence

Both fixed-rate API variants used executable SHA-256
`916734e9d3f57011fe2eb4f38b3d1e68b5981259cf26d79574e2eef83c733d75`.
The build record identifies commit `4f30ef1054ed57e5e092625f2eb71cc50cc7b14e`
plus source patch
`0d9c1d135d248cbe96b78a1436ea7cb244420a77b31378691ba4ade8214ec818`.
The compiled benchmark and service sources still match the recorded build.
Later changes affect reporting and omit an unnecessary zero-valued environment
variable when running historical saturated benchmarks.

Use the [recorded build procedure](MEMORY-READ.md#verification-and-artifacts)
to create a fresh executable and build record. Then compare it with itself:

```sh
python3 scripts/compare-memory-fanout.py \
  --baseline "$read_build/bin/bench_memory_fanout" \
  --baseline-record "$read_build/build-record.json" \
  --candidate "$read_build/bin/bench_memory_fanout" \
  --candidate-record "$read_build/build-record.json" \
  --baseline-read-api atomic --candidate-read-api snapshot \
  --output /home/mewhhaha/dd-memory-latency-reproduction \
  --pairs 3 --concurrency 32 --cpu-count 8 \
  --requests-per-second 3000 --warmup-ms 3000 --duration-ms 12000 \
  --case mixed:16:256:4096:16:varied
```

For the hot-entity comparison, use a new output directory,
`--requests-per-second 200`, and `--case mixed:16:16:128`. Output directories
must reside on a physical filesystem. The archive includes the exact measured
runner, build records, source patches, profiles, raw samples, and check logs.

The [raw archive](/home/mewhhaha/dd-memory-tail-20260908.tar.gz) contains all
18 benchmark processes and evidence for 677,616 verified writes, including
profiling, overload, and runner checks. No database stores or executables are
included. Archive SHA-256:
`1ad867a207ebcdf11afb609604f5e9d23b36493316f6a5ff5c9968b07e0a4520`.
