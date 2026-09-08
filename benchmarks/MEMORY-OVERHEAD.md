# Memory overhead investigation — 2026-09-08

This investigation did **not establish another general service throughput
improvement**. Three prototypes were measured against `964f7a7`; the broader
changes were rejected. The retained production change defers SHA-256 shard
routing until a snapshot actually needs a SQL load. Warm cache hits already
have a fully qualified entity key, so they can return without that hash.
Its standalone throughput benefit was not measured. None of the prototype
gains below should be attributed to the retained change.

The other retained change adds amortized CPU milliseconds per request to
`scripts/summarize-memory-fanout.py`. It divides whole-process user and system
CPU time by completed warmup and timed requests. Setup and verification CPU
are included; this is not an isolated measurement of service CPU cost.

## What was tried

1. **Concurrent cache readers.** Replace the snapshot cache mutex and exact LRU
   bookkeeping with a read/write lock, atomic reference bits, and CLOCK
   eviction. Readers share the lock; publication and eviction take it
   exclusively. This also included deferred shard hashing. Initial samples
   looked promising, but the repeated core matrix did not support keeping it.
2. **Synchronous clock refresh.** Remove redundant promise turns around the
   already synchronous native clock operation, while preserving frozen clocks
   during JavaScript computation. Host I/O remains awaited. Together with
   deferred shard hashing, one preliminary pair per workload showed +3.0% for
   small warm reads, −0.9% for larger reads, and −6.7% for mixed traffic. This
   was insufficient evidence of an improvement.
3. **Clock refresh plus request-context reuse.** Extend the second prototype
   by reusing the existing request context for synchronous snapshot callbacks;
   restore the temporary memory scope on return or error. Atomic transactions
   keep their own context. This produced modest warm-read gains in the core
   matrix, but failed the additional workloads and write-tail checks.

All three prototypes are reverted. Their exact source patches and raw runs
are preserved in the evidence archive. No optional implementation or tuning
flag remains in the service.

## Core scaling

Both sides use `memory.read()` with the same benchmark sources, optimized
`dist` profile, 32 callers, and sixteen concurrent memories per request.
The population has 256 entities, each with sixteen varied 4 KiB values; reads
select two fields. Mixed traffic is 90% read requests and 10% write requests.
Each row has three alternating pairs, 2s warmup and 8s timed work.

The following changes are medians of **matched candidate/baseline ratios**,
not ratios of separately calculated throughput medians. Ranges retain every
pair and are not confidence intervals.

| Physical cores | Workload | Cache prototype change | Clock/context prototype change | Clock/context pair range |
|---:|---|---:|---:|---:|
| 1 | Read | +1.1% | +5.7% | −15.1% to +10.8% |
| 1 | Mixed | +1.4% | +2.1% | −1.2% to +7.1% |
| 4 | Read | −5.2% | +6.4% | +2.2% to +7.8% |
| 4 | Mixed | −5.7% | +5.9% | −0.7% to +6.3% |
| 8 | Read | +1.0% | +4.5% | −44.8% to +12.2% |
| 8 | Mixed | −25.0% | −6.0% | −6.9% to +17.5% |

The clock/context prototype reduced amortized CPU cost in all three four-core
read pairs by roughly 6–7%. That saving did not establish an improvement for
the complete workload mix. In eight-core mixed traffic, write p99 increased
in **every pair**: 79.10 → 107.40 ms, 168.10 → 199.00 ms, and
66.57 → 74.57 ms. Commands per durable commit did not consistently decrease,
so these observations alone do not identify commit batching as the cause.

## Additional workloads

These compare the clock/context prototype with the same baseline on eight
physical cores and 32 callers. There are three alternating pairs per row,
3s warmup and 12s timed work. Small memories contain one 128-byte value.

| Workload | Fanout / entities | Median paired throughput change | Every pair's change | Median combined p99 ms before → prototype |
|---|---|---:|---|---:|
| Small mixed | 4 / 1,024 | +1.0% | +1.0%, −6.1%, +128.9% | 32.28 → 29.28 |
| Small read | 16 / 1,024 | +4.0% | +9.4%, +4.0%, −17.0% | 11.49 → 11.66 |
| Small mixed | 16 / 1,024 | +2.7% | +2.7%, −53.1%, +12.3% | 42.54 → 62.94 |
| Beyond cache: read | 16 / 512 | −15.8% | −46.2%, −15.8%, +0.3% | 328.35 → 381.41 |
| Durable write | 16 / 256 | +1.7% | +5.4%, −8.9%, +1.7% | 113.26 → 159.35 |

The beyond-cache population contains sixteen varied 16 KiB fields per entity:
128 MiB of value payload against a 64 MiB native snapshot cache. Its native
cache miss rate was approximately 93% on both sides. The write-only population
uses sixteen varied 4 KiB fields per entity.

The large positive and negative outliers are both retained. These shared-host
measurements cannot cleanly isolate implementation effects from changing CPU
and storage contention. They also cannot justify shipping the prototype as a
general improvement. In particular, a better median read result does not
compensate for worse cold-read throughput and durable-write tails.

## Outcome and limits

The service retains its existing snapshot cache, read-context isolation,
JavaScript scheduling, write leases, bounded busy retries, version floors,
and durable commit acknowledgements. Only the location of shard hashing
changes in production code. No durability or consistency tradeoff was made.

The [previous commit-latency investigation](MEMORY-LATENCY.md#where-writes-wait)
remains the strongest measured lead for improving durable-write performance.
These trials do not prove that cache locking or JavaScript scheduling can
never improve. They show that these particular replacements are not supported
by the measured workload mix. Further write work should first measure durable
commit cost on the intended deployment storage; this round does not establish
new production capacity or a limit on the architecture.

All 114 benchmark processes validated responses and exact state before
shutdown and after reopening, covering **4,919,444 completed writes**. The
[complete diagnostic tables](MEMORY-OVERHEAD-RESULTS.md) include both preliminary
campaigns, every repeated campaign, operation-specific latency, cache misses,
commit density, CPU, and RSS. Measurements call RuntimeService directly on the
local Ryzen 7800X3D host; they do not include the public HTTP path or Fly.

On the final retained implementation, `just check` passes: 407 Rust tests,
four existing ignored tests, Clippy with warnings denied, formatting,
JavaScript, TypeScript, naming, and vendored-source checks. A rebuilt
`dd_server` also passes `just check-state-crash` using a disposable store on
physical storage: acknowledged KV/memory writes and deletes survive SIGKILL,
idempotent commands replay once, and worker namespaces remain isolated.

## Reproduction and evidence

Every build starts from commit
`964f7a77cdd4b12f0a4432444ba5947092cdad63`. The build records identify the
toolchain, locked dependencies, exact source patch, benchmark source hashes,
and executable hashes. The baseline has an empty source patch. Candidate
patches include the prototype tests used during development.

| Build directory suffix | Prototype | Source patch SHA-256 |
|---|---|---|
| `clock-baseline-build-20260908` | Baseline | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `clock-candidate-build-20260908` | Concurrent cache readers | `384a2392d120193f369270792098e1c023adc21bee4ec95b333e6b58d501f833` |
| `time-candidate-build-20260908` | Synchronous clock refresh | `e15f28267b165c52fd0763d4a9f940d51ec76365882ac54dd4c35b572cb8b7a3` |
| `boundary-candidate-build-20260908` | Clock refresh/context reuse | `3656a81d119921608466ddf513e7236b67e3367234886c9ca280c48f01c88fae` |

Directory names above have the prefix `dd-memory-`. Extract the archive,
create isolated checkouts of the recorded commit, apply the desired
`source.patch` within its checkout, and use the archived
`build-state-benchmarks.py` with explicit `--source`, `--target-dir`,
`--output`, and `--bin bench_memory_fanout`. Copy the resulting executable
from the target's `dist/` directory into the build directory's `bin/` directory.
The recorded baseline and candidate benchmark sources are identical.

For example, reproduce the final prototype's core matrix with newly built
binaries and their build records:

```sh
python3 scripts/compare-memory-fanout.py \
  --baseline "$baseline_build/bin/bench_memory_fanout" \
  --baseline-record "$baseline_build/build-record.json" \
  --candidate "$candidate_build/bin/bench_memory_fanout" \
  --candidate-record "$candidate_build/build-record.json" \
  --baseline-read-api snapshot --candidate-read-api snapshot \
  --output /path/on/physical-storage/new-comparison \
  --pairs 3 --concurrency 32 --warmup-ms 2000 --duration-ms 8000 \
  --cpu-count 1 --cpu-count 4 --cpu-count 8 \
  --case read:16:256:4096:16:varied \
  --case mixed:16:256:4096:16:varied
```

All campaign commands are retained in the task scripts. Raw manifests also
record the exact settings and host state. The archive excludes binaries and
database directories; rebuilding and rerunning regenerates them.

Raw evidence: `/home/mewhhaha/dd-memory-overhead-20260908.tar.gz`
(1,809,105 bytes, 453 files). SHA-256:
`0e38501b7a900999a9815306455982f7f676011b482eed93993b73e2b67ef04a`.
The archive includes source patches, build records, per-process stdout/stderr,
paired summaries, the build and campaign scripts, the final production patch,
and final check logs. Its internal manifest hashes every included evidence
file; archive creation verified those hashes by reading the completed archive.
