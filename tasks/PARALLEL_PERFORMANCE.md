# Parallel performance review

This review was written against `main` at commit `a079003` and has been updated
after the bounded parallel-performance backlog was implemented.

## Current state

The keyed-memory architecture is now fundamentally sound for parallel workloads.
The previous major bottlenecks have been addressed:

- memory requests are queued by physical shard and by owner, so a leased entity
  no longer hides unrelated entities in the same shard;
- atomic owner leases preserve same-entity serialization;
- memory shard affinity prefers warm isolates but falls back when the preferred
  isolate is saturated;
- database initialization is deduplicated with `OnceCell` and does not hold the
  cache mutex across database build/schema awaits;
- database and shared-snapshot caches are bounded and maintain recency without a
  full-map sort on each access;
- outbox claim and acknowledgement storage I/O run outside the runtime manager;
- scheduler, affinity, queue, and outbox counters are exposed;
- benchmark matrices are planned/budgeted, summaries are generated, and
  structural regression checks exist;
- isolate allocation is governed by a CPU-aware global budget with explicit
  production tuning;
- outbox shard drains run concurrently with bounded per-shard scheduling;
- memory version and snapshot cache locks are striped by entity;
- memory owner readiness uses explicit ready/blocked queues;
- memory database handles reuse configured connections with pooled readers and a
  single writer lane before SQLite/Turso contention.

The five-sample 16-shard benchmark summary shows useful scaling:

| Cross-shard workload | 1 isolate | 8 isolates | 16 isolates | 32 isolates |
| --- | ---: | ---: | ---: | ---: |
| Direct write | 5,504 req/s | 15,575 req/s | 15,085 req/s | 17,077 req/s |
| Atomic read + write | 2,367 req/s | 8,395 req/s | 8,168 req/s | 9,069 req/s |
| Atomic write + effect | 1,491 req/s | 3,906 req/s | 3,673 req/s | 4,103 req/s |

The app-shaped snapshot is also credible: approximately 8,891 req/s for the
memory-backed rate limiter and 7,521 req/s for the multi-worker auth flow on the
recorded 16-logical-CPU host.

The shape matters more than the absolute values:

- parallel improvement is strong from 1 to 8 isolates;
- throughput mostly plateaus from 8 to 16;
- 32 isolates provide only a modest gain on a 16-logical-CPU host;
- same-shard controls remain flat, as required by ordered transactional work.

## Completed bounded levers

The bounded levers identified in this review have been implemented:

1. Global isolate budget and production configuration.
2. Concurrent outbox shard draining.
3. Striped memory cache/version locks.
4. Constant-time owner-ready scheduling.
5. Connection reuse with pooled readers and serialized writers.

## What not to do yet

A multi-threaded or partitioned `WorkerManager` is a plausible future ceiling,
but it is not the next reasonable change. The existing results do not yet prove
that the manager loop dominates after 8 isolates. Splitting manager ownership
would affect deployment, cancellation, sessions, dynamic RPC, queue limits, and
shutdown correctness.

Revisit manager partitioning only if manager event wait, budget exhaustion, or
dispatch CPU remains dominant after the completed bounded changes.

## Benchmark discipline

Every performance PR should run the relative regression policy plus the relevant
real-world workload. Use at least three samples on a clean worktree and record
commit, CPU count, concurrency, isolate count, shard count, p95, and p99. Raw
result JSON remains ignored.
