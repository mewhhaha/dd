# Parallel performance review

This review targets current `main` at commit `a079003` and focuses on bounded,
reasonably implementable changes that can improve multi-core throughput without
rewriting the runtime.

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
  structural regression checks exist.

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

## Remaining practical levers

### 1. Allocate isolates globally and expose production tuning

`RuntimeConfig` still defaults to 8 isolates and 4 inflight requests per isolate,
while the server CLI does not expose runtime or memory tuning. The isolate limit
is per worker pool, so multiple busy workers can each reach the cap and
oversubscribe the host. Conversely, one busy worker on a larger host can be
artificially limited.

The most useful near-term change is a CPU-aware global isolate budget with fair
allocation across worker pools, plus explicit server configuration. This should
improve multi-worker parallelism and prevent thread explosion.

### 2. Drain independent outbox shards concurrently

Outbox database I/O is no longer on the manager, but one background task still
receives every `DrainShard` command and processes commands serially. Effect-heavy
work on independent storage shards therefore shares one claim/ack pipeline.

A bounded concurrent shard worker should improve atomic write/effect throughput
without changing delivery semantics.

### 3. Stripe memory cache/version locks within physical shards

Each physical `MemoryShard` still has one mutex for memory-version metadata and
one mutex for the shared snapshot cache. Independent entity IDs mapped to the
same physical shard contend on those locks even though the scheduler now treats
them independently.

Striping these structures by entity hash is a contained way to extend the
scheduler’s parallelism into the storage cache layer.

### 4. Make the owner-ready scheduler constant time

The per-shard scheduler uses a `VecDeque<String>` owner ring. Selection can scan
blocked owners; advancing finds the selected owner linearly and rotates the
ring; removing an owner retains across the full deque. This is acceptable at the
current keyspace but becomes manager-thread work proportional to active owners.

Maintaining explicit ready/blocked owner queues and waking an owner when its
lease is released can reduce single-thread scheduling overhead under large
keyspaces and hot-key skew.

### 5. Reuse configured database connections

The `Database` handle is cached, but each operation still calls
`database.connect()` and configures a new connection. At high concurrency this
adds repeated setup and can amplify SQLite/Turso lock contention.

A small bounded per-database connection pool, or one writer lane plus pooled
readers, should be evaluated. The implementation must be driven by profiling and
Turso thread-safety constraints rather than assumed to help.

## What not to do yet

A multi-threaded or partitioned `WorkerManager` is a plausible future ceiling,
but it is not the next reasonable change. The existing results do not yet prove
that the manager loop dominates after 8 isolates. Splitting manager ownership
would affect deployment, cancellation, sessions, dynamic RPC, queue limits, and
shutdown correctness.

First implement or measure the five bounded levers above. Revisit manager
partitioning only if manager event wait, budget exhaustion, or dispatch CPU
remains dominant afterward.

## Recommended order

1. Global isolate budget and production configuration.
2. Concurrent outbox shard draining.
3. Stripe memory cache/version locks.
4. Constant-time owner-ready scheduling.
5. Connection reuse/pooling after profiling.

Every performance PR should run the relative regression policy plus the relevant
real-world workload. Use at least three samples on a clean worktree and record
commit, CPU count, concurrency, isolate count, shard count, p95, and p99. Raw
result JSON remains ignored.
