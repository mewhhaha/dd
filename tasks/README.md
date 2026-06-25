# Parallel performance tasks

This directory is the active implementation backlog for bounded performance
work on current `main`. Completed task briefs are removed when their behavior is
present and tested in the repository.

Read [`PARALLEL_PERFORMANCE.md`](PARALLEL_PERFORMANCE.md) for the current-state
summary, benchmark interpretation, and why larger runtime-manager partitioning
is deferred.

Each task is written so it can be assigned directly to a GPT-5.5-medium
implementation model without relying on conversation history.

## Recommended order

| Order | Task | Priority | Main expected benefit |
| ---: | --- | --- | --- |
| 1 | [Add a CPU-aware global isolate budget and production tuning surface](001-global-isolate-budget-and-runtime-tuning.md) | P0 | Uses host parallelism without multiplying a per-worker isolate cap across many workers. |
| 2 | [Drain independent memory outbox shards concurrently](002-parallelize-memory-outbox-shard-drains.md) | P0 | Removes the remaining single background pipeline for cross-shard durable effects. |
| 3 | [Stripe memory-store version and snapshot locks by entity](003-stripe-memory-store-hot-locks.md) | P1 | Lets independent IDs in one physical shard avoid shared cache/version mutexes. |
| 4 | [Make memory-owner readiness and rotation constant time](004-make-memory-owner-ready-queue-constant-time.md) | P1 | Reduces single-manager scheduling work for large keyspaces and hot-owner skew. |
| 5 | [Reuse memory database connections and serialize writers before SQLite contention](005-reuse-memory-database-connections.md) | P1 | Avoids per-request connection setup and reduces same-database writer lock thrash. |

## Completed work removed from the backlog

Current `main` already contains the major items from the previous task set:

- persisted/validated memory shard layouts with legacy and mixed-layout adoption;
- per-owner scheduling inside physical memory shards;
- background outbox storage I/O;
- deduplicated database initialization outside cache locks;
- bounded database and snapshot caches;
- safe benchmark matrix planning and run budgets;
- generated scaling summaries;
- memory scheduler/outbox observability;
- relative performance regression policies;
- broad benchmark filtering, lifecycle markers, explicit shutdown, and crash
  reproduction tooling.

## Task execution rules

- Implement one task per pull request unless the task explicitly requires a
  small prerequisite refactor.
- Preserve same-owner atomic FIFO and single-writer semantics.
- Keep concurrency bounded; do not replace one bottleneck with unbounded tasks,
  threads, connections, or queues.
- Add focused correctness tests and deterministic concurrency tests.
- Run the relevant relative regression policy and real-world workload with at
  least three samples on a clean worktree.
- Record commit, logical CPU count, requests, concurrency, isolates, shards,
  throughput, p95, and p99 for performance claims.
- Run `just check` before requesting review.
- Do not commit raw files under `benchmarks/results/`; update durable summaries
  when conclusions materially change.
