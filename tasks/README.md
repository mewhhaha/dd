# Parallel performance tasks

This directory tracks bounded performance work for current `main`. Completed
task briefs are removed when their behavior is present and tested in the
repository.

Read [`PARALLEL_PERFORMANCE.md`](PARALLEL_PERFORMANCE.md) for the current-state
summary, benchmark interpretation, and why larger runtime-manager partitioning
is deferred.

Each task is written so it can be assigned directly to a GPT-5.5-medium
implementation model without relying on conversation history.

## Active backlog

No active parallel performance task briefs remain in this batch.

## Completed work removed from the backlog

Current `main` already contains the major items from the previous task set:

- CPU-aware global isolate budgeting with explicit production tuning knobs;
- bounded concurrent memory outbox shard drains;
- striped memory-store version and snapshot cache locks;
- constant-time memory-owner ready queue management;
- bounded memory database connection reuse with a single writer lane and pooled
  readers;
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
