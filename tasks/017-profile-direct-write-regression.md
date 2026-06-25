# Profile direct-write throughput variance and regression

**Priority:** P2 performance analysis  
**Primary area:** direct write benchmark path, memory store write path, benchmark summaries  
**Dependencies:** task 008 helps diagnosis, but this can start with existing profiles

## Problem

Direct-write throughput has varied significantly across recent measurements. The
fast path improved the original write bottleneck, but later repeated runs showed
lower cross-shard direct-write throughput than earlier one-sample runs. Some of
this is normal measurement variance, but the project should understand whether
there is a real regression, a benchmark artifact, or a trade-off introduced by
fairer scheduling and outbox changes.

## Objective

Isolate the source of direct-write throughput variance and document whether any
runtime or benchmark change is needed.

## Required analysis

Compare at least:

- storage-only direct write;
- runtime direct write;
- atomic read/write;
- atomic write/effect;
- same-shard and cross-shard distributions;
- 1, 2, 4, 8, and 16 isolates;
- 3 or 5 samples on a clean worktree.

Break down direct-write time into:

- JS stub overhead;
- op entry and serialization;
- `MemoryStore.apply_batch` validation;
- transaction/write duration;
- snapshot/cache update;
- verification phase;
- scheduler queue/dispatch wait if any.

## Candidate causes to rule in or out

- benchmark verification dominates or overlaps incorrectly;
- direct-write requests still pass through a runtime scheduling bottleneck;
- connection/cache changes increase tail latency;
- snapshot cache updates clone too much state;
- CPU oversubscription changes p95/p99 but not median;
- one-sample historical numbers were simply optimistic.

## Tests and tooling

Add or improve profiling output so direct-write benchmarks can emit a compact
phase breakdown. The result should be machine-readable enough to include in
future summaries.

## Acceptance criteria

- A short Markdown note or generated summary explains the variance.
- If a real regression exists, a follow-up implementation task or fix is filed.
- Benchmarks distinguish timed workload from verification overhead.
- No raw JSON is committed.

## Non-goals

- Chasing small RPS changes without repeated samples.
- Replacing the direct-write fast path.
- Changing correctness semantics.
