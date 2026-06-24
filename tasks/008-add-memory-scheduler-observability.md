# Add memory scheduler observability

**Priority:** P1 operations and performance diagnosis  
**Primary area:** runtime stats/debug models, memory profiling, scheduler and outbox tracing  
**Dependencies:** coordinate field names with tasks 002 and 003

## Problem

The runtime now has shard-aware queues, entity leases, shard-to-isolate affinity,
and bounded outbox drains, but operator-visible stats expose mostly aggregate
worker values:

- total queued requests;
- total busy/inflight/wait-until counts;
- isolate count and spawn/reuse/scale-down counts.

Memory profiling has useful phase timers, but it does not provide a complete
picture of scheduler pressure. When throughput plateaus or p99 grows, it is hard
to distinguish:

- one leased hot entity from a fully saturated physical shard;
- shard-head or owner-head blocking from isolate saturation;
- affinity hits from fallback/overflow dispatch;
- manager event-loop delay from JavaScript execution time;
- queue admission rejection from queue wait expiration;
- outbox claim, delivery, and acknowledgement backlog;
- stale affinity entries after isolate scale-down;
- cache lock contention from scheduler contention.

Raw entity keys and namespaces must not become metric labels because that would
create unbounded cardinality and leak tenant identifiers.

## Objective

Add bounded, privacy-safe scheduler and outbox observability sufficient to
explain sharding performance and hot-key behavior in production and benchmarks.

## Required metrics

### Queue and lease gauges

Expose process/worker-pool values for:

- total memory-lane queued requests;
- number of physical memory shards with queued work;
- maximum and median queued depth across active memory shards;
- number of owner queues with pending work if task 002 has landed;
- active memory entity leases;
- number of queued requests blocked by an active owner lease;
- oldest memory queue age;
- targeted, general, and memory lane depths separately;
- global queued bytes and configured limits.

Avoid exporting one metric series per shard by default. For debug snapshots,
include a bounded top-N list of shard summaries.

### Dispatch counters and timings

Add counters for:

- memory shard affinity hit;
- affinity miss because no mapping exists;
- affinity miss because mapped isolate no longer exists;
- affinity miss because mapped isolate is saturated;
- least-loaded fallback;
- atomic overflow dispatch above the ordinary inflight threshold;
- candidate rejected due to active owner lease;
- candidate rejected due to isolate startup/wait-until constraints;
- number of shard heads/owner heads inspected per successful dispatch;
- dispatch loops that found queued work but no ready candidate;
- per-memory-call queue wait and dispatch wait histograms/profile totals.

### Runtime manager responsiveness

Measure manager-loop delay without adding a blocking timer:

- event queue wait from event creation to handling;
- command queue wait where a creation timestamp is available;
- completion wait already tracked for atomics;
- periodic tick drift;
- maximum ready-work drain batch size;
- count of times `RUNTIME_READY_WORK_BUDGET` is exhausted;
- duration of one manager batch and its maximum.

Use monotonic timestamps. Make detailed timing conditional on the existing
profile flag if always-on cost is measurable.

### Outbox metrics

Expose:

- pending scheduled shard count;
- claim batches/rows and saturated batches;
- delivery success, retry, and terminal-drop counts;
- acknowledgement failures;
- oldest due record age if available cheaply;
- background in-flight batches after task 003;
- channel full/reschedule counts.

### Cache correlation

Expose aggregate memory database/snapshot cache hit/miss/eviction counts when
task 005 lands. Define metric names now so future tasks do not invent
incompatible naming.

## Debug dump changes

Extend `WorkerDebugDump` or add a nested memory scheduler section. Suggested
shape:

```rust
pub struct MemorySchedulerDebug {
    pub queued: usize,
    pub active_leases: usize,
    pub active_shards: usize,
    pub affinity_entries: usize,
    pub stale_affinity_entries: usize,
    pub oldest_queue_ms: u64,
    pub top_shards: Vec<MemoryShardDebug>,
}

pub struct MemoryShardDebug {
    pub shard_index: usize,
    pub queued: usize,
    pub ready_owners: usize,
    pub blocked_owners: usize,
    pub affinity_isolate_id: Option<u64>,
}
```

Requirements:

- limit `top_shards` to a small deterministic number, configurable or fixed;
- sort by queue depth, then shard index;
- never include raw entity keys, values, payloads, or namespace names;
- include stale affinity count and clear stale mappings during normal cleanup;
- use `#[serde(default)]` or equivalent compatibility where debug output is
  consumed externally.

## Logging and tracing

Add structured fields to existing queue/dispatch spans rather than high-volume
info logs. Candidate fields include:

- queue lane;
- physical shard index;
- whether the call is atomic;
- affinity outcome enum;
- queue wait duration;
- owner blocked boolean without owner identity;
- inspected candidate count;
- isolate inflight count at dispatch.

Do not log `memory_key` at info level. If debug logs already expose it, retain
existing policy but do not broaden exposure.

## Metrics API choice

Use the project's existing profiling/tracing mechanisms. Do not add a new
metrics backend solely for this task.

If `MemoryProfileSnapshot` is the practical surface:

- add scheduler counters/gauges in a backward-compatible way;
- distinguish cumulative counters from reset-on-take profile metrics;
- avoid representing gauges as misleading durations.

If worker stats are returned over an API, update all serialization, CLI display,
and tests consistently.

## Tests

Add tests that exercise metric transitions rather than only field existence:

1. enqueueing general and memory work updates separate lane counts;
2. acquiring/releasing a lease updates active and blocked counts;
3. affinity hit, missing, stale, saturated, fallback, and overflow paths each
   increment the correct counter;
4. removing/scaling down an isolate clears or reports stale affinity entries;
5. dispatch inspection count is bounded for a known queue shape;
6. manager event wait records a deliberately delayed event;
7. ready-work budget exhaustion increments its counter;
8. debug dump top-N is bounded and deterministically sorted;
9. debug output contains no memory owner keys;
10. outbox scheduling, saturation, retry, and success update counters;
11. profile reset resets resettable fields but does not corrupt live gauges;
12. concurrent updates use atomics or manager ownership without data races.

Use test-only direct accessors where necessary rather than parsing logs.

## Benchmark integration

Update `bench_memory_storage` profile output so a profiled benchmark prints a
compact scheduler breakdown, including averages such as:

```text
atomic queue wait
candidate heads inspected
lease-blocked selections
affinity hit rate
manager event wait
outbox claim/delivery/ack time
```

Keep output machine-parseable and extend the benchmark result schema if these
metrics are captured in JSON.

Run cross-shard, same-shard, and skewed-hotspot modes and confirm the metrics tell
an internally consistent story. For example, same-shard runs should show more
blocked-owner or storage contention than cross-shard runs.

## Acceptance criteria

- Worker stats/debug output can show memory queue, lease, shard, and affinity
  state without exposing entity IDs.
- Scheduler counters distinguish affinity and fallback paths.
- Manager-loop wait and budget exhaustion are measurable.
- Outbox backlog and outcomes are measurable.
- Debug output is bounded in size and deterministic.
- Profile/benchmark output includes a concise scheduler breakdown.
- Tests verify transitions and privacy constraints.
- Always-on overhead is measured and documented; disable expensive detail when
  profiling is off if necessary.
- `just check` passes.

## Non-goals

- Building a metrics server or dashboard.
- Adding per-tenant metric labels.
- Setting performance regression thresholds; task 009 does that.
- Rewriting the scheduler.

## Implementation cautions

- Counts derived from manager-owned maps can be read without atomics inside the
  manager, but cross-thread profile fields require atomics or messages.
- Avoid scanning every queued request merely to produce a frequently requested
  stats snapshot. Maintain cheap counters or bound the scan.
- Do not let observability change dispatch order or hold queue locks across
  serialization.
