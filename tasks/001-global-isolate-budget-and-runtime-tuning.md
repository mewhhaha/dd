# Add a CPU-aware global isolate budget and production tuning surface

**Priority:** P0 parallel performance and resource control  
**Primary areas:** `RuntimeConfig`, autoscaling/dispatch, `dd_server` CLI/env config  
**Scope:** bounded scheduler/configuration change; no manager partitioning

## Problem

`RuntimeConfig::default()` currently uses:

```text
max_isolates = 8
max_inflight_per_isolate = 4
```

The isolate limit is applied independently to every worker pool. There is no
global isolate budget across workers, and the production `dd_server` CLI does
not expose runtime/memory tuning values.

This creates two opposite failure modes:

1. A single busy worker on a host with more than eight useful CPUs can remain
   capped below available parallelism.
2. Many busy workers can each scale toward eight isolates, creating far more
   isolate threads and potential V8 heaps than the host can run efficiently.

The new multi-worker auth benchmark makes this operationally relevant. Parallel
performance should come from allocating a finite host budget to active workers,
not multiplying a per-worker default indefinitely.

## Objective

Introduce a process-wide isolate budget, allocate it fairly across worker pools,
and expose the relevant settings through `dd_server` CLI/environment variables.
Keep the existing per-worker cap as a secondary limit.

## Proposed configuration

Add fields with names consistent with project conventions, for example:

```rust
pub max_global_isolates: usize,
pub max_isolates: usize,               // per worker generation
pub max_inflight_per_isolate: usize,
```

Recommended server-facing flags/env variables:

```text
--runtime-max-global-isolates / DD_RUNTIME_MAX_GLOBAL_ISOLATES
--runtime-max-isolates-per-worker / DD_RUNTIME_MAX_ISOLATES_PER_WORKER
--runtime-max-inflight-per-isolate / DD_RUNTIME_MAX_INFLIGHT_PER_ISOLATE
--runtime-min-isolates-per-worker / DD_RUNTIME_MIN_ISOLATES_PER_WORKER
```

Use `std::thread::available_parallelism()` to choose the built-in global default.
A reasonable starting policy is one isolate per logical CPU, clamped to at least
one. Do not silently make the per-worker minimum equal to CPU count.

Keep programmatic `RuntimeServiceConfig` fully supported.

## Budget accounting

Count all isolate slots that consume host resources:

- ready isolates;
- starting isolates;
- retiring isolates until their thread has actually exited, if the runtime can
  observe that state;
- isolates belonging to older worker generations that are still draining.

Do not allow concurrent scale-up decisions to overshoot the budget.

Add cheap manager-owned counters rather than scanning every worker pool for each
spawn attempt.

## Fair allocation

When several pools have queued work and the global budget has free capacity,
allocate new isolate slots fairly.

The current `dispatch_pool` call can request a spawn synchronously for one pool.
Avoid a policy where whichever worker happens to enqueue first repeatedly takes
all remaining slots.

A contained design is:

1. `dispatch_pool` marks a pool as needing scale-up when it has no dispatch
   capacity;
2. the manager stores scale-up requests in an ordered/deduplicated queue;
3. a bounded scale-up pass grants slots round-robin across pools;
4. successful spawn consumes a global slot immediately, including startup time;
5. failure releases the slot and preserves or rejects queued work according to
   existing behavior.

Worker generations should be identified by `(worker_name, generation)`.

## Minimum isolates

Define startup behavior when the sum of requested per-worker minimums would
exceed the global budget.

Preferred behavior:

- validate `min_isolates <= max_isolates_per_worker`;
- do not prestart every deployed worker indefinitely when `min_isolates = 0`;
- if nonzero minimums exceed the global budget, fail configuration or deployment
  clearly rather than oversubscribing silently;
- draining old generations count against the global budget until retired.

Document the semantics.

## Scale down and slot release

Release the global slot exactly once when an isolate is removed. Cover:

- normal idle scale-down;
- startup failure/timeout;
- wall-time termination;
- deployment generation retirement;
- explicit service shutdown;
- channel-send failure that retires an isolate.

Add debug assertions or invariant checks in tests so counter drift cannot remain
silent.

## Observability

Extend runtime stats/debug output with bounded aggregate fields:

```text
global_isolate_budget
global_isolates_total
global_isolates_starting
global_isolate_slots_available
scale_up_waiting_pools
scale_up_budget_denied_count
```

Do not add worker names as metrics labels if that creates unbounded cardinality.
Worker debug dumps may show per-pool counts as they already identify workers.

## Server configuration validation

Validate before starting the runtime:

- all isolate/inflight values are positive where zero is not meaningful;
- min per worker does not exceed max per worker;
- max per worker does not exceed the global budget unless explicitly allowed as
  a ceiling that can never be fully reached;
- values that would imply unreasonable overflow in queue/capacity arithmetic are
  rejected;
- error messages name the exact CLI/env setting.

Update `--help`, deployment documentation, Fly configuration examples, and any
dev-runtime configuration path that should match production.

## Tests

Add focused unit/integration tests for at least:

1. one busy worker can grow to the global budget when its per-worker cap permits;
2. two busy workers share a small global budget rather than one consuming every
   slot;
3. a third worker eventually receives a slot under sustained load;
4. starting isolates count toward the budget;
5. startup failure releases a slot;
6. idle scale-down releases a slot;
7. old and new generations share the same global budget during deployment;
8. min-isolate configuration that exceeds the budget fails clearly;
9. cancellation and request completion do not alter isolate accounting;
10. server CLI/env parsing reaches `RuntimeServiceConfig` correctly;
11. defaults use `available_parallelism()` deterministically through an injected
    test helper;
12. repeated deploy/scale/retire cycles leave global count equal to actual live
    isolate count.

Use barriers or test hooks for deterministic simultaneous scale-up tests rather
than timing-only assertions.

## Benchmark validation

Run clean, repeated benchmarks for:

- single-worker atomic read/write;
- real-world multi-worker auth;
- at least four independent busy workers;
- global budgets `1, 2, 4, 8, 16` on the recorded 16-CPU host;
- an oversubscription control where per-worker caps would otherwise exceed the
  global budget.

Report throughput, p95/p99, isolate count, startup count, and CPU utilization.

Expected result:

- single-worker scaling remains comparable to current results;
- multi-worker throughput improves as the global budget increases up to useful
  host parallelism;
- tail latency and CPU scheduling improve versus unconstrained per-worker
  multiplication;
- no regression in same-owner atomic ordering.

## Acceptance criteria

- The runtime enforces a process-wide isolate count limit.
- Scale-up is fair across active worker pools.
- Starting and draining isolates are included in accounting.
- `dd_server` exposes and validates global/per-worker/inflight settings.
- The default global budget is CPU-aware.
- Multi-worker benchmarks demonstrate bounded scaling without thread explosion.
- Existing lifecycle, scheduler, memory, and shutdown tests pass.
- `just check` passes.

## Non-goals

- Partitioning `WorkerManager` across threads.
- Dynamically changing budgets at runtime.
- CPU pinning or NUMA-aware placement.
- Guaranteeing one isolate equals one fully utilized core.

## Implementation cautions

- `max_isolates` currently appears in tests and struct literals; preserve source
  compatibility where practical or update all call sites deliberately.
- A starting isolate consumes substantial memory even before it becomes ready.
- Avoid holding mutable pool borrows while updating global scheduling queues.
- Do not use global atomics where manager ownership can keep accounting simpler.
