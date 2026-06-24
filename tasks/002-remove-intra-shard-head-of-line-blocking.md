# Remove intra-shard head-of-line blocking

**Priority:** P0 performance  
**Primary area:** `crates/runtime/src/service/model.rs`, `runtime.rs`, scheduler tests  
**Dependencies:** task 001 is recommended before changing physical shard behavior

## Problem

The memory scheduler currently stores requests in one FIFO `BTreeMap` per
physical storage shard. Fair selection considers only `first_key_value()` for
each shard.

That design prevents an entire shard from making progress when its oldest
request belongs to an entity that already has an active atomic lease. A later
request for a different entity ID in the same physical shard is hidden behind
the blocked head even though entity ordering would allow it to run.

The unit test
`pending_invoke_queue_memory_lookup_only_considers_shard_heads` explicitly
captures this behavior. It made shard-level fairness cheap, but it also means
that independent user or room IDs are only independent when they hash to
different physical shard files.

This is especially harmful for:

- a large number of entities mapped onto a modest shard count;
- skewed workloads where one hot entity repeatedly occupies a shard head;
- long atomic callbacks that block short callbacks for unrelated IDs;
- socket or transport wakeups queued behind a busy atomic owner.

The required correctness rule is per owner key, not per physical shard:
atomic work for the same binding and entity ID must remain ordered and
non-concurrent. Different owner keys should be schedulable independently even
when they share a storage shard.

## Objective

Redesign the memory queue so the scheduler can select ready work from another
owner within the same physical shard while preserving strict FIFO and
single-writer semantics for each owner key.

## Required behavior

### Per-owner ordering

For every `MemoryRoute.owner_key`:

- requests must be observed in enqueue order;
- at most one atomic request may hold the active entity lease;
- a second atomic request for that owner must remain blocked until the lease is
  released;
- cancellation, expiry, isolate failure, and generation retirement must not
  reorder the remaining requests for that owner.

### Intra-shard independence

Given this queue order on one physical shard:

```text
A1, A2, B1
```

and an active lease for owner A:

- `A1` or `A2` must not dispatch while A is leased;
- `B1` must be eligible to dispatch immediately;
- after A's lease is released, A's oldest remaining request must dispatch before
  later A requests.

The scheduler must not require B to hash to a different physical shard.

### Fairness

Retain fairness both:

- across physical shard indices; and
- across ready owners within one shard.

A continuously busy owner must not monopolize its shard. A continuously busy
shard must not monopolize the memory lane.

Use round-robin, deficit round-robin, or another deterministic bounded-cost
policy. Document the policy in code.

## Suggested data model

Do not fix this by performing an unbounded linear scan of every pending request
on every dispatch.

A suitable shape is a two-level queue:

```rust
struct MemoryShardQueue {
    owners: HashMap<String, MemoryOwnerQueue>,
    ready_owners: VecDeque<String>,
    // optional indexes for deterministic shard/owner cursor maintenance
}

struct MemoryOwnerQueue {
    pending: BTreeMap<u64, PendingInvoke>,
    in_ready_ring: bool,
}
```

The top-level `PendingInvokeQueue` can continue to own:

- stable global sequence numbers;
- lookup by runtime request ID;
- lookup by target isolate;
- lookup by memory owner key;
- expiry indexing.

A `PendingQueueKey` must still identify an item without scanning. It may need an
owner identifier in addition to lane, sequence, and shard index.

Alternative structures are acceptable if they provide:

- O(1) or O(log n) enqueue and direct removal;
- O(number of checked shards/owners), not O(total queued requests), dispatch;
- stable cancellation and expiry indexes;
- no duplicate ownership of `PendingInvoke` values.

## Atomic versus other memory calls

The memory lane also carries message, close, and transport calls. Preserve
existing semantics for them.

Use `owner_key` as the ordering domain for all routed memory work unless current
behavior or tests require a narrower rule. At minimum:

- an atomic lease must block another atomic for the same owner;
- bypassing a blocked head must never cause later work for that same owner to
  overtake it;
- targeted nested calls must retain their target-isolate rules.

If a non-atomic call is intentionally allowed to overlap an atomic for the same
owner, document and test that decision. Do not accidentally change it as a side
effect of the queue refactor.

## Required scheduler changes

Update `select_dispatch_candidate` and queue selection so the closure can reject
a leased owner without hiding other ready owners on that shard.

When an entity lease is acquired or released:

- ensure the owner's ready state is updated correctly;
- avoid leaving an owner permanently absent from the ready ring;
- avoid repeatedly selecting a known blocked owner in a tight loop.

When an isolate is removed or scaled down:

- clear shard affinity entries pointing to that isolate;
- release any active entity leases owned by it;
- make newly unblocked owner queues eligible for dispatch;
- preserve indexes for requests that remain queued.

## Tests

Replace the test that asserts shard tails are hidden with tests for the desired
behavior. Add at least:

1. blocked owner A does not prevent ready owner B on the same shard;
2. A2 cannot overtake A1 for the same owner;
3. two different owners on one shard can be dispatched concurrently when
   isolates are available;
4. round-robin rotates between hot owners within a shard;
5. round-robin still rotates across physical shards;
6. cancellation of an owner head exposes the next request correctly;
7. expiry of an owner head exposes the next request correctly;
8. removing an isolate releases its lease and re-enables that owner's queue;
9. direct removal by runtime request ID keeps all indexes coherent;
10. draining by target isolate and draining matching requests leave no stale
    ready-ring entries;
11. same-owner atomic concurrency remains exactly one under a multithreaded
    integration test;
12. socket/transport routed work is not starved by a hot atomic owner.

Add debug assertions that verify queue/index invariants in tests.

## Benchmark validation

Use the existing `same-shard`, `cross-shard`, and `skewed-hotspot` key modes.
Run at least:

```bash
DD_BENCH_MATRIX_ISOLATES="1 2 4 8 16" \
DD_BENCH_MATRIX_MEMORY_NAMESPACE_SHARDS="16" \
DD_BENCH_MATRIX_KEY_MODES="same-shard cross-shard skewed-hotspot" \
DD_BENCH_MATRIX_MODES="atomic-readwrite-memory-wide atomic-write-memory-wide" \
node benchmarks/run.mjs --samples 3 \
  --config scaling-atomic-memory-matrix.sh \
  --out benchmarks/results/local-intra-shard-scheduler.json
```

Expected outcome:

- same-shard distinct-ID throughput should improve materially, especially for
  atomic read/write;
- skewed-hotspot tail latency should improve or remain stable;
- cross-shard throughput must not regress by more than normal run variance;
- same-owner ordering tests remain green.

Do not encode an exact RPS target in unit tests.

## Acceptance criteria

- A leased entity no longer blocks unrelated entities in the same physical
  shard.
- Per-owner atomic FIFO and single-writer behavior are preserved.
- Queue selection remains bounded by shard/owner heads rather than total queue
  length.
- Cancellation, expiry, isolate removal, and generation cleanup maintain all
  indexes and ready rings.
- New unit and integration tests cover the behavior above.
- The same-shard or skewed-hotspot benchmark shows a documented improvement or,
  if storage serialization dominates, profiling demonstrates that scheduler
  head-of-line wait has been removed.
- `just check` passes.

## Non-goals

- Making conflicting operations for one entity concurrent.
- Changing the on-disk shard count or hash function.
- Replacing the entire runtime manager architecture.
- Guaranteeing that one physical SQLite/Turso shard can commit multiple writes
  simultaneously.

## Implementation cautions

- The `by_memory_owner_key` index already exists; reuse or reshape it rather
  than introducing a second inconsistent owner index.
- Avoid storing cloned `PendingInvoke` objects in multiple structures.
- Cursor updates must handle owner/shard removal and wraparound without panic.
- Maintain deterministic behavior in tests; do not depend on `HashMap`
  iteration order for scheduling.
