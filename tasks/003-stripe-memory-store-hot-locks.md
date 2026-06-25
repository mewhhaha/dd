# Stripe memory-store version and snapshot locks by entity

**Priority:** P1 parallel storage performance  
**Primary area:** `crates/runtime/src/memory.rs`  
**Scope:** in-memory cache synchronization only; no on-disk layout change

## Problem

The scheduler now permits unrelated entity owners in the same physical memory
shard to run independently. The storage cache layer still funnels those entities
through shared shard-wide mutexes:

```rust
struct MemoryShard {
    databases: Mutex<MemoryDatabaseCache>,
    memory_versions: Mutex<HashMap<String, i64>>,
    shared_snapshots: Mutex<MemorySharedSnapshotCache>,
    version: AtomicU64,
}
```

Every cached point read, snapshot read, commit update, version observation, and
cache eviction for one physical shard contends on the same
`memory_versions`/`shared_snapshots` locks.

This means scheduler independence is not fully reflected in the memory hot path.
A same-physical-shard workload with many independent room/user IDs can still
serialize on cache metadata even when database operations and isolates are
available in parallel.

## Objective

Partition entity-local version and snapshot cache state into deterministic
stripes. Independent entities in the same physical storage shard should usually
use different mutexes, while all operations for one `(namespace, entity_key)`
remain coordinated.

Do not change physical shard selection, manifest data, or persisted database
files.

## Proposed data model

Replace the two shard-wide maps with a fixed stripe array:

```rust
struct MemoryShard {
    databases: Mutex<MemoryDatabaseCache>,
    cache_stripes: Box<[MemoryEntityCacheStripe]>,
    version: AtomicU64,
}

struct MemoryEntityCacheStripe {
    state: Mutex<MemoryEntityCacheState>,
}

struct MemoryEntityCacheState {
    memory_versions: HashMap<String, i64>,
    snapshots: MemorySharedSnapshotCache,
}
```

Combining version and snapshot metadata in one stripe lock is preferred because
cache validation currently reads both. It avoids lock-order complexity and a
version/snapshot race between separate stripe locks.

A different structure is acceptable if it preserves the same invariants.

## Stripe selection

Select a stripe from a stable in-process hash of both namespace and entity key:

```text
stripe = hash(namespace, entity_key) % stripe_count
```

This hash is only for in-memory lock distribution. It is not part of the
persisted shard layout and does not need the storage manifest hash version.

Requirements:

- identical namespace/entity pairs always select the same stripe during one
  process;
- different namespaces with the same entity key need not contend systematically;
- avoid allocating a combined string on every lookup if possible;
- use a power-of-two stripe count only if the hash distribution and mask are
  documented;
- start with a fixed default such as 32 or 64 stripes per physical shard unless
  profiling justifies configuration.

## Budget preservation

The snapshot cache currently has bounded entry and approximate-byte budgets.
Striping must not multiply those budgets.

Choose and document one policy:

### Deterministic partitioning

Divide each physical shard's existing entry/byte budget across stripes, including
remainders. Each stripe independently enforces its assigned budget.

This is simple and fast but can waste capacity when one stripe is hot.

### Shared budget with striped storage

Keep global/per-physical-shard atomic counters and let stripes evict locally when
shared limits are exceeded. This uses capacity better but needs careful
coordination.

Prefer deterministic partitioning for the first implementation unless benchmark
data shows unacceptable imbalance. The sum of stripe budgets must equal the
current budget, not `budget × stripe_count`.

## Functions to update

Audit every access to the current maps, including:

- `observe_memory_version`;
- `cached_snapshot_entry`;
- `put_full_snapshot`;
- `put_partial_snapshot`;
- `update_cached_snapshot_after_commit`;
- cache invalidation/removal;
- profile reset or debug/cache metrics;
- tests that inspect cache length/bytes.

Create helpers such as:

```rust
fn entity_cache_stripe(&self, namespace: &str, memory_key: &str)
    -> &MemoryEntityCacheStripe;
```

Keep lock acquisition in one helper where practical.

## Correctness requirements

- A cached snapshot is never returned when the observed version is newer.
- Commit cache updates remain ordered for one entity.
- Partial snapshot `loaded_keys` and `complete` semantics remain unchanged.
- Eviction does not corrupt approximate-byte accounting.
- No operation acquires two entity stripes simultaneously.
- Database transaction consistency remains the source of truth.
- Existing mixed/legacy physical shard overrides continue to select the correct
  physical `MemoryShard` before selecting an in-memory stripe.

## Contention instrumentation

Add profile counters/timers for:

```text
entity_cache_lock_wait_us
entity_cache_lock_calls
entity_cache_stripe_count
entity_cache_peak_entries_per_stripe
entity_cache_max_entries_per_stripe
```

If precise mutex wait measurement is too expensive always-on, collect it only
when memory profiling is enabled. Aggregate across stripes without entity labels.

Add a debug-only distribution summary: active stripe count, maximum entries, and
median entries. Do not expose entity keys.

## Tests

Add deterministic tests for at least:

1. one namespace/entity always maps to one stripe;
2. test-generated independent entities cover multiple stripes;
3. namespace participates in stripe selection;
4. concurrent operations on different stripes can enter their critical sections
   simultaneously using a test barrier/hook;
5. concurrent operations on the same entity remain serialized;
6. version invalidation prevents stale cached reads;
7. full and partial snapshots retain existing semantics;
8. commit updates preserve ordering and byte accounting;
9. total configured cache entry/byte budget is not multiplied by stripe count;
10. eviction in one stripe does not remove entries from another stripe;
11. oversized snapshots retain the current skip/eviction policy;
12. physical shard override routing still reaches the correct stripe array.

Avoid timing-only assertions. A `cfg(test)` lock-entry hook is acceptable.

## Benchmark validation

Add a focused mode where many distinct keys are deliberately generated for one
physical storage shard. Compare before/after at:

- 1, 2, 4, 8, and 16 isolates;
- direct point reads;
- direct writes;
- atomic read/write;
- 1,024 and 16,384 entity keys;
- warm-cache and cold-cache phases.

Use the existing same-shard and skewed-hotspot modes where possible.

Report:

- throughput;
- p95/p99;
- entity-cache lock wait;
- maximum/median stripe occupancy;
- cache hit rate;
- CPU utilization.

Expected result:

- better same-physical-shard scaling for independent owners;
- reduced cache-lock wait;
- no cross-shard regression beyond normal variance;
- unchanged same-owner correctness.

## Acceptance criteria

- Version and snapshot cache state is striped by namespace/entity.
- Unrelated owners on one physical shard no longer require the same cache mutex
  in the common case.
- Existing cache budgets are preserved exactly or conservatively.
- Cache correctness and mixed-layout behavior remain intact.
- Contention/distribution metrics are available.
- Focused same-shard benchmarks show reduced lock wait and no correctness
  regression.
- `just check` passes.

## Non-goals

- Striping the persisted SQLite/Turso writer lock.
- Changing physical shard count or hash input.
- Making one entity's atomic operations concurrent.
- Replacing the snapshot record representation.

## Implementation cautions

- `std::collections::hash_map::DefaultHasher` is acceptable for in-process
  striping but must never leak into persisted layout decisions.
- Do not allocate one large cache budget per stripe.
- Keep lock ordering trivial; ideally one entity stripe lock per operation.
- Snapshot updates clone entity-local maps when readers hold `Arc`s. Lock
  striping does not remove that cost, so measure large-entity workloads
  separately rather than claiming it does.
