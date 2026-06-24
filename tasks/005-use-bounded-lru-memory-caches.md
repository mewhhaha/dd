# Replace full-map cache pruning with bounded LRU caches

**Priority:** P1 performance and memory predictability  
**Primary area:** `crates/runtime/src/memory.rs`, `RuntimeStorageConfig`  
**Dependencies:** task 004 should land first or be coordinated closely

## Problem

The memory database and shared-snapshot caches use `HashMap` plus manual pruning.
On cache access or mutation, pruning currently:

1. scans the entire map with `retain` to remove idle entries;
2. if over the limit, clones every key into a vector;
3. sorts the vector by `last_used_at`;
4. removes the oldest excess entries.

This work occurs while holding the shard cache mutex. It is O(n) on ordinary
access and O(n log n) when over capacity. High-cardinality memory workloads can
therefore turn cache hits into long critical sections.

The configured limits are also ambiguous in practice:

- each `MemoryShard` independently applies `memory_db_cache_max_open`;
- the default 4,096 limit can therefore permit up to 4,096 entries per shard,
  not per process;
- the current thread ID in the database cache key can multiply entries again;
- `snapshot_cache_max_entries` is derived from the database limit and is also
  applied independently to every shard;
- snapshots are limited by entry count only, although one entity snapshot can
  be much larger than another.

The result is unpredictable memory and file-handle use as shard and isolate
counts increase.

## Objective

Provide bounded, low-overhead database and snapshot caches with explicit
process-wide budget semantics, O(1) or O(log n) hit/update behavior, and useful
cache metrics.

## Configuration changes

Clarify and document whether each limit is global or per shard. Prefer global
process-wide limits because the public names do not say “per shard.”

Add explicit snapshot settings instead of deriving them from the database
handle count, for example:

```rust
pub memory_db_cache_max_open: usize,
pub memory_snapshot_cache_max_entries: usize,
pub memory_snapshot_cache_max_bytes: usize,
```

Choose conservative defaults and expose the settings through the same server or
environment configuration path as existing runtime storage settings.

For compatibility, retain existing configuration fields where possible. If a
new required field would break struct literals, use defaults or a migration
strategy consistent with the project style.

## Required cache behavior

### Database cache

- Maintain a true bounded global count of live cache slots.
- Touching an entry should update recency without scanning all entries.
- Idle TTL expiration should be incremental or periodic, not a full-map scan on
  every hit.
- Eviction must not invalidate an `Arc<Database>` currently being used by a
  request.
- In-progress initialization slots from task 004 must not be evicted in a way
  that loses waiters or creates duplicate initialization storms.
- If per-shard caches remain, partition the global budget across shards and
  handle remainders deterministically. Do not silently multiply the configured
  maximum by the shard count.

### Snapshot cache

- Maintain recency in O(1) or O(log n).
- Enforce both entry and approximate byte budgets.
- Include the sizes of keys, record values, encodings, map/set overhead using a
  documented approximation. Exact allocator accounting is not required.
- A single snapshot larger than the total byte budget should not evict the
  entire cache and then remain resident indefinitely. Either skip caching it or
  use an explicitly documented oversized-entry rule.
- Updating one cached entity must update its recorded size correctly.
- Expired entries should be removed incrementally or by a bounded maintenance
  pass.

Use an existing well-maintained LRU crate already allowed by workspace policy,
or implement a small internal structure with tests. Avoid an unsafe intrusive
list unless there is a compelling measured reason.

## Locking model

Keep cache lock hold times short and never perform database or filesystem I/O
while holding an eviction lock.

Consider one of these approaches:

- per-shard LRU caches with explicitly partitioned budgets;
- a global metadata LRU plus shard-local entry maps;
- a small number of cache stripes independent of physical storage shards.

The chosen design must avoid introducing a new global mutex on every memory
read. Explain the trade-off in code comments or an architecture note.

## Metrics

Expose at least:

- database cache hit, miss, insert, eviction, idle-expiration counts;
- snapshot full-hit, point-hit, partial-hit, miss, insert, update, skip-oversize,
  eviction, and expiration counts;
- current and peak entry counts;
- current approximate snapshot bytes;
- configured budgets;
- cache lock wait or maintenance duration when profiling is enabled.

Metrics should be process- or shard-aggregated without unbounded label
cardinality. Do not label by namespace or entity key.

## Tests

Add deterministic unit tests for the cache structure and integration tests for
`MemoryStore`. Cover at least:

1. recently used entries survive eviction over older entries;
2. entry budget is never exceeded after maintenance completes;
3. byte budget is enforced after inserts and updates;
4. replacing a snapshot with a larger or smaller version updates accounting;
5. oversized snapshots follow the documented skip/evict policy;
6. idle entries expire without scanning or deleting active entries;
7. cache hits do not allocate and sort a vector of all keys;
8. global limits are not multiplied by physical shard count;
9. concurrent readers can retain evicted `Arc` values safely;
10. an initializing database slot is handled safely;
11. partial snapshot semantics (`loaded_keys`, `complete`, and version checks)
    remain correct through eviction and reinsertion;
12. configuration validation rejects zero or nonsensical budgets with clear
    errors.

If the implementation introduces a maintenance task, test shutdown and paused
Tokio-time expiry behavior.

## Benchmark validation

Create or use a benchmark that exceeds cache capacity with many entity IDs while
measuring:

- direct point reads;
- full snapshot reads;
- direct writes that update cached snapshots;
- cache hit and miss phases;
- 1, 8, and 16 isolates;
- memory usage and open file descriptors.

Compare:

- throughput;
- p95/p99 latency;
- cache lock time;
- resident memory;
- open DB object/connection counts;
- eviction work per operation.

The goal is predictable bounded resource use and removal of latency spikes, not
maximizing hit rate at any cost.

## Acceptance criteria

- Cache hits no longer perform full-map retain/sort work.
- Configured database and snapshot budgets have documented process-wide
  semantics and are actually enforced.
- Snapshot caching has an approximate byte limit.
- No I/O occurs while holding cache eviction locks.
- Active users remain safe when an entry is evicted.
- Cache metrics expose hits, misses, evictions, current size, and budget.
- High-cardinality benchmark tail latency and lock time improve or remain stable
  while resource use becomes bounded.
- Existing memory correctness tests and `just check` pass.

## Non-goals

- Persisting cache contents across process restarts.
- Implementing a distributed cache.
- Changing keyed-memory consistency semantics.
- Optimizing JavaScript-side isolate-local caches in the same pull request.

## Implementation cautions

- Be precise about whether a connection, database object, or initialization slot
  consumes the “open database” budget.
- Avoid cache callbacks that re-enter the same cache lock.
- Account for key/value replacement rather than only insertion.
- Do not use wall-clock time for TTL; keep `Instant`-based behavior.
