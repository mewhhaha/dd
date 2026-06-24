# Deduplicate memory database initialization without holding shard locks across I/O

**Priority:** P1 performance  
**Primary area:** `crates/runtime/src/memory.rs`  
**Dependencies:** coordinate with task 005 because both change cache ownership

## Problem

`MemoryStore::connect_shard` currently locks the shard-wide `databases` mutex and
keeps that lock while it performs cold-path asynchronous work:

- creates parent directories;
- calls `Builder::new_local(...).build().await`;
- runs `ensure_schema(&database).await`;
- inserts and prunes the cache.

Any other request mapped to the same in-memory shard must wait for that entire
cold open, even when it targets a different namespace and different database
file. Concurrent first access to many namespaces can therefore form a lock
convoy.

The cache key also includes `std::thread::current().id()`. With many isolate
threads, the same namespace/shard pair can produce many cached `Database`
objects. This may be required by a Turso thread-affinity constraint, but the code
does not document or test that requirement. If it is not required, the current
key multiplies open database state as isolate count grows.

## Objective

Make database acquisition concurrency-safe and deduplicated while ensuring no
shard-wide mutex is held across filesystem or database awaits. Determine whether
`Database` can be shared across runtime/isolate threads and remove thread-based
duplication when safe.

## Required investigation

Before choosing the final cache key, establish the actual Turso constraints for
the locked dependency version:

- whether `Database` is `Send` and `Sync`;
- whether `Database::connect()` may be called safely from multiple runtime
  threads;
- whether connections themselves must remain on their creating thread;
- whether the previous thread-ID key was added to work around a known crash or
  upstream issue.

Use compile-time assertions where possible and add a stress test rather than
relying only on type signatures.

Document the conclusion next to the cache structure.

## Required design

### Keyed initialization slots

Use a short-held map lock to locate or insert an initialization slot, then run
initialization outside the map lock. A suitable shape is:

```rust
struct MemoryDatabaseSlot {
    database: tokio::sync::OnceCell<Arc<Database>>,
    // last-used/eviction state
}
```

or an equivalent shared future/state machine.

The acquisition flow should be:

1. calculate the cache key;
2. lock the map briefly;
3. retrieve or insert an `Arc<MemoryDatabaseSlot>`;
4. release the map lock;
5. initialize the slot with `get_or_try_init` outside the map lock;
6. create and configure a connection;
7. update last-used metadata without reintroducing a long global critical
   section.

Concurrent opens of the same key should share one initialization attempt rather
than building and migrating the same database repeatedly.

### Failure and retry behavior

If initialization fails:

- all current waiters should receive the error;
- a later request must be able to retry;
- a permanently failed `OnceCell` or slot must not poison the cache forever;
- partially initialized cache entries should be removed or reset safely;
- schema migration failures must retain their original context.

Be careful about a race where a failed initializer removes a slot that has
already been replaced by another attempt. Use slot identity checks.

### Cache identity

Prefer one cache entry per `(namespace, physical_shard_index)` if `Database` is
safe to share. In that case remove the thread ID from `database_key`.

If upstream constraints require thread-local databases:

- explicitly model a per-thread sub-cache or thread-local holder;
- do not make all threads contend on one async mutex while initializing;
- document why thread locality is necessary;
- add a test that would fail if a database were accidentally moved across the
  unsupported boundary.

Do not preserve the current thread-ID key merely because it already exists.

### Schema setup

Ensure schema creation/migration runs exactly once per database initialization
slot. It must not run for every connection.

If multiple processes can open the same file concurrently, schema setup must
remain idempotent and tolerate the expected SQLite/Turso locking behavior.

## Instrumentation

Add profile or tracing measurements for:

- cache lookup hit/miss;
- time waiting for an existing initialization;
- database build duration;
- schema setup duration;
- connection creation/configuration duration;
- initialization failures and retries;
- count of live database slots;
- duplicate initialization attempts prevented.

Do not log database paths at info level if they may expose encoded tenant or
binding information; use structured debug fields.

## Tests

Add focused tests for:

1. many concurrent requests for one cold namespace/shard create one database
   slot and one schema initialization;
2. a slow cold open for namespace A does not block an already warm database for
   namespace B on the same in-memory shard;
3. a slow cold open for A does not block a cold open for B beyond the brief map
   insertion critical section;
4. initialization failure is returned to all waiters and a later request can
   retry successfully;
5. concurrent retry does not remove a newer successful slot;
6. schema setup remains idempotent under concurrent process-style opens where
   the test harness permits it;
7. database sharing across isolate/runtime threads is safe if the thread ID is
   removed;
8. cache eviction during or immediately after initialization does not drop the
   only live initializer or panic;
9. no async mutex guard is held across the build/schema awaits (enforce by code
   structure and review; a helper API can make this obvious).

A test-only initialization hook or barrier is acceptable for deterministic
concurrency tests. Keep it behind `cfg(test)`.

## Benchmark validation

Add or run a cold-open benchmark with:

- many unique namespaces mapping to the same in-memory shard;
- one namespace with many concurrent first requests;
- isolate counts 1, 8, 16, and 32;
- a warm steady-state control.

Report cold p50/p95/p99, slot count, and initialization count. Ensure warm direct
read/write throughput does not regress.

## Acceptance criteria

- No shard-wide database-cache mutex is held while awaiting directory creation,
  database build, schema setup, or connection configuration.
- Concurrent requests for one cache key deduplicate initialization.
- Failed initialization can be retried safely.
- The thread-ID component is removed if Turso supports shared `Database`
  objects; otherwise its necessity is documented and contention is still
  isolated.
- Schema initialization is once per slot, not once per connection.
- New metrics and deterministic concurrency tests are present.
- Existing memory tests and `just check` pass.

## Non-goals

- Replacing Turso/SQLite.
- Implementing the cache eviction policy from task 005 in the same pull request
  unless the slot API makes a minimal integration necessary.
- Changing the persisted memory schema or public API.

## Implementation cautions

- Avoid awaiting while holding any `tokio::sync::MutexGuard` for the cache map.
- `OnceCell::get_or_try_init` failure semantics must be understood and tested.
- Do not let eviction remove a slot that has active users without retaining an
  `Arc` until acquisition completes.
- Preserve path normalization and namespace validation performed by the current
  code.
