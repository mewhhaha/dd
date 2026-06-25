# Reuse memory database connections and serialize writers before SQLite contention

**Priority:** P1 storage throughput and tail latency  
**Primary area:** `crates/runtime/src/memory.rs`, `RuntimeStorageConfig`  
**Scope:** per-database connection lifecycle; no schema or API change

## Problem

The runtime now caches one shareable Turso `Database` handle for each
`(namespace, physical_shard)`, but every memory operation still does:

```rust
let conn = database.connect()?;
configure_connection(&conn).await?;
```

For writes, multiple requests then race to `BEGIN IMMEDIATE` on separate
connections and retry retryable lock errors with sleeps.

Repeated connection creation/configuration and database-level writer contention
can limit parallel throughput in two ways:

- independent physical shards pay setup overhead on every request;
- writers targeting the same database create lock competition that SQLite/Turso
  cannot turn into true parallel commits.

The correct parallel shape is many database files operating independently, with
bounded reuse inside each file and one deliberate writer lane per database.

## Objective

Add a small bounded connection pool per cached database slot:

- reusable read connections for snapshot/point/version queries;
- one reusable writer connection or writer actor for transactional mutations;
- independent pools for different namespace/shard databases;
- bounded total connection/resource use.

Validate Turso connection thread-safety before selecting the exact ownership
model.

## Required investigation

For the locked Turso version, determine and document:

- whether `Connection` is `Send` and/or `Sync`;
- whether it may be used on a different thread from creation;
- whether concurrent methods on one connection are supported;
- how a connection behaves after failed `BEGIN`, rollback, or busy errors;
- whether connection setup pragmas persist for the connection lifetime;
- whether dropping/recreating connections is required after specific errors.

Add compile-time assertions where possible and a stress test across isolate
threads. Do not assume safety from the `Database` handle behavior.

## Proposed slot structure

If `Connection` is safely movable between tasks/threads:

```rust
struct MemoryDatabaseHandle {
    database: Arc<Database>,
    writer: Mutex<PooledWriter>,
    readers: MemoryReadPool,
}
```

If it is thread-affine, use a per-database actor task that owns the connection(s)
and receives typed commands. The task count must be bounded by the database
cache and idle eviction policy.

The existing `MemoryDatabaseSlot::database` `OnceCell` can become a
`OnceCell<Arc<MemoryDatabaseHandle>>`.

## Writer lane

SQLite/Turso permits one writer transaction per database file. Make that
serialization explicit before entering the database:

1. acquire the per-database writer lane;
2. ensure the connection is healthy/configured;
3. run `BEGIN IMMEDIATE`;
4. execute the current mutation/command/outbox transaction;
5. commit or roll back;
6. release the lane;
7. recreate the connection if the error class makes reuse unsafe.

Benefits expected:

- fewer busy/locked retries;
- no repeated writer connection setup;
- predictable FIFO or documented fair writer ordering;
- cross-shard writes still proceed in parallel because each database has its own
  writer lane.

Do not hold a global cache lock while waiting for the writer.

A writer actor is acceptable if it simplifies cancellation and connection
ownership. Do not batch unrelated transactions in this task; group commit can be
evaluated separately.

## Reader pool

Use a bounded reader pool, for example:

```rust
struct MemoryReadPool {
    idle: Mutex<Vec<Connection>>,
    permits: Semaphore,
    max_connections: usize,
}
```

Required behavior:

- checkout is bounded;
- an idle healthy connection is reused;
- a new connection is created only while under the maximum;
- connection configuration occurs once at creation;
- returned connections are reset to a clean state;
- poisoned/failed connections are discarded;
- cancellation while waiting does not leak a permit;
- eviction of the parent database cache slot prevents new checkout but does not
  invalidate checked-out connections.

If profiling shows one reader connection is sufficient because operations are
short and Turso serializes internally, choose that simpler design and document
the evidence.

## Resource budgeting

Add clear configuration, such as:

```rust
pub memory_db_read_connections_per_database: usize,
pub memory_db_writer_connections_per_database: usize, // fixed to 1 initially
pub memory_db_max_total_connections: usize,
```

A process-wide cap is important because the database cache may contain many
namespace/shard slots. Do not multiply an unbounded per-database default by
thousands of cached databases.

A reasonable implementation can use:

- one writer per initialized active slot;
- lazy readers;
- a global semaphore for total live connections;
- idle eviction integrated with existing database-slot eviction.

Validate all values and expose them through the production tuning surface from
task 001.

## API refactoring

Replace raw `connect()` use with intent-specific helpers:

```rust
async fn with_read_connection<T>(...);
async fn with_writer_transaction<T>(...);
```

or RAII guards:

```rust
let conn = handle.checkout_reader().await?;
let mut writer = handle.checkout_writer().await?;
```

Audit every call site:

- point/full/key snapshot reads;
- version queries;
- batch apply;
- command result reads/writes;
- outbox claim;
- outbox acknowledgement/retry;
- startup floor/layout adoption helpers, which may remain uncached because they
  are startup-only.

Keep transaction boundaries explicit and easy to review.

## Fairness and cancellation

- Writer waiters should not starve under a continuous stream.
- Cancelled requests must release reader/writer permits.
- Request wall-time cancellation must not leave a transaction open.
- On panic/error, a writer guard should attempt rollback or discard the
  connection.
- Outbox work and foreground atomic writes share a database; ensure background
  outbox acknowledgement cannot monopolize the writer lane. A simple FIFO is
  acceptable initially, but measure it.

## Observability

Add profile/counters for:

```text
memory_connection_create_count
memory_connection_reuse_count
memory_reader_pool_wait_us
memory_writer_lane_wait_us
memory_writer_busy_retry_count
memory_connection_discard_count
memory_connections_live
memory_connections_peak
```

Separate connection checkout wait from actual store transaction time.

## Tests

Add deterministic tests for at least:

1. repeated reads reuse a configured connection;
2. repeated writes reuse the writer connection;
3. two different physical shard databases can hold writer transactions
   concurrently;
4. two writes to one database are serialized before `BEGIN IMMEDIATE`;
5. writer order is deterministic/fair enough for documented policy;
6. reader concurrency never exceeds its limit;
7. global connection cap is enforced across many database slots;
8. cancellation while waiting releases permits;
9. failed transaction rolls back before reuse or discards the connection;
10. cache eviction with checked-out connections is safe;
11. database initialization failure does not leak connection permits;
12. outbox and foreground writes both make progress;
13. cross-thread stress is safe under the chosen Turso ownership model;
14. service shutdown closes actor tasks/connections without hanging.

Use test hooks to hold transactions open rather than relying on arbitrary
sleeps.

## Benchmark validation

Compare before/after for:

- direct point read, warm and cold;
- direct write, same-shard and cross-shard;
- atomic read/write;
- atomic write/effect;
- real-world rate limiter;
- 1, 2, 4, 8, and 16 isolates;
- connection pool sizes 1, 2, 4 where relevant.

Record connection creates/reuses, writer wait, busy retries, throughput, p95,
and p99.

Expected result:

- connection creation/configuration count drops sharply;
- writer busy retries approach zero or materially decrease;
- cross-shard throughput improves or remains stable;
- same-database tail latency becomes more predictable;
- total live connections remain bounded.

## Acceptance criteria

- Normal memory operations no longer create/configure a fresh connection per
  request.
- Each database has one deliberate writer lane.
- Reader and total connection counts are bounded.
- Cross-shard databases can operate concurrently.
- Connection failure/cancellation/eviction semantics are test-covered.
- Metrics distinguish pool wait from database transaction time.
- Relevant repeated benchmarks show no regression and document gains or limits.
- `just check` passes.

## Non-goals

- Transaction batching or group commit.
- Multiple simultaneous SQLite writers for one database file.
- Changing on-disk schema or shard count.
- Replacing Turso.

## Implementation cautions

- Do not place a non-`Send` Turso connection into a cross-thread pool.
- Do not return a connection with an open transaction to the idle pool.
- Parent cache eviction and child connection lifetime must be coordinated using
  `Arc` ownership, not raw references.
- A connection pool that creates too many connections can reduce performance;
  conservative defaults and a global cap are required.
