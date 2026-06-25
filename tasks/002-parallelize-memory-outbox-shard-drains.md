# Drain independent memory outbox shards concurrently

**Priority:** P0 effect-heavy parallel performance  
**Primary areas:** `service/sessions.rs`, runtime startup/config, outbox metrics  
**Scope:** bounded background concurrency; preserve current manager ownership

## Problem

Outbox database claim and acknowledgement I/O has correctly been moved off the
single runtime manager. However, `run_memory_outbox_worker` still owns one
receiver loop and awaits each `DrainShard` command to completion before reading
the next command.

That serializes independent physical shards through one background pipeline:

```text
receive shard A
claim A
manager delivery A
ack A
receive shard B
claim B
manager delivery B
ack B
```

Atomic write/effect throughput scales less than atomic read/write throughput.
Independent shards should be able to overlap their database claim and
acknowledgement I/O while manager-owned delivery remains bounded.

## Objective

Turn the outbox worker into a bounded concurrent shard-drain coordinator. Allow
several different physical shards to have claim/delivery/ack work in flight,
while guaranteeing at most one active drain per shard.

## Proposed configuration

Add a storage/runtime setting such as:

```rust
pub memory_outbox_max_concurrent_shards: usize,
```

Recommended default:

```text
min(memory_namespace_shards, available_parallelism, 8)
```

The exact cap may be tuned, but it must be finite and configurable. Expose it
through the production server configuration added by task 001 or an equivalent
configuration path.

Validate that the value is greater than zero.

## Coordinator design

Replace the sequential worker loop with a coordinator that owns:

```rust
pending: ordered set or queue of shard indices
in_flight: set of shard indices
join_set or completion channel
max_concurrent_shards
```

Required behavior:

1. `DrainShard { shard_index }` inserts the shard into `pending` unless it is
   already pending or in flight.
2. While capacity is available, start drains for pending shards in fair order.
3. Each drain performs the existing claim -> manager delivery -> batch ack flow.
4. Completion reports whether the shard saturated, failed, or became empty.
5. A saturated shard is requeued behind other pending shards instead of
   immediately monopolizing a slot.
6. `in_flight` is cleared exactly once on success, error, cancellation, or panic.
7. A new schedule request received while a shard is in flight records one
   follow-up pass rather than being lost or spawning a duplicate.

Use a `JoinSet`, bounded task queue, or fixed worker pool. Do not spawn an
unbounded task per command.

## Per-shard exclusivity

At most one drain task may claim rows from a given shard at a time. The existing
outbox lease protects crash recovery, but duplicate in-process drainers would:

- create avoidable transaction contention;
- duplicate manager delivery work;
- distort attempt counts;
- reduce fairness.

Make the invariant explicit and test it.

## Manager interaction

The manager should continue receiving `MemoryOutboxDelivery` events and
performing only in-memory effect delivery.

Several shard drain tasks may wait for manager replies concurrently. Keep this
bounded by `memory_outbox_max_concurrent_shards`.

Ensure event ordering requirements are understood:

- effects for one entity/shard should preserve the ordering guaranteed by the
  query and current delivery behavior;
- no cross-shard total ordering is required unless existing tests say otherwise;
- the manager must not hold a mutable borrow across reply sends.

## Error handling

Classify completion outcomes:

- empty: clear pending/in-flight state;
- saturated: requeue once at the back;
- claim failure: clear in-flight and schedule retry through existing tick/backoff
  behavior;
- manager delivery channel closed: terminate cleanly during shutdown;
- acknowledgement failure: preserve current retry semantics and requeue;
- task panic: observe it, increment a metric, release the shard, and make the
  periodic recovery path able to retry.

Do not busy-loop a failing shard. Add bounded retry delay or rely on the periodic
runtime tick as appropriate.

## Shutdown

Runtime shutdown should:

1. close the command sender;
2. stop accepting new shard requests;
3. optionally allow in-flight batches a bounded grace period;
4. abort or finish remaining drain tasks;
5. observe every task result;
6. rely on lease expiry for any claim not acknowledged.

No task should remain detached after service shutdown.

## Observability

Add aggregate metrics:

```text
outbox_shards_pending
outbox_shards_in_flight
outbox_parallelism_limit
outbox_parallelism_peak
outbox_duplicate_schedule_coalesced_count
outbox_task_failure_count
outbox_shard_requeue_count
outbox_slot_wait_us
```

Retain existing claim/ack/outcome counters. Avoid shard-index metric labels.
Debug output may show a bounded list of in-flight shard indices.

## Tests

Add deterministic tests with a test-only drain hook/barrier. Cover at least:

1. two different shards enter the claim phase concurrently when the limit is 2;
2. concurrency never exceeds the configured limit;
3. the same shard is never active twice;
4. a schedule received during an active drain triggers exactly one follow-up
   pass;
5. a saturated hot shard yields to a cold pending shard;
6. claim failure releases the shard and allows later retry;
7. acknowledgement failure preserves retry behavior;
8. manager reply delay on shard A does not stop shard B from claiming;
9. limit 1 preserves sequential behavior;
10. shutdown observes or aborts all tasks without hanging;
11. task panic does not leave a permanently in-flight shard;
12. metrics report pending, in-flight, peak, and coalesced schedules correctly.

Use paused Tokio time or explicit channels instead of fragile sleeps.

## Benchmark validation

Run clean three- or five-sample comparisons for:

- `atomic-write-memory-wide` cross-shard;
- `atomic-write-memory-wide` skewed-hotspot;
- the rate limiter with an emitted audit effect variant, if practical;
- parallelism values `1, 2, 4, 8`;
- fixed 16 isolates and 16 shards on the recorded host.

Also record manager event wait and outbox in-flight peak.

Expected result:

- cross-shard effect throughput improves until storage or manager delivery
  becomes limiting;
- p95/p99 should improve or remain stable;
- same-shard semantics and throughput should not regress materially;
- retries and delivery counts remain correct.

## Acceptance criteria

- Independent outbox shards can run claim/ack I/O concurrently.
- Concurrency is bounded and configurable.
- One shard has at most one active drain.
- Saturated shards requeue fairly.
- Shutdown observes all drain tasks.
- Existing at-least-once delivery and lease recovery semantics remain intact.
- Metrics and deterministic concurrency tests are added.
- Relevant benchmarks show no regression and document any gain.
- `just check` passes.

## Non-goals

- Parallel delivery of effects for the same shard/entity.
- Exactly-once external effects.
- Moving manager-owned websocket/transport state into worker tasks.
- Increasing the outbox batch size as the primary optimization.

## Implementation cautions

- A semaphore alone does not provide per-shard deduplication or fair requeueing.
- Do not hold coordinator locks while awaiting database or manager work.
- Ensure a full runtime event channel cannot deadlock all outbox tasks during
  shutdown.
- Keep retry scheduling bounded; failure storms must not create an unbounded
  pending queue.
