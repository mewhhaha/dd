# Move memory outbox storage I/O off the runtime manager

**Priority:** P1 performance and responsiveness  
**Primary area:** `crates/runtime/src/service/sessions.rs`, `control.rs`, `runtime.rs`, memory outbox APIs  
**Dependencies:** none; coordinate with task 008 if observability lands first

## Problem

The outbox drain is bounded, but it still executes inside the single runtime
manager event loop.

`RuntimeEvent::MemoryOutboxDrain` currently awaits
`drain_scheduled_memory_outbox_shard`. That function may:

1. open or connect to a shard database;
2. claim up to 64 due outbox rows in a transaction;
3. iterate every claim;
4. deliver each effect through manager-owned state;
5. await `mark_outbox_delivered` or `retry_outbox_record` for each item.

The one-batch limit prevents an unbounded loop, but a batch can still perform
many storage awaits while the manager is unable to process unrelated commands,
completions, cancellations, queue expiry, or dispatch. The atomic write/effect
benchmark remains much slower than atomic read/write, and this path is a likely
source of manager-loop latency.

The manager owns websocket and transport session maps, so effect delivery cannot
simply be moved wholesale to an arbitrary task. The storage portions can and
should be separated from the manager-owned in-memory portions.

## Objective

Keep the runtime manager event loop non-blocking with respect to outbox database
claim and acknowledgement I/O. The manager should perform only bounded,
non-awaiting state transitions for effects that require its session maps.

Preserve durable outbox semantics, lease behavior, retries, and ordering.

## Required architecture

Implement a staged outbox pipeline with bounded channels.

A recommended flow is:

```text
manager schedules shard
  -> background worker claims rows from MemoryStore
  -> worker sends claimed batch to manager as RuntimeEvent
  -> manager performs in-memory delivery and returns outcomes
  -> background worker persists delivered/retry outcomes
  -> worker reschedules shard if the claim was saturated or retries remain due
```

Exact type names may differ, but the ownership split must be clear.

### Stage 1: claim in a background task

Create a coordinator or worker set owned by the runtime thread setup but running
as Tokio tasks. It should:

- accept shard drain requests through a bounded channel;
- deduplicate scheduled shard indices;
- call `claim_due_outbox_records_for_shard_index` outside `WorkerManager`;
- enforce the existing batch limit and lease duration;
- send a claimed batch back as a runtime event;
- retain enough state to persist final outcomes after manager delivery.

Do not spawn one unbounded task per atomic completion. Use one bounded worker,
a small configurable worker pool, or one worker per active shard with a strict
upper bound.

### Stage 2: deliver using manager-owned state

Add a runtime event carrying a claimed batch and a reply channel. The manager
must process it without awaiting database I/O.

For every claim, classify the outcome as one of:

- delivered;
- terminally rejected and safe to mark delivered/dropped;
- retryable with the original error context.

Delivery of audit-only effects should avoid expensive formatting when the
corresponding log level is disabled.

Keep manager work bounded. If a claim batch can contain 64 entries, either prove
that processing 64 in-memory deliveries is acceptably small or split it into a
smaller event budget.

### Stage 3: acknowledge in the background

The worker should persist outcomes using `mark_outbox_delivered` or
`retry_outbox_record` after it receives the manager's response.

Prefer a batched MemoryStore API so one claim batch does not require one
transaction or connection round trip per record. Add methods such as:

```rust
async fn apply_outbox_delivery_outcomes(
    &self,
    shard_index: usize,
    outcomes: &[MemoryOutboxDeliveryOutcome],
) -> Result<()>;
```

Perform updates in one transaction per physical shard where practical.
Preserve the retry delay calculation and attempt counts.

### Scheduling and saturation

- If a claim returns the maximum batch size, schedule another pass without
  monopolizing the manager loop.
- If a channel is full, retain a deduplicated pending bit and retry on the next
  tick; do not drop the drain request.
- The periodic tick must continue to recover records after crashes or missed
  notifications.
- Shutdown should stop accepting new work, finish or safely abandon leased
  batches, and not hang indefinitely.

## Correctness requirements

- Outbox rows remain durable before a request response is returned.
- A successfully delivered effect is eventually marked delivered.
- A retryable failure remains retryable and is not dropped.
- A terminal error is marked according to current semantics.
- Worker or process failure between claim and acknowledgement is recovered by
  the lease timeout.
- No effect is acknowledged before the manager reports its delivery outcome.
- Existing at-least-once behavior must not accidentally become at-most-once.
- Shard-specific drains must not claim another shard's records.

## Instrumentation

Split the existing aggregate outbox metric into at least:

- claim calls, rows, and duration;
- manager delivery calls, rows, and duration;
- acknowledgement calls, rows, and duration;
- retry counts;
- terminal drop counts;
- scheduled-channel full count;
- batches that saturated the claim limit;
- in-flight outbox batches.

Expose enough information through debug output or tracing to identify a stuck
shard without logging effect payloads by default.

## Tests

Add deterministic tests with injected delay or a test-only storage hook.
Cover at least:

1. a deliberately slow claim does not delay a stats command or unrelated
   request completion on the runtime manager;
2. a deliberately slow acknowledgement does not block dispatch;
3. successful audit effect is marked delivered;
4. socket/transport effect is applied through manager-owned state and then
   acknowledged;
5. retryable delivery failure schedules the expected retry;
6. terminal delivery failure is finalized according to existing policy;
7. saturated batch reschedules the shard;
8. duplicate schedule requests coalesce;
9. full worker/event channel does not lose the pending shard;
10. process-style interruption after claim is recovered after lease expiry;
11. shutdown completes with no task leak or channel panic;
12. multiple shards can have claim/ack work in flight without violating a
    configurable concurrency bound.

Keep tests independent of wall-clock sleeps where possible by injecting a clock
or using paused Tokio time.

## Benchmark validation

Run the atomic write/effect matrix for cross-shard and skewed-hotspot workloads,
plus a responsiveness scenario that issues stats or fast fetches while an
outbox backlog drains.

Document:

- throughput and p95/p99 for atomic write/effect;
- runtime manager event-loop lag;
- fast-request latency during drain;
- outbox backlog drain rate;
- sample count and hardware.

A successful implementation should primarily reduce manager stalls and tail
latency. Absolute effect throughput improvement is desirable but not the sole
acceptance signal.

## Acceptance criteria

- `WorkerManager::handle_event` no longer awaits outbox database claim or
  acknowledgement operations.
- Claim and acknowledgement I/O run in bounded background tasks.
- Manager-owned effect delivery remains correct and bounded.
- Storage acknowledgements are batched per shard where practical.
- Recovery after task/process interruption preserves at-least-once semantics.
- New stage-specific metrics are available.
- Responsiveness tests prove that slow outbox storage does not block unrelated
  manager commands.
- Existing memory/session tests and `just check` pass.

## Non-goals

- Changing the public effect API.
- Guaranteeing exactly-once external side effects.
- Replacing the persisted outbox schema unless batching requires a compatible
  migration.
- Making audit logging itself asynchronous outside the established tracing
  infrastructure.

## Implementation cautions

- Do not move `WorkerManager` or its non-`Send` state into spawned tasks.
- Do not hold manager borrows while sending or awaiting on channels.
- Bound all channels and task counts.
- Ensure failed background tasks are observed and restarted or surfaced; silent
  task death would strand durable effects until a periodic recovery path runs.
