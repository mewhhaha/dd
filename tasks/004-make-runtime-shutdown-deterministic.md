# Make runtime, isolate, and outbox shutdown deterministic

**Priority:** P1 reliability and lifecycle correctness  
**Primary area:** `crates/runtime/src/service/facade.rs`, `runtime.rs`, `model.rs`, `lifecycle.rs`, benchmark lifecycle  
**Dependencies:** none

## Current behavior

`RuntimeService::shutdown()` sends `RuntimeCommand::Shutdown` and waits for a
oneshot reply from the runtime manager. The manager calls `shutdown_all()` and
its event loop eventually exits.

However:

- `spawn_runtime_thread` discards the `std::thread::JoinHandle`;
- `spawn_isolate_thread` also discards each isolate thread join handle;
- `IsolateHandle` stores command and V8 handles but not thread completion;
- the memory outbox worker is started with `tokio::spawn` and its `JoinHandle` is
  discarded;
- the shutdown reply can be observed before the runtime thread and isolate
  threads have actually terminated;
- `try_send(IsolateCommand::Shutdown)` can fail when a channel is full or closed,
  and shutdown does not wait for confirmation;
- repeated runtime construction and teardown can overlap detached V8/isolate
  lifetimes.

The newer benchmark tooling helps isolate signal failures, but lifecycle
ownership is still nondeterministic.

## Objective

Make shutdown explicit, idempotent, and awaitable. A successful shutdown call
must mean:

- no new commands are accepted;
- pending requests have completed or received deterministic errors;
- the outbox worker has stopped;
- all isolate threads owned by the service have terminated;
- the runtime manager thread has terminated;
- V8/source/snapshot-owned state is no longer in use by detached threads.

## Required ownership model

### Shared service lifecycle state

`RuntimeService` is cloneable, so store shutdown ownership in shared state, for
example:

```rust
struct RuntimeLifecycle {
    state: AtomicU8,
    runtime_join: Mutex<Option<std::thread::JoinHandle<()>>>,
    completion: Notify,
    result: Mutex<Option<Result<()>>>,
}
```

The exact design may differ, but it must support:

- one caller initiating shutdown;
- concurrent callers waiting for the same completion;
- later callers returning the stored result immediately;
- no caller joining the same thread twice;
- clear states such as running, shutting down, and stopped.

Do not hold an async mutex while blocking on a thread join.

### Runtime thread handle

Change `spawn_runtime_thread` to return a handle or lifecycle object rather than
`Result<()>` after detaching the thread.

The handle must retain:

- the manager thread join handle;
- a completion signal/result;
- any shutdown channel needed by `RuntimeService`.

Join the runtime thread through `spawn_blocking` or another mechanism that cannot
block the async executor needed for shutdown progress.

### Isolate thread handles

Store each isolate thread's `JoinHandle` in `IsolateHandle` or a separate owned
record.

Update every removal path:

- isolate failure;
- scale-down;
- worker generation retirement;
- dynamic worker deletion;
- complete runtime shutdown.

A normal removal must:

1. transition the isolate to retiring;
2. stop accepting new dispatches;
3. send shutdown through a reliable path;
4. abort V8 execution when necessary;
5. close/drop command senders so a full channel cannot prevent shutdown;
6. join the thread outside mutable pool borrows;
7. report join panic/failure with worker, generation, and isolate ID context.

Do not keep joined handles in live pool indexes.

### Outbox worker shutdown

Retain the outbox worker task handle and add an explicit stop path.

Requirements:

- close or send a shutdown command to the drain channel;
- stop scheduling new shard drains;
- decide whether an in-flight claim/ack batch is allowed a bounded grace period
  or abandoned for lease-based recovery;
- await the task handle;
- surface worker panic or unexpected early exit;
- avoid waiting forever on manager events after the manager has begun shutdown.

At-least-once recovery through outbox lease expiry must remain intact.

## Shutdown ordering

Define and document one ordering. A recommended sequence is:

1. atomically mark service as shutting down;
2. reject new public commands at the facade or manager boundary;
3. stop periodic scheduling and new outbox claims;
4. fail queued requests and notify stream/session waiters;
5. abort or finish in-flight requests according to existing timeout policy;
6. close isolate command channels and join isolate threads;
7. stop and await the outbox worker;
8. release worker source modules, snapshots, and manager-owned state;
9. exit and join the runtime manager thread;
10. publish the final shutdown result to all waiters.

If outbox acknowledgement needs manager-owned in-memory delivery, ensure the
chosen ordering cannot deadlock worker and manager waiting on each other.

## API behavior

Keep `RuntimeService::shutdown(&self) -> Result<()>` if possible.

Required semantics:

- idempotent;
- safe when called concurrently from cloned services;
- bounded by a configurable or documented timeout;
- returns an error for thread panic, timeout, or incomplete shutdown;
- returns success only after joins finish;
- commands invoked after shutdown begins return a clear stopped/shutting-down
  error rather than hanging on a closed channel.

Implement `Drop` as best-effort initiation only if useful. Do not block in
`Drop`. Tests and benchmarks requiring deterministic teardown must call and await
`shutdown()`.