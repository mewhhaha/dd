## Current-main review

`main` has meaningfully improved since the previous review. The obvious P0 flaws are mostly fixed:

1. **MemoryStore cache state is now shard-local.** `MemoryStore` now owns `shards: Arc<[MemoryShard]>`, and each `MemoryShard` has its own `databases`, `memory_versions`, `shared_snapshots`, and `version` counter.

2. **The write path no longer uses an uncached DB-open path.** `apply_batch()` now calls `self.connect(namespace, memory_key)` for writes instead of a separate uncached connection path, then uses `BEGIN IMMEDIATE` on that connection.

3. **Version counters are now per memory shard and relaxed-ordering.** `reserve_version_after()` selects `shard_for_key(memory_key)` and uses that shard’s `AtomicU64` with `Ordering::Relaxed`, which removes the previous global version bottleneck.

That is real progress. But the results on `main` say the system still does **not** scale correctly for writes. Cross-shard reads improved a lot, but cross-shard writes are still terrible: same-shard writes report **1,955 req/s**, while cross-shard writes report only **1,040 req/s** under the same 16-isolate, 16-shard matrix. Cross-shard writes being slower than same-shard writes is the opposite of the desired property.

## The new bottleneck picture

### ✅ Reads are much healthier now

Wide cross-shard memory read is now **62,942 req/s**, much closer to the fast-fetch baseline of **91,817 req/s** than before. Same-shard read is **26,660 req/s**, so the same-shard/cross-shard distinction is now visible and directionally correct for reads.

That means the shard-local cache work did help.

### ❌ Writes still do not scale

The new write results are the smoking gun:

```text
cross-shard write: 1,040 req/s
same-shard write:  1,955 req/s
```

Cross-shard should be higher than same-shard if independent user/room IDs are actually running independently. It is lower.

The benchmark config is not tiny either: it uses 4,096 requests, concurrency 128, 16 min/max isolates, 16 max inflight, 1,024-key space, 16 memory namespace shards, and `DD_BENCH_MEMORY_KEY_MODE=cross-shard`.

So this is not just “we forgot to test it.”

## Remaining architectural problem

The storage layer is sharded now, but the **actor/runtime scheduling model is still central**.

The runtime manager still runs on a single `dd-runtime` thread with `Builder::new_current_thread()`, and that one manager owns the event loop for commands, cancellations, isolate events, ticking, queue expiration, memory outbox draining, and scale-down.

Memory invocations are still scheduled through `WorkerPool` queues. The pool keeps a single `memory_entity_leases` map, a single `PendingInvokeQueue`, and shared isolate state.

The queue has a memory lane, but it is still one queue structure per worker pool, not one independent scheduler per memory shard.

Dispatch checks `memory_atomic_route_is_active(pool, pending)` and skips a memory atomic if that owner key is currently leased. That preserves same-key ordering, but it is enforced in the central worker-pool scheduler rather than by shard-local actors.

## Harsh summary

The current main has moved from:

```text
bad storage sharding + central scheduler
```

to:

```text
better storage sharding + still central scheduler
```

That is why reads improved, but writes still fail the product goal.

For the product goal, “memory sharded by user id / room id should work independently,” the central runtime queue should not be the thing that decides whether independent memory actors can make progress. The memory shard should own that.

## What I would do next

### 1. Introduce real memory shard actors

Make memory shard scheduling explicit:

```rust
struct MemoryShardRuntime {
    tx: mpsc::Sender<MemoryShardCommand>,
}

enum MemoryShardCommand {
    Atomic {
        namespace: String,
        memory_key: String,
        call: MemoryExecutionCall,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    }
}
```

Then route:

```text
memory_key -> shard_index -> MemoryShardRuntime[shard_index]
```

Same key stays ordered. Different keys on different shards bypass each other completely.

### 2. Stop using worker-pool queue as the memory actor scheduler

The worker pool can still own general fetch requests. But memory `atomic(...)` should not be just another lane in the same pool queue. That queue is currently still global per worker/generation, with memory, targeted, and general lanes mixed together.

The memory subsystem needs independent queues per shard. Otherwise increasing cores can still increase pressure on one central scheduling point.

### 3. Measure scheduler time separately from storage time

The current memory profile tracks useful store timings like `StoreApplyBatch`, `StoreApplyBatchValidate`, and `StoreApplyBatchWrite`.

Add equivalent runtime/scheduler timings:

```text
memory_enqueue_to_dispatch_us
memory_dispatch_to_js_start_us
memory_js_atomic_us
memory_store_commit_us
memory_completion_to_reply_us
```

Right now the write regression could be DB contention, queue contention, isolate contention, or outbox/tick interference. The benchmark result proves the symptom, not the cause.

### 4. Add a “no JS, storage-only” write scaling benchmark

You need to isolate storage from isolate scheduling:

```text
storage-only cross-shard write
runtime memory atomic cross-shard write
runtime memory atomic same-shard write
```

If storage-only cross-shard scales but runtime atomic cross-shard does not, the scheduler is guilty. If storage-only cross-shard also does not scale, Turso/SQLite/file locking is guilty.

### 5. Fix the benchmark docs honesty

The benchmark README says the worktree is “dirty with local benchmark and memory-store changes,” while the repo is now at a later commit.

That makes the numbers harder to trust. The benchmark report should be regenerated on clean `main` and should include the actual commit under test.

## Verdict

Current `main` is substantially better than before for read scaling. The storage hot-path cleanup was the right first move.

But write scaling is still not solved. The key failure is this:

> Cross-shard writes are slower than same-shard writes on the new scaling benchmark.

That means the system still does not deliver the core product promise: independently sharded memory should get faster as you add cores and shard-distributed keys. The next work should move memory `atomic(...)` scheduling out of the central worker-pool queue and into real per-shard actor runtimes.

