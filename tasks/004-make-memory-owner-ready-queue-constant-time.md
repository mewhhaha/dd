# Make memory-owner readiness and rotation constant time

**Priority:** P1 single-thread scheduler efficiency  
**Primary areas:** `service/model.rs`, `service/runtime.rs`, lease release paths  
**Scope:** queue data-structure refinement; preserve dispatch policy

## Problem

The memory queue correctly groups pending work by physical shard and owner, but
its owner fairness structure still performs linear work on the single runtime
manager thread.

Current `MemoryShardQueue` behavior includes:

- `find_round_robin_owner_head_map` iterates `owner_ring` until a selectable
  owner head is found;
- leased owner heads are rejected by the dispatch callback and remain in the
  ring, so they are inspected again on later attempts;
- `advance_owner_cursor` searches the deque with `position()` and rotates it;
- removing an empty owner calls `retain()` across the deque;
- `PendingQueueKey` clones the owner `String` for candidate selection and index
  storage.

At the current 1,024-key benchmarks this is acceptable. With much larger active
keyspaces or a hot-key distribution, manager CPU can grow with the number of
owners rather than the amount of useful dispatched work.

## Objective

Maintain an explicit ready-owner queue and blocked-owner set for each physical
shard. Selecting, rotating, blocking, waking, and removing the common front
owner should be O(1), with bounded fallback cleanup for stale entries.

This task targets the remaining single-thread scheduler cost without splitting
`WorkerManager` across threads.

## Required invariants

- One owner queue preserves FIFO request order.
- A same-owner atomic lease blocks later work that must not overtake it.
- Independent owners remain dispatchable.
- A blocked owner is not repeatedly inspected until a state change can make it
  ready.
- Releasing a lease wakes the corresponding owner exactly once if it has pending
  work.
- Cancellation, expiry, isolate failure, and queue draining cannot leave a
  permanently blocked or duplicated owner.
- Physical-shard round-robin fairness remains intact.

## Proposed data model

A suitable shape is:

```rust
struct MemoryShardQueue {
    owners: BTreeMap<OwnerId, MemoryOwnerQueue>,
    ready: VecDeque<OwnerId>,
    ready_membership: HashSet<OwnerId>,
    blocked: HashSet<OwnerId>,
}
```

`OwnerId` may be:

- `Arc<str>` reused by queue keys/indexes; or
- a small numeric ID allocated by `PendingInvokeQueue` with a reverse map.

Prefer `Arc<str>` for a contained first implementation unless profiling shows
string hashing/cloning remains material. Avoid unsafe interning.

Do not store duplicate `PendingInvoke` values.

## Readiness model

Readiness depends on the owner head and active lease state.

Add explicit queue APIs such as:

```rust
fn mark_owner_blocked(&mut self, shard: usize, owner: &str);
fn mark_owner_ready(&mut self, shard: usize, owner: &str);
fn owner_head(&mut self, shard: usize) -> Option<(PendingQueueKey, &PendingInvoke)>;
fn rotate_ready_owner(&mut self, shard: usize, owner: &str);
```

Exact signatures can differ, but state transitions should not be hidden inside a
closure that merely returns `None`.

### On enqueue

- New owner with no blocking lease enters the ready queue once.
- New owner whose head is blocked may enter `blocked` directly if lease state is
  available at enqueue time; otherwise it can be classified on first selection.
- Additional requests for an existing owner do not add another ready entry.

### On dispatch attempt

- Pop or inspect the ready front owner.
- If its head is blocked by an active atomic lease, move it to `blocked` and
  continue.
- If it has no pending head, remove stale membership and continue.
- If dispatch succeeds and the owner still has queued work, reclassify/rotate it
  based on the new head and acquired lease.
- If no isolate has capacity, preserve the ready owner without scanning the same
  ring repeatedly in a tight loop.

### On lease release

`WorkerPool::release_memory_entity_lease` should report whether a lease was
actually removed. The completion/failure path must then call
`queue.mark_owner_ready(...)` for that owner if pending work exists.

The queue needs the owner’s physical shard. Store it in owner metadata or derive
it from the owner index without scanning every shard.

### On removal/cancellation/expiry

When removing the owner head:

- expose the next head;
- classify it against current lease state;
- remove owner metadata entirely if empty;
- remove stale ready/blocked membership in O(1) where possible.

For arbitrary removal from the middle, do not change head readiness.

## Physical-shard fairness

The top-level `memory_next_shard_cursor` can remain, but integrate it with the
new ready state:

- shards with zero ready owners should be skipped cheaply;
- a shard becoming ready should become visible without scanning all shards;
- consider a top-level ready-shard deque plus membership set if profiling shows
  the current BTreeMap range scan matters;
- do not add the top-level ready-shard queue unless tests/metrics justify it in
  this PR.

Keep the first change focused on owner-level cost.

## Dispatch API changes

Refactor `select_dispatch_candidate` so it can distinguish:

- rejected because owner lease is active;
- rejected because target/affinity isolate is unavailable;
- rejected because all isolates are at capacity.

Only the owner-lease case should move the owner to the blocked set. Isolate
capacity can change on any completion, so the owner remains ready.

A small enum is preferable to overloading `Option`:

```rust
enum CandidateReadiness {
    Ready(DispatchSelection),
    OwnerBlocked,
    TemporarilyNoCapacity,
    StaleTarget,
}
```

Use a form that fits existing code without duplicating dispatch logic.

## Observability

Retain current counters and add:

```text
memory_ready_owner_count
memory_blocked_owner_count
memory_owner_wake_count
memory_owner_duplicate_ready_prevented_count
memory_ready_owner_stale_cleanup_count
memory_owner_heads_inspected_per_dispatch
```

The expected result is fewer inspected heads under hot-owner skew.

## Tests

Add focused tests for at least:

1. a leased owner moves out of the ready queue after one inspection;
2. repeated dispatch attempts do not inspect that blocked owner again;
3. releasing the lease wakes the owner exactly once;
4. two ready owners rotate in strict round-robin order;
5. enqueueing multiple requests for one owner creates one ready membership;
6. successful dispatch reclassifies the next same-owner head correctly;
7. cancelling the head exposes and classifies the next request;
8. expiring the head does the same;
9. removing the final request removes ready/blocked metadata;
10. isolate failure releases a lease and wakes pending owner work;
11. a temporarily saturated affinity isolate does not permanently block the
    owner;
12. direct removal by request ID leaves all queue indexes coherent;
13. 10,000 blocked owners plus one ready owner requires bounded inspection after
    initial classification;
14. owner key storage does not duplicate large strings per queue index more than
    the chosen representation requires.

Add invariant assertions in test builds:

- an owner is not both ready and blocked;
- ready membership matches the deque;
- empty owners have no membership;
- every queued memory item remains in request/expiry/owner indexes.

## Benchmark validation

Create a scheduler-heavy benchmark configuration with:

- 16 physical shards;
- 16 isolates;
- 16,384 or more entity IDs;
- one hot owner per shard holding a longer atomic callback;
- many short independent owner callbacks;
- same-shard and skewed-hotspot controls.

Measure:

- throughput;
- p95/p99;
- manager batch duration;
- owner heads inspected per successful dispatch;
- lease-rejection count;
- CPU time on the runtime manager thread if available.

Expected result:

- inspected heads approach O(successful dispatches + actual state changes), not
  O(blocked owners × dispatch attempts);
- cold owners are not starved;
- current 1,024-key cross-shard results do not regress.

## Acceptance criteria

- Blocked owners are removed from the active ready rotation until woken.
- Common ready rotation, wake, and final removal are O(1).
- Per-owner FIFO and atomic single-writer semantics remain unchanged.
- Queue/index invariants are test-covered.
- Large-keyspace/hot-owner benchmarks reduce manager inspection work and do not
  regress normal cross-shard throughput.
- `just check` passes.

## Non-goals

- Partitioning `WorkerManager` across threads.
- Changing memory shard affinity policy.
- Changing physical storage sharding.
- Removing all BTreeMaps from the queue.

## Implementation cautions

- A blocked owner can become ready through completion, failure, cancellation, or
  lease replacement; cover every release path.
- Avoid retaining stale `Arc<str>` owner IDs after queues empty.
- Do not move an owner to `blocked` merely because no isolate currently has
  capacity.
- Keep selection deterministic for tests and operational debugging.
