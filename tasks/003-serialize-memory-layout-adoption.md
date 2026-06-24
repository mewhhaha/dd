# Serialize memory layout adoption across processes and tasks

**Priority:** P0 startup correctness  
**Primary area:** `crates/runtime/src/memory.rs`  
**Dependencies:** coordinate with task 001 because both change adoption validation

## Current behavior

`load_or_adopt_memory_layout` currently follows this sequence:

1. check whether `memory-layout.json` exists;
2. if it exists, read and validate it;
3. otherwise scan and adopt the legacy physical layout;
4. write a temporary manifest;
5. rename the temporary file into place.

The rename protects readers from a partially written file, but the entire
check/adopt/write sequence is not serialized.

Two `MemoryStore::new` calls can both observe no manifest and concurrently scan
or modify the same root. This can happen through:

- two server processes starting against the same persistent volume;
- overlapping tests or embedded runtime instances;
- concurrent async initialization on one process;
- a deployment rolling restart where old and new processes overlap.

The temporary filename uses process ID and thread ID. Concurrent async tasks on
the same thread can choose the same path. Different processes can produce
independent candidate manifests and each proceed using its own in-memory result,
even though only one rename wins.

Conflicting configured shard counts or hash assumptions must never both start
successfully against the same store.

## Objective

Make layout initialization a single-writer operation for each memory root.
Acquire a cross-process lock before deciding whether adoption is needed, re-read
state under that lock, and return only the manifest that is actually persisted.

## Required locking behavior

### Cross-process exclusive lock

Create a lock file under the memory root, for example:

```text
.memory-layout.lock
```

Use a real operating-system advisory lock where supported, through a small
well-maintained crate or existing project dependency. Merely creating a file is
not sufficient unless stale-owner recovery and atomic ownership are implemented
correctly.

Requirements:

- lock acquisition is exclusive across processes;
- multiple async tasks in one process are also serialized;
- acquisition has a configurable or fixed bounded timeout;
- timeout errors include the root path and useful diagnostics;
- the lock is released on all success and error paths through RAII;
- the lock file itself may remain after unlock;
- unsupported platforms receive an explicit implementation or clear error,
  not silently unlocked behavior.

Perform blocking filesystem lock acquisition in `spawn_blocking` or before
entering latency-sensitive async work. Do not block a Tokio worker indefinitely.

### Recheck under lock

The correct sequence is:

1. ensure the root exists;
2. acquire the layout lock;
3. re-read `memory-layout.json` while holding the lock;
4. if present, validate and return it;
5. if absent, scan/adopt the legacy store;
6. atomically write and durably sync the manifest;
7. re-read or retain the exact persisted manifest;
8. release the lock;
9. continue `MemoryStore` initialization.

Never rely on a pre-lock existence check for correctness.

If another initializer completed while this caller waited, this caller must
validate and use that persisted manifest rather than repeat adoption.

## Atomic and durable manifest write

Strengthen `write_memory_layout_atomic`:

- use a collision-resistant unique temporary name, not only PID/thread ID;
- create the temporary file with `create_new` to avoid clobbering another
  writer's file;
- write deterministic pretty JSON plus newline;
- flush and `sync_all` the file;
- rename within the same directory;
- sync the parent directory after rename where supported;
- clean up the caller's temporary file on failure;
- never remove another initializer's temporary file;
- preserve the original manifest if rename or sync fails.

On Windows or filesystems where replacing an existing file differs, handle the
platform explicitly. Under the lock, an adoption write should normally target a
missing manifest; updates should use a safe replace strategy.

## Conflicting initialization

Add a deterministic rule for simultaneous configurations:

- the first process that acquires the lock and adopts the root persists its
  manifest;
- all later processes validate against that persisted manifest;
- a process configured with a different shard count or unsupported hash version
  fails before opening shard databases for normal operation;
- no process proceeds using an unpersisted candidate layout.

The error should preserve existing migration guidance.

## Cancellation and crash recovery

- Dropping/cancelling an initializer must release the OS lock.
- A process crash must release the lock through OS semantics.
- A leftover temporary file must not block a later startup.
- Later startup may remove stale temporary files only when holding the lock and
  only when filenames match the owned manifest-temp pattern.
- Do not treat the existence of the lock file itself as evidence that a live
  owner exists.

## Suggested code structure

Separate locking and adoption for testability:

```rust
struct MemoryLayoutLock { ... }

async fn acquire_memory_layout_lock(
    root: &Path,
    timeout: Duration,
) -> Result<MemoryLayoutLock>;

async fn load_or_adopt_memory_layout_locked(
    root: &Path,
    configured_shards: usize,
) -> Result<MemoryLayoutManifest>;
```

The outer function should acquire the guard and call the locked helper. Keep the
lock guard alive across the legacy scan and final durable write.

Avoid exposing the lock as a public API unless a future reshard tool needs it.

## Tests

Add deterministic concurrency tests covering at least:

1. two concurrent initializers with the same configuration both succeed and
   return the same persisted manifest;
2. adoption logic executes once, using a test hook or counter;
3. two concurrent initializers with conflicting shard counts result in exactly
   one successful layout and one mismatch failure;
4. an initializer waiting on the lock rechecks a manifest created while it
   waited;
5. concurrent async tasks on the same runtime thread do not collide on a temp
   filename;
6. separate child processes serialize against one temporary root;
7. lock timeout produces an actionable error;
8. panic/cancellation or dropped guard releases the lock;
9. a simulated crash leaves a harmless lock file and later startup succeeds;
10. a stale temporary manifest file is ignored or safely cleaned under lock;
11. failed serialization/write/rename leaves no truncated final manifest;
12. parent-directory sync errors are surfaced where they can be injected;
13. existing legacy, mixed, and duplicate-layout tests still pass under the
    locked path.

Use subprocess tests for true cross-process behavior. Keep them bounded by
timeouts and clean up temporary roots.

## Acceptance criteria

- Layout existence check, adoption scan, and manifest write are protected by one
  cross-process exclusive lock.
- Every waiter re-reads and validates the persisted manifest after acquiring the
  lock.
- Conflicting startup configurations cannot both proceed.
- Temporary filenames cannot collide between tasks or processes.
- File and parent-directory durability are handled explicitly.
- Cancellation and process crashes do not permanently lock the store.
- Tests cover same-config and conflicting-config concurrent startup.
- `just check` passes.

## Non-goals

- Coordinating normal memory reads and writes through this lock.
- Implementing online resharding.
- Locking unrelated memory roots together.
- Building a distributed lease service for network filesystems that do not
  honor local advisory locks.

## Implementation cautions

- Confirm the persistent filesystem used in deployment supports the selected
  advisory-lock semantics; document limitations for NFS-like stores.
- Do not hold an async mutex guard while performing blocking lock acquisition.
- A unique temporary path is still required even with the lock for crash-safe
  cleanup and future maintenance tools.
- Keep the critical section broad enough to include the complete legacy scan;
  otherwise the physical layout can change between validation and manifest
  persistence.
