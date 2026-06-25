# Add an explicit memory resharding tool

**Priority:** P2 operations / migration safety  
**Primary area:** memory storage tooling, `MemoryStore` layout metadata, CLI or maintenance binary  
**Dependencies:** task 001 should land first

## Problem

Task 001 intentionally makes a memory shard-count mismatch fail closed instead
of silently remapping entity IDs. That protects data, but it leaves operators
with no supported way to intentionally change `memory_namespace_shards` after a
store has data.

The scaling benchmarks show that shard count matters. Operators will eventually
want to increase it as a deployment grows. Without an explicit resharding tool,
the safe answer is only “do not change the shard count,” which is operationally
limiting.

## Objective

Provide an offline, explicit resharding tool that migrates a memory store from
one physical shard count to another while preserving entity data, command
results, metadata, outbox records, owner epochs, and version floors.

This task should not run automatically at normal startup. Resharding must be a
conscious maintenance operation.

## Required behavior

The tool should accept:

```bash
dd memory reshard \
  --store ./store/memory \
  --from-shards 16 \
  --to-shards 64 \
  --dry-run
```

Exact command placement can follow project conventions. A standalone maintenance
binary is also acceptable if that is simpler than extending the public CLI.

### Safety requirements

- Refuse to run while the runtime is actively using the store. Use a lock file
  or equivalent exclusive guard.
- Require an explicit source shard count and target shard count.
- Reject zero or equal shard counts.
- Validate the existing layout manifest from task 001.
- In dry-run mode, scan and report exactly what would move without writing.
- Write into a new destination layout first; do not mutate source files in place.
- Verify destination contents before swapping manifests or directories.
- Leave a recoverable backup or checkpoint if interruption occurs.
- Produce a clear rollback/retry story.

### Data requirements

For every namespace and entity key, migrate all related tables consistently:

- memory metadata;
- memory state rows;
- command result rows;
- outbox rows;
- any future tables that are part of the memory entity domain.

Do not split one entity across multiple physical files. The new destination shard
is determined only by `stable_memory_shard_index(entity_key, to_shards)` and the
current hash version.

### Version and owner-epoch handling

- Preserve entity-level max versions.
- Preserve owner epochs.
- Recompute global/shard version floors for the target layout.
- Ensure `MemoryStore::new` on the destination passes manifest validation and
  floor detection.

### Outbox handling

- Preserve pending, leased, delivered, and retry metadata exactly unless there is
  a deliberate documented normalization.
- If leased rows exist, warn or fail unless `--allow-leased-outbox` is supplied.
  Leases may indicate a live or recently crashed runtime.

## Implementation strategy

A robust approach is:

1. acquire exclusive migration lock;
2. load and validate source manifest;
3. enumerate source namespaces and shard databases;
4. stream distinct entity keys and determine destination shards;
5. create destination namespace/shard databases with current schema;
6. copy all rows for each entity inside transactions;
7. verify row counts and per-entity max versions;
8. write target manifest;
9. atomically swap or instruct the operator to swap directories;
10. keep source backup until the operator confirms.

For very large stores, avoid loading all values into memory. Stream rows and use
bounded transactions.

## Tests

Add integration tests with temporary stores. Cover at least:

1. dry-run reports moves and creates no destination writes;
2. 1-to-4 shard migration preserves all entities and values;
3. 16-to-64 migration preserves metadata and versions;
4. 64-to-16 migration preserves data and reports increased shard coalescing;
5. command results migrate with their entity;
6. outbox rows migrate with status, attempts, and lease fields intact;
7. malformed source manifest fails;
8. source count mismatch fails;
9. target count equal to source fails;
10. interrupted destination from a prior run can be retried or cleaned safely;
11. exclusive lock prevents running against a live store;
12. post-migration `MemoryStore::new` opens the target and reads expected data.

## Acceptance criteria

- Operators can intentionally migrate a memory store to a new shard count.
- Normal runtime startup never performs implicit resharding.
- The tool validates source and destination layouts.
- The migration is dry-run capable and recoverable.
- Entity data, versions, owner epochs, command results, and outbox records are
  preserved.
- Large stores are streamed rather than fully loaded into memory.
- Tests cover up-shard, down-shard, dry-run, failure, and recovery paths.
- Documentation explains when and how to run the tool.

## Non-goals

- Online/live resharding with concurrent writes.
- Changing the shard hash algorithm.
- Cross-machine distributed migration.
- Optimizing migration throughput before correctness is proven.

## Implementation cautions

- Do not trust filenames alone; validate entity hashes against source manifest.
- Do not overwrite source data until destination verification succeeds.
- Make retry behavior explicit; half-finished migrations must not be confused
  with a valid store.
- Be careful with SQLite/Turso transaction boundaries when copying multiple
  tables for one entity.
