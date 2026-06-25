# Audit memory shard hash inputs

**Priority:** P2 correctness / consistency  
**Primary area:** keyed-memory routing, storage sharding, benchmark key generation  
**Dependencies:** task 001 is recommended first

## Problem

Memory storage sharding currently hashes the memory entity key. Runtime routing
uses the same key to assign a physical memory shard. Benchmarks also generate
same-shard and cross-shard keys using the same hash domain.

This is coherent if all memory bindings intentionally share the same shard ring,
but it also means two bindings with the same entity key route to the same
physical shard. Depending on intended semantics, that may be desirable for
locality or undesirable because unrelated bindings can collide more than needed.

Before changing anything, the project should explicitly decide and test the hash
input contract.

## Objective

Document and test whether the shard key should be:

- entity key only; or
- memory binding plus entity key; or
- namespace plus entity key; or
- another stable route key.

The goal is not to change the hash casually. The goal is to eliminate ambiguity
before locking the layout through manifests and resharding tools.

## Required investigation

Answer:

1. What is the intended independence domain for memory sharding?
2. Can two different memory bindings share an entity key but represent unrelated
   data?
3. Does the on-disk namespace already correspond to binding name, worker name,
   or another tenant boundary?
4. Would including binding/namespace in the hash improve distribution without
   breaking existing stores?
5. Are there security or isolation reasons to avoid cross-binding shard
   co-location?
6. Do benchmarks currently overstate or understate real production distribution
   because they hash only keys?

## Deliverables

- A short architecture note in `tasks/` or `docs/` explaining the chosen contract.
- Tests that assert the contract at the routing layer, storage layer, and
  benchmark key generator.
- If the contract changes, update task 001 and task 012 to include hash-input
  versioning and migration implications.

## Acceptance criteria

- The shard hash input is explicitly documented.
- Runtime routing, storage lookup, and benchmark generation use the same
  contract.
- Tests fail if one layer drifts from the others.
- Any decision to include or exclude binding/namespace is justified.

## Non-goals

- Performing a hash-input migration in this task.
- Changing the public keyed-memory API.
- Optimizing the hash function itself.

## Implementation cautions

- A hash input change is a storage-layout change and must be guarded by task 001.
- Do not break existing stores silently.
- Do not update benchmark expectations without explaining how the production
  distribution changed.
