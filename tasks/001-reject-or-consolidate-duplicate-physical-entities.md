# Reject or losslessly consolidate duplicate physical memory entities

**Priority:** P0 correctness and durability  
**Primary area:** `crates/runtime/src/memory.rs`  
**Dependencies:** none; complete before relying on mixed-layout adoption in production

## Current behavior

Legacy layout adoption can discover the same `(namespace, entity_key)` in more
than one physical shard database. Current `collect_physical_memory_key_shard_overrides`
reduces each copy to one scalar `max_revision`, selects the copy with the highest
value, and stores only that selected shard in
`namespace_key_shard_overrides`. Equal revisions are resolved by shard index.

That is not enough information to prove one physical copy supersedes another.
An entity is represented by several tables:

- `memory_state`, keyed by item key and carrying per-row versions;
- `memory_meta`, carrying max version and owner epoch;
- `memory_commands`, carrying idempotency results;
- `memory_outbox`, carrying durable effects and delivery state.

Two copies can have the same maximum revision while containing different item
keys, command results, outbox rows, deletion markers, owner epochs, or effect
status. A copy with a higher maximum state revision can still lack a pending
outbox effect or idempotency result present in the other copy.

Choosing one shard therefore hides data from the losing shard without proving
that data is redundant. The losing database remains on disk and can still be
found by physical-shard maintenance paths.

## Objective

Make startup fail closed whenever duplicate physical copies are not proven to be
logically identical. Do not infer a winner from maximum revision alone.

The first implementation should prioritize safety and diagnosability over
automatic repair. A separate explicit repair or reshard command can be added
later.

## Required behavior

### Detect duplicates by physical location

During legacy or physical-layout adoption, build an inventory keyed by:

```text
(namespace, entity_key) -> [physical shard locations]
```

The inventory must include entity keys found in every managed table, including
entities represented only by command or outbox rows.

When a key exists in only one physical shard, retain the existing adoption
behavior.

When a key exists in multiple physical shards, compare the complete logical
entity contents rather than only maximum revisions.

### Canonical logical comparison

Read and normalize all rows for the entity from:

- `memory_meta`;
- `memory_state` ordered by `item_key`;
- `memory_commands` ordered by `idempotency_key`;
- `memory_outbox` ordered by `effect_id`.

Comparison must include all correctness-relevant fields. At minimum:

- state value bytes, encoding, deleted flag, version, and item key;
- meta max version and owner epoch;
- command result bytes, revision, and idempotency key;
- outbox effect ID, kind, payload, revision, status, attempt count, and next
  attempt time where delivery semantics depend on them.

Timestamps that are purely observational may be ignored only if code comments
explain why they do not change behavior.

Use streaming or per-entity reads. Do not load every row from every namespace
into one unbounded process-wide structure.

### Fail closed for divergence

If duplicate copies differ logically, `MemoryStore::new` must return a clear
startup error before serving reads or writes and before writing an adoption
manifest.

The error must include:

- memory root;
- namespace;
- entity key, or a safe hash of it if logging raw keys is against project policy;
- all physical shard indices containing the entity;
- a concise difference summary such as `state`, `commands`, `outbox`, or
  `owner_epoch`;
- guidance that an explicit repair/reshard operation is required.

Do not delete, merge, rewrite, or silently select a divergent copy.

### Identical duplicate copies

Choose one of these policies and document it:

1. **Preferred conservative policy:** reject every duplicate, even identical
   copies, because retaining two physical copies can confuse maintenance scans.
2. **Acceptable policy:** permit byte-for-byte/logically identical copies only
   if every runtime and maintenance path is guaranteed to ignore the
   noncanonical copy. Record the canonical physical shard and the ignored
   duplicate locations explicitly, and ensure outbox scanning cannot claim the
   ignored rows.

Do not permit identical duplicates merely because the read path selects one
copy; physical shard scans must follow the same decision.

### Manifest safety

Write `memory-layout.json` only after duplicate validation succeeds completely.
A failed validation must leave the existing store and manifest state unchanged.

If identical duplicates are accepted, validate any persisted duplicate metadata
on every startup and reject a later divergence.

## Suggested code structure

Introduce explicit types rather than extending the current scalar helper:

```rust
struct PhysicalEntityLocation {
    namespace: String,
    entity_key: String,
    shard_index: usize,
    path: PathBuf,
}

struct LogicalEntityDigest {
    state: [u8; 32],
    meta: [u8; 32],
    commands: [u8; 32],
    outbox: [u8; 32],
}
```

A deterministic digest is acceptable if collisions are cryptographically
negligible and tests also exercise direct row comparison helpers. Include table
and field boundaries in hash input.

Replace or remove `PhysicalMemoryKeyShard::is_older_than`; maximum revision may
remain useful for diagnostics, but not for selecting a safe winner.

## Tests

Add temporary-store tests covering at least:

1. one physical copy adopts successfully;
2. duplicate state rows with different values fail;
3. duplicate copies with the same max revision but different item keys fail;
4. one copy contains a deletion marker absent from the other and fails;
5. command result exists only in one copy and fails;
6. pending outbox effect exists only in one copy and fails;
7. delivered versus pending outbox status differs and fails;
8. owner epoch differs and fails;
9. a key represented only in `memory_commands` is still detected;
10. a key represented only in `memory_outbox` is still detected;
11. identical duplicate policy behaves exactly as documented;
12. failed adoption does not create or replace `memory-layout.json`;
13. multiple divergent entities report a bounded, useful summary rather than an
    unbounded error message;
14. normal stable and legacy-hash adoption tests remain green.

## Acceptance criteria

- No duplicate entity is selected solely by maximum revision.
- Divergent physical copies cause deterministic startup failure.
- All state, metadata, command-result, and outbox tables participate in the
  comparison.
- Adoption never hides unique durable effects or idempotency records.
- The failure message is actionable and identifies physical locations.
- Manifest writes occur only after full validation.
- Existing memory correctness tests and `just check` pass.

## Non-goals

- Automatically merging divergent entity histories.
- Designing conflict resolution for independently written copies.
- Deleting losing shard files.
- Changing public memory APIs or the stable shard hash.

## Implementation cautions

- Do not compare only row counts or maximum versions.
- Preserve binary values exactly; do not round-trip them through UTF-8.
- A pending outbox row is durable user-visible work, not disposable metadata.
- Keep error reporting bounded when a store contains many duplicates.
