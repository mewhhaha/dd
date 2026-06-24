# Preserve physical shard identity through the outbox lifecycle

**Priority:** P0 correctness and effect durability  
**Primary area:** `crates/runtime/src/memory.rs`, `crates/runtime/src/service/sessions.rs`  
**Dependencies:** compatible with task 001; both should land before mixed-layout adoption is considered production-safe

## Current behavior

Outbox claims are produced by scanning one explicit physical shard:

```rust
claim_due_outbox_records_for_shard_index(shard_index, ...)
```

The claim path discovers namespaces containing `shard-NNNN.db` and calls
`connect_shard(namespace, shard_index)`. However, `MemoryOutboxClaim` records
only:

- namespace;
- memory key;
- outbox record.

It does not retain the physical shard from which the row was claimed.

After in-memory delivery, `apply_outbox_delivery_outcomes` groups outcomes by
`(namespace, memory_key)` and reconnects with:

```rust
self.connect(&namespace, &memory_key)
```

That connection resolves the current canonical shard using the namespace hash
version and any per-key override. In mixed or duplicate physical layouts, the
canonical shard can differ from the physical shard that supplied the claim.
The acknowledgement or retry update then executes against a different database,
can affect zero rows, and currently still returns success.

The original row remains `inflight` until its lease expires and may be claimed
and delivered again. For external effects, that can produce repeated audit,
socket, transport, or trace actions beyond the expected at-least-once retry
boundary.

## Objective

Carry an immutable physical location token from claim through delivery and
acknowledgement. Every delivered, dropped, or retried claim must be updated in
the exact database that supplied it.

Do not derive acknowledgement location again from the entity key.

## Required data model

Add physical identity to the claim and outcome types. A minimum shape is:

```rust
pub struct MemoryOutboxClaim {
    pub namespace: String,
    pub physical_shard_index: usize,
    pub memory_key: String,
    pub record: MemoryOutboxRecord,
}

pub struct MemoryOutboxDeliveryOutcome {
    pub namespace: String,
    pub physical_shard_index: usize,
    pub memory_key: String,
    pub effect_id: String,
    pub action: MemoryOutboxDeliveryAction,
}
```

If a stronger opaque location token is preferable, use a private type containing
namespace, physical shard index, and any future layout generation. The token
must not be constructible from only a memory key at acknowledgement time.

Update every constructor, serializer/debug formatter, test helper, and channel
payload that carries these structures.

## Exact-shard acknowledgement

Change `apply_outbox_delivery_outcomes` to group by physical database, preferably:

```text
(namespace, physical_shard_index)
```

Within one transaction, update all outcomes for that database.

Use `connect_shard(namespace, physical_shard_index)`, not `connect(namespace,
memory_key)`.

For each update:

- match `effect_id` and `entity_key`;
- optionally match the expected current status/lease state where that improves
  stale-ack protection;
- inspect the affected-row count;
- treat zero affected rows as an error or explicit stale outcome, not silent
  success;
- reject an outcome whose shard index is outside the configured range;
- retain retry/backoff semantics.

If two claims in one batch target different entities in the same physical shard,
acknowledge them in one transaction where practical.

## Claim behavior

Every physical-shard claim function must populate the location directly from its
input shard index.

The non-sharded convenience claim path that iterates all shards must preserve
the same physical identity.

Entity-specific helper methods such as `claim_outbox_records`,
`mark_outbox_delivered`, and `retry_outbox_record` may continue using canonical
routing for direct entity operations, but internal background delivery must use
an exact-shard API. Name APIs so the distinction is difficult to misuse.

Suggested separation:

```rust
apply_outbox_delivery_outcomes_exact(...)
mark_outbox_delivered_for_entity(...)
```

or make the exact location mandatory for all post-claim operations.

## Interaction with duplicate copies

Task 001 should reject divergent duplicate entities. Until that lands, this task
must still prevent a claimed row from being acknowledged elsewhere.

Consider adding a canonical-route check at claim time:

- if the physical shard is not the canonical location for that entity, emit a
  bounded warning/metric;
- do not silently discard the row;
- exact-shard acknowledgement must remain correct if delivery proceeds;
- avoid claiming ignored identical duplicates if task 001 records them.

The correctness property is location fidelity, independent of duplicate policy.

## Failure and retry semantics

- A successfully delivered effect is marked delivered in its source database.
- A retryable effect returns to pending in its source database.
- A terminal drop is finalized in its source database.
- A database failure leaves the row recoverable after lease expiry.
- An acknowledgement for an already transitioned row must be idempotent or
  explicitly classified as stale.
- An outcome must never update a same-named effect in another physical shard.

Preserve at-least-once semantics. Do not acknowledge before in-memory delivery
reports its result.

## Observability

Add or extend counters for:

- exact-shard acknowledgement batches and rows;
- zero-row acknowledgement attempts;
- stale or mismatched physical-location outcomes;
- claims from noncanonical physical copies;
- acknowledgement retry/failure count by action class, without namespace or key
  labels.

Debug logs may include shard index and a hashed entity identifier. Do not add raw
entity keys to high-volume info logs.

## Tests

Add focused tests covering at least:

1. claim includes its physical shard index;
2. delivered outcome updates the source shard and only the source shard;
3. retry outcome updates the source shard and preserves retry time;
4. terminal drop updates the source shard;
5. canonical routing points to shard A while a row is claimed from shard B, and
   acknowledgement still updates B;
6. a same effect ID or entity key in another shard is not modified;
7. zero affected rows is surfaced rather than accepted silently;
8. outcomes for several entities in one physical shard are batched in one
   transaction;
9. outcomes spanning physical shards are separated correctly;
10. out-of-range physical shard index is rejected;
11. failure during acknowledgement rolls back the batch and rows become
    recoverable after lease expiry;
12. background worker/channel integration retains physical identity end to end;
13. existing audit, socket, transport, and trace effect tests remain green.

Use a mixed-layout fixture with a per-key override so the test would fail under
the current key-derived acknowledgement implementation.

## Acceptance criteria

- `MemoryOutboxClaim` or its private location token identifies the exact source
  physical shard.
- Background delivery outcomes reconnect with `connect_shard` using that source
  identity.
- Zero-row updates are observable errors or explicit stale outcomes.
- No claimed effect can be repeatedly delivered merely because canonical key
  routing differs from its physical source.
- Batching and retry behavior remain correct.
- New mixed-layout tests prove source-shard fidelity.
- `just check` passes.

## Non-goals

- Providing exactly-once external side effects.
- Automatically deleting duplicate outbox rows.
- Changing public memory/effect APIs.
- Replacing the outbox worker architecture.

## Implementation cautions

- Do not trust a caller-provided shard index without range validation.
- Preserve physical identity across every channel hop; adding it only at the
  final update layer is insufficient.
- Check Turso `execute` return semantics and assert affected rows explicitly.
- Keep batching by physical database so the fix does not regress effect
  throughput unnecessarily.
