//! Memory operation limits and handle tests.

use super::*;

fn empty_batch(owner_epoch: i64) -> MemoryBatchHandle {
    MemoryBatchHandle {
        _lease: None,
        request_context_handle: 1,
        namespace: "ns".to_string(),
        memory_key: "entity".to_string(),
        owner_epoch,
        command_handle: 0,
        snapshot: Arc::new(crate::memory::MemorySnapshot {
            entries: Vec::new(),
            max_version: -1,
        }),
        staged_bytes: 0,
        accepted: false,
        command_result: None,
        mutations: Vec::new(),
        effects: Vec::new(),
    }
}

fn mutation(key: impl Into<String>, value_len: usize) -> MemoryBatchMutation {
    MemoryBatchMutation {
        key: key.into(),
        value: vec![b'x'; value_len].into(),
        encoding: "utf8".to_string(),
        deleted: false,
    }
}

#[test]
fn memory_batch_staging_rejects_excess_mutations() {
    let mut batch = empty_batch(1);
    for index in 0..MEMORY_BATCH_MAX_MUTATIONS {
        stage_memory_batch_mutation(&mut batch, mutation(format!("k-{index}"), 1))
            .expect("mutation within limit should stage");
    }

    let error =
        stage_memory_batch_mutation(&mut batch, mutation("overflow", 1)).expect_err("limit");
    assert!(
        error
            .to_string()
            .contains("memory batch exceeded 1024 mutations")
    );
    assert_eq!(batch.mutations.len(), MEMORY_BATCH_MAX_MUTATIONS);
}

#[test]
fn memory_batch_staging_replacement_updates_byte_accounting() {
    let mut batch = empty_batch(1);
    stage_memory_batch_mutation(&mut batch, mutation("same", 1024))
        .expect("first mutation should stage");
    let first_bytes = batch.staged_bytes;
    stage_memory_batch_mutation(&mut batch, mutation("same", 1))
        .expect("replacement mutation should stage");

    assert_eq!(batch.mutations.len(), 1);
    assert!(batch.staged_bytes < first_bytes);
    assert_eq!(
        batch.staged_bytes,
        memory_batch_mutation_size(&batch.mutations[0])
    );
}

#[test]
fn memory_batch_staging_rejects_total_byte_overflow() {
    let mut batch = empty_batch(1);
    let error =
        stage_memory_batch_mutation(&mut batch, mutation("huge", MEMORY_BATCH_MAX_STAGED_BYTES))
            .expect_err("oversized mutation should fail");
    assert!(
        error
            .to_string()
            .contains("memory batch staged data exceeded")
    );
    assert_eq!(batch.staged_bytes, 0);
    assert!(batch.mutations.is_empty());
}

#[test]
fn memory_batch_command_result_replacement_updates_byte_accounting() {
    let mut batch = empty_batch(1);
    stage_memory_batch_command_result(
        &mut batch,
        MemoryBatchCommandResult {
            value: vec![1; 1024],
        },
    )
    .expect("first result should stage");
    let first_bytes = batch.staged_bytes;
    stage_memory_batch_command_result(&mut batch, MemoryBatchCommandResult { value: vec![1] })
        .expect("replacement result should stage");

    assert!(batch.staged_bytes < first_bytes);
    assert_eq!(
        batch.staged_bytes,
        memory_batch_command_result_size(batch.command_result.as_ref().expect("result"))
    );
}

#[test]
fn memory_batch_effect_staging_enforces_count_and_kind_limits() {
    let mut batch = empty_batch(1);
    let error = stage_memory_batch_effect(
        &mut batch,
        MemoryOutboxEffectWrite {
            kind: " ".to_string(),
            payload: Vec::new(),
        },
    )
    .expect_err("empty kind should fail");
    assert!(error.to_string().contains("effect kind must not be empty"));

    for index in 0..MEMORY_BATCH_MAX_EFFECTS {
        stage_memory_batch_effect(
            &mut batch,
            MemoryOutboxEffectWrite {
                kind: format!("audit.{index}"),
                payload: Vec::new(),
            },
        )
        .expect("effect within limit should stage");
    }
    let error = stage_memory_batch_effect(
        &mut batch,
        MemoryOutboxEffectWrite {
            kind: "audit.overflow".to_string(),
            payload: Vec::new(),
        },
    )
    .expect_err("effect count limit should fail");
    assert!(
        error
            .to_string()
            .contains("memory batch exceeded 256 effects")
    );
}

#[test]
fn memory_batch_commit_owner_epoch_rejects_unowned_committing_batch() {
    let mut batch = empty_batch(0);
    stage_memory_batch_mutation(&mut batch, mutation("count", 1)).expect("mutation should stage");

    let error = memory_batch_commit_owner_epoch(&batch).expect_err("unowned commit must reject");

    assert_eq!(
        error.to_string(),
        "memory batch commit requires memory owner epoch"
    );
}

#[test]
fn memory_batch_commit_owner_epoch_accepts_owned_committing_batch() {
    let mut batch = empty_batch(7);
    stage_memory_batch_mutation(&mut batch, mutation("count", 1)).expect("mutation should stage");

    assert_eq!(memory_batch_commit_owner_epoch(&batch).expect("owner"), 7);
}

fn byte_handle(request_context_handle: u32, value: &'static [u8]) -> MemoryByteHandle {
    MemoryByteHandle {
        request_context_handle,
        value: Bytes::from_static(value),
    }
}

#[test]
fn memory_byte_handles_account_owner_bytes_and_clear() {
    let mut handles = MemoryByteHandles::default();
    let first = handles
        .insert(byte_handle(7, &[1, 2, 3]), MEMORY_BATCH_MAX_STAGED_BYTES)
        .expect("first handle should insert");
    let second = handles
        .insert(byte_handle(7, &[4, 5]), MEMORY_BATCH_MAX_STAGED_BYTES)
        .expect("second handle should insert");
    assert_eq!(handles.usage(7), 5);

    let mismatch = handles
        .take(8, first)
        .expect_err("wrong owner must not consume handle");
    assert!(mismatch.to_string().contains("owner mismatch"));
    assert_eq!(handles.usage(7), 5);

    assert_eq!(
        handles.take(7, first).expect("first handle"),
        Bytes::from_static(&[1, 2, 3])
    );
    assert_eq!(handles.usage(7), 2);
    handles.clear_owner(7);
    assert_eq!(handles.usage(7), 0);
    assert!(handles.take(7, second).is_err());
}

#[test]
fn memory_byte_handles_reject_owner_byte_overflow() {
    let mut handles = MemoryByteHandles::default();
    handles
        .insert(byte_handle(7, &[1; 8]), 8)
        .expect("initial bytes should fit");
    let error = handles
        .insert(byte_handle(7, &[2]), 8)
        .expect_err("owner byte budget should reject overflow");
    assert!(
        error
            .to_string()
            .contains("memory byte handles exceeded 8 bytes")
    );
    assert_eq!(handles.usage(7), 8);

    handles
        .insert(byte_handle(8, &[3]), 8)
        .expect("other owner has independent budget");
    assert_eq!(handles.usage(8), 1);
}

fn command_handle(request_context_handle: u32, suffix: usize) -> MemoryCommandHandle {
    MemoryCommandHandle {
        request_context_handle,
        namespace: "ns".to_string(),
        memory_key: "entity".to_string(),
        idempotency_key: format!("command-{suffix}"),
    }
}

#[test]
fn memory_command_handles_enforce_owner_count_limit() {
    let mut handles = MemoryCommandHandles::default();
    let first = handles
        .insert(command_handle(7, 0), 2)
        .expect("first command handle should insert");
    handles
        .insert(command_handle(7, 1), 2)
        .expect("second command handle should insert");
    let error = handles
        .insert(command_handle(7, 2), 2)
        .expect_err("third owner handle should fail");
    assert!(
        error
            .to_string()
            .contains("memory command handles exceeded 2 active handles")
    );
    assert_eq!(handles.usage(7), 2);

    handles.remove(first).expect("first handle should remove");
    assert_eq!(handles.usage(7), 1);
    handles
        .insert(command_handle(7, 3), 2)
        .expect("slot should be reusable after remove");
    handles
        .insert(command_handle(8, 0), 2)
        .expect("other owner should have independent quota");
    handles.clear_owner(7);
    assert_eq!(handles.usage(7), 0);
    assert_eq!(handles.usage(8), 1);
}

#[test]
fn read_handles_release_snapshots_and_keep_request_quotas_separate() {
    let snapshot = Arc::new(crate::memory::MemorySnapshot {
        entries: Vec::new(),
        max_version: -1,
    });
    let read = |request_context_handle| MemoryReadHandle {
        request_context_handle,
        snapshot: Arc::clone(&snapshot),
    };
    let mut handles = MemoryReadHandles::default();
    let first = handles.insert(read(1), 1).unwrap();
    assert!(handles.insert(read(1), 1).is_err());
    let second = handles.insert(read(2), 1).unwrap();
    assert_eq!(Arc::strong_count(&snapshot), 3);
    handles.clear_owner(1);
    assert!(handles.get(first).is_none());
    assert!(handles.get(second).is_some());
    assert_eq!(handles.usage(1), 0);
    assert_eq!(handles.usage(2), 1);
    assert_eq!(Arc::strong_count(&snapshot), 2);
    handles.insert(read(1), 1).unwrap();
    handles.remove(second).unwrap();
    handles.clear_owner(1);
    assert_eq!(Arc::strong_count(&snapshot), 1);
}

#[test]
fn memory_batch_handles_enforce_owner_count_limit() {
    let mut handles = MemoryBatchHandles::default();
    let first = handles
        .insert(empty_batch(7), 2)
        .expect("first batch handle should insert");
    handles
        .insert(empty_batch(7), 2)
        .expect("second batch handle should insert");
    let error = handles
        .insert(empty_batch(7), 2)
        .expect_err("third owner batch should fail");
    assert!(
        error
            .to_string()
            .contains("memory batch handles exceeded 2 active handles")
    );
    assert_eq!(handles.usage(1), 2);

    handles.remove(first).expect("first batch should remove");
    assert_eq!(handles.usage(1), 1);
    handles
        .insert(empty_batch(7), 2)
        .expect("slot should be reusable after remove");

    let mut other = empty_batch(7);
    other.request_context_handle = 2;
    handles
        .insert(other, 2)
        .expect("other owner should have independent quota");
    handles.clear_owner(1);
    assert_eq!(handles.usage(1), 0);
    assert_eq!(handles.usage(2), 1);
}
