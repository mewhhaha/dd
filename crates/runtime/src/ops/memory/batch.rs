//! Memory batch handles and operations.

use super::*;

pub(super) fn memory_batch_requires_commit(batch: &MemoryBatchHandle) -> bool {
    batch.command_result.is_some()
        || !batch.mutations.is_empty()
        || !batch.effects.is_empty()
        || batch.accepted
}

pub(super) fn memory_batch_commit_owner_epoch(batch: &MemoryBatchHandle) -> Result<i64> {
    if batch.owner_epoch <= 0 {
        return Err(PlatformError::bad_request(
            "memory batch commit requires memory owner epoch",
        ));
    }
    Ok(batch.owner_epoch)
}

pub(super) const MEMORY_BATCH_MAX_MUTATIONS: usize = 1024;
pub(super) const MEMORY_BATCH_MAX_EFFECTS: usize = 256;
pub(super) const MEMORY_BATCH_MAX_STAGED_BYTES: usize =
    storage::memory::MEMORY_ENTITY_MAX_VALUE_BYTES;
pub(super) const MEMORY_BATCH_MAX_KEY_BYTES: usize = 4096;
pub(super) const MEMORY_BATCH_MAX_EFFECT_KIND_BYTES: usize = 256;
pub(super) const MEMORY_MAX_COMMAND_HANDLES_PER_REQUEST: usize = 128;
pub(super) const MEMORY_MAX_BATCH_HANDLES_PER_REQUEST: usize = 128;
pub(super) const MEMORY_MAX_READ_HANDLES_PER_REQUEST: usize = 128;

pub(super) fn memory_batch_mutation_size(mutation: &MemoryBatchMutation) -> usize {
    mutation
        .key
        .len()
        .saturating_add(mutation.value.len())
        .saturating_add(mutation.encoding.len())
        .saturating_add(64)
}

pub(super) fn memory_batch_effect_size(effect: &MemoryOutboxEffectWrite) -> usize {
    effect
        .kind
        .len()
        .saturating_add(effect.payload.len())
        .saturating_add(64)
}

pub(super) fn memory_batch_command_result_size(result: &MemoryBatchCommandResult) -> usize {
    result.value.len().saturating_add(64)
}

pub(super) fn memory_batch_staged_bytes_without_command_result(batch: &MemoryBatchHandle) -> usize {
    batch.staged_bytes.saturating_sub(
        batch
            .command_result
            .as_ref()
            .map(memory_batch_command_result_size)
            .unwrap_or(0),
    )
}

pub(super) fn stage_memory_batch_mutation(
    batch: &mut MemoryBatchHandle,
    next: MemoryBatchMutation,
) -> Result<()> {
    if next.key.trim().is_empty() {
        return Err(PlatformError::bad_request(
            "memory batch mutation key must not be empty",
        ));
    }
    if next.key.len() > MEMORY_BATCH_MAX_KEY_BYTES {
        return Err(PlatformError::bad_request(format!(
            "memory batch mutation key must be at most {MEMORY_BATCH_MAX_KEY_BYTES} bytes"
        )));
    }
    let next_size = memory_batch_mutation_size(&next);
    let existing_idx = batch
        .mutations
        .iter()
        .position(|mutation| mutation.key == next.key);
    if existing_idx.is_none() && batch.mutations.len() >= MEMORY_BATCH_MAX_MUTATIONS {
        return Err(PlatformError::bad_request(format!(
            "memory batch exceeded {MEMORY_BATCH_MAX_MUTATIONS} mutations"
        )));
    }
    let existing_size = existing_idx
        .and_then(|idx| batch.mutations.get(idx))
        .map(memory_batch_mutation_size)
        .unwrap_or(0);
    let next_staged_bytes = batch
        .staged_bytes
        .saturating_sub(existing_size)
        .saturating_add(next_size);
    if next_staged_bytes > MEMORY_BATCH_MAX_STAGED_BYTES {
        return Err(PlatformError::bad_request(format!(
            "memory batch staged data exceeded {MEMORY_BATCH_MAX_STAGED_BYTES} bytes"
        )));
    }
    if let Some(existing_idx) = existing_idx {
        batch.mutations[existing_idx] = next;
    } else {
        batch.mutations.push(next);
    }
    batch.staged_bytes = next_staged_bytes;
    Ok(())
}

pub(super) fn stage_memory_batch_effect(
    batch: &mut MemoryBatchHandle,
    effect: MemoryOutboxEffectWrite,
) -> Result<()> {
    let kind = effect.kind.trim().to_string();
    if kind.is_empty() {
        return Err(PlatformError::bad_request(
            "memory batch effect kind must not be empty",
        ));
    }
    if kind.len() > MEMORY_BATCH_MAX_EFFECT_KIND_BYTES {
        return Err(PlatformError::bad_request(format!(
            "memory batch effect kind must be at most {MEMORY_BATCH_MAX_EFFECT_KIND_BYTES} bytes"
        )));
    }
    if batch.effects.len() >= MEMORY_BATCH_MAX_EFFECTS {
        return Err(PlatformError::bad_request(format!(
            "memory batch exceeded {MEMORY_BATCH_MAX_EFFECTS} effects"
        )));
    }
    let mut effect = effect;
    effect.kind = kind;
    let next_staged_bytes = batch
        .staged_bytes
        .saturating_add(memory_batch_effect_size(&effect));
    if next_staged_bytes > MEMORY_BATCH_MAX_STAGED_BYTES {
        return Err(PlatformError::bad_request(format!(
            "memory batch staged data exceeded {MEMORY_BATCH_MAX_STAGED_BYTES} bytes"
        )));
    }
    batch.effects.push(effect);
    batch.staged_bytes = next_staged_bytes;
    Ok(())
}

pub(super) fn stage_memory_batch_command_result(
    batch: &mut MemoryBatchHandle,
    result: MemoryBatchCommandResult,
) -> Result<()> {
    let next_staged_bytes = memory_batch_staged_bytes_without_command_result(batch)
        .saturating_add(memory_batch_command_result_size(&result));
    if next_staged_bytes > MEMORY_BATCH_MAX_STAGED_BYTES {
        return Err(PlatformError::bad_request(format!(
            "memory batch staged data exceeded {MEMORY_BATCH_MAX_STAGED_BYTES} bytes"
        )));
    }
    batch.command_result = Some(result);
    batch.staged_bytes = next_staged_bytes;
    Ok(())
}

/// The failure the isolate's next `op_memory_batch_begin` throws instead of
/// beginning a batch: a storage failure, or an ordinary one. The runtime's
/// tests set it with `op_test_memory_batch_begin_fail`.
#[cfg(test)]
pub(in crate::ops) struct MemoryBatchBeginFailure {
    storage: bool,
    message: String,
}

#[cfg(test)]
pub(in crate::ops) fn op_test_memory_batch_begin_fail(
    state: &mut OpState,
    storage: bool,
    message: String,
) {
    state.put(MemoryBatchBeginFailure { storage, message });
}

/// Opens a batch on the entry's committed snapshot, for an atomic callback
/// under the entry's transaction lease.
pub(in crate::ops) async fn op_memory_batch_begin(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    memory_scope_handle: u32,
    binding: String,
    key: String,
    command_handle: u32,
) -> std::result::Result<u32, OpError> {
    #[cfg(test)]
    if let Some(failure) = state.borrow_mut().try_take::<MemoryBatchBeginFailure>() {
        return Err(if failure.storage {
            OpError::custom(MEMORY_STORAGE_FAILURE, failure.message)
        } else {
            OpError::new(failure.message)
        });
    }
    if request_context_handle == 0 {
        return Err(OpError::new(
            "memory batch begin requires request_context_handle",
        ));
    }
    let scope = memory_scope_for_payload_with_epoch(&state, memory_scope_handle, &binding, &key)
        .map_err(js_error)?;
    if scope.lease.is_none() {
        return Err(OpError::new(format!(
            "memory batch for {binding}/{key} requires an active transaction lease"
        )));
    }
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let started = Instant::now();
    let snapshot = store
        .snapshot(&scope.namespace, &scope.memory_key)
        .await
        .map_err(|error| OpError::custom(MEMORY_STORAGE_FAILURE, error.to_string()))?;
    store.record_profile(
        MemoryProfileMetricKind::OpSnapshot,
        started.elapsed().as_micros() as u64,
        1,
    );
    // Existing oversized entities must remain writable so a callback can delete
    // or shrink their values. Storage checks the projected size before commit.
    if state
        .borrow()
        .borrow::<RequestSecretContexts>()
        .get(request_context_handle)
        .is_none_or(|request| request.canceled.load(Ordering::SeqCst))
    {
        return Err(OpError::new("memory transaction request was canceled"));
    }
    let batch = MemoryBatchHandle {
        request_context_handle,
        namespace: scope.namespace,
        memory_key: scope.memory_key,
        owner_epoch: scope.owner_epoch,
        _lease: scope.lease,
        command_handle,
        snapshot,
        staged_bytes: 0,
        accepted: false,
        command_result: None,
        mutations: Vec::new(),
        effects: Vec::new(),
    };
    state
        .borrow_mut()
        .borrow_mut::<MemoryBatchHandles>()
        .insert(batch, MEMORY_MAX_BATCH_HANDLES_PER_REQUEST)
        .map_err(js_error)
}

pub(in crate::ops) fn op_memory_batch_close(state: &mut OpState, batch_handle: u32) {
    if batch_handle != 0 {
        let _ = state
            .borrow_mut::<MemoryBatchHandles>()
            .remove(batch_handle);
    }
}

pub(super) fn memory_batch_handle(
    state: &OpState,
    batch_handle: u32,
) -> std::result::Result<&MemoryBatchHandle, OpError> {
    state
        .borrow::<MemoryBatchHandles>()
        .get(batch_handle)
        .ok_or_else(|| OpError::new("memory batch handle is invalid"))
}

pub(super) fn memory_batch_handle_mut(
    state: &mut OpState,
    batch_handle: u32,
) -> std::result::Result<&mut MemoryBatchHandle, OpError> {
    state
        .borrow_mut::<MemoryBatchHandles>()
        .get_mut(batch_handle)
        .ok_or_else(|| OpError::new("memory batch handle is invalid"))
}

pub(in crate::ops) fn op_memory_batch_accept(
    state: &mut OpState,
    batch_handle: u32,
) -> std::result::Result<(), OpError> {
    memory_batch_handle_mut(state, batch_handle)?.accepted = true;
    Ok(())
}

pub(in crate::ops) fn op_memory_batch_write(
    state: &mut OpState,
    batch_handle: u32,
    key: String,
    value: JsBuffer,
    encoding: String,
    deleted: bool,
) -> std::result::Result<(), OpError> {
    let batch = memory_batch_handle_mut(state, batch_handle)?;
    let next = MemoryBatchMutation {
        key,
        value: Bytes::from(value.into_vec()),
        encoding,
        deleted,
    };
    stage_memory_batch_mutation(batch, next).map_err(js_error)
}

/// The value the batch holds for `key`, its own writes over its snapshot, or
/// `null` when it holds none.
pub(in crate::ops) fn op_memory_batch_get(
    state: &mut OpState,
    batch_handle: u32,
    key: String,
) -> std::result::Result<Option<MemoryReadValue>, OpError> {
    let batch = memory_batch_handle(state, batch_handle)?;
    Ok(
        if let Some(mutation) = batch.mutations.iter().find(|mutation| mutation.key == key) {
            (!mutation.deleted).then(|| MemoryReadValue {
                value: mutation.value.to_vec().into(),
                encoding: mutation.encoding.clone(),
            })
        } else {
            memory_snapshot_record(&batch.snapshot, &key)
        },
    )
}

pub(in crate::ops) fn op_memory_batch_list(
    state: &mut OpState,
    batch_handle: u32,
    prefix: String,
) -> std::result::Result<Vec<String>, OpError> {
    let batch = memory_batch_handle(state, batch_handle)?;
    let mut keys = batch
        .snapshot
        .entries
        .iter()
        .filter(|entry| !entry.deleted && entry.key.starts_with(&prefix))
        .map(|entry| entry.key.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for mutation in batch
        .mutations
        .iter()
        .filter(|mutation| mutation.key.starts_with(&prefix))
    {
        if mutation.deleted {
            keys.remove(mutation.key.as_str());
        } else {
            keys.insert(&mutation.key);
        }
    }
    Ok(keys.into_iter().map(str::to_owned).collect())
}

pub(in crate::ops) fn op_memory_batch_emit(
    state: &mut OpState,
    batch_handle: u32,
    kind: String,
    payload: JsBuffer,
) -> std::result::Result<(), OpError> {
    let batch = memory_batch_handle_mut(state, batch_handle)?;
    stage_memory_batch_effect(
        batch,
        MemoryOutboxEffectWrite {
            kind,
            payload: payload.to_vec(),
        },
    )
    .map_err(js_error)
}

pub(in crate::ops) fn op_memory_batch_command_result_set(
    state: &mut OpState,
    batch_handle: u32,
    value: JsBuffer,
) -> std::result::Result<(), OpError> {
    let batch = memory_batch_handle_mut(state, batch_handle)?;
    stage_memory_batch_command_result(
        batch,
        MemoryBatchCommandResult {
            value: value.to_vec(),
        },
    )
    .map_err(js_error)
}

pub(super) fn memory_command_result_for_batch(
    state: &Rc<RefCell<OpState>>,
    batch: &MemoryBatchHandle,
) -> Result<Option<MemoryCommandResultWrite>> {
    let Some(result) = batch.command_result.as_ref() else {
        if batch.command_handle != 0 {
            return Err(PlatformError::bad_request(
                "memory command handle requires command_result",
            ));
        }
        return Ok(None);
    };

    if batch.command_handle == 0 {
        return Err(PlatformError::bad_request(
            "memory command result requires command handle",
        ));
    }

    let state_ref = state.borrow();
    let handle = state_ref
        .borrow::<MemoryCommandHandles>()
        .get(batch.command_handle)
        .ok_or_else(|| PlatformError::bad_request("memory command handle is invalid"))?;
    if handle.request_context_handle != batch.request_context_handle {
        return Err(PlatformError::bad_request(
            "memory command handle owner mismatch",
        ));
    }
    if handle.namespace != batch.namespace || handle.memory_key != batch.memory_key {
        return Err(PlatformError::bad_request(
            "memory command handle entity mismatch",
        ));
    }
    Ok(Some(MemoryCommandResultWrite {
        idempotency_key: handle.idempotency_key.clone(),
        result: result.value.clone(),
    }))
}

pub(super) fn close_memory_command_for_committed_batch(
    state: &Rc<RefCell<OpState>>,
    batch: &MemoryBatchHandle,
) {
    if batch.command_handle != 0 {
        let _ = state
            .borrow_mut()
            .borrow_mut::<MemoryCommandHandles>()
            .remove(batch.command_handle);
    }
}

/// Commits the batch, unless it is read-only and has nothing to commit.
pub(in crate::ops) async fn op_memory_batch_apply(
    state: Rc<RefCell<OpState>>,
    batch_handle: u32,
) -> std::result::Result<MemoryBatchApplied, OpError> {
    let started = std::time::Instant::now();
    let mut batch = state
        .borrow_mut()
        .borrow_mut::<MemoryBatchHandles>()
        .remove(batch_handle)
        .ok_or_else(|| OpError::new("memory batch handle is invalid"))?;
    let mutation_count = batch.mutations.len();
    let effect_count = batch.effects.len();
    if !memory_batch_requires_commit(&batch) {
        return Ok(MemoryBatchApplied {
            applied: false,
            mutation_count,
            output_gate_required: false,
        });
    }
    let owner_epoch = memory_batch_commit_owner_epoch(&batch).map_err(js_error)?;
    let command_result = memory_command_result_for_batch(&state, &batch).map_err(js_error)?;
    let store = state.borrow().borrow::<MemoryStore>().clone();
    store
        .apply_batch(
            &batch.namespace,
            &batch.memory_key,
            storage::memory::MemoryCommit {
                mutations: std::mem::take(&mut batch.mutations),
                command_result,
                outbox_effects: std::mem::take(&mut batch.effects),
                owner_epoch: Some(owner_epoch),
                lease: batch._lease.clone(),
            },
        )
        .await
        .map_err(js_error)?;
    close_memory_command_for_committed_batch(&state, &batch);
    store.record_profile(
        MemoryProfileMetricKind::OpApplyBatch,
        started.elapsed().as_micros() as u64,
        mutation_count as u64 + 1,
    );
    Ok(MemoryBatchApplied {
        applied: true,
        mutation_count,
        output_gate_required: batch.accepted || effect_count > 0,
    })
}
