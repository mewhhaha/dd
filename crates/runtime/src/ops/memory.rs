use super::*;

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_lease_acquire(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] key: String,
) -> MemoryLeaseResult {
    let result = async {
        if binding.is_empty() || key.is_empty() || binding.len() + key.len() > 1024 {
            return Err(PlatformError::bad_request(format!(
                "memory lease requires binding and key within 1024 bytes; got {} and {} bytes",
                binding.len(), key.len()
            )));
        }
        let (store, namespace, canceled, canceled_notify) = {
            let op_state = state.borrow();
            let request = op_state.borrow::<RequestSecretContexts>().get(request_context_handle)
                .ok_or_else(|| PlatformError::runtime("memory lease request is unavailable"))?;
            let worker = &op_state.borrow::<WorkerCacheNamespace>().0;
            (
                op_state.borrow::<MemoryStore>().clone(),
                crate::memory::worker_namespace(worker, &binding),
                Arc::clone(&request.canceled),
                Arc::clone(&request.canceled_notify),
            )
        };
        let cancellation = canceled_notify.notified();
        tokio::pin!(cancellation);
        cancellation.as_mut().enable();
        if canceled.load(Ordering::SeqCst) {
            return Err(PlatformError::runtime("memory lease request was canceled"));
        }
        let lease = tokio::select! {
            biased;
            _ = &mut cancellation => return Err(PlatformError::runtime("memory lease request was canceled")),
            result = store.acquire_lease(&namespace, &key) => result?,
        };
        let mut op_state = state.borrow_mut();
        if canceled.load(Ordering::SeqCst) {
            return Err(PlatformError::runtime("memory lease request was canceled"));
        }
        Ok(op_state.borrow_mut::<MemoryRequestScopes>().insert(MemoryRequestScope {
            namespace: binding,
            memory_key: key,
            owner_epoch: lease.owner_epoch(),
            request_context_handle,
            lease: Some(lease),
        }))
    }.await;
    match result {
        Ok(handle) => MemoryLeaseResult {
            handle,
            error: String::new(),
        },
        Err(error) => MemoryLeaseResult {
            handle: 0,
            error: error.to_string(),
        },
    }
}

struct ResolvedMemoryScope {
    lease: Option<Arc<crate::memory::MemoryLease>>,
    namespace: String,
    memory_key: String,
    owner_epoch: i64,
}

pub(crate) fn clear_memory_command_handles(state: &mut OpState, request_context_handle: u32) {
    state
        .borrow_mut::<MemoryCommandHandles>()
        .clear_owner(request_context_handle);
}

pub(crate) fn clear_memory_byte_handles(state: &mut OpState, request_context_handle: u32) {
    state
        .borrow_mut::<MemoryByteHandles>()
        .clear_owner(request_context_handle);
}

pub(crate) fn clear_memory_batch_handles(state: &mut OpState, request_context_handle: u32) {
    state
        .borrow_mut::<MemoryBatchHandles>()
        .clear_owner(request_context_handle);
}

pub(crate) fn clear_memory_read_handles(state: &mut OpState, request_context_handle: u32) {
    state
        .borrow_mut::<MemoryReadHandles>()
        .clear_owner(request_context_handle);
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_read_begin(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] key: String,
) -> MemoryBeginResult {
    let mut storage_failure = false;
    let result = async {
        if binding.is_empty() || key.is_empty() || binding.len() + key.len() > 1024 {
            return Err(PlatformError::bad_request(
                "memory read requires binding and key within 1024 bytes",
            ));
        }
        let (store, namespace, canceled, canceled_notify) = {
            let op_state = state.borrow();
            let request = op_state
                .borrow::<RequestSecretContexts>()
                .get(request_context_handle)
                .ok_or_else(|| PlatformError::runtime("memory read request is unavailable"))?;
            let worker = &op_state.borrow::<WorkerCacheNamespace>().0;
            (
                op_state.borrow::<MemoryStore>().clone(),
                crate::memory::worker_namespace(worker, &binding),
                Arc::clone(&request.canceled),
                Arc::clone(&request.canceled_notify),
            )
        };
        let cancellation = canceled_notify.notified();
        tokio::pin!(cancellation);
        cancellation.as_mut().enable();
        if canceled.load(Ordering::SeqCst) {
            return Err(PlatformError::runtime("memory read request was canceled"));
        }
        let started = Instant::now();
        let snapshot = tokio::select! {
            biased;
            _ = &mut cancellation => return Err(PlatformError::runtime("memory read request was canceled")),
            result = store.snapshot(&namespace, &key) => result.inspect_err(|_| {
                storage_failure = true;
            })?,
        };
        store.record_profile(
            MemoryProfileMetricKind::OpSnapshot,
            started.elapsed().as_micros() as u64,
            1,
        );
        let value_bytes: usize = snapshot.entries.iter().map(|entry| entry.value.len()).sum();
        if value_bytes > MEMORY_BATCH_MAX_STAGED_BYTES {
            return Err(PlatformError::bad_request(format!(
                "memory snapshot exceeded {MEMORY_BATCH_MAX_STAGED_BYTES} bytes"
            )));
        }
        let mut op_state = state.borrow_mut();
        if op_state
            .borrow::<RequestSecretContexts>()
            .get(request_context_handle)
            .is_none_or(|request| request.canceled.load(Ordering::SeqCst))
        {
            return Err(PlatformError::runtime("memory read request was canceled"));
        }
        op_state.borrow_mut::<MemoryReadHandles>().insert(
            MemoryReadHandle {
                request_context_handle,
                snapshot,
            },
            MEMORY_MAX_READ_HANDLES_PER_REQUEST,
        )
    }
    .await;
    match result {
        Ok(handle) => MemoryBeginResult {
            ok: true,
            storage_failure: false,
            handle,
            error: String::new(),
        },
        Err(error) => MemoryBeginResult {
            ok: false,
            storage_failure,
            handle: 0,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_read_close(state: &mut OpState, read_handle: u32) {
    state.borrow_mut::<MemoryReadHandles>().remove(read_handle);
}

fn memory_snapshot_record(
    snapshot: &crate::memory::MemorySnapshot,
    key: &str,
) -> Option<MemoryReadValue> {
    // Persisted snapshots and committed deltas both keep binary key order.
    let index = snapshot
        .entries
        .binary_search_by(|entry| entry.key.as_str().cmp(key))
        .ok()?;
    let entry = &snapshot.entries[index];
    (!entry.deleted).then(|| MemoryReadValue {
        // JavaScript receives its own mutable buffer, never the shared snapshot's storage.
        value: entry.value.to_vec().into(),
        encoding: entry.encoding.clone(),
    })
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_read_get(
    state: &mut OpState,
    read_handle: u32,
    #[string] key: String,
) -> MemoryGetResult {
    let Some(read) = state.borrow::<MemoryReadHandles>().get(read_handle) else {
        return MemoryGetResult {
            ok: false,
            record: None,
            error: "memory read handle is invalid".into(),
        };
    };
    MemoryGetResult {
        ok: true,
        record: memory_snapshot_record(&read.snapshot, &key),
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_read_keys(
    state: &mut OpState,
    read_handle: u32,
    #[string] prefix: String,
) -> MemoryKeysResult {
    let Some(read) = state.borrow::<MemoryReadHandles>().get(read_handle) else {
        return MemoryKeysResult {
            ok: false,
            keys: Vec::new(),
            error: "memory read handle is invalid".into(),
        };
    };
    MemoryKeysResult {
        ok: true,
        keys: read
            .snapshot
            .entries
            .iter()
            .filter(|entry| !entry.deleted && entry.key.starts_with(&prefix))
            .map(|entry| entry.key.clone())
            .collect(),
        error: String::new(),
    }
}

fn memory_batch_requires_commit(batch: &MemoryBatchHandle) -> bool {
    batch.command_result.is_some()
        || !batch.mutations.is_empty()
        || !batch.effects.is_empty()
        || batch.accepted
}

fn memory_batch_commit_owner_epoch(batch: &MemoryBatchHandle) -> Result<i64> {
    if batch.owner_epoch <= 0 {
        return Err(PlatformError::bad_request(
            "memory batch commit requires memory owner epoch",
        ));
    }
    Ok(batch.owner_epoch)
}

const MEMORY_BATCH_MAX_MUTATIONS: usize = 1024;
const MEMORY_BATCH_MAX_EFFECTS: usize = 256;
const MEMORY_BATCH_MAX_STAGED_BYTES: usize = 16 * 1024 * 1024;
const MEMORY_BATCH_MAX_KEY_BYTES: usize = 4096;
const MEMORY_BATCH_MAX_EFFECT_KIND_BYTES: usize = 256;
const MEMORY_MAX_COMMAND_HANDLES_PER_REQUEST: usize = 128;
const MEMORY_MAX_BATCH_HANDLES_PER_REQUEST: usize = 128;
const MEMORY_MAX_READ_HANDLES_PER_REQUEST: usize = 128;

fn memory_batch_mutation_size(mutation: &MemoryBatchMutation) -> usize {
    mutation
        .key
        .len()
        .saturating_add(mutation.value.len())
        .saturating_add(mutation.encoding.len())
        .saturating_add(64)
}

fn memory_batch_effect_size(effect: &MemoryOutboxEffectWrite) -> usize {
    effect
        .kind
        .len()
        .saturating_add(effect.payload.len())
        .saturating_add(64)
}

fn memory_batch_command_result_size(result: &MemoryBatchCommandResult) -> usize {
    result.value.len().saturating_add(64)
}

fn memory_batch_staged_bytes_without_command_result(batch: &MemoryBatchHandle) -> usize {
    batch.staged_bytes.saturating_sub(
        batch
            .command_result
            .as_ref()
            .map(memory_batch_command_result_size)
            .unwrap_or(0),
    )
}

fn stage_memory_batch_mutation(
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

fn stage_memory_batch_effect(
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

fn stage_memory_batch_command_result(
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

fn memory_bytes_for_handle_state(
    state: &mut OpState,
    request_context_handle: u32,
    handle: u32,
) -> Result<Bytes> {
    state
        .borrow_mut::<MemoryByteHandles>()
        .take(request_context_handle, handle)
}

fn memory_output_bytes_insert(
    state: &mut OpState,
    request_context_handle: u32,
    value: Bytes,
) -> Result<u32> {
    if value.is_empty() {
        return Ok(0);
    }
    let max_owner_bytes = MEMORY_BATCH_MAX_STAGED_BYTES;
    state
        .borrow_mut::<MemoryByteHandles>()
        .insert(request_context_handle, value, max_owner_bytes)
}

#[deno_core::op2]
#[buffer]
pub(super) fn op_memory_bytes_take(
    state: &mut OpState,
    request_context_handle: u32,
    handle: u32,
) -> Vec<u8> {
    if request_context_handle == 0 || handle == 0 {
        return Vec::new();
    }
    memory_bytes_for_handle_state(state, request_context_handle, handle)
        .map(|value| value.to_vec())
        .unwrap_or_default()
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_batch_begin(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    memory_scope_handle: u32,
    #[string] binding: String,
    #[string] key: String,
    command_handle: u32,
) -> MemoryBeginResult {
    if request_context_handle == 0 {
        return MemoryBeginResult {
            ok: false,
            storage_failure: false,
            handle: 0,
            error: "memory batch begin requires request_context_handle".to_string(),
        };
    }
    let scope =
        match memory_scope_for_payload_with_epoch(&state, memory_scope_handle, &binding, &key) {
            Ok(scope) => scope,
            Err(error) => {
                return MemoryBeginResult {
                    ok: false,
                    storage_failure: false,
                    handle: 0,
                    error: error.to_string(),
                };
            }
        };
    if scope.lease.is_none() {
        return MemoryBeginResult {
            ok: false,
            storage_failure: false,
            handle: 0,
            error: format!("memory batch for {binding}/{key} requires an active transaction lease"),
        };
    }
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let started = Instant::now();
    let snapshot = match store.snapshot(&scope.namespace, &scope.memory_key).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return MemoryBeginResult {
                ok: false,
                storage_failure: true,
                handle: 0,
                error: error.to_string(),
            };
        }
    };
    store.record_profile(
        MemoryProfileMetricKind::OpSnapshot,
        started.elapsed().as_micros() as u64,
        1,
    );
    let value_bytes: usize = snapshot.entries.iter().map(|entry| entry.value.len()).sum();
    if value_bytes > MEMORY_BATCH_MAX_STAGED_BYTES {
        return MemoryBeginResult {
            ok: false,
            storage_failure: true,
            handle: 0,
            error: format!("memory snapshot exceeded {MEMORY_BATCH_MAX_STAGED_BYTES} bytes"),
        };
    }
    if state
        .borrow()
        .borrow::<RequestSecretContexts>()
        .get(request_context_handle)
        .is_none_or(|request| request.canceled.load(Ordering::SeqCst))
    {
        return MemoryBeginResult {
            ok: false,
            storage_failure: false,
            handle: 0,
            error: "memory transaction request was canceled".into(),
        };
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
    let handle = match state
        .borrow_mut()
        .borrow_mut::<MemoryBatchHandles>()
        .insert(batch, MEMORY_MAX_BATCH_HANDLES_PER_REQUEST)
    {
        Ok(handle) => handle,
        Err(error) => {
            return MemoryBeginResult {
                ok: false,
                storage_failure: false,
                handle: 0,
                error: error.to_string(),
            };
        }
    };
    MemoryBeginResult {
        ok: true,
        storage_failure: false,
        handle,
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_batch_close(state: &mut OpState, batch_handle: u32) {
    if batch_handle != 0 {
        let _ = state
            .borrow_mut::<MemoryBatchHandles>()
            .remove(batch_handle);
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_batch_accept(state: &mut OpState, batch_handle: u32) -> bool {
    let Some(batch) = state
        .borrow_mut::<MemoryBatchHandles>()
        .get_mut(batch_handle)
    else {
        return false;
    };
    batch.accepted = true;
    true
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_batch_mutation(
    state: &mut OpState,
    batch_handle: u32,
    #[string] key: String,
    #[buffer] value: &[u8],
    #[string] encoding: String,
    deleted: bool,
) -> MemoryBatchMutationResult {
    let Some(batch) = state
        .borrow_mut::<MemoryBatchHandles>()
        .get_mut(batch_handle)
    else {
        return MemoryBatchMutationResult {
            ok: false,
            error: "memory batch handle is invalid".to_string(),
        };
    };
    let next = MemoryBatchMutation {
        key,
        value: Bytes::copy_from_slice(value),
        encoding,
        deleted,
    };
    match stage_memory_batch_mutation(batch, next) {
        Ok(()) => MemoryBatchMutationResult {
            ok: true,
            error: String::new(),
        },
        Err(error) => MemoryBatchMutationResult {
            ok: false,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_batch_get(
    state: &mut OpState,
    batch_handle: u32,
    #[string] key: String,
) -> MemoryGetResult {
    let Some(batch) = state.borrow::<MemoryBatchHandles>().get(batch_handle) else {
        return MemoryGetResult {
            ok: false,
            record: None,
            error: "memory batch handle is invalid".into(),
        };
    };
    let record = if let Some(mutation) = batch.mutations.iter().find(|mutation| mutation.key == key)
    {
        (!mutation.deleted).then(|| MemoryReadValue {
            value: mutation.value.to_vec().into(),
            encoding: mutation.encoding.clone(),
        })
    } else {
        memory_snapshot_record(&batch.snapshot, &key)
    };
    MemoryGetResult {
        ok: true,
        record,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_batch_keys(
    state: &mut OpState,
    batch_handle: u32,
    #[string] prefix: String,
) -> MemoryKeysResult {
    let Some(batch) = state.borrow::<MemoryBatchHandles>().get(batch_handle) else {
        return MemoryKeysResult {
            ok: false,
            keys: Vec::new(),
            error: "memory batch handle is invalid".into(),
        };
    };
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
    MemoryKeysResult {
        ok: true,
        keys: keys.into_iter().map(str::to_owned).collect(),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_batch_effect(
    state: &mut OpState,
    batch_handle: u32,
    #[string] kind: String,
    #[buffer] payload: &[u8],
) -> bool {
    let Some(batch) = state
        .borrow_mut::<MemoryBatchHandles>()
        .get_mut(batch_handle)
    else {
        return false;
    };
    stage_memory_batch_effect(
        batch,
        MemoryOutboxEffectWrite {
            kind,
            payload: payload.to_vec(),
        },
    )
    .is_ok()
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_batch_command_result(
    state: &mut OpState,
    batch_handle: u32,
    #[buffer] value: &[u8],
) -> bool {
    let Some(batch) = state
        .borrow_mut::<MemoryBatchHandles>()
        .get_mut(batch_handle)
    else {
        return false;
    };
    stage_memory_batch_command_result(
        batch,
        MemoryBatchCommandResult {
            value: value.to_vec(),
        },
    )
    .is_ok()
}

pub(super) fn memory_scope_for_payload(
    state: &Rc<RefCell<OpState>>,
    memory_scope_handle: u32,
    binding: &str,
    key: &str,
) -> Result<(String, String)> {
    let scope = memory_scope_for_payload_with_epoch(state, memory_scope_handle, binding, key)?;
    Ok((scope.namespace, scope.memory_key))
}

fn memory_scope_for_payload_with_epoch(
    state: &Rc<RefCell<OpState>>,
    memory_scope_handle: u32,
    binding: &str,
    key: &str,
) -> Result<ResolvedMemoryScope> {
    let binding = binding.trim();
    let key = key.trim();
    let worker = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
    let request_scope = if memory_scope_handle > 0 {
        let op_state = state.borrow();
        Some(
            op_state
                .borrow::<MemoryRequestScopes>()
                .get(memory_scope_handle)
                .cloned()
                .ok_or_else(|| {
                    PlatformError::runtime(format!("memory scope {memory_scope_handle} is closed"))
                })?,
        )
    } else {
        None
    };
    if let Some(scope) = request_scope {
        if (!binding.is_empty() && binding != scope.namespace)
            || (!key.is_empty() && key != scope.memory_key)
        {
            return Err(PlatformError::runtime(
                "memory storage scope does not match payload binding/key",
            ));
        }
        return Ok(ResolvedMemoryScope {
            lease: scope.lease,
            namespace: crate::memory::worker_namespace(&worker, &scope.namespace),
            memory_key: scope.memory_key,
            owner_epoch: scope.owner_epoch,
        });
    }
    if !binding.is_empty() && !key.is_empty() {
        return Ok(ResolvedMemoryScope {
            lease: None,
            namespace: crate::memory::worker_namespace(&worker, binding),
            memory_key: key.to_string(),
            owner_epoch: 0,
        });
    }
    Err(PlatformError::runtime(
        "memory storage scope is unavailable",
    ))
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_profile_record_js(
    state: &mut OpState,
    #[string] metric: String,
    duration_us: u32,
    items: u32,
) {
    let kind = match metric.as_str() {
        "js_read_only_commit" => MemoryProfileMetricKind::JsReadOnlyCommit,
        "js_txn_begin" => MemoryProfileMetricKind::JsTxnBegin,
        "js_txn_commit" => MemoryProfileMetricKind::JsTxnCommit,
        _ => return,
    };
    let store = state.borrow::<MemoryStore>().clone();
    store.record_profile(kind, u64::from(duration_us), u64::from(items.max(1)));
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_profile_take(state: &mut OpState) -> MemoryProfileResult {
    let store = state.borrow::<MemoryStore>().clone();
    MemoryProfileResult {
        ok: true,
        snapshot: Some(store.take_profile_snapshot_and_reset()),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_profile_reset(state: &mut OpState) {
    let store = state.borrow::<MemoryStore>().clone();
    store.reset_profile();
}

fn memory_command_result_for_batch(
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

fn close_memory_command_for_committed_batch(
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

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_batch_apply(
    state: Rc<RefCell<OpState>>,
    batch_handle: u32,
) -> MemoryStateApplyBatchResult {
    let started = std::time::Instant::now();
    let Some(mut batch) = state
        .borrow_mut()
        .borrow_mut::<MemoryBatchHandles>()
        .remove(batch_handle)
    else {
        return MemoryStateApplyBatchResult {
            ok: false,
            applied: false,
            read_only: false,
            mutation_count: 0,
            effect_count: 0,
            accepted: false,
            output_gate_required: false,
            error: "memory batch handle is invalid".to_string(),
        };
    };
    let mutation_count = batch.mutations.len();
    let effect_count = batch.effects.len();
    if !memory_batch_requires_commit(&batch) {
        return MemoryStateApplyBatchResult {
            ok: true,
            applied: false,
            read_only: true,
            mutation_count,
            effect_count,
            accepted: batch.accepted,
            output_gate_required: false,
            error: String::new(),
        };
    }
    let owner_epoch = match memory_batch_commit_owner_epoch(&batch) {
        Ok(owner_epoch) => owner_epoch,
        Err(error) => {
            return MemoryStateApplyBatchResult {
                ok: false,
                applied: false,
                read_only: false,
                mutation_count,
                effect_count,
                accepted: batch.accepted,
                output_gate_required: false,
                error: error.to_string(),
            };
        }
    };
    let command_result = match memory_command_result_for_batch(&state, &batch) {
        Ok(result) => result,
        Err(error) => {
            return MemoryStateApplyBatchResult {
                ok: false,
                applied: false,
                read_only: false,
                mutation_count,
                effect_count,
                accepted: batch.accepted,
                output_gate_required: false,
                error: error.to_string(),
            };
        }
    };
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let apply_result = store
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
        .await;
    match apply_result {
        Ok(_) => {
            close_memory_command_for_committed_batch(&state, &batch);
            store.record_profile(
                MemoryProfileMetricKind::OpApplyBatch,
                started.elapsed().as_micros() as u64,
                mutation_count as u64 + 1,
            );
            MemoryStateApplyBatchResult {
                ok: true,
                applied: true,
                read_only: false,
                mutation_count,
                effect_count,
                accepted: batch.accepted,
                output_gate_required: batch.accepted || effect_count > 0,
                error: String::new(),
            }
        }
        Err(error) => MemoryStateApplyBatchResult {
            ok: false,
            applied: false,
            read_only: false,
            mutation_count,
            effect_count,
            accepted: batch.accepted,
            output_gate_required: false,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_command_begin(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    memory_scope_handle: u32,
    #[string] binding: String,
    #[string] key: String,
    #[string] idempotency_key: String,
) -> MemoryCommandBeginResult {
    if request_context_handle == 0 {
        return MemoryCommandBeginResult {
            ok: false,
            handle: 0,
            hit: false,
            value_handle: 0,
            revision: -1,
            error: "memory command begin requires request_context_handle".to_string(),
        };
    }
    let idempotency_key = idempotency_key.trim().to_string();
    if idempotency_key.is_empty() {
        return MemoryCommandBeginResult {
            ok: true,
            handle: 0,
            hit: false,
            value_handle: 0,
            revision: -1,
            error: String::new(),
        };
    }
    if idempotency_key.len() > 512 {
        return MemoryCommandBeginResult {
            ok: false,
            handle: 0,
            hit: false,
            value_handle: 0,
            revision: -1,
            error: "memory command idempotency key must be at most 512 characters".to_string(),
        };
    }

    let (namespace, memory_key) =
        match memory_scope_for_payload(&state, memory_scope_handle, &binding, &key) {
            Ok(scope) => scope,
            Err(error) => {
                return MemoryCommandBeginResult {
                    ok: false,
                    handle: 0,
                    hit: false,
                    value_handle: 0,
                    revision: -1,
                    error: error.to_string(),
                };
            }
        };
    let store = state.borrow().borrow::<MemoryStore>().clone();
    match store
        .command_result(&namespace, &memory_key, &idempotency_key)
        .await
    {
        Ok(Some(result)) => {
            let value_handle = match memory_output_bytes_insert(
                &mut state.borrow_mut(),
                request_context_handle,
                result.result.into(),
            ) {
                Ok(handle) => handle,
                Err(error) => {
                    return MemoryCommandBeginResult {
                        ok: false,
                        handle: 0,
                        hit: false,
                        value_handle: 0,
                        revision: -1,
                        error: error.to_string(),
                    };
                }
            };
            MemoryCommandBeginResult {
                ok: true,
                handle: 0,
                hit: true,
                value_handle,
                revision: result.revision,
                error: String::new(),
            }
        }
        Ok(None) => {
            let handle = match state
                .borrow_mut()
                .borrow_mut::<MemoryCommandHandles>()
                .insert(
                    MemoryCommandHandle {
                        request_context_handle,
                        namespace,
                        memory_key,
                        idempotency_key,
                    },
                    MEMORY_MAX_COMMAND_HANDLES_PER_REQUEST,
                ) {
                Ok(handle) => handle,
                Err(error) => {
                    return MemoryCommandBeginResult {
                        ok: false,
                        handle: 0,
                        hit: false,
                        value_handle: 0,
                        revision: -1,
                        error: error.to_string(),
                    };
                }
            };
            MemoryCommandBeginResult {
                ok: true,
                handle,
                hit: false,
                value_handle: 0,
                revision: -1,
                error: String::new(),
            }
        }
        Err(error) => MemoryCommandBeginResult {
            ok: false,
            handle: 0,
            hit: false,
            value_handle: 0,
            revision: -1,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_command_close(state: &mut OpState, handle: u32) {
    if handle != 0 {
        let _ = state.borrow_mut::<MemoryCommandHandles>().remove(handle);
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_socket_send(
    state: Rc<RefCell<OpState>>,
    memory_scope_handle: u32,
    #[string] handle: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] message_kind: String,
    #[buffer] message: JsBuffer,
) -> MemorySocketSendResult {
    let message = message.as_ref().to_vec();
    if handle.trim().is_empty() {
        return MemorySocketSendResult {
            ok: false,
            error: "memory socket send requires handle".to_string(),
        };
    }
    let normalized_kind = message_kind.as_str();
    let is_text = match normalized_kind {
        "text" => true,
        "binary" => false,
        _ => {
            return MemorySocketSendResult {
                ok: false,
                error: format!("unsupported message kind: {normalized_kind}"),
            };
        }
    };

    let (binding, key) = match memory_scope_for_payload(&state, memory_scope_handle, &binding, &key)
    {
        Ok(value) => value,
        Err(error) => {
            return MemorySocketSendResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::MemorySocketSend(MemorySocketSendEvent {
            reply: reply_tx,
            handle,
            binding,
            key,
            is_text,
            message,
        }),
    )
    .is_err()
    {
        return MemorySocketSendResult {
            ok: false,
            error: "memory socket send runtime is unavailable".to_string(),
        };
    }

    drop(reply_rx);
    MemorySocketSendResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_memory_socket_close(
    state: Rc<RefCell<OpState>>,
    memory_scope_handle: u32,
    #[string] handle: String,
    #[string] binding: String,
    #[string] key: String,
    code: u16,
    #[string] reason: String,
) -> MemorySocketCloseResult {
    if handle.trim().is_empty() {
        return MemorySocketCloseResult {
            ok: false,
            error: "memory socket close requires handle".to_string(),
        };
    }

    let (binding, key) = match memory_scope_for_payload(&state, memory_scope_handle, &binding, &key)
    {
        Ok(value) => value,
        Err(error) => {
            return MemorySocketCloseResult {
                ok: false,
                error: error.to_string(),
            };
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::MemorySocketClose(MemorySocketCloseEvent {
            reply: reply_tx,
            handle,
            binding,
            key,
            code,
            reason,
        }),
    )
    .is_err()
    {
        return MemorySocketCloseResult {
            ok: false,
            error: "memory socket close runtime is unavailable".to_string(),
        };
    }

    drop(reply_rx);
    MemorySocketCloseResult {
        ok: true,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_memory_socket_list(
    state: Rc<RefCell<OpState>>,
    memory_scope_handle: u32,
    #[string] binding: String,
    #[string] key: String,
) -> MemorySocketListResult {
    let (binding, key) = match memory_scope_for_payload(&state, memory_scope_handle, &binding, &key)
    {
        Ok(value) => value,
        Err(error) => {
            return MemorySocketListResult {
                ok: false,
                handles: Vec::new(),
                error: error.to_string(),
            };
        }
    };

    let registry = state.borrow().borrow::<MemoryOpenHandleRegistry>().clone();
    MemorySocketListResult {
        ok: true,
        handles: registry.list_socket_handles(&binding, &key),
        error: String::new(),
    }
}

#[cfg(test)]
mod tests {
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
        let error = stage_memory_batch_mutation(
            &mut batch,
            mutation("huge", MEMORY_BATCH_MAX_STAGED_BYTES),
        )
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
        stage_memory_batch_mutation(&mut batch, mutation("count", 1))
            .expect("mutation should stage");

        let error =
            memory_batch_commit_owner_epoch(&batch).expect_err("unowned commit must reject");

        assert_eq!(
            error.to_string(),
            "memory batch commit requires memory owner epoch"
        );
    }

    #[test]
    fn memory_batch_commit_owner_epoch_accepts_owned_committing_batch() {
        let mut batch = empty_batch(7);
        stage_memory_batch_mutation(&mut batch, mutation("count", 1))
            .expect("mutation should stage");

        assert_eq!(memory_batch_commit_owner_epoch(&batch).expect("owner"), 7);
    }

    #[test]
    fn memory_byte_handles_account_owner_bytes_and_clear() {
        let mut handles = MemoryByteHandles::default();
        let first = handles
            .insert(
                7,
                Bytes::from_static(&[1, 2, 3]),
                MEMORY_BATCH_MAX_STAGED_BYTES,
            )
            .expect("first handle should insert");
        let second = handles
            .insert(
                7,
                Bytes::from_static(&[4, 5]),
                MEMORY_BATCH_MAX_STAGED_BYTES,
            )
            .expect("second handle should insert");
        assert_eq!(handles.owner_bytes(7), 5);

        let mismatch = handles
            .take(8, first)
            .expect_err("wrong owner must not consume handle");
        assert!(mismatch.to_string().contains("owner mismatch"));
        assert_eq!(handles.owner_bytes(7), 5);

        assert_eq!(
            handles.take(7, first).expect("first handle"),
            Bytes::from_static(&[1, 2, 3])
        );
        assert_eq!(handles.owner_bytes(7), 2);
        handles.clear_owner(7);
        assert_eq!(handles.owner_bytes(7), 0);
        assert!(handles.take(7, second).is_err());
    }

    #[test]
    fn memory_byte_handles_reject_owner_byte_overflow() {
        let mut handles = MemoryByteHandles::default();
        handles
            .insert(7, Bytes::copy_from_slice(&[1; 8]), 8)
            .expect("initial bytes should fit");
        let error = handles
            .insert(7, Bytes::from_static(&[2]), 8)
            .expect_err("owner byte budget should reject overflow");
        assert!(
            error
                .to_string()
                .contains("memory byte handles exceeded 8 bytes")
        );
        assert_eq!(handles.owner_bytes(7), 8);

        handles
            .insert(8, Bytes::from_static(&[3]), 8)
            .expect("other owner has independent budget");
        assert_eq!(handles.owner_bytes(8), 1);
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
        assert_eq!(handles.owner_count(7), 2);

        handles.remove(first).expect("first handle should remove");
        assert_eq!(handles.owner_count(7), 1);
        handles
            .insert(command_handle(7, 3), 2)
            .expect("slot should be reusable after remove");
        handles
            .insert(command_handle(8, 0), 2)
            .expect("other owner should have independent quota");
        handles.clear_owner(7);
        assert_eq!(handles.owner_count(7), 0);
        assert_eq!(handles.owner_count(8), 1);
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
        assert_eq!(handles.owner_count(1), 0);
        assert_eq!(handles.owner_count(2), 1);
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
        assert_eq!(handles.owner_count(1), 2);

        handles.remove(first).expect("first batch should remove");
        assert_eq!(handles.owner_count(1), 1);
        handles
            .insert(empty_batch(7), 2)
            .expect("slot should be reusable after remove");

        let mut other = empty_batch(7);
        other.request_context_handle = 2;
        handles
            .insert(other, 2)
            .expect("other owner should have independent quota");
        handles.clear_owner(1);
        assert_eq!(handles.owner_count(1), 0);
        assert_eq!(handles.owner_count(2), 1);
    }
}
