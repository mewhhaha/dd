//! Memory scopes, command handles and socket operations.

#[path = "memory/batch.rs"]
mod batch;
#[path = "memory/read.rs"]
mod read;

use super::*;
pub(in crate::ops) use batch::*;
pub(in crate::ops) use read::*;

/// The class of error memory ops throw when storage itself failed, rather
/// than the request or what it asked for. The execute-worker bundle builds
/// these as plain `Error`s it can tell apart, and fails the whole memory
/// entry on one (failMemoryEntry), not just the call.
const MEMORY_STORAGE_FAILURE: &str = "MemoryStorageFailure";

/// Acquires the entry's transaction lease, and returns the memory scope
/// handle that holds it.
pub(super) async fn op_memory_lease_acquire(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    binding: String,
    key: String,
) -> std::result::Result<u32, OpError> {
    let result: Result<u32> = async {
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
    result.map_err(js_error)
}

struct ResolvedMemoryScope {
    lease: Option<Arc<crate::memory::MemoryLease>>,
    namespace: String,
    memory_key: String,
    owner_epoch: i64,
}

/// Drops every memory handle a request holds.
pub(crate) fn clear_memory_handles(state: &mut OpState, request_context_handle: u32) {
    state
        .borrow_mut::<MemoryCommandHandles>()
        .clear_owner(request_context_handle);
    state
        .borrow_mut::<MemoryByteHandles>()
        .clear_owner(request_context_handle);
    state
        .borrow_mut::<MemoryBatchHandles>()
        .clear_owner(request_context_handle);
    state
        .borrow_mut::<MemoryReadHandles>()
        .clear_owner(request_context_handle);
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
    state.borrow_mut::<MemoryByteHandles>().insert(
        MemoryByteHandle {
            request_context_handle,
            value,
        },
        MEMORY_BATCH_MAX_STAGED_BYTES,
    )
}

pub(super) fn op_memory_bytes_take(
    state: &mut OpState,
    request_context_handle: u32,
    handle: u32,
) -> dd_v8::ToJsBuffer {
    if request_context_handle == 0 || handle == 0 {
        return Vec::new().into();
    }
    memory_bytes_for_handle_state(state, request_context_handle, handle)
        .map(|value| value.to_vec())
        .unwrap_or_default()
        .into()
}

fn memory_scope_for_payload(
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

pub(super) fn op_memory_profile_record(
    state: &mut OpState,
    metric: String,
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

pub(super) fn op_memory_profile_take(state: &mut OpState) -> crate::memory::MemoryProfileSnapshot {
    let store = state.borrow::<MemoryStore>().clone();
    store.take_profile_snapshot_and_reset()
}

pub(super) fn op_memory_profile_reset(state: &mut OpState) {
    let store = state.borrow::<MemoryStore>().clone();
    store.reset_profile();
}

/// Starts the idempotent command `idempotency_key` names: its stored result
/// when it already committed, or a command handle for the batch that will
/// commit it.
pub(super) async fn op_memory_command_begin(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    memory_scope_handle: u32,
    binding: String,
    key: String,
    idempotency_key: String,
) -> std::result::Result<MemoryCommandBegin, OpError> {
    if request_context_handle == 0 {
        return Err(OpError::new(
            "memory command begin requires request_context_handle",
        ));
    }
    let idempotency_key = idempotency_key.trim().to_string();
    if idempotency_key.is_empty() {
        return Ok(MemoryCommandBegin {
            handle: 0,
            hit: false,
            value_handle: 0,
        });
    }
    if idempotency_key.len() > 512 {
        return Err(OpError::new(
            "memory command idempotency key must be at most 512 characters",
        ));
    }

    let (namespace, memory_key) =
        memory_scope_for_payload(&state, memory_scope_handle, &binding, &key).map_err(js_error)?;
    let store = state.borrow().borrow::<MemoryStore>().clone();
    let stored = store
        .command_result(&namespace, &memory_key, &idempotency_key)
        .await
        .map_err(js_error)?;
    if let Some(stored) = stored {
        let value_handle = memory_output_bytes_insert(
            &mut state.borrow_mut(),
            request_context_handle,
            stored.result.into(),
        )
        .map_err(js_error)?;
        return Ok(MemoryCommandBegin {
            handle: 0,
            hit: true,
            value_handle,
        });
    }
    let handle = state
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
        )
        .map_err(js_error)?;
    Ok(MemoryCommandBegin {
        handle,
        hit: false,
        value_handle: 0,
    })
}

pub(super) fn op_memory_command_close(state: &mut OpState, handle: u32) {
    if handle != 0 {
        let _ = state.borrow_mut::<MemoryCommandHandles>().remove(handle);
    }
}

pub(super) async fn op_memory_socket_close(
    state: Rc<RefCell<OpState>>,
    memory_scope_handle: u32,
    handle: String,
    binding: String,
    key: String,
    code: u16,
    reason: String,
) -> std::result::Result<(), OpError> {
    if handle.trim().is_empty() {
        return Err(OpError::new("memory socket close requires handle"));
    }

    let (binding, key) =
        memory_scope_for_payload(&state, memory_scope_handle, &binding, &key).map_err(js_error)?;

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
        return Err(OpError::new("memory socket close runtime is unavailable"));
    }

    drop(reply_rx);
    Ok(())
}

/// The handles of the entry's open sockets.
pub(super) fn op_memory_socket_list(
    state: Rc<RefCell<OpState>>,
    memory_scope_handle: u32,
    binding: String,
    key: String,
) -> std::result::Result<Vec<String>, OpError> {
    let (binding, key) =
        memory_scope_for_payload(&state, memory_scope_handle, &binding, &key).map_err(js_error)?;
    let registry = state.borrow().borrow::<MemoryOpenHandleRegistry>().clone();
    Ok(registry.list_socket_handles(&binding, &key))
}

#[cfg(test)]
#[path = "memory/tests.rs"]
mod tests;
