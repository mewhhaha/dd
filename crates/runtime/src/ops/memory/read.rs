//! Memory read handles and operations.

use super::*;

/// Opens a read handle on the entry's committed snapshot.
pub(in crate::ops) async fn op_memory_read_begin(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    binding: String,
    key: String,
) -> std::result::Result<u32, OpError> {
    let mut storage_failure = false;
    let result: Result<u32> = async {
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
        snapshot.validate_limits()?;
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
    result.map_err(|error| {
        if storage_failure {
            OpError::custom(MEMORY_STORAGE_FAILURE, error.to_string())
        } else {
            js_error(error)
        }
    })
}

pub(in crate::ops) fn op_memory_read_close(state: &mut OpState, read_handle: u32) {
    state.borrow_mut::<MemoryReadHandles>().remove(read_handle);
}

pub(super) fn memory_snapshot_record(
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

pub(super) fn memory_read_handle(
    state: &OpState,
    read_handle: u32,
) -> std::result::Result<&MemoryReadHandle, OpError> {
    state
        .borrow::<MemoryReadHandles>()
        .get(read_handle)
        .ok_or_else(|| OpError::new("memory read handle is invalid"))
}

/// The value the snapshot holds for `key`, or `null` when it holds none.
pub(in crate::ops) fn op_memory_read_get(
    state: &mut OpState,
    read_handle: u32,
    key: String,
) -> std::result::Result<Option<MemoryReadValue>, OpError> {
    let read = memory_read_handle(state, read_handle)?;
    Ok(memory_snapshot_record(&read.snapshot, &key))
}

pub(in crate::ops) fn op_memory_read_list(
    state: &mut OpState,
    read_handle: u32,
    prefix: String,
) -> std::result::Result<Vec<String>, OpError> {
    let read = memory_read_handle(state, read_handle)?;
    Ok(read
        .snapshot
        .entries
        .iter()
        .filter(|entry| !entry.deleted && entry.key.starts_with(&prefix))
        .map(|entry| entry.key.clone())
        .collect())
}
