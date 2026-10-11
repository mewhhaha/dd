use super::*;
use dd_v8::ToJsBuffer;

pub(super) fn op_request_invocation_take(
    state: &mut OpState,
    request_handle: u32,
) -> Option<WorkerRequestPayload> {
    state
        .borrow_mut::<RequestInvocationHandles>()
        .remove(request_handle)
}

pub(super) fn op_worker_deployment_take(
    state: &mut OpState,
    deployment_handle: u32,
) -> Option<WorkerDeploymentPayload> {
    state
        .borrow_mut::<WorkerDeploymentHandles>()
        .remove(deployment_handle)
}

/// The next chunk of the request body, or `null` at its end.
pub(super) async fn op_request_body_read(
    state: Rc<RefCell<OpState>>,
    stream_handle: u32,
) -> std::result::Result<Option<ToJsBuffer>, OpError> {
    let stream = {
        let state_ref = state.borrow();
        state_ref
            .borrow::<RequestBodyStreams>()
            .get(stream_handle)
            .cloned()
    };
    let Some(stream) = stream else {
        return Ok(None);
    };

    if stream.is_canceled() {
        return Err(OpError::new("request body stream canceled"));
    }

    let canceled = stream.canceled_notify.notified();
    tokio::pin!(canceled);
    let mut receiver = stream.receiver.lock().await;
    tokio::select! {
        chunk = receiver.recv() => {
            match chunk {
                Some(Ok(bytes)) => {
                    let previous = stream.bytes_read.fetch_add(bytes.len(), Ordering::SeqCst);
                    if previous.saturating_add(bytes.len()) > stream.max_bytes {
                        clear_request_body_stream_entry(&state, stream_handle);
                        return Err(OpError::new(format!(
                            "request body exceeded max_request_body_bytes ({} bytes)",
                            stream.max_bytes
                        )));
                    }
                    Ok(Some(bytes.to_vec().into()))
                }
                Some(Err(error)) => {
                    clear_request_body_stream_entry(&state, stream_handle);
                    Err(OpError::new(error))
                }
                None => {
                    clear_request_body_stream_entry(&state, stream_handle);
                    Ok(None)
                }
            }
        }
        _ = &mut canceled => {
            clear_request_body_stream_entry(&state, stream_handle);
            Err(OpError::new("request body stream canceled"))
        }
    }
}

pub(super) fn op_request_wait_until_register(state: &mut OpState, completion_handle: u32) -> u32 {
    state
        .borrow_mut::<ActiveRequestContextHandles>()
        .increment_wait_until(completion_handle)
        .unwrap_or_default()
        .min(u32::MAX as usize) as u32
}

pub(super) fn op_request_body_close(state: &mut OpState, stream_handle: u32) {
    clear_request_body_stream(state, stream_handle);
}

pub(super) fn op_request_context_close(state: &mut OpState, request_context_handle: u32) {
    clear_request_secret_context(state, request_context_handle);
}

pub(super) fn op_request_context_cancel(state: &mut OpState, request_context_handle: u32) {
    cancel_request_secret_context(state, request_context_handle);
}

pub(super) fn op_memory_request_scope_close(state: &mut OpState, memory_scope_handle: u32) {
    clear_memory_request_scope(state, memory_scope_handle);
}

fn clear_request_body_stream_entry(state: &Rc<RefCell<OpState>>, stream_handle: u32) {
    if let Some(stream) = state
        .borrow_mut()
        .borrow_mut::<RequestBodyStreams>()
        .remove(stream_handle)
    {
        stream.cancel();
    }
}
