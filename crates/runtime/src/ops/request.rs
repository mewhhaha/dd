use super::*;

#[deno_core::op2]
#[serde]
pub(super) fn op_request_invocation_descriptor(
    state: &mut OpState,
    request_handle: u32,
) -> Option<WorkerRequestDescriptorPayload> {
    state
        .borrow_mut::<RequestInvocationHandles>()
        .take_descriptor(request_handle)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_take_worker_deployment_config(
    state: &mut OpState,
    deployment_handle: u32,
) -> Option<WorkerDeploymentPayload> {
    state
        .borrow_mut::<WorkerDeploymentHandles>()
        .remove(deployment_handle)
}

#[deno_core::op2]
#[serde]
pub(super) async fn op_request_body_read(
    state: Rc<RefCell<OpState>>,
    stream_handle: u32,
) -> RequestBodyReadResult {
    let stream = {
        let state_ref = state.borrow();
        state_ref.borrow::<RequestBodyStreams>().get(stream_handle)
    };
    let Some(stream) = stream else {
        return RequestBodyReadResult {
            ok: true,
            done: true,
            body_handle: 0,
            error: String::new(),
        };
    };

    if stream.is_canceled() {
        return RequestBodyReadResult {
            ok: false,
            done: true,
            body_handle: 0,
            error: "request body stream canceled".to_string(),
        };
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
                        RequestBodyReadResult {
                            ok: false,
                            done: true,
                            body_handle: 0,
                            error: format!(
                                "request body exceeded max_request_body_bytes ({} bytes)",
                                stream.max_bytes
                            ),
                        }
                    } else {
                        let body_handle = state
                            .borrow_mut()
                            .borrow_mut::<HttpPreparedBodies>()
                            .insert(bytes);
                        RequestBodyReadResult {
                            ok: true,
                            done: false,
                            body_handle,
                            error: String::new(),
                        }
                    }
                }
                Some(Err(error)) => {
                    clear_request_body_stream_entry(&state, stream_handle);
                    RequestBodyReadResult {
                        ok: false,
                        done: true,
                        body_handle: 0,
                        error,
                    }
                }
                None => {
                    clear_request_body_stream_entry(&state, stream_handle);
                    RequestBodyReadResult {
                        ok: true,
                        done: true,
                        body_handle: 0,
                        error: String::new(),
                    }
                }
            }
        }
        _ = &mut canceled => {
            clear_request_body_stream_entry(&state, stream_handle);
            RequestBodyReadResult {
                ok: false,
                done: true,
                body_handle: 0,
                error: "request body stream canceled".to_string(),
            }
        }
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_request_wait_until_register(state: &mut OpState, completion_handle: u32) -> u32 {
    state
        .borrow_mut::<ActiveRequestContextHandles>()
        .increment_wait_until(completion_handle)
        .unwrap_or_default()
        .min(u32::MAX as usize) as u32
}

#[deno_core::op2(fast)]
pub(super) fn op_request_body_cancel(state: &mut OpState, stream_handle: u32) {
    clear_request_body_stream(state, stream_handle);
}

#[deno_core::op2(fast)]
pub(super) fn op_request_context_close(state: &mut OpState, request_context_handle: u32) {
    clear_request_secret_context(state, request_context_handle);
}

#[deno_core::op2(fast)]
pub(super) fn op_request_context_cancel(state: &mut OpState, request_context_handle: u32) {
    cancel_request_secret_context(state, request_context_handle);
}

#[deno_core::op2(fast)]
pub(super) fn op_memory_request_scope_close(state: &mut OpState, memory_scope_handle: u32) {
    clear_memory_request_scope(state, memory_scope_handle);
}

pub(super) fn clear_request_body_stream_entry(state: &Rc<RefCell<OpState>>, stream_handle: u32) {
    if let Some(stream) = state
        .borrow_mut()
        .borrow_mut::<RequestBodyStreams>()
        .remove(stream_handle)
    {
        stream.cancel();
    }
}
