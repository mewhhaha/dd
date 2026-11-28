use super::*;

pub(crate) fn op_response_complete(
    state: &mut OpState,
    completion_handle: u32,
    status: u16,
    headers_handle: u32,
    body_handle: u32,
) {
    let Some(context) = active_request_context_for_completion(state, completion_handle) else {
        return;
    };
    let headers = state
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default();
    let body = state
        .borrow_mut::<HttpPreparedBodies>()
        .take(body_handle)
        .unwrap_or_default();
    emit_completion_result(
        state,
        context.request_id,
        context.completion_token,
        context.request_context_handle,
        context.wait_until_count,
        Ok(WorkerOutput {
            status,
            headers,
            body: body.to_vec(),
        }),
    );
}

#[allow(clippy::too_many_arguments)]
fn complete_response_inline(
    state: &mut OpState,
    completion_handle: u32,
    request_context_handle: u32,
    memory_scope_handle: u32,
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    finish_wait_until: bool,
) -> (u64, f64) {
    if let Some(context) = active_request_context_for_completion(state, completion_handle) {
        emit_completion_result(
            state,
            context.request_id,
            context.completion_token,
            context.request_context_handle,
            context.wait_until_count,
            Ok(WorkerOutput {
                status,
                headers,
                body,
            }),
        );
    }
    if finish_wait_until && op_request_wait_until_finish(state, completion_handle) {
        if memory_scope_handle > 0 {
            clear_memory_request_scope(state, memory_scope_handle);
        }
        if request_context_handle > 0 {
            clear_request_secret_context(state, request_context_handle);
        }
    }
    let time = current_time_boundary();
    (time.now_ms, time.perf_ms)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn op_response_complete_inline(
    state: &mut OpState,
    completion_handle: u32,
    request_context_handle: u32,
    memory_scope_handle: u32,
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
    finish_wait_until: bool,
) -> (u64, f64) {
    complete_response_inline(
        state,
        completion_handle,
        request_context_handle,
        memory_scope_handle,
        status,
        headers,
        body.into_bytes(),
        finish_wait_until,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn op_response_complete_inline_bytes(
    state: &mut OpState,
    completion_handle: u32,
    request_context_handle: u32,
    memory_scope_handle: u32,
    status: u16,
    headers: Vec<(String, String)>,
    body: JsBuffer,
    finish_wait_until: bool,
) -> (u64, f64) {
    complete_response_inline(
        state,
        completion_handle,
        request_context_handle,
        memory_scope_handle,
        status,
        headers,
        body.into_vec(),
        finish_wait_until,
    )
}

pub(crate) fn op_response_fail(state: &mut OpState, completion_handle: u32, error: String) {
    let Some(context) = active_request_context_for_completion(state, completion_handle) else {
        return;
    };
    let message = if error.is_empty() {
        "worker execution failed".to_string()
    } else {
        error
    };
    emit_completion_result(
        state,
        context.request_id,
        context.completion_token,
        context.request_context_handle,
        context.wait_until_count,
        Err(PlatformError::runtime(message)),
    );
}

fn emit_completion_result(
    state: &mut OpState,
    request_id: String,
    completion_token: String,
    request_context_handle: u32,
    wait_until_count: usize,
    result: Result<WorkerOutput>,
) {
    if wait_until_count == 0 && request_context_handle > 0 {
        clear_memory_handles(state, request_context_handle);
    }
    let _ = emit_isolate_event(
        state,
        IsolateEventPayload::Completion {
            request_id,
            completion_token,
            wait_until_count,
            result,
        },
    );
}

pub(crate) fn op_request_wait_until_finish(state: &mut OpState, completion_handle: u32) -> bool {
    let Some(context) = state
        .borrow_mut::<ActiveRequestContextHandles>()
        .mark_wait_until_done(completion_handle)
    else {
        return false;
    };
    let request_context_handle = context.request_context_handle;
    if request_context_handle > 0 {
        clear_memory_handles(state, request_context_handle);
    }
    let _ = emit_isolate_event(
        state,
        IsolateEventPayload::WaitUntilDone {
            request_id: context.request_id,
            completion_token: context.completion_token,
        },
    );
    true
}

pub(crate) fn op_response_start(
    state: &mut OpState,
    completion_handle: u32,
    status: u16,
    headers_handle: u32,
) {
    let Some(context) = active_request_context_for_completion(state, completion_handle) else {
        return;
    };
    let headers = state
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default();
    let _ = emit_isolate_event(
        state,
        IsolateEventPayload::ResponseStart {
            request_id: context.request_id,
            completion_token: context.completion_token,
            status,
            headers,
        },
    );
}

struct BufferedResponseChunk {
    bytes: Bytes,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl AsRef<[u8]> for BufferedResponseChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

pub(crate) async fn op_response_write(
    state: Rc<RefCell<OpState>>,
    completion_handle: u32,
    chunk: JsBuffer,
) -> std::result::Result<(), OpError> {
    let result: Result<()> = async {
        let (context, limits, canceled, canceled_notify) = {
            let op_state = state.borrow();
            let context = active_request_context_for_completion(&op_state, completion_handle)
                .ok_or_else(|| PlatformError::runtime("response request context is unavailable"))?;
            let request = op_state.borrow::<RequestSecretContexts>()
                .get(context.request_context_handle)
                .ok_or_else(|| PlatformError::runtime("response request context is unavailable"))?;
            (
                context,
                op_state.borrow::<RuntimeExecutionLimits>().clone(),
                Arc::clone(&request.canceled),
                Arc::clone(&request.canceled_notify),
            )
        };
        let cancellation = canceled_notify.notified();
        tokio::pin!(cancellation);
        cancellation.as_mut().enable();
        let chunk_size = limits.max_buffered_response_bytes.min(64 * 1024);
        for bytes in chunk.as_ref().chunks(chunk_size) {
            if canceled.load(Ordering::SeqCst) {
                return Err(PlatformError::runtime("response request was canceled"));
            }
            let permit = tokio::select! {
                biased;
                _ = &mut cancellation => return Err(PlatformError::runtime("response request was canceled")),
                permit = Arc::clone(&limits.response_byte_budget).acquire_many_owned(bytes.len() as u32) => {
                    permit.map_err(|_| PlatformError::internal("response byte budget closed"))?
                }
            };
            if canceled.load(Ordering::SeqCst) {
                return Err(PlatformError::runtime("response request was canceled"));
            }
            let chunk = Bytes::from_owner(BufferedResponseChunk {
                bytes: Bytes::copy_from_slice(bytes),
                _permit: permit,
            });
            let (reply_tx, reply_rx) = oneshot::channel();
            emit_isolate_event_from_rc(
                &state,
                IsolateEventPayload::ResponseChunk {
                    request_id: context.request_id.clone(),
                    completion_token: context.completion_token.clone(),
                    chunk,
                    reply: reply_tx,
                },
            ).map_err(|_| PlatformError::internal("runtime response stream is unavailable"))?;
            tokio::select! {
                biased;
                _ = &mut cancellation => return Err(PlatformError::runtime("response request was canceled")),
                result = reply_rx => {
                    result.map_err(|_| PlatformError::internal("response stream acknowledgment channel closed"))??;
                }
            }
        }
        Ok(())
    }.await;
    result.map_err(js_error)
}
