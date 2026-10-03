use super::*;

#[derive(Debug, Serialize)]
pub(crate) struct ResponseChunkEmitResult {
    ok: bool,
    error: String,
}

#[deno_core::op2(fast)]
pub(crate) fn op_emit_completion_ok(
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

#[deno_core::op2(fast)]
pub(crate) fn op_emit_completion_error(
    state: &mut OpState,
    completion_handle: u32,
    #[string] error: String,
) {
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
        clear_memory_command_handles(state, request_context_handle);
        clear_memory_byte_handles(state, request_context_handle);
        clear_memory_batch_handles(state, request_context_handle);
        clear_memory_read_handles(state, request_context_handle);
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

#[deno_core::op2(fast)]
pub(crate) fn op_emit_wait_until_done(state: &mut OpState, completion_handle: u32) -> bool {
    let Some(context) = state
        .borrow_mut::<ActiveRequestContextHandles>()
        .mark_wait_until_done(completion_handle)
    else {
        return false;
    };
    let request_context_handle = context.request_context_handle;
    if request_context_handle > 0 {
        clear_memory_command_handles(state, request_context_handle);
        clear_memory_byte_handles(state, request_context_handle);
        clear_memory_batch_handles(state, request_context_handle);
        clear_memory_read_handles(state, request_context_handle);
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

#[deno_core::op2(fast)]
pub(crate) fn op_emit_response_start(
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

#[deno_core::op2]
#[serde]
pub(crate) async fn op_emit_response_chunk(
    state: Rc<RefCell<OpState>>,
    completion_handle: u32,
    #[buffer] chunk: JsBuffer,
) -> ResponseChunkEmitResult {
    let result = async {
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
    match result {
        Ok(()) => ResponseChunkEmitResult {
            ok: true,
            error: String::new(),
        },
        Err(error) => ResponseChunkEmitResult {
            ok: false,
            error: error.to_string(),
        },
    }
}
