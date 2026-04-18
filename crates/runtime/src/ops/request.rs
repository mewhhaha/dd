use super::*;

#[deno_core::op2]
#[serde]
pub(super) async fn op_request_body_read(
    state: Rc<RefCell<OpState>>,
    #[string] request_id: String,
) -> RequestBodyReadResult {
    let stream = {
        let state_ref = state.borrow();
        state_ref
            .borrow::<RequestBodyStreams>()
            .streams
            .get(&request_id)
            .cloned()
    };
    let Some(stream) = stream else {
        return RequestBodyReadResult {
            ok: true,
            done: true,
            chunk: Vec::new(),
            error: String::new(),
        };
    };

    if stream.is_canceled() {
        return RequestBodyReadResult {
            ok: false,
            done: true,
            chunk: Vec::new(),
            error: "request body stream canceled".to_string(),
        };
    }

    let canceled = stream.canceled_notify.notified();
    tokio::pin!(canceled);
    let mut receiver = stream.receiver.lock().await;
    tokio::select! {
        chunk = receiver.recv() => {
            match chunk {
                Some(Ok(bytes)) => RequestBodyReadResult {
                    ok: true,
                    done: false,
                    chunk: bytes,
                    error: String::new(),
                },
                Some(Err(error)) => {
                    clear_request_body_stream_entry(&state, &request_id);
                    RequestBodyReadResult {
                        ok: false,
                        done: true,
                        chunk: Vec::new(),
                        error,
                    }
                }
                None => {
                    clear_request_body_stream_entry(&state, &request_id);
                    RequestBodyReadResult {
                        ok: true,
                        done: true,
                        chunk: Vec::new(),
                        error: String::new(),
                    }
                }
            }
        }
        _ = &mut canceled => {
            clear_request_body_stream_entry(&state, &request_id);
            RequestBodyReadResult {
                ok: false,
                done: true,
                chunk: Vec::new(),
                error: "request body stream canceled".to_string(),
            }
        }
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_request_body_cancel(state: &mut OpState, #[string] request_id: String) {
    cancel_request_body_stream(state, &request_id);
}

pub(super) fn clear_request_body_stream_entry(state: &Rc<RefCell<OpState>>, request_id: &str) {
    if let Some(stream) = state
        .borrow_mut()
        .borrow_mut::<RequestBodyStreams>()
        .streams
        .remove(request_id)
    {
        stream.cancel();
    }
}

pub(super) fn completion_meta(payload: &str) -> Option<CompletionMeta> {
    let mut bytes = payload.as_bytes().to_vec();
    simd_json::serde::from_slice::<CompletionMeta>(&mut bytes).ok()
}

pub(super) fn wait_until_request_id(payload: &str) -> Option<String> {
    let mut bytes = payload.as_bytes().to_vec();
    simd_json::serde::from_slice::<WaitUntilRequestId>(&mut bytes)
        .ok()
        .map(|value| value.request_id)
}
