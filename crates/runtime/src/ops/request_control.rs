use super::*;

static SERVICE_FETCH_REQUEST_SEQ: AtomicU64 = AtomicU64::new(1);

fn request_owner_for_scope(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
    empty_request_context: &'static str,
    unavailable_scope: &'static str,
    validate: impl FnOnce(&RequestSecretContext) -> Result<()>,
) -> Result<(String, u64, u64)> {
    if request_context_handle == 0 {
        return Err(PlatformError::bad_request(empty_request_context));
    }
    let owner = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .get(request_context_handle)
            .ok_or_else(|| PlatformError::runtime(unavailable_scope))?;
        validate(context)?;
        (
            context.execution.worker_name.as_ref().to_string(),
            context.execution.generation,
            context.isolate_id,
        )
    };
    Ok(owner)
}

fn reply_start_ok(reply_id: String) -> PendingReplyStartResult {
    PendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

fn reply_start_error(error: impl ToString) -> PendingReplyStartResult {
    PendingReplyStartResult {
        ok: false,
        reply_id: String::new(),
        error: error.to_string(),
    }
}

pub(super) fn service_binding_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
    binding: &str,
) -> Result<(String, u64, u64, String)> {
    let binding = binding.trim();
    if binding.is_empty() {
        return Err(PlatformError::bad_request(
            "service binding must not be empty",
        ));
    }
    if request_context_handle == 0 {
        return Err(PlatformError::bad_request(
            "service binding request context handle must not be empty",
        ));
    }
    let op_state = state.borrow();
    let contexts = op_state.borrow::<RequestSecretContexts>();
    let context = contexts
        .get(request_context_handle)
        .ok_or_else(|| PlatformError::runtime("service binding request scope is unavailable"))?;
    let target_worker = context
        .execution
        .service_bindings
        .get(binding)
        .cloned()
        .ok_or_else(|| {
            PlatformError::runtime(format!("service binding is not allowed: {binding}"))
        })?;
    Ok((
        context.execution.worker_name.as_ref().to_string(),
        context.execution.generation,
        context.isolate_id,
        target_worker,
    ))
}

pub(super) fn request_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
) -> Result<(String, u64, u64)> {
    request_owner_for_scope(
        state,
        request_context_handle,
        "request context handle must not be empty",
        "request scope is unavailable",
        |_| Ok(()),
    )
}

#[deno_core::op2(fast)]
pub(super) fn op_request_reply_cancel(state: &mut OpState, #[string] reply_id: String) {
    let pending = state.borrow::<PendingReplies>().clone();
    pending.cancel(reply_id.trim());
}

#[deno_core::op2]
#[serde]
pub(super) fn op_request_control_take(state: &mut OpState) -> Vec<RequestControlItem> {
    let inbox = state.borrow::<RequestControlInbox>().clone();
    let mut batch = inbox.take_batch();
    for item in &mut batch {
        match item {
            RequestControlItem::Reply(PushedReplyPayload::Fetch(reply)) => {
                if let Some(output) = reply.pending_output.take() {
                    reply.headers_handle = state
                        .borrow_mut::<HttpPreparedHeaders>()
                        .insert(output.headers);
                    reply.body_handle =
                        state.borrow_mut::<HttpPreparedBodies>().insert(output.body);
                }
            }
            RequestControlItem::Reply(PushedReplyPayload::TestAsync(_)) => {}
        }
    }
    batch
}

#[deno_core::op2]
#[serde]
pub(super) fn op_test_async_reply_start(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    delay_ms: f64,
    ok: bool,
    #[string] value: String,
    #[string] error: String,
) -> PendingReplyStartResult {
    let delay_ms = if delay_ms.is_finite() && delay_ms > 0.0 {
        delay_ms.trunc() as u64
    } else {
        0
    };
    let (worker_name, generation, isolate_id) =
        match request_owner_for_request(&state, request_context_handle) {
            Ok(value) => value,
            Err(error) => {
                return reply_start_error(error);
            }
        };
    let replies = state.borrow().borrow::<TestAsyncReplies>().clone();
    let reply_id = replies.allocate(PendingReplyOwner {
        worker_name,
        generation,
        isolate_id,
    });
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::TestAsyncReply(TestAsyncReplyEvent {
            reply_id: reply_id.clone(),
            replies: replies.clone(),
            delay_ms,
            ok,
            value,
            error,
        }),
    )
    .is_err()
    {
        replies.cancel(&reply_id);
        return reply_start_error("test async reply runtime is unavailable");
    }
    reply_start_ok(reply_id)
}

#[deno_core::op2(fast)]
pub(super) fn op_test_async_reply_cancel(state: &mut OpState, #[string] reply_id: String) {
    let replies = state.borrow::<TestAsyncReplies>().clone();
    replies.cancel(reply_id.trim());
}

#[deno_core::op2]
#[serde]
pub(super) fn op_service_binding_fetch_start(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] method: String,
    #[string] url: String,
    headers_handle: u32,
    body_handle: u32,
) -> PendingReplyStartResult {
    let (headers, body) = {
        let mut op_state = state.borrow_mut();
        let headers = op_state
            .borrow_mut::<HttpPreparedHeaders>()
            .take(headers_handle)
            .unwrap_or_default();
        let body = op_state
            .borrow_mut::<HttpPreparedBodies>()
            .take(body_handle)
            .unwrap_or_default();
        (headers, body)
    };
    let caller_request_id = {
        let op_state = state.borrow();
        match op_state
            .borrow::<ActiveRequestContextHandles>()
            .get_handle(request_context_handle)
        {
            Some(context) => context.request_id.clone(),
            None => {
                return reply_start_error("service binding fetch request context is unavailable");
            }
        }
    };
    let (owner_worker, owner_generation, _owner_isolate_id, target_worker) =
        match service_binding_owner_for_request(&state, request_context_handle, &binding) {
            Ok(value) => value,
            Err(error) => {
                return reply_start_error(error);
            }
        };
    let subrequest_id = format!(
        "{}:service:{}",
        caller_request_id,
        SERVICE_FETCH_REQUEST_SEQ.fetch_add(1, Ordering::Relaxed)
    );

    let request = WorkerInvocation {
        method,
        url,
        headers,
        body: body.to_vec(),
        request_id: subrequest_id,
    };

    let pending_replies = state.borrow().borrow::<PendingReplies>().clone();
    let reply_id = pending_replies.allocate();
    let command_sender = state
        .borrow()
        .borrow::<crate::service::RuntimeFastCommandSender>()
        .clone();
    if command_sender
        .0
        .try_send(crate::service::RuntimeCommand::ServiceBindingFetchStart {
            reply_inbox: state.borrow().borrow::<RequestControlInbox>().clone(),
            owner_worker,
            owner_generation,
            binding,
            target_worker,
            request,
            reply_id: reply_id.clone(),
            pending_replies: pending_replies.clone(),
        })
        .is_err()
    {
        pending_replies.cancel(&reply_id);
        return reply_start_error("service binding runtime is unavailable");
    }
    reply_start_ok(reply_id)
}
