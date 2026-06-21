use super::*;

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_profile_take(state: &mut OpState) -> DynamicProfileResult {
    let profile = state.borrow::<DynamicProfile>().clone();
    DynamicProfileResult {
        ok: true,
        snapshot: Some(profile.snapshot()),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_dynamic_profile_reset(state: &mut OpState) {
    let profile = state.borrow::<DynamicProfile>().clone();
    profile.reset();
}

pub(super) fn dynamic_worker_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
) -> Result<(String, u64, u64)> {
    let binding = binding.trim();
    if binding.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic worker binding must not be empty",
        ));
    }
    request_owner_for_scope(
        state,
        request_id,
        "dynamic worker request_id must not be empty",
        "dynamic worker request scope is unavailable",
        |context| {
            if context.execution.dynamic_bindings.contains(binding) {
                return Ok(());
            }
            Err(PlatformError::runtime(format!(
                "dynamic worker binding is not allowed: {binding}"
            )))
        },
    )
}

fn request_owner_for_scope(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    empty_request_id: &'static str,
    unavailable_scope: &'static str,
    validate: impl FnOnce(&RequestSecretContext) -> Result<()>,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request(empty_request_id));
    }
    let owner = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .contexts
            .get(request_id)
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

fn allocate_dynamic_pending_reply(
    state: &Rc<RefCell<OpState>>,
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
) -> (String, DynamicPendingReplies) {
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(PendingReplyOwner {
        worker_name: worker_name.to_string(),
        generation,
        isolate_id,
    });
    (reply_id, pending_replies)
}

fn dynamic_start_ok(reply_id: String) -> DynamicPendingReplyStartResult {
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

fn dynamic_start_error(error: impl ToString) -> DynamicPendingReplyStartResult {
    DynamicPendingReplyStartResult {
        ok: false,
        reply_id: String::new(),
        error: error.to_string(),
    }
}

pub(super) fn dynamic_host_rpc_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
) -> Result<(String, u64, u64)> {
    let binding = binding.trim();
    if binding.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic host rpc binding must not be empty",
        ));
    }
    request_owner_for_scope(
        state,
        request_id,
        "dynamic host rpc request_id must not be empty",
        "dynamic host rpc request scope is unavailable",
        |context| {
            if context.execution.dynamic_rpc_bindings.contains(binding) {
                return Ok(());
            }
            Err(PlatformError::runtime(format!(
                "dynamic host rpc binding is not allowed: {binding}"
            )))
        },
    )
}

pub(super) fn memory_invoke_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, u64, u64)> {
    request_owner_for_scope(
        state,
        request_id,
        "memory invoke caller_request_id must not be empty",
        "memory invoke request scope is unavailable",
        |_| Ok(()),
    )
}

pub(super) fn request_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, u64, u64)> {
    request_owner_for_scope(
        state,
        request_id,
        "request_id must not be empty",
        "request scope is unavailable",
        |_| Ok(()),
    )
}

#[derive(Deserialize)]
struct TestAsyncReplyStartPayload {
    request_id: String,
    #[serde(default)]
    delay_ms: u64,
    #[serde(default = "test_async_reply_start_ok_default")]
    ok: bool,
    #[serde(default)]
    value: String,
    #[serde(default)]
    error: String,
}

pub(super) const fn test_async_reply_start_ok_default() -> bool {
    true
}

#[derive(Deserialize)]
struct TestNestedTargetedInvokeStartPayload {
    request_id: String,
    #[serde(default)]
    target_mode: String,
    target_id: String,
    method_name: String,
    #[serde(default)]
    args: Vec<u8>,
}

#[deno_core::op2(fast)]
pub(super) fn op_dynamic_cancel_reply(state: &mut OpState, #[string] reply_id: String) {
    let pending = state.borrow::<DynamicPendingReplies>().clone();
    pending.cancel(reply_id.trim());
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_take_control_items(state: &mut OpState) -> Vec<DynamicControlItem> {
    let inbox = state.borrow::<DynamicControlInbox>().clone();
    let batch = inbox.take_batch();
    if !batch.is_empty() {
        let profile = state.borrow::<DynamicProfile>().clone();
        profile.record_control_drain(batch.len());
        profile.record_reply_drain(batch.len());
    }
    batch
}

#[deno_core::op2]
#[serde]
pub(super) fn op_test_async_reply_start(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: TestAsyncReplyStartPayload,
) -> DynamicPendingReplyStartResult {
    let request_id = payload.request_id.trim();
    if request_id.is_empty() {
        return dynamic_start_error("test async reply requires request_id");
    }
    let (worker_name, generation, isolate_id) = match request_owner_for_request(&state, request_id)
    {
        Ok(value) => value,
        Err(error) => {
            return dynamic_start_error(error);
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
            replies,
            delay_ms: payload.delay_ms,
            ok: payload.ok,
            value: payload.value,
            error: payload.error,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("test async reply runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2(fast)]
pub(super) fn op_test_async_reply_cancel(state: &mut OpState, #[string] reply_id: String) {
    let replies = state.borrow::<TestAsyncReplies>().clone();
    replies.cancel(reply_id.trim());
}

#[deno_core::op2]
#[serde]
pub(super) fn op_test_nested_targeted_invoke_start(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: TestNestedTargetedInvokeStartPayload,
) -> DynamicPendingReplyStartResult {
    let request_id = payload.request_id.trim();
    if request_id.is_empty() {
        return dynamic_start_error("test nested targeted invoke requires request_id");
    }
    let target_id = payload.target_id.trim();
    if target_id.is_empty() {
        return dynamic_start_error("test nested targeted invoke requires target_id");
    }
    let method_name = payload.method_name.trim();
    if method_name.is_empty() {
        return dynamic_start_error("test nested targeted invoke requires method_name");
    }
    let (worker_name, generation, isolate_id) = match request_owner_for_request(&state, request_id)
    {
        Ok(value) => value,
        Err(error) => {
            return dynamic_start_error(error);
        }
    };
    let replies = state.borrow().borrow::<TestAsyncReplies>().clone();
    let reply_id = replies.allocate(PendingReplyOwner {
        worker_name: worker_name.clone(),
        generation,
        isolate_id,
    });
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::TestNestedTargetedInvoke(TestNestedTargetedInvokeEvent {
            worker_name,
            generation,
            caller_isolate_id: isolate_id,
            target_mode: payload.target_mode,
            target_id: target_id.to_string(),
            method_name: method_name.to_string(),
            args: payload.args,
            reply_id: reply_id.clone(),
            replies,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("test nested targeted invoke runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_create(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerCreatePayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return dynamic_start_error("dynamic worker id must not be empty");
    }
    if payload.source.trim().is_empty() {
        return dynamic_start_error("dynamic worker source must not be empty");
    }
    if payload.timeout == 0 {
        return dynamic_start_error("dynamic worker timeout must be greater than 0");
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };
    let (reply_id, pending_replies) =
        allocate_dynamic_pending_reply(&state, &owner_worker, owner_generation, owner_isolate_id);
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::DynamicWorkerCreate(DynamicWorkerCreateEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id,
            binding: payload.binding,
            id: id.to_string(),
            source: payload.source,
            bindings: payload.bindings,
            env: payload.env,
            timeout: payload.timeout,
            policy: payload.policy,
            host_rpc_bindings: payload.host_rpc_bindings,
            reply_id: reply_id.clone(),
            pending_replies,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_lookup(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerLookupPayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return dynamic_start_error("dynamic worker id must not be empty");
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };
    let (reply_id, pending_replies) =
        allocate_dynamic_pending_reply(&state, &owner_worker, owner_generation, owner_isolate_id);
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::DynamicWorkerLookup(DynamicWorkerLookupEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id,
            binding: payload.binding,
            id: id.to_string(),
            reply_id: reply_id.clone(),
            pending_replies,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_list(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerListPayload,
) -> DynamicPendingReplyStartResult {
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };
    let (reply_id, pending_replies) =
        allocate_dynamic_pending_reply(&state, &owner_worker, owner_generation, owner_isolate_id);
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::DynamicWorkerList(DynamicWorkerListEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id,
            binding: payload.binding,
            reply_id: reply_id.clone(),
            pending_replies,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_delete(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerDeletePayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return dynamic_start_error("dynamic worker id must not be empty");
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };
    let (reply_id, pending_replies) =
        allocate_dynamic_pending_reply(&state, &owner_worker, owner_generation, owner_isolate_id);
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::DynamicWorkerDelete(DynamicWorkerDeleteEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id,
            binding: payload.binding,
            id: id.to_string(),
            reply_id: reply_id.clone(),
            pending_replies,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_invoke(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerInvokePayload,
) -> DynamicPendingReplyStartResult {
    if payload.subrequest_id.trim().is_empty() {
        return dynamic_start_error("dynamic worker subrequest_id must not be empty");
    }
    if payload.handle.trim().is_empty() {
        return dynamic_start_error("dynamic worker handle must not be empty");
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };

    let request = WorkerInvocation {
        method: payload.method,
        url: payload.url,
        headers: payload.headers,
        body: payload.body,
        request_id: payload.subrequest_id,
    };

    let (reply_id, pending_replies) =
        allocate_dynamic_pending_reply(&state, &owner_worker, owner_generation, owner_isolate_id);
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::DynamicWorkerInvoke(DynamicWorkerInvokeEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id,
            binding: payload.binding,
            handle: payload.handle,
            request,
            reply_id: reply_id.clone(),
            pending_replies,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_fetch_start(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerInvokePayload,
) -> DynamicPendingReplyStartResult {
    if payload.subrequest_id.trim().is_empty() {
        return dynamic_start_error("dynamic worker subrequest_id must not be empty");
    }
    if payload.handle.trim().is_empty() {
        return dynamic_start_error("dynamic worker handle must not be empty");
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };

    let request = WorkerInvocation {
        method: payload.method,
        url: payload.url,
        headers: payload.headers,
        body: payload.body,
        request_id: payload.subrequest_id,
    };

    let (reply_id, pending_replies) =
        allocate_dynamic_pending_reply(&state, &owner_worker, owner_generation, owner_isolate_id);
    let command_sender = state
        .borrow()
        .borrow::<crate::service::RuntimeFastCommandSender>()
        .clone();
    if command_sender
        .0
        .send(crate::service::RuntimeCommand::DynamicWorkerFetchStart {
            owner_worker,
            owner_generation,
            binding: payload.binding,
            handle: payload.handle,
            request,
            reply_id: reply_id.clone(),
            pending_replies,
        })
        .is_err()
    {
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_host_rpc_invoke(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicHostRpcInvokePayload,
) -> DynamicPendingReplyStartResult {
    let method_name = payload.method_name.trim();
    if method_name.is_empty() {
        return dynamic_start_error("dynamic host rpc method_name must not be empty");
    }
    let (caller_worker, caller_generation, caller_isolate_id) =
        match dynamic_host_rpc_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };
    let (reply_id, pending_replies) = allocate_dynamic_pending_reply(
        &state,
        &caller_worker,
        caller_generation,
        caller_isolate_id,
    );
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::DynamicHostRpcInvoke(DynamicHostRpcInvokeEvent {
            caller_worker,
            caller_generation,
            _caller_isolate_id: caller_isolate_id,
            binding: payload.binding,
            method_name: method_name.to_string(),
            args: payload.args,
            reply_id: reply_id.clone(),
            pending_replies,
        }),
    )
    .is_err()
    {
        return dynamic_start_error("dynamic host rpc runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}
