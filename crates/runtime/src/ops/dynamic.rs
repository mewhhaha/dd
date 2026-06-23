use super::*;

static DYNAMIC_FETCH_REQUEST_SEQ: AtomicU64 = AtomicU64::new(1);

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

#[derive(Serialize)]
pub(super) struct DynamicModuleGraphRegisterResult {
    ok: bool,
    graph_id: String,
    entrypoint: String,
    error: String,
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_module_graph_register(
    #[string] entrypoint: String,
    #[serde] modules: HashMap<String, String>,
) -> DynamicModuleGraphRegisterResult {
    match crate::dynamic_modules::register_dynamic_module_graph(&entrypoint, modules) {
        Ok((graph_id, entrypoint)) => DynamicModuleGraphRegisterResult {
            ok: true,
            graph_id,
            entrypoint,
            error: String::new(),
        },
        Err(error) => DynamicModuleGraphRegisterResult {
            ok: false,
            graph_id: String::new(),
            entrypoint: String::new(),
            error: error.to_string(),
        },
    }
}

#[deno_core::op2(fast)]
pub(super) fn op_dynamic_module_graph_release(#[string] graph_id: String) {
    crate::dynamic_modules::release_dynamic_module_graph(graph_id.trim());
}

pub(super) fn dynamic_worker_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
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
        request_context_handle,
        "dynamic worker request context handle must not be empty",
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
    request_context_handle: u32,
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
        request_context_handle,
        "dynamic host rpc request context handle must not be empty",
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
    request_context_handle: u32,
) -> Result<(String, u64, u64)> {
    request_owner_for_scope(
        state,
        request_context_handle,
        "memory invoke request context handle must not be empty",
        "memory invoke request scope is unavailable",
        |_| Ok(()),
    )
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
pub(super) fn op_dynamic_cancel_reply(state: &mut OpState, #[string] reply_id: String) {
    let pending = state.borrow::<DynamicPendingReplies>().clone();
    pending.cancel(reply_id.trim());
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_take_control_items(state: &mut OpState) -> Vec<DynamicControlItem> {
    let inbox = state.borrow::<DynamicControlInbox>().clone();
    let mut batch = inbox.take_batch();
    for item in &mut batch {
        match item {
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Fetch(reply)) => {
                if let Some(output) = reply.pending_output.take() {
                    reply.headers_handle = state
                        .borrow_mut::<HttpPreparedHeaders>()
                        .insert(output.headers);
                    reply.body_handle =
                        state.borrow_mut::<HttpPreparedBodies>().insert(output.body);
                }
            }
            DynamicControlItem::Reply(DynamicPushedReplyPayload::Dynamic(reply)) => {
                if let Some(value) = reply.pending_value.take() {
                    reply.value_handle = state
                        .borrow_mut::<HttpPreparedBodies>()
                        .insert(Bytes::from(value));
                }
            }
            DynamicControlItem::Reply(DynamicPushedReplyPayload::TestAsync(_)) => {}
        }
    }
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
    request_context_handle: u32,
    delay_ms: f64,
    ok: bool,
    #[string] value: String,
    #[string] error: String,
) -> DynamicPendingReplyStartResult {
    let delay_ms = if delay_ms.is_finite() && delay_ms > 0.0 {
        delay_ms.trunc() as u64
    } else {
        0
    };
    let (worker_name, generation, isolate_id) =
        match request_owner_for_request(&state, request_context_handle) {
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
    request_context_handle: u32,
    #[string] target_mode: String,
    #[string] target_id: String,
    #[string] method_name: String,
    #[buffer] args: JsBuffer,
) -> DynamicPendingReplyStartResult {
    let args = args.as_ref().to_vec();
    let target_id = target_id.trim();
    if target_id.is_empty() {
        return dynamic_start_error("test nested targeted invoke requires target_id");
    }
    let method_name = method_name.trim();
    if method_name.is_empty() {
        return dynamic_start_error("test nested targeted invoke requires method_name");
    }
    let (worker_name, generation, isolate_id) =
        match request_owner_for_request(&state, request_context_handle) {
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
            target_mode,
            target_id: target_id.to_string(),
            method_name: method_name.to_string(),
            args,
            reply_id: reply_id.clone(),
            replies: replies.clone(),
        }),
    )
    .is_err()
    {
        replies.cancel(&reply_id);
        return dynamic_start_error("test nested targeted invoke runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_create(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] id: String,
    #[string] source: String,
    #[string] module_graph_id: String,
    #[string] module_entrypoint: String,
    #[serde] bindings: Vec<common::DeployBinding>,
    #[serde] env: HashMap<String, String>,
    timeout: f64,
    #[serde] policy: DynamicWorkerPolicy,
    #[serde] host_rpc_bindings: Vec<DynamicHostRpcBindingSpec>,
) -> DynamicPendingReplyStartResult {
    let id = id.trim();
    if id.is_empty() {
        return dynamic_start_error("dynamic worker id must not be empty");
    }
    let source =
        match dynamic_worker_source_from_payload(source, module_graph_id, module_entrypoint) {
            Ok(source) => source,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };
    if !timeout.is_finite() || timeout <= 0.0 {
        return dynamic_start_error("dynamic worker timeout must be greater than 0");
    }
    let timeout = timeout.trunc() as u64;
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, request_context_handle, &binding) {
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
            binding,
            id: id.to_string(),
            source,
            bindings,
            env,
            timeout,
            policy,
            host_rpc_bindings,
            reply_id: reply_id.clone(),
            pending_replies: pending_replies.clone(),
        }),
    )
    .is_err()
    {
        pending_replies.cancel(&reply_id);
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

fn dynamic_worker_source_from_payload(
    source: String,
    module_graph_id: String,
    module_entrypoint: String,
) -> std::result::Result<WorkerSource, String> {
    let source = source.trim().to_string();
    let module_graph_id = module_graph_id.trim().to_string();
    let module_entrypoint = module_entrypoint.trim().to_string();
    let has_module = !module_graph_id.is_empty() || !module_entrypoint.is_empty();
    let has_source = !source.is_empty();
    if has_source && has_module {
        return Err("dynamic worker must specify source or modules, not both".to_string());
    }
    if has_source {
        return Ok(WorkerSource::inline(source));
    }
    let graph_id = (!module_graph_id.is_empty()).then_some(module_graph_id);
    let entrypoint = (!module_entrypoint.is_empty()).then_some(module_entrypoint);
    match (graph_id, entrypoint) {
        (Some(graph_id), Some(entrypoint)) => {
            let entrypoint = crate::dynamic_modules::normalize_dynamic_module_path(&entrypoint)
                .map_err(|error| format!("invalid dynamic worker module entrypoint: {error}"))?;
            if crate::dynamic_modules::dynamic_module_source(&graph_id, &entrypoint).is_none() {
                return Err(format!(
                    "dynamic worker module graph {graph_id} does not contain entrypoint: {entrypoint}"
                ));
            }
            Ok(WorkerSource::DynamicModule {
                graph_id,
                entrypoint,
            })
        }
        (None, None) => Err("dynamic worker source or modules must be provided".to_string()),
        (None, Some(_)) => Err("dynamic worker module graph id must not be empty".to_string()),
        (Some(_), None) => Err("dynamic worker module entrypoint must not be empty".to_string()),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_lookup(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] id: String,
) -> DynamicPendingReplyStartResult {
    let id = id.trim();
    if id.is_empty() {
        return dynamic_start_error("dynamic worker id must not be empty");
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, request_context_handle, &binding) {
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
            binding,
            id: id.to_string(),
            reply_id: reply_id.clone(),
            pending_replies: pending_replies.clone(),
        }),
    )
    .is_err()
    {
        pending_replies.cancel(&reply_id);
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_list(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
) -> DynamicPendingReplyStartResult {
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, request_context_handle, &binding) {
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
            binding,
            reply_id: reply_id.clone(),
            pending_replies: pending_replies.clone(),
        }),
    )
    .is_err()
    {
        pending_replies.cancel(&reply_id);
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_delete(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] id: String,
) -> DynamicPendingReplyStartResult {
    let id = id.trim();
    if id.is_empty() {
        return dynamic_start_error("dynamic worker id must not be empty");
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, request_context_handle, &binding) {
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
            binding,
            id: id.to_string(),
            reply_id: reply_id.clone(),
            pending_replies: pending_replies.clone(),
        }),
    )
    .is_err()
    {
        pending_replies.cancel(&reply_id);
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_fetch_start(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] handle: String,
    #[string] method: String,
    #[string] url: String,
    headers_handle: u32,
    body_handle: u32,
) -> DynamicPendingReplyStartResult {
    if handle.trim().is_empty() {
        return dynamic_start_error("dynamic worker handle must not be empty");
    }
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
                return dynamic_start_error("dynamic worker fetch request context is unavailable");
            }
        }
    };
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, request_context_handle, &binding) {
            Ok(value) => value,
            Err(error) => {
                return dynamic_start_error(error);
            }
        };
    let subrequest_id = format!(
        "{}:dynamic:{}",
        caller_request_id,
        DYNAMIC_FETCH_REQUEST_SEQ.fetch_add(1, Ordering::Relaxed)
    );

    let request = WorkerInvocation {
        method,
        url,
        headers,
        body: body.to_vec(),
        request_id: subrequest_id,
    };

    let (reply_id, pending_replies) =
        allocate_dynamic_pending_reply(&state, &owner_worker, owner_generation, owner_isolate_id);
    let command_sender = state
        .borrow()
        .borrow::<crate::service::RuntimeFastCommandSender>()
        .clone();
    if command_sender
        .0
        .try_send(crate::service::RuntimeCommand::DynamicWorkerFetchStart {
            owner_worker,
            owner_generation,
            binding,
            handle,
            request,
            reply_id: reply_id.clone(),
            pending_replies: pending_replies.clone(),
        })
        .is_err()
    {
        pending_replies.cancel(&reply_id);
        return dynamic_start_error("dynamic worker runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_host_rpc_invoke(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] binding: String,
    #[string] method_name: String,
    #[buffer] args: JsBuffer,
) -> DynamicPendingReplyStartResult {
    let args = args.as_ref().to_vec();
    let method_name = method_name.trim();
    if method_name.is_empty() {
        return dynamic_start_error("dynamic host rpc method_name must not be empty");
    }
    let (caller_worker, caller_generation, caller_isolate_id) =
        match dynamic_host_rpc_owner_for_request(&state, request_context_handle, &binding) {
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
            binding,
            method_name: method_name.to_string(),
            args,
            reply_id: reply_id.clone(),
            pending_replies: pending_replies.clone(),
        }),
    )
    .is_err()
    {
        pending_replies.cancel(&reply_id);
        return dynamic_start_error("dynamic host rpc runtime is unavailable");
    }
    dynamic_start_ok(reply_id)
}
