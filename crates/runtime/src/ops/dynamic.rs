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
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic worker request_id must not be empty",
        ));
    }
    let binding = binding.trim();
    if binding.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic worker binding must not be empty",
        ));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .contexts
            .get(request_id)
            .ok_or_else(|| PlatformError::runtime("dynamic worker request scope is unavailable"))?;
        if !context.dynamic_bindings.contains(binding) {
            return Err(PlatformError::runtime(format!(
                "dynamic worker binding is not allowed: {binding}"
            )));
        }
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
}

pub(super) fn dynamic_host_rpc_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
    binding: &str,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic host rpc request_id must not be empty",
        ));
    }
    let binding = binding.trim();
    if binding.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic host rpc binding must not be empty",
        ));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts.contexts.get(request_id).ok_or_else(|| {
            PlatformError::runtime("dynamic host rpc request scope is unavailable")
        })?;
        if !context.dynamic_rpc_bindings.contains(binding) {
            return Err(PlatformError::runtime(format!(
                "dynamic host rpc binding is not allowed: {binding}"
            )));
        }
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
}

pub(super) fn memory_invoke_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request(
            "memory invoke caller_request_id must not be empty",
        ));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .contexts
            .get(request_id)
            .ok_or_else(|| PlatformError::runtime("memory invoke request scope is unavailable"))?;
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
}

pub(super) fn request_owner_for_request(
    state: &Rc<RefCell<OpState>>,
    request_id: &str,
) -> Result<(String, u64, u64)> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(PlatformError::bad_request("request_id must not be empty"));
    }
    let (worker_name, generation, isolate_id) = {
        let op_state = state.borrow();
        let contexts = op_state.borrow::<RequestSecretContexts>();
        let context = contexts
            .contexts
            .get(request_id)
            .ok_or_else(|| PlatformError::runtime("request scope is unavailable"))?;
        (
            context.worker_name.clone(),
            context.generation,
            context.isolate_id,
        )
    };
    Ok((worker_name, generation, isolate_id))
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
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test async reply requires request_id".to_string(),
        };
    }
    let (worker_name, generation, isolate_id) = match request_owner_for_request(&state, request_id)
    {
        Ok(value) => value,
        Err(error) => {
            return DynamicPendingReplyStartResult {
                ok: false,
                reply_id: String::new(),
                error: error.to_string(),
            };
        }
    };
    let replies = state.borrow().borrow::<TestAsyncReplies>().clone();
    let reply_id = replies.allocate(TestAsyncReplyOwner {
        worker_name,
        generation,
        isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::TestAsyncReply(TestAsyncReplyEvent {
            reply_id: reply_id.clone(),
            replies,
            delay_ms: payload.delay_ms,
            ok: payload.ok,
            value: payload.value,
            error: payload.error,
        }))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test async reply runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
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
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke requires request_id".to_string(),
        };
    }
    let target_id = payload.target_id.trim();
    if target_id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke requires target_id".to_string(),
        };
    }
    let method_name = payload.method_name.trim();
    if method_name.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke requires method_name".to_string(),
        };
    }
    let (worker_name, generation, isolate_id) = match request_owner_for_request(&state, request_id)
    {
        Ok(value) => value,
        Err(error) => {
            return DynamicPendingReplyStartResult {
                ok: false,
                reply_id: String::new(),
                error: error.to_string(),
            };
        }
    };
    let replies = state.borrow().borrow::<TestAsyncReplies>().clone();
    let reply_id = replies.allocate(TestAsyncReplyOwner {
        worker_name: worker_name.clone(),
        generation,
        isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::TestNestedTargetedInvoke(
            TestNestedTargetedInvokeEvent {
                worker_name,
                generation,
                caller_isolate_id: isolate_id,
                target_mode: payload.target_mode,
                target_id: target_id.to_string(),
                method_name: method_name.to_string(),
                args: payload.args,
                reply_id: reply_id.clone(),
                replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "test nested targeted invoke runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_create(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerCreatePayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker id must not be empty".to_string(),
        };
    }
    if payload.source.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker source must not be empty".to_string(),
        };
    }
    if payload.timeout == 0 {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker timeout must be greater than 0".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerCreate(
            DynamicWorkerCreateEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                id: id.to_string(),
                source: payload.source,
                env: payload.env,
                timeout: payload.timeout,
                host_rpc_bindings: payload.host_rpc_bindings,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_lookup(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerLookupPayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker id must not be empty".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerLookup(
            DynamicWorkerLookupEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                id: id.to_string(),
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
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
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerList(
            DynamicWorkerListEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_delete(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerDeletePayload,
) -> DynamicPendingReplyStartResult {
    let id = payload.id.trim();
    if id.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker id must not be empty".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerDelete(
            DynamicWorkerDeleteEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                id: id.to_string(),
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_invoke(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerInvokePayload,
) -> DynamicPendingReplyStartResult {
    if payload.subrequest_id.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker subrequest_id must not be empty".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker handle must not be empty".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };

    let request = WorkerInvocation {
        method: payload.method,
        url: payload.url,
        headers: payload.headers,
        body: payload.body,
        request_id: payload.subrequest_id,
    };

    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicWorkerInvoke(
            DynamicWorkerInvokeEvent {
                owner_worker,
                owner_generation,
                owner_isolate_id,
                binding: payload.binding,
                handle: payload.handle,
                request,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_worker_fetch_start(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicWorkerInvokePayload,
) -> DynamicPendingReplyStartResult {
    if payload.subrequest_id.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker subrequest_id must not be empty".to_string(),
        };
    }
    if payload.handle.trim().is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker handle must not be empty".to_string(),
        };
    }
    let (owner_worker, owner_generation, owner_isolate_id) =
        match dynamic_worker_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };

    let request = WorkerInvocation {
        method: payload.method,
        url: payload.url,
        headers: payload.headers,
        body: payload.body,
        request_id: payload.subrequest_id,
    };

    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: owner_worker.clone(),
        generation: owner_generation,
        isolate_id: owner_isolate_id,
    });
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
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic worker runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(super) fn op_dynamic_host_rpc_invoke(
    state: Rc<RefCell<OpState>>,
    #[serde] payload: DynamicHostRpcInvokePayload,
) -> DynamicPendingReplyStartResult {
    let method_name = payload.method_name.trim();
    if method_name.is_empty() {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic host rpc method_name must not be empty".to_string(),
        };
    }
    let (caller_worker, caller_generation, caller_isolate_id) =
        match dynamic_host_rpc_owner_for_request(&state, &payload.request_id, &payload.binding) {
            Ok(value) => value,
            Err(error) => {
                return DynamicPendingReplyStartResult {
                    ok: false,
                    reply_id: String::new(),
                    error: error.to_string(),
                };
            }
        };
    let pending_replies = state.borrow().borrow::<DynamicPendingReplies>().clone();
    let reply_id = pending_replies.allocate(DynamicPendingReplyOwner {
        worker_name: caller_worker.clone(),
        generation: caller_generation,
        isolate_id: caller_isolate_id,
    });
    let sender = state.borrow().borrow::<IsolateEventSender>().clone();
    if sender
        .0
        .send(IsolateEventPayload::DynamicHostRpcInvoke(
            DynamicHostRpcInvokeEvent {
                caller_worker,
                caller_generation,
                _caller_isolate_id: caller_isolate_id,
                binding: payload.binding,
                method_name: method_name.to_string(),
                args: payload.args,
                reply_id: reply_id.clone(),
                pending_replies,
            },
        ))
        .is_err()
    {
        return DynamicPendingReplyStartResult {
            ok: false,
            reply_id: String::new(),
            error: "dynamic host rpc runtime is unavailable".to_string(),
        };
    }
    DynamicPendingReplyStartResult {
        ok: true,
        reply_id,
        error: String::new(),
    }
}
