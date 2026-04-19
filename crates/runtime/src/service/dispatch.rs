use super::*;

impl WorkerManager {
    pub(crate) fn register_stream(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
    ) {
        let (body_sender, body_receiver) = mpsc::unbounded_channel();
        self.stream_registrations.insert(
            runtime_request_id,
            StreamRegistration {
                worker_name,
                completion_token: None,
                ready: Some(ready),
                body_sender,
                body_receiver: Some(body_receiver),
                started: false,
            },
        );
    }

    pub(crate) fn enqueue_invoke(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        memory_route: Option<MemoryRoute>,
        memory_call: Option<MemoryExecutionCall>,
        host_rpc_call: Option<HostRpcExecutionCall>,
        target_isolate_id: Option<u64>,
        target_generation: Option<u64>,
        internal_origin: bool,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        reply_kind: PendingReplyKind,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let worker_name = worker_name.trim().to_string();
        if self
            .pre_canceled
            .get_mut(&worker_name)
            .map(|request_ids| request_ids.remove(&runtime_request_id))
            .unwrap_or(false)
        {
            let _ = reply.send(Err(PlatformError::runtime("request was aborted")));
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
            return;
        }
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        let Some(entry) = self.workers.get(&worker_name) else {
            let error = PlatformError::not_found("Worker not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        };
        let generation = target_generation.unwrap_or(entry.current_generation);
        if !entry.pools.contains_key(&generation) {
            let error = PlatformError::not_found("Worker generation not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        }

        if let Some(pool) = self.get_pool_mut(&worker_name, generation) {
            if let Some(route) = &memory_route {
                if !pool
                    .memory_bindings
                    .iter()
                    .any(|binding| binding == &route.binding)
                {
                    let error = PlatformError::bad_request(format!(
                        "unknown memory binding for worker {}: {}",
                        worker_name, route.binding
                    ));
                    let _ = reply.send(Err(error.clone()));
                    self.fail_stream_registration(&worker_name, &runtime_request_id, error);
                    return;
                }
            }
            pool.queue.push_back(PendingInvoke {
                runtime_request_id,
                request,
                request_body,
                memory_route,
                memory_call,
                host_rpc_call,
                target_isolate_id,
                internal_origin,
                reply,
                reply_kind,
                enqueued_at: Instant::now(),
            });
            pool.update_queue_warning(&warn_thresholds);
        } else {
            let error = PlatformError::not_found("Worker not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        }

        self.dispatch_pool(&worker_name, generation, event_tx);
    }

    pub(crate) fn try_dispatch_direct_dynamic_fetch(
        &mut self,
        worker_name: &str,
        generation: u64,
        target_isolate_id: u64,
        runtime_request_id: String,
        request: WorkerInvocation,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        handle: String,
    ) -> DirectDynamicFetchDispatch {
        let config_max_inflight = self.config.max_inflight_per_isolate;
        let dispatch_result = {
            let Some(pool) = self.get_pool_mut(worker_name, generation) else {
                return DirectDynamicFetchDispatch::Fallback {
                    reply,
                    clear_preferred: true,
                };
            };
            let max_inflight = if pool.strict_request_isolation {
                1
            } else {
                config_max_inflight
            };
            let Some(isolate_idx) = pool
                .isolates
                .iter()
                .position(|isolate| isolate.id == target_isolate_id)
            else {
                return DirectDynamicFetchDispatch::Fallback {
                    reply,
                    clear_preferred: true,
                };
            };
            let isolate_busy = pool.isolates[isolate_idx].inflight_count >= max_inflight
                || (pool.strict_request_isolation
                    && !pool.isolates[isolate_idx].pending_wait_until.is_empty());
            if isolate_busy {
                return DirectDynamicFetchDispatch::Fallback {
                    reply,
                    clear_preferred: false,
                };
            }

            let worker_name_json = Arc::clone(&pool.worker_name_json);
            let kv_bindings_json = Arc::clone(&pool.kv_bindings_json);
            let kv_read_cache_config_json = Arc::clone(&pool.kv_read_cache_config_json);
            let memory_bindings_json = Arc::clone(&pool.memory_bindings_json);
            let dynamic_bindings_json = Arc::clone(&pool.dynamic_bindings_json);
            let dynamic_rpc_bindings_json = Arc::clone(&pool.dynamic_rpc_bindings_json);
            let dynamic_env_json = Arc::clone(&pool.dynamic_env_json);
            let dynamic_bindings = pool.dynamic_bindings.clone();
            let dynamic_rpc_bindings = pool
                .dynamic_rpc_bindings
                .iter()
                .map(|binding| binding.binding.clone())
                .collect::<Vec<_>>();
            let secret_replacements = pool.secret_replacements.clone();
            let egress_allow_hosts = pool.egress_allow_hosts.clone();
            let allow_cache = pool
                .dynamic_child_policy
                .as_ref()
                .map(|policy| policy.allow_state_bindings)
                .unwrap_or(true);
            let max_outbound_requests = pool
                .dynamic_child_policy
                .as_ref()
                .map(|policy| policy.max_outbound_requests);
            let dynamic_quota_state = pool.dynamic_quota_state.clone();
            let counted_reuse = pool.isolates[isolate_idx].served_requests > 0;
            if counted_reuse {
                pool.stats.reuse_count += 1;
            }

            let completion_token = next_runtime_token("done");
            let completion_meta = Some(PendingReplyMeta {
                method: request.method.clone(),
                url: request.url.clone(),
                traceparent: None,
                user_request_id: request.request_id.clone(),
            });
            let command = IsolateCommand::Execute {
                runtime_request_id: runtime_request_id.clone(),
                completion_token: completion_token.clone(),
                worker_name_json,
                kv_bindings_json,
                kv_read_cache_config_json,
                memory_bindings_json,
                dynamic_bindings_json,
                dynamic_rpc_bindings_json,
                dynamic_env_json,
                dynamic_bindings,
                dynamic_rpc_bindings,
                secret_replacements,
                egress_allow_hosts,
                allow_cache,
                max_outbound_requests,
                dynamic_quota_state,
                request,
                request_body: None,
                stream_response: false,
                memory_call: None,
                host_rpc_call: None,
                memory_route: None,
            };

            let isolate = &mut pool.isolates[isolate_idx];
            isolate.served_requests += 1;
            isolate.inflight_count += 1;
            isolate.pending_replies.insert(
                runtime_request_id.clone(),
                PendingReply {
                    completion_token,
                    canceled: false,
                    memory_key: None,
                    internal_origin: true,
                    reply,
                    completion_meta,
                    kind: PendingReplyKind::DynamicFetch { handle },
                    dispatched_at: Instant::now(),
                },
            );

            if isolate.sender.send(command).is_ok() {
                return DirectDynamicFetchDispatch::Dispatched;
            }

            let pending_reply = isolate
                .pending_replies
                .remove(&runtime_request_id)
                .expect("direct dynamic fetch pending reply should exist");
            isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
            isolate.served_requests = isolate.served_requests.saturating_sub(1);
            let restored_reply = pending_reply.reply;
            if counted_reuse {
                pool.stats.reuse_count = pool.stats.reuse_count.saturating_sub(1);
            }
            (isolate_idx, restored_reply)
        };
        let (isolate_idx, restored_reply) = dispatch_result;
        let failed = self.remove_isolate(worker_name, generation, isolate_idx);
        for (request_id, reply) in failed {
            if request_id != runtime_request_id {
                let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
            }
        }
        DirectDynamicFetchDispatch::Fallback {
            reply: restored_reply,
            clear_preferred: true,
        }
    }

    pub(crate) fn enqueue_memory_invoke(
        &mut self,
        payload: MemoryInvokeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let decoded = match decode_memory_invoke_request(&payload.request_frame) {
            Ok(decoded) => decoded,
            Err(error) => {
                let _ = payload.reply.send(Err(error));
                return;
            }
        };
        let (request, memory_call, prefer_caller_isolate) = match decoded.call {
            MemoryInvokeCall::Method {
                name,
                args,
                request_id,
            } => (
                WorkerInvocation {
                    method: "MEMORY-RPC".to_string(),
                    url: format!("http://memory/__dd_rpc/{}", name),
                    headers: Vec::new(),
                    body: args.clone(),
                    request_id,
                },
                MemoryExecutionCall::Method {
                    binding: decoded.binding.clone(),
                    key: decoded.key.clone(),
                    name: name.clone(),
                    args,
                },
                name == MEMORY_ATOMIC_METHOD && payload.prefer_caller_isolate,
            ),
            MemoryInvokeCall::Fetch(_) => {
                let _ = payload.reply.send(Err(PlatformError::bad_request(
                    "memory fetch invoke is no longer supported; use fetch + wake + stub.atomic",
                )));
                return;
            }
        };
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = MemoryRoute {
            binding: decoded.binding.trim().to_string(),
            key: decoded.key.trim().to_string(),
        };
        if route.binding.is_empty() || route.key.is_empty() {
            let _ = payload.reply.send(Err(PlatformError::bad_request(
                "memory binding/key must not be empty",
            )));
            return;
        }
        let mut target_generation = None;
        let mut target_isolate_id = None;
        if payload.caller_worker_name == decoded.worker_name {
            if let Some(pool) = self
                .workers
                .get(&decoded.worker_name)
                .and_then(|entry| entry.pools.get(&payload.caller_generation))
            {
                target_generation = Some(payload.caller_generation);
                if prefer_caller_isolate
                    && pool
                        .isolates
                        .iter()
                        .any(|isolate| isolate.id == payload.caller_isolate_id)
                {
                    let owner_key = route.owner_key();
                    match pool.memory_owners.get(&owner_key).copied() {
                        Some(owner_id) if owner_id == payload.caller_isolate_id => {
                            target_isolate_id = Some(payload.caller_isolate_id);
                        }
                        None => {
                            target_isolate_id = Some(payload.caller_isolate_id);
                        }
                        _ => {}
                    }
                }
            }
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            decoded.worker_name,
            runtime_request_id,
            request,
            None,
            Some(route),
            Some(memory_call),
            None,
            target_isolate_id,
            target_generation,
            false,
            reply_tx,
            PendingReplyKind::Normal,
            event_tx,
        );
        tokio::spawn(async move {
            let result = match reply_rx.await {
                Ok(Ok(output)) => encode_memory_invoke_response(&MemoryInvokeResponse::Method {
                    value: output.body,
                }),
                Ok(Err(error)) => {
                    encode_memory_invoke_response(&MemoryInvokeResponse::Error(error.to_string()))
                }
                Err(_) => encode_memory_invoke_response(&MemoryInvokeResponse::Error(
                    "memory invoke response channel closed".to_string(),
                )),
            };
            match result {
                Ok(frame) => {
                    let _ = payload.reply.send(Ok(frame));
                }
                Err(error) => {
                    let _ = payload.reply.send(Err(error));
                }
            }
        });
    }

    pub(crate) fn cancel_invoke(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return;
        }

        let mut touched_generations = Vec::new();
        let mut abort_commands = Vec::new();
        let mut matched = false;
        let mut cleared_request_ids = Vec::new();
        let mut websocket_waiters_to_abort = Vec::new();

        if let Some(entry) = self.workers.get_mut(&worker_name) {
            for (generation, pool) in &mut entry.pools {
                let mut generation_touched = false;

                if let Some(idx) = pool
                    .queue
                    .iter()
                    .position(|pending| pending.runtime_request_id == runtime_request_id)
                {
                    if let Some(pending) = pool.queue.remove(idx) {
                        if let PendingReplyKind::WebsocketOpen { session_id } = pending.reply_kind {
                            websocket_waiters_to_abort.push(session_id);
                        }
                        cleared_request_ids.push(pending.runtime_request_id.clone());
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::runtime("request was aborted")));
                        generation_touched = true;
                        matched = true;
                    }
                }

                for isolate in &mut pool.isolates {
                    if let Some(pending_reply) =
                        isolate.pending_replies.get_mut(&runtime_request_id)
                    {
                        if let PendingReplyKind::WebsocketOpen { session_id } = &pending_reply.kind
                        {
                            websocket_waiters_to_abort.push(session_id.clone());
                        }
                        pending_reply.canceled = true;
                        abort_commands.push((*generation, isolate.id, isolate.sender.clone()));
                        generation_touched = true;
                        matched = true;
                    }
                }

                if generation_touched {
                    pool.log_stats("cancel");
                    touched_generations.push(*generation);
                }
            }
        }

        for request_id in cleared_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }

        websocket_waiters_to_abort.sort();
        websocket_waiters_to_abort.dedup();
        for session_id in websocket_waiters_to_abort {
            if let Some(waiter) = self.websocket_open_waiters.remove(&session_id) {
                let _ = waiter.send(Err(PlatformError::runtime("request was aborted")));
            }
        }

        for (generation, isolate_id, sender) in abort_commands {
            if sender
                .send(IsolateCommand::Abort {
                    runtime_request_id: runtime_request_id.clone(),
                })
                .is_err()
            {
                let failed = self.remove_isolate_by_id(&worker_name, generation, isolate_id);
                for (request_id, reply) in failed {
                    self.clear_revalidation_for_request(&request_id);
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
            }
        }

        touched_generations.sort_unstable();
        touched_generations.dedup();
        for generation in touched_generations {
            self.dispatch_pool(&worker_name, generation, event_tx);
        }
        if !matched {
            self.pre_canceled
                .entry(worker_name.clone())
                .or_default()
                .insert(runtime_request_id.clone());
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
        }
        self.cleanup_drained_generations_for(&worker_name);
    }

    pub(crate) fn dispatch_pool(
        &mut self,
        worker_name: &str,
        generation: u64,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        if self.runtime_batch_depth > 0 {
            self.pending_dispatches
                .insert((worker_name.to_string(), generation));
            return;
        }
        let max_inflight_per_isolate = self.config.max_inflight_per_isolate;
        let max_isolates = self.config.max_isolates;
        loop {
            let (selection, spawn_needed) = {
                let Some(pool) = self.get_pool_mut(worker_name, generation) else {
                    return;
                };
                if pool.queue.is_empty() {
                    pool.log_stats("dispatch");
                    return;
                }
                let max_inflight = if pool.strict_request_isolation {
                    1
                } else {
                    max_inflight_per_isolate
                };
                let has_capacity = pool.isolates.iter().any(|isolate| {
                    isolate.inflight_count < max_inflight
                        && (!pool.strict_request_isolation || isolate.pending_wait_until.is_empty())
                });
                let selection =
                    select_dispatch_candidate(pool, max_inflight, pool.strict_request_isolation);
                let spawn_needed =
                    selection.is_none() && !has_capacity && pool.isolates.len() < max_isolates;
                (selection, spawn_needed)
            };

            if spawn_needed {
                if let Err(error) = self.spawn_isolate(worker_name, generation, event_tx.clone()) {
                    if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                        if let Some(pending) = pool.queue.pop_front() {
                            let _ = pending.reply.send(Err(error));
                        }
                    }
                    return;
                }
                continue;
            }

            let Some(selection) = selection else {
                return;
            };
            let candidate = match selection {
                DispatchSelection::Dispatch(candidate) => candidate,
                DispatchSelection::DropStaleTarget { queue_idx } => {
                    if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                        if let Some(stale) = pool.queue.remove(queue_idx) {
                            let _ = stale
                                .reply
                                .send(Err(PlatformError::runtime("target isolate is unavailable")));
                        }
                    }
                    continue;
                }
            };
            let isolate_idx = candidate.isolate_idx;
            let Some(pending_invoke) = self
                .get_pool_mut(worker_name, generation)
                .and_then(|pool| pool.queue.remove(candidate.queue_idx))
            else {
                return;
            };

            let runtime_request_id = pending_invoke.runtime_request_id.clone();
            let internal_origin = pending_invoke.internal_origin;
            let info_tracing_enabled = tracing::enabled!(Level::INFO);
            let needs_completion_meta = info_tracing_enabled
                || self
                    .get_pool_mut(worker_name, generation)
                    .and_then(|pool| pool.internal_trace.as_ref())
                    .is_some();
            let traceparent = if needs_completion_meta {
                traceparent_from_headers(&pending_invoke.request.headers)
            } else {
                None
            };
            let dispatch_span = if info_tracing_enabled {
                let queue_wait_ms = pending_invoke.enqueued_at.elapsed().as_millis() as u64;
                Some(tracing::info_span!(
                    "runtime.dispatch",
                    worker.name = %worker_name,
                    worker.generation = generation,
                    runtime.request_id = %runtime_request_id,
                    request.id = %pending_invoke.request.request_id,
                    queue.wait_ms = queue_wait_ms
                ))
            } else {
                None
            };
            if let Some(span) = dispatch_span.as_ref() {
                set_span_parent_from_traceparent(span, traceparent.as_deref());
            }
            let _dispatch_guard = dispatch_span.as_ref().map(|span| span.enter());

            let mut pending_reply = Some(pending_invoke.reply);
            let completion_token = next_runtime_token("done");
            if let Some(registration) = self.stream_registrations.get_mut(&runtime_request_id) {
                if registration.worker_name == worker_name {
                    registration.completion_token = Some(completion_token.clone());
                }
            }
            let stream_response = self.stream_registrations.contains_key(&runtime_request_id);
            let mut send_failed = false;
            if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                if isolate_idx >= pool.isolates.len() {
                    continue;
                }

                let worker_name_json = Arc::clone(&pool.worker_name_json);
                let kv_bindings_json = Arc::clone(&pool.kv_bindings_json);
                let kv_read_cache_config_json = Arc::clone(&pool.kv_read_cache_config_json);
                let memory_bindings_json = Arc::clone(&pool.memory_bindings_json);
                let dynamic_bindings_json = Arc::clone(&pool.dynamic_bindings_json);
                let dynamic_rpc_bindings_json = Arc::clone(&pool.dynamic_rpc_bindings_json);
                let dynamic_env_json = Arc::clone(&pool.dynamic_env_json);
                let dynamic_bindings = pool.dynamic_bindings.clone();
                let dynamic_rpc_bindings = pool
                    .dynamic_rpc_bindings
                    .iter()
                    .map(|binding| binding.binding.clone())
                    .collect::<Vec<_>>();
                let secret_replacements = pool.secret_replacements.clone();
                let egress_allow_hosts = pool.egress_allow_hosts.clone();
                let allow_cache = pool
                    .dynamic_child_policy
                    .as_ref()
                    .map(|policy| policy.allow_state_bindings)
                    .unwrap_or(true);
                let max_outbound_requests = pool
                    .dynamic_child_policy
                    .as_ref()
                    .map(|policy| policy.max_outbound_requests);
                let dynamic_quota_state = pool.dynamic_quota_state.clone();
                let should_count_reuse = pool.isolates[isolate_idx].served_requests > 0;
                if should_count_reuse {
                    pool.stats.reuse_count += 1;
                }
                let pending_kind = pending_invoke.reply_kind.clone();
                if let Some(memory_key) = &candidate.memory_key {
                    if candidate.assign_owner {
                        let owner_id = pool.isolates[isolate_idx].id;
                        pool.memory_owners.insert(memory_key.clone(), owner_id);
                    }
                    let entry = pool.memory_inflight.entry(memory_key.clone()).or_insert(0);
                    *entry += 1;
                }
                let isolate = &mut pool.isolates[isolate_idx];
                isolate.served_requests += 1;
                let completion_meta = Some(PendingReplyMeta {
                    method: pending_invoke.request.method.clone(),
                    url: pending_invoke.request.url.clone(),
                    traceparent: traceparent.clone(),
                    user_request_id: pending_invoke.request.request_id.clone(),
                });
                let command = IsolateCommand::Execute {
                    runtime_request_id: runtime_request_id.clone(),
                    completion_token: completion_token.clone(),
                    worker_name_json,
                    kv_bindings_json,
                    kv_read_cache_config_json,
                    memory_bindings_json,
                    dynamic_bindings_json,
                    dynamic_rpc_bindings_json,
                    dynamic_env_json,
                    dynamic_bindings,
                    dynamic_rpc_bindings,
                    secret_replacements,
                    egress_allow_hosts,
                    allow_cache,
                    max_outbound_requests,
                    dynamic_quota_state,
                    request: pending_invoke.request,
                    request_body: pending_invoke.request_body,
                    stream_response,
                    memory_call: pending_invoke.memory_call,
                    host_rpc_call: pending_invoke.host_rpc_call,
                    memory_route: pending_invoke.memory_route,
                };
                isolate.inflight_count += 1;
                isolate.pending_replies.insert(
                    runtime_request_id.clone(),
                    PendingReply {
                        completion_token,
                        canceled: false,
                        memory_key: candidate.memory_key.clone(),
                        internal_origin,
                        reply: pending_reply
                            .take()
                            .expect("pending reply must exist before dispatch"),
                        completion_meta,
                        kind: pending_kind,
                        dispatched_at: Instant::now(),
                    },
                );
                if isolate.sender.send(command).is_err() {
                    isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                    if let Some(memory_key) = candidate.memory_key.as_ref() {
                        decrement_memory_inflight(&mut pool.memory_inflight, memory_key);
                        if candidate.assign_owner {
                            pool.memory_owners.remove(memory_key);
                        }
                    }
                    pending_reply = isolate
                        .pending_replies
                        .remove(&runtime_request_id)
                        .map(|pending| pending.reply);
                    send_failed = true;
                }
            }

            if send_failed {
                let failed = self.remove_isolate(worker_name, generation, isolate_idx);
                self.clear_revalidation_for_request(&runtime_request_id);
                if let Some(reply) = pending_reply.take() {
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
                for (request_id, reply) in failed {
                    self.clear_revalidation_for_request(&request_id);
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
                self.fail_stream_registration(
                    worker_name,
                    &runtime_request_id,
                    PlatformError::internal("isolate is unavailable"),
                );
                continue;
            }
        }
    }

    pub(crate) fn spawn_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let (snapshot, snapshot_preloaded, source) = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .map(|pool| (pool.snapshot, pool.snapshot_preloaded, pool.source.clone()))
            .ok_or_else(|| PlatformError::not_found("Worker not found"))?;
        let isolate_id = self.next_isolate_id;
        self.next_isolate_id += 1;
        let kv_store = self.kv_store.clone();
        let memory_store = self.memory_store.clone();
        let cache_store = self.cache_store.clone();
        let dynamic_profile = self.dynamic_profile.clone();
        let open_handle_registry = self.open_handle_registry.clone();
        let isolate = spawn_isolate_thread(
            snapshot,
            snapshot_preloaded,
            source,
            kv_store,
            memory_store,
            cache_store,
            open_handle_registry,
            dynamic_profile,
            self.runtime_fast_sender.clone(),
            worker_name.to_string(),
            generation,
            isolate_id,
            event_tx,
        )?;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.stats.spawn_count += 1;
            pool.isolates.push(isolate);
            pool.log_stats("spawn");
            Ok(())
        } else {
            Err(PlatformError::internal("worker pool missing"))
        }
    }

    pub(crate) fn finish_request(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        request_id: &str,
        completion_token: &str,
        wait_until_count: usize,
        mut result: Result<WorkerOutput>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let mut reply = None;
        let mut canceled = false;
        let mut clear_revalidation = false;
        let mut completion_traceparent: Option<String> = None;
        let mut user_request_id = String::new();
        let mut request_method = String::new();
        let mut request_url = String::new();
        let mut execution_ms: Option<u64> = None;
        let mut internal_origin = false;
        let mut pending_kind = PendingReplyKind::Normal;
        let trace_destination = self
            .get_pool_mut(worker_name, generation)
            .and_then(|pool| pool.internal_trace.clone());
        let info_tracing_enabled = tracing::enabled!(Level::INFO);
        let stream_registered = self.stream_registrations.contains_key(request_id);
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                let Some(pending) = isolate.pending_replies.get(request_id) else {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion for unknown request id"
                    );
                    return;
                };
                let token_mismatch = pending.completion_token != completion_token;
                if token_mismatch {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion with invalid token"
                    );
                }

                isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                if isolate.inflight_count == 0 {
                    isolate.last_used_at = Instant::now();
                }
                if let Some(pending) = isolate.pending_replies.remove(request_id) {
                    if let Some(memory_key) = &pending.memory_key {
                        decrement_memory_inflight(&mut pool.memory_inflight, memory_key);
                    }
                    canceled = pending.canceled;
                    internal_origin = pending.internal_origin;
                    if let Some(meta) = pending.completion_meta {
                        request_method = meta.method;
                        request_url = meta.url;
                        completion_traceparent = meta.traceparent;
                        user_request_id = meta.user_request_id;
                    }
                    if token_mismatch {
                        result = Err(PlatformError::internal(format!(
                            "completion token mismatch for runtime request {request_id}"
                        )));
                    } else if wait_until_count > 0 {
                        isolate
                            .pending_wait_until
                            .insert(request_id.to_string(), completion_token.to_string());
                    }
                    clear_revalidation = true;
                    execution_ms = Some(pending.dispatched_at.elapsed().as_millis() as u64);
                    pending_kind = pending.kind;
                    reply = Some(pending.reply);
                }
            }
            pool.log_stats("complete");
        }
        if clear_revalidation {
            self.clear_revalidation_for_request(request_id);
        }
        let complete_span = if info_tracing_enabled {
            let result_status = match &result {
                Ok(output) => output.status as i64,
                Err(_) => -1,
            };
            let result_ok = result.is_ok();
            let span = tracing::info_span!(
                "runtime.complete",
                worker.name = %worker_name,
                worker.generation = generation,
                isolate.id = isolate_id,
                runtime.request_id = %request_id,
                request.id = %user_request_id,
                request.ok = result_ok,
                response.status = result_status,
                request.execution_ms = execution_ms.unwrap_or_default(),
                request.wait_until_count = wait_until_count as u64
            );
            set_span_parent_from_traceparent(&span, completion_traceparent.as_deref());
            Some(span)
        } else {
            None
        };
        let _complete_guard = complete_span.as_ref().map(|span| span.enter());

        let stream_result = if stream_registered {
            Some(result.clone())
        } else {
            None
        };
        let trace_result = if trace_destination.is_some() {
            Some(result.clone())
        } else {
            None
        };

        if !canceled {
            match pending_kind {
                PendingReplyKind::Normal => {
                    if let Some(reply) = reply {
                        let _ = reply.send(result);
                    }
                }
                PendingReplyKind::DynamicInvoke { handle }
                | PendingReplyKind::DynamicFetch { handle } => {
                    if let Some(entry) = self.dynamic_worker_handles.get_mut(&handle) {
                        match &result {
                            Ok(_) => {
                                entry.preferred_isolate_id = Some(isolate_id);
                            }
                            Err(_) if entry.preferred_isolate_id == Some(isolate_id) => {
                                entry.preferred_isolate_id = None;
                            }
                            Err(_) => {}
                        }
                    }
                    if let Some(reply) = reply {
                        let _ = reply.send(result);
                    }
                }
                PendingReplyKind::WebsocketOpen { session_id } => {
                    self.complete_websocket_open(
                        worker_name,
                        generation,
                        isolate_id,
                        session_id,
                        result,
                    );
                }
                PendingReplyKind::WebsocketFrame { session_id } => {
                    self.complete_websocket_frame(session_id, reply, result);
                }
                PendingReplyKind::TransportOpen { session_id } => {
                    self.complete_transport_open(
                        worker_name,
                        generation,
                        isolate_id,
                        session_id,
                        result,
                    );
                }
            }
            if info_tracing_enabled {
                tracing::info!("request completion delivered");
            }
        } else {
            if info_tracing_enabled {
                info!(
                    worker = %worker_name,
                    generation,
                    isolate_id,
                    request_id,
                    "dropped completion for canceled request"
                );
            }
        }
        if let (Some(trace_destination), Some(trace_result)) =
            (trace_destination, trace_result.as_ref())
        {
            self.enqueue_trace_forward(
                worker_name,
                generation,
                &request_method,
                &request_url,
                request_id,
                &user_request_id,
                trace_result,
                execution_ms.unwrap_or_default(),
                wait_until_count,
                internal_origin,
                Some(trace_destination),
                event_tx,
            );
        }
        if let Some(stream_result) = stream_result {
            self.complete_stream_registration(
                worker_name,
                request_id,
                completion_token,
                stream_result,
            );
        }
    }

    pub(crate) fn enqueue_trace_forward(
        &mut self,
        worker_name: &str,
        generation: u64,
        request_method: &str,
        request_url: &str,
        runtime_request_id: &str,
        user_request_id: &str,
        result: &Result<WorkerOutput>,
        execution_ms: u64,
        wait_until_count: usize,
        internal_origin: bool,
        trace_destination: Option<InternalTraceDestination>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let Some(trace_destination) = trace_destination else {
            return;
        };
        if internal_origin {
            return;
        }

        let (status, error) = match result {
            Ok(output) => (Some(output.status), None),
            Err(error) => (None, Some(error.to_string())),
        };

        let payload = TraceEventPayload {
            ts_ms: match epoch_ms_i64() {
                Ok(ts_ms) => ts_ms,
                Err(error) => {
                    warn!(
                        worker = %worker_name,
                        generation,
                        runtime_request_id,
                        error = %error,
                        "skipping trace forward due invalid clock"
                    );
                    return;
                }
            },
            worker: worker_name.to_string(),
            generation,
            request_id: user_request_id.to_string(),
            runtime_request_id: runtime_request_id.to_string(),
            method: request_method.to_string(),
            url: request_url.to_string(),
            status,
            ok: status.is_some(),
            error,
            execution_ms,
            wait_until_count,
        };

        // Use serde_json for trace forwarding payloads to avoid simd-json's
        // mutable-buffer serialization path on this hot async boundary.
        let body = match serde_json::to_vec(&payload) {
            Ok(body) => body,
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    runtime_request_id,
                    error = %error,
                    "skipping trace forward due payload serialization failure"
                );
                return;
            }
        };
        let mut headers = vec![(
            CONTENT_TYPE_HEADER.to_string(),
            JSON_CONTENT_TYPE.to_string(),
        )];
        append_internal_trace_headers(&mut headers, worker_name, generation);
        let trace_request = WorkerInvocation {
            method: "POST".to_string(),
            url: format!(
                "http://{}{}",
                trace_destination.worker,
                normalize_trace_path(&trace_destination.path)
            ),
            headers,
            body,
            request_id: Uuid::new_v4().to_string(),
        };
        let (reply, reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            trace_destination.worker,
            Uuid::new_v4().to_string(),
            trace_request,
            None,
            None,
            None,
            None,
            None,
            None,
            true,
            reply,
            PendingReplyKind::Normal,
            event_tx,
        );
        let request_id_for_warning = runtime_request_id.to_string();
        tokio::spawn(async move {
            match reply_rx.await {
                Ok(Ok(output)) => {
                    if !matches!(output.status, 200..=299) {
                        warn!(
                            request_id = %request_id_for_warning,
                            status = output.status,
                            "internal trace forward responded non-2xx"
                        );
                    }
                }
                Ok(Err(error)) => {
                    warn!(error = %error, "internal trace forward failed");
                }
                Err(error) => {
                    warn!(error = %error, "internal trace forward receiver dropped");
                }
            }
        });
    }
}
