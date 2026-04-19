use super::*;

impl WorkerManager {
    pub(super) async fn handle_dynamic_worker_create(
        &mut self,
        payload: crate::ops::DynamicWorkerCreateEvent,
    ) {
        let crate::ops::DynamicWorkerCreateEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id,
            binding,
            id,
            source,
            env,
            timeout,
            policy,
            host_rpc_bindings,
            reply_id,
            pending_replies,
        } = payload;
        let normalized_id = id.trim().to_string();
        if normalized_id.is_empty() {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Create(Err(PlatformError::bad_request(
                    "dynamic worker id must not be empty",
                ))),
            );
            return;
        }
        let timeout = timeout.clamp(1, 60_000);
        if !policy.allow_host_rpc && !host_rpc_bindings.is_empty() {
            self.dynamic_profile.record_rpc_deny();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Create(Err(PlatformError::bad_request(
                    "dynamic child policy blocks host RPC bindings; set allow_host_rpc: true",
                ))),
            );
            return;
        }
        let id_key = DynamicWorkerIdKey {
            owner_worker: owner_worker.clone(),
            owner_generation,
            binding: binding.clone(),
        };
        if let Some(handle) = self
            .dynamic_worker_ids
            .get(&id_key)
            .and_then(|by_id| by_id.get(&normalized_id))
            .cloned()
        {
            if let Some(entry) = self.dynamic_worker_handles.get(&handle) {
                self.dynamic_profile.record_async_reply_completion();
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Ok(
                        crate::ops::DynamicWorkerCreateReply {
                            handle,
                            worker_name: entry.worker_name.clone(),
                            timeout: entry.timeout,
                        },
                    )),
                );
                return;
            }
            if let Some(by_id) = self.dynamic_worker_ids.get_mut(&id_key) {
                by_id.remove(&normalized_id);
            }
        }
        let mut dynamic_rpc_bindings = Vec::new();
        let mut created_provider_ids = Vec::new();
        let mut seen_binding_names = HashSet::new();
        for binding_spec in host_rpc_bindings {
            let binding_name = binding_spec.binding.trim().to_string();
            let target_id = binding_spec.target_id.trim().to_string();
            if binding_name.is_empty() {
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request("dynamic host rpc binding must not be empty"),
                    )),
                );
                return;
            }
            if target_id.is_empty() {
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request("dynamic host rpc target_id must not be empty"),
                    )),
                );
                return;
            }
            if !seen_binding_names.insert(binding_name.clone()) {
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request(format!(
                            "duplicate dynamic host rpc binding: {binding_name}"
                        )),
                    )),
                );
                return;
            }
            let provider_id = format!("hrpc-{}", Uuid::new_v4().simple());
            let methods = binding_spec
                .methods
                .into_iter()
                .map(|method| method.trim().to_string())
                .filter(|method| !method.is_empty())
                .collect::<HashSet<_>>();
            if methods.is_empty() {
                self.dynamic_profile.record_rpc_deny();
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request(format!(
                            "dynamic host rpc binding must expose at least one method: {binding_name}"
                        )),
                    )),
                );
                return;
            }
            if methods.len() > MAX_DYNAMIC_HOST_RPC_METHODS {
                self.dynamic_profile.record_rpc_deny();
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request(format!(
                            "dynamic host rpc binding exceeds method limit ({MAX_DYNAMIC_HOST_RPC_METHODS}): {binding_name}"
                        )),
                    )),
                );
                return;
            }
            if let Some(blocked) = methods.iter().find(|method| host_rpc_method_blocked(method)) {
                self.dynamic_profile.record_rpc_deny();
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request(format!(
                            "dynamic host rpc method is blocked: {blocked}"
                        )),
                    )),
                );
                return;
            }
            self.host_rpc_providers.insert(
                provider_id.clone(),
                HostRpcProvider {
                    owner_worker: owner_worker.clone(),
                    owner_generation,
                    owner_isolate_id,
                    target_id,
                    methods,
                },
            );
            created_provider_ids.push(provider_id.clone());
            dynamic_rpc_bindings.push(DynamicRpcBinding {
                binding: binding_name,
                provider_id,
            });
        }
        let validated_policy = match build_dynamic_worker_config(
            env.clone(),
            policy.clone(),
            dynamic_rpc_bindings.clone(),
        ) {
            Ok(config) => config.policy,
            Err(error) => {
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(error)),
                );
                return;
            }
        };
        let result = self
            .deploy_dynamic_internal(
                source,
                env,
                validated_policy.clone(),
                dynamic_rpc_bindings,
                false,
            )
            .await;
        let owner_worker_for_handle = owner_worker.clone();
        let result = result.map(|deployed| {
            let handle = format!("dynh-{}", Uuid::new_v4().simple());
            let worker_name = deployed.worker;
            let worker_generation = self
                .workers
                .get(&worker_name)
                .map(|entry| entry.current_generation)
                .unwrap_or_default();
            let quota_state = self
                .workers
                .get(&worker_name)
                .and_then(|entry| entry.pools.get(&worker_generation))
                .and_then(|pool| pool.dynamic_quota_state.clone())
                .unwrap_or_else(|| Arc::new(DynamicQuotaState::default()));
            self.dynamic_worker_handles.insert(
                handle.clone(),
                DynamicWorkerHandle {
                    id: normalized_id.clone(),
                    owner_worker: owner_worker_for_handle.clone(),
                    owner_generation,
                    binding,
                    worker_name: worker_name.clone(),
                    worker_generation,
                    timeout,
                    policy: validated_policy.clone(),
                    host_rpc_provider_ids: created_provider_ids.clone(),
                    preferred_isolate_id: None,
                    quota_state,
                },
            );
            self.dynamic_worker_ids
                .entry(id_key)
                .or_default()
                .insert(normalized_id, handle.clone());
            crate::ops::DynamicWorkerCreateReply {
                handle,
                worker_name,
                timeout,
            }
        });
        if result.is_err() {
            for provider_id in created_provider_ids {
                self.host_rpc_providers.remove(&provider_id);
            }
        }
        self.dynamic_profile.record_async_reply_completion();
        self.finish_dynamic_reply(
            pending_replies,
            reply_id,
            crate::ops::DynamicPendingReplyPayload::Create(result),
        );
    }

    pub(super) fn handle_dynamic_worker_lookup(
        &mut self,
        payload: crate::ops::DynamicWorkerLookupEvent,
    ) {
        let crate::ops::DynamicWorkerLookupEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id: _owner_isolate_id,
            binding,
            id,
            reply_id,
            pending_replies,
        } = payload;
        let key = DynamicWorkerIdKey {
            owner_worker,
            owner_generation,
            binding,
        };
        let id = id.trim().to_string();
        if id.is_empty() {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Lookup(Err(PlatformError::bad_request(
                    "dynamic worker id must not be empty",
                ))),
            );
            return;
        }
        let handle = self
            .dynamic_worker_ids
            .get(&key)
            .and_then(|by_id| by_id.get(&id))
            .cloned();
        let Some(handle) = handle else {
            self.dynamic_profile.record_async_reply_completion();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Lookup(Ok(None)),
            );
            return;
        };
        let Some(entry) = self.dynamic_worker_handles.get(&handle).cloned() else {
            if let Some(by_id) = self.dynamic_worker_ids.get_mut(&key) {
                by_id.remove(&id);
            }
            self.dynamic_profile.record_async_reply_completion();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Lookup(Ok(None)),
            );
            return;
        };
        self.dynamic_profile.record_async_reply_completion();
        self.finish_dynamic_reply(
            pending_replies,
            reply_id,
            crate::ops::DynamicPendingReplyPayload::Lookup(Ok(Some(
                crate::ops::DynamicWorkerCreateReply {
                    handle,
                    worker_name: entry.worker_name,
                    timeout: entry.timeout,
                },
            ))),
        );
    }

    pub(super) fn handle_dynamic_worker_list(
        &mut self,
        payload: crate::ops::DynamicWorkerListEvent,
    ) {
        let crate::ops::DynamicWorkerListEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id: _owner_isolate_id,
            binding,
            reply_id,
            pending_replies,
        } = payload;
        let key = DynamicWorkerIdKey {
            owner_worker,
            owner_generation,
            binding,
        };
        let mut ids = self
            .dynamic_worker_ids
            .get(&key)
            .map(|by_id| by_id.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        ids.sort();
        self.dynamic_profile.record_async_reply_completion();
        self.finish_dynamic_reply(
            pending_replies,
            reply_id,
            crate::ops::DynamicPendingReplyPayload::List(Ok(ids)),
        );
    }

    pub(super) fn handle_dynamic_worker_delete(
        &mut self,
        payload: crate::ops::DynamicWorkerDeleteEvent,
    ) {
        let crate::ops::DynamicWorkerDeleteEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id: _owner_isolate_id,
            binding,
            id,
            reply_id,
            pending_replies,
        } = payload;
        let key = DynamicWorkerIdKey {
            owner_worker,
            owner_generation,
            binding,
        };
        let id = id.trim().to_string();
        if id.is_empty() {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Delete(Err(PlatformError::bad_request(
                    "dynamic worker id must not be empty",
                ))),
            );
            return;
        }
        let handle = self
            .dynamic_worker_ids
            .get_mut(&key)
            .and_then(|by_id| by_id.remove(&id));
        if self
            .dynamic_worker_ids
            .get(&key)
            .map(|by_id| by_id.is_empty())
            .unwrap_or(false)
        {
            self.dynamic_worker_ids.remove(&key);
        }
        let Some(handle) = handle else {
            self.dynamic_profile.record_async_reply_completion();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Delete(Ok(false)),
            );
            return;
        };
        let Some(_entry) = self.dynamic_worker_handles.get(&handle).cloned() else {
            self.dynamic_profile.record_async_reply_completion();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Delete(Ok(false)),
            );
            return;
        };
        self.retire_dynamic_worker_handle(&handle, "dynamic worker deleted", false);
        self.dynamic_profile.record_async_reply_completion();
        self.finish_dynamic_reply(
            pending_replies,
            reply_id,
            crate::ops::DynamicPendingReplyPayload::Delete(Ok(true)),
        );
    }

    pub(super) fn handle_dynamic_worker_invoke(
        &mut self,
        payload: crate::ops::DynamicWorkerInvokeEvent,
        command_tx: &mpsc::UnboundedSender<RuntimeCommand>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::DynamicWorkerInvokeEvent {
            owner_worker,
            owner_generation,
            owner_isolate_id: _owner_isolate_id,
            binding,
            handle,
            request,
            reply_id,
            pending_replies,
        } = payload;
        let Some(mut handle_entry) = self.dynamic_worker_handles.get(&handle).cloned() else {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::not_found(
                    "dynamic worker handle not found",
                ))),
            );
            return;
        };
        if handle_entry.owner_worker != owner_worker {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::bad_request(
                    "dynamic worker handle owner mismatch",
                ))),
            );
            return;
        }
        if handle_entry.owner_generation != owner_generation {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::bad_request(
                    "dynamic worker handle generation mismatch",
                ))),
            );
            return;
        }
        if handle_entry.binding != binding {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::bad_request(
                    "dynamic worker binding mismatch",
                ))),
            );
            return;
        }
        if request.body.len() > handle_entry.policy.max_request_bytes {
            handle_entry
                .quota_state
                .total_request_bytes
                .fetch_add(request.body.len() as u64, Ordering::Relaxed);
            handle_entry
                .quota_state
                .quota_kill_count
                .fetch_add(1, Ordering::Relaxed);
            self.dynamic_profile.record_quota_kill();
            self.retire_dynamic_worker_handle(
                &handle,
                "dynamic child exceeded max_request_bytes",
                true,
            );
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::runtime(
                    format!(
                        "dynamic child request exceeds max_request_bytes ({})",
                        handle_entry.policy.max_request_bytes
                    ),
                ))),
            );
            return;
        }
        if !self.try_acquire_dynamic_inflight(&handle_entry) {
            self.dynamic_profile.record_quota_kill();
            self.retire_dynamic_worker_handle(
                &handle,
                "dynamic child exceeded max_concurrency",
                true,
            );
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::runtime(
                    format!(
                        "dynamic child exceeded max_concurrency ({})",
                        handle_entry.policy.max_concurrency
                    ),
                ))),
            );
            return;
        }
        handle_entry
            .quota_state
            .total_request_bytes
            .fetch_add(request.body.len() as u64, Ordering::Relaxed);

        let target_generation = Some(handle_entry.worker_generation);
        let mut target_isolate_id = handle_entry.preferred_isolate_id;
        if let Some(preferred_id) = target_isolate_id {
            let preferred_alive = self
                .workers
                .get(&handle_entry.worker_name)
                .and_then(|entry| entry.pools.get(&handle_entry.worker_generation))
                .map(|pool| {
                    pool.isolates
                        .iter()
                        .any(|isolate| isolate.id == preferred_id)
                })
                .unwrap_or(false);
            if preferred_alive {
                self.dynamic_profile.record_warm_isolate_hit();
            } else {
                target_isolate_id = None;
                handle_entry.preferred_isolate_id = None;
                if let Some(entry) = self.dynamic_worker_handles.get_mut(&handle) {
                    entry.preferred_isolate_id = None;
                }
                self.dynamic_profile.record_fallback_dispatch();
            }
        } else {
            self.dynamic_profile.record_fallback_dispatch();
        }

        let runtime_request_id = next_runtime_token("dyn");
        let timeout = handle_entry.timeout;
        let timeout_diagnostic = DynamicTimeoutDiagnostic {
            stage: "invoke-reply",
            owner_worker: owner_worker.clone(),
            owner_generation,
            binding: binding.clone(),
            handle: handle.clone(),
            target_worker: handle_entry.worker_name.clone(),
            target_isolate_id,
            target_generation,
            provider_id: None,
            provider_owner_isolate_id: None,
            provider_target_id: None,
            timeout_ms: timeout,
        };
        let (inner_reply_tx, inner_reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            handle_entry.worker_name,
            runtime_request_id,
            request,
            None,
            None,
            None,
            None,
            target_isolate_id,
            target_generation,
            true,
            inner_reply_tx,
            PendingReplyKind::DynamicInvoke {
                handle: handle.clone(),
            },
            event_tx,
        );
        let event_tx = event_tx.clone();
        let command_tx = command_tx.clone();
        let profile = self.dynamic_profile.clone();
        let handle_for_task = handle.clone();
        let policy = handle_entry.policy.clone();
        let quota_state = handle_entry.quota_state.clone();
        tokio::spawn(async move {
            let result =
                match tokio::time::timeout(Duration::from_millis(timeout), inner_reply_rx).await {
                    Ok(Ok(output)) => output,
                    Ok(Err(_)) => Err(PlatformError::internal(
                        "dynamic worker invoke response channel closed",
                    )),
                    Err(_) => {
                        let _ = event_tx
                            .send(RuntimeEvent::DynamicTimeoutDiagnostic(timeout_diagnostic));
                        Err(PlatformError::runtime(format!(
                            "dynamic worker invoke timed out after {timeout}ms"
                        )))
                    }
                };
            quota_state.inflight.fetch_sub(1, Ordering::Relaxed);
            let result = match result {
                Ok(output) if output.body.len() > policy.max_response_bytes => {
                    quota_state
                        .total_response_bytes
                        .fetch_add(output.body.len() as u64, Ordering::Relaxed);
                    quota_state.quota_kill_count.fetch_add(1, Ordering::Relaxed);
                    profile.record_quota_kill();
                    let _ = command_tx.send(RuntimeCommand::RetireDynamicWorkerHandle {
                        handle: handle_for_task.clone(),
                        reason: "dynamic child exceeded max_response_bytes".to_string(),
                    });
                    Err(PlatformError::runtime(format!(
                        "dynamic child response exceeds max_response_bytes ({})",
                        policy.max_response_bytes
                    )))
                }
                Ok(output) => {
                    quota_state
                        .total_response_bytes
                        .fetch_add(output.body.len() as u64, Ordering::Relaxed);
                    Ok(output)
                }
                Err(error) => Err(error),
            };
            profile.record_async_reply_completion();
            if let Some(delivery) = pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(result),
            ) {
                let _ = event_tx.send(RuntimeEvent::DynamicReplyReady(delivery));
            }
        });
    }

    pub(super) fn start_dynamic_worker_fetch(
        &mut self,
        owner_worker: String,
        owner_generation: u64,
        binding: String,
        handle: String,
        request: WorkerInvocation,
        reply_id: String,
        pending_replies: crate::ops::DynamicPendingReplies,
        command_tx: &mpsc::UnboundedSender<RuntimeCommand>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let Some(mut handle_entry) = self.dynamic_worker_handles.get(&handle).cloned() else {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Fetch {
                    result: Err(PlatformError::not_found("dynamic worker handle not found")),
                    stale_handle: true,
                    boundary: None,
                },
            );
            return;
        };
        if handle_entry.owner_worker != owner_worker {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Fetch {
                    result: Err(PlatformError::bad_request(
                        "dynamic worker handle owner mismatch",
                    )),
                    stale_handle: true,
                    boundary: None,
                },
            );
            return;
        }
        if handle_entry.owner_generation != owner_generation {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Fetch {
                    result: Err(PlatformError::bad_request(
                        "dynamic worker handle generation mismatch",
                    )),
                    stale_handle: true,
                    boundary: None,
                },
            );
            return;
        }
        if handle_entry.binding != binding {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Fetch {
                    result: Err(PlatformError::bad_request(
                        "dynamic worker binding mismatch",
                    )),
                    stale_handle: true,
                    boundary: None,
                },
            );
            return;
        }
        if request.body.len() > handle_entry.policy.max_request_bytes {
            handle_entry
                .quota_state
                .total_request_bytes
                .fetch_add(request.body.len() as u64, Ordering::Relaxed);
            handle_entry
                .quota_state
                .quota_kill_count
                .fetch_add(1, Ordering::Relaxed);
            self.dynamic_profile.record_quota_kill();
            self.retire_dynamic_worker_handle(
                &handle,
                "dynamic child exceeded max_request_bytes",
                true,
            );
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Fetch {
                    result: Err(PlatformError::runtime(format!(
                        "dynamic child request exceeds max_request_bytes ({})",
                        handle_entry.policy.max_request_bytes
                    ))),
                    stale_handle: true,
                    boundary: None,
                },
            );
            return;
        }
        if !self.try_acquire_dynamic_inflight(&handle_entry) {
            self.dynamic_profile.record_quota_kill();
            self.retire_dynamic_worker_handle(
                &handle,
                "dynamic child exceeded max_concurrency",
                true,
            );
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Fetch {
                    result: Err(PlatformError::runtime(format!(
                        "dynamic child exceeded max_concurrency ({})",
                        handle_entry.policy.max_concurrency
                    ))),
                    stale_handle: true,
                    boundary: None,
                },
            );
            return;
        }
        handle_entry
            .quota_state
            .total_request_bytes
            .fetch_add(request.body.len() as u64, Ordering::Relaxed);

        let target_generation = Some(handle_entry.worker_generation);
        let mut target_isolate_id = handle_entry.preferred_isolate_id;
        if let Some(preferred_id) = target_isolate_id {
            let preferred_alive = self
                .workers
                .get(&handle_entry.worker_name)
                .and_then(|entry| entry.pools.get(&handle_entry.worker_generation))
                .map(|pool| {
                    pool.isolates
                        .iter()
                        .any(|isolate| isolate.id == preferred_id)
                })
                .unwrap_or(false);
            if preferred_alive {
                self.dynamic_profile.record_warm_isolate_hit();
            } else {
                target_isolate_id = None;
                handle_entry.preferred_isolate_id = None;
                if let Some(entry) = self.dynamic_worker_handles.get_mut(&handle) {
                    entry.preferred_isolate_id = None;
                }
                self.dynamic_profile.record_fallback_dispatch();
            }
        } else {
            self.dynamic_profile.record_fallback_dispatch();
        }

        let runtime_request_id = next_runtime_token("dyn");
        let timeout = handle_entry.timeout;
        let timeout_diagnostic = DynamicTimeoutDiagnostic {
            stage: "invoke-reply",
            owner_worker: owner_worker.clone(),
            owner_generation,
            binding: binding.clone(),
            handle: handle.clone(),
            target_worker: handle_entry.worker_name.clone(),
            target_isolate_id,
            target_generation,
            provider_id: None,
            provider_owner_isolate_id: None,
            provider_target_id: None,
            timeout_ms: timeout,
        };
        let dispatch_started = Instant::now();
        let (inner_reply_tx, inner_reply_rx) = oneshot::channel();
        let child_worker_name = handle_entry.worker_name.clone();
        let mut queue_target_isolate_id = None;
        if let Some(preferred_id) = target_isolate_id {
            match self.try_dispatch_direct_dynamic_fetch(
                &child_worker_name,
                handle_entry.worker_generation,
                preferred_id,
                runtime_request_id.clone(),
                request.clone(),
                inner_reply_tx,
                handle.clone(),
            ) {
                DirectDynamicFetchDispatch::Dispatched => {
                    self.dynamic_profile.record_direct_fetch_fast_path_hit();
                }
                DirectDynamicFetchDispatch::Fallback {
                    reply,
                    clear_preferred,
                } => {
                    self.dynamic_profile
                        .record_direct_fetch_fast_path_fallback();
                    self.dynamic_profile.record_fallback_dispatch();
                    if clear_preferred {
                        handle_entry.preferred_isolate_id = None;
                        if let Some(entry) = self.dynamic_worker_handles.get_mut(&handle) {
                            entry.preferred_isolate_id = None;
                        }
                    }
                    self.enqueue_invoke(
                        child_worker_name,
                        runtime_request_id,
                        request,
                        None,
                        None,
                        None,
                        None,
                        queue_target_isolate_id.take(),
                        target_generation,
                        true,
                        reply,
                        PendingReplyKind::DynamicFetch {
                            handle: handle.clone(),
                        },
                        event_tx,
                    );
                }
            }
        } else {
            self.dynamic_profile
                .record_direct_fetch_fast_path_fallback();
            self.enqueue_invoke(
                child_worker_name,
                runtime_request_id,
                request,
                None,
                None,
                None,
                None,
                queue_target_isolate_id.take(),
                target_generation,
                true,
                inner_reply_tx,
                PendingReplyKind::DynamicFetch {
                    handle: handle.clone(),
                },
                event_tx,
            );
        }
        self.dynamic_profile
            .record_direct_fetch_dispatch(dispatch_started.elapsed());

        let event_tx = event_tx.clone();
        let command_tx = command_tx.clone();
        let profile = self.dynamic_profile.clone();
        let handle_for_task = handle.clone();
        let policy = handle_entry.policy.clone();
        let quota_state = handle_entry.quota_state.clone();
        tokio::spawn(async move {
            let child_started = Instant::now();
            let result =
                match tokio::time::timeout(Duration::from_millis(timeout), inner_reply_rx).await {
                    Ok(Ok(output)) => output,
                    Ok(Err(_)) => Err(PlatformError::internal(
                        "dynamic worker invoke response channel closed",
                    )),
                    Err(_) => {
                        let _ = event_tx
                            .send(RuntimeEvent::DynamicTimeoutDiagnostic(timeout_diagnostic));
                        Err(PlatformError::runtime(format!(
                            "dynamic worker invoke timed out after {timeout}ms"
                        )))
                    }
                };
            quota_state.inflight.fetch_sub(1, Ordering::Relaxed);
            let result = match result {
                Ok(output) => {
                    quota_state
                        .total_response_bytes
                        .fetch_add(output.body.len() as u64, Ordering::Relaxed);
                    if output.body.len() > policy.max_response_bytes {
                        quota_state.quota_kill_count.fetch_add(1, Ordering::Relaxed);
                        profile.record_quota_kill();
                        let _ = command_tx.send(RuntimeCommand::RetireDynamicWorkerHandle {
                            handle: handle_for_task.clone(),
                            reason: "dynamic child exceeded max_response_bytes".to_string(),
                        });
                        Err(PlatformError::runtime(format!(
                            "dynamic child response exceeds max_response_bytes ({})",
                            policy.max_response_bytes
                        )))
                    } else if !policy.allow_websocket
                        && internal_header_value(&output.headers, INTERNAL_WS_ACCEPT_HEADER)
                            .as_deref()
                            == Some("1")
                    {
                        quota_state.upgrade_deny_count.fetch_add(1, Ordering::Relaxed);
                        profile.record_upgrade_deny();
                        let _ = command_tx.send(RuntimeCommand::RetireDynamicWorkerHandle {
                            handle: handle_for_task.clone(),
                            reason: "dynamic child attempted websocket upgrade without permission"
                                .to_string(),
                        });
                        Err(PlatformError::runtime(
                            "dynamic child policy blocks websocket upgrade",
                        ))
                    } else if !policy.allow_transport
                        && internal_header_value(&output.headers, INTERNAL_TRANSPORT_ACCEPT_HEADER)
                            .as_deref()
                            == Some("1")
                    {
                        quota_state.upgrade_deny_count.fetch_add(1, Ordering::Relaxed);
                        profile.record_upgrade_deny();
                        let _ = command_tx.send(RuntimeCommand::RetireDynamicWorkerHandle {
                            handle: handle_for_task.clone(),
                            reason: "dynamic child attempted transport upgrade without permission"
                                .to_string(),
                        });
                        Err(PlatformError::runtime(
                            "dynamic child policy blocks transport upgrade",
                        ))
                    } else {
                        Ok(output)
                    }
                }
                Err(error) => Err(error),
            };
            profile.record_async_reply_completion();
            profile.record_direct_fetch_child_execute(child_started.elapsed());
            let stale_handle = result.is_err();
            if let Some(delivery) = pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Fetch {
                    result,
                    stale_handle,
                    boundary: Some(crate::ops::current_time_boundary()),
                },
            ) {
                let _ = event_tx.send(RuntimeEvent::DynamicFetchReplyReady(delivery));
            }
        });
    }

    pub(super) fn handle_dynamic_host_rpc_invoke(
        &mut self,
        payload: crate::ops::DynamicHostRpcInvokeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::DynamicHostRpcInvokeEvent {
            caller_worker,
            caller_generation,
            _caller_isolate_id: _,
            binding,
            method_name,
            args,
            reply_id,
            pending_replies,
        } = payload;
        if host_rpc_method_blocked(&method_name) {
            self.record_dynamic_rpc_deny_for_pool(&caller_worker, caller_generation);
            self.dynamic_profile.record_rpc_deny();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::bad_request(
                    format!("dynamic host rpc method is blocked: {method_name}"),
                ))),
            );
            return;
        }
        if args.len() > MAX_DYNAMIC_HOST_RPC_ARG_BYTES {
            self.record_dynamic_rpc_deny_for_pool(&caller_worker, caller_generation);
            self.dynamic_profile.record_rpc_deny();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::bad_request(
                    format!(
                        "dynamic host rpc args exceed limit ({MAX_DYNAMIC_HOST_RPC_ARG_BYTES} bytes)"
                    ),
                ))),
            );
            return;
        }

        let provider_id = {
            let Some(pool) = self.get_pool_mut(&caller_worker, caller_generation) else {
                self.finish_dynamic_reply(
                    pending_replies,
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::not_found(
                        "dynamic worker pool not found",
                    ))),
                );
                return;
            };
            if let Some(policy) = &pool.dynamic_child_policy {
                if !policy.allow_host_rpc {
                    if let Some(quota_state) = &pool.dynamic_quota_state {
                        quota_state.rpc_deny_count.fetch_add(1, Ordering::Relaxed);
                    }
                    self.dynamic_profile.record_rpc_deny();
                    self.finish_dynamic_reply(
                        pending_replies,
                        reply_id,
                        crate::ops::DynamicPendingReplyPayload::HostRpc(Err(
                            PlatformError::bad_request(
                                "dynamic child policy blocks host RPC; set allow_host_rpc: true",
                            ),
                        )),
                    );
                    return;
                }
            }
            pool.dynamic_rpc_bindings
                .iter()
                .find(|entry| entry.binding == binding)
                .map(|entry| entry.provider_id.clone())
        };
        let Some(provider_id) = provider_id else {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::bad_request(
                    format!("dynamic host rpc binding not found: {binding}"),
                ))),
            );
            return;
        };
        let Some(provider) = self.host_rpc_providers.get(&provider_id).cloned() else {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::not_found(
                    "dynamic host rpc provider not found",
                ))),
            );
            return;
        };
        if !provider.methods.contains(&method_name) {
            self.record_dynamic_rpc_deny_for_pool(&caller_worker, caller_generation);
            self.dynamic_profile.record_rpc_deny();
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::bad_request(
                    format!("dynamic host rpc method is not allowed: {method_name}"),
                ))),
            );
            return;
        }
        let owner_pool_generation = self
            .get_pool_mut(&provider.owner_worker, provider.owner_generation)
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .any(|isolate| isolate.id == provider.owner_isolate_id)
                    .then_some(pool.generation)
            });
        let Some(owner_pool_generation) = owner_pool_generation else {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::runtime(
                    "dynamic host rpc provider isolate is unavailable",
                ))),
            );
            return;
        };
        let provider_delivery = self
            .get_pool_mut(&provider.owner_worker, owner_pool_generation)
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == provider.owner_isolate_id)
                    .map(|isolate| isolate.id)
            });
        let Some(provider_isolate_id) = provider_delivery else {
            self.finish_dynamic_reply(
                pending_replies,
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::runtime(
                    "dynamic host rpc provider isolate is unavailable",
                ))),
            );
            return;
        };
        let reply_id_for_error = reply_id.clone();
        let pending_replies_for_error = pending_replies.clone();
        if let Err(error) = self.start_targeted_host_rpc_invoke(
            provider.owner_worker,
            owner_pool_generation,
            provider_isolate_id,
            provider.target_id,
            method_name,
            args,
            TargetedHostRpcReply::Dynamic {
                reply_id,
                pending_replies,
            },
            event_tx,
        ) {
            self.finish_dynamic_reply(
                pending_replies_for_error,
                reply_id_for_error,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(error)),
            );
        }
    }

    pub(super) async fn validate_worker_cached(&mut self, source: &str) -> Result<()> {
        let source_hash = Sha256::digest(source.as_bytes()).into();
        if self.validated_worker_sources.contains(&source_hash) {
            return Ok(());
        }
        validate_worker(self.bootstrap_snapshot, source).await?;
        self.validated_worker_sources.insert(source_hash);
        Ok(())
    }

    pub(super) fn try_acquire_dynamic_inflight(&self, handle: &DynamicWorkerHandle) -> bool {
        let inflight = handle.quota_state.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        if inflight <= handle.policy.max_concurrency {
            return true;
        }
        handle.quota_state.inflight.fetch_sub(1, Ordering::Relaxed);
        handle
            .quota_state
            .quota_kill_count
            .fetch_add(1, Ordering::Relaxed);
        false
    }

    fn record_dynamic_rpc_deny_for_pool(&mut self, worker_name: &str, generation: u64) {
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(quota_state) = &pool.dynamic_quota_state {
                quota_state.rpc_deny_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn retire_dynamic_worker_handle(
        &mut self,
        handle: &str,
        reason: &str,
        policy_violation: bool,
    ) {
        let Some(entry) = self.dynamic_worker_handles.remove(handle) else {
            return;
        };
        for provider_id in &entry.host_rpc_provider_ids {
            self.host_rpc_providers.remove(provider_id);
        }
        let id_key = DynamicWorkerIdKey {
            owner_worker: entry.owner_worker.clone(),
            owner_generation: entry.owner_generation,
            binding: entry.binding.clone(),
        };
        if let Some(by_id) = self.dynamic_worker_ids.get_mut(&id_key) {
            by_id.remove(&entry.id);
            if by_id.is_empty() {
                self.dynamic_worker_ids.remove(&id_key);
            }
        }
        if policy_violation {
            warn!(
                handle = handle,
                worker = %entry.worker_name,
                owner_worker = %entry.owner_worker,
                binding = %entry.binding,
                reason,
                "retiring dynamic child worker after policy violation"
            );
        } else {
            info!(
                handle = handle,
                worker = %entry.worker_name,
                owner_worker = %entry.owner_worker,
                binding = %entry.binding,
                reason,
                "retiring dynamic child worker"
            );
        }
        self.retire_worker_completely(&entry.worker_name);
    }

    pub(super) fn retire_dynamic_worker_by_worker_name(
        &mut self,
        worker_name: &str,
        reason: &str,
        policy_violation: bool,
    ) {
        let handle = self
            .dynamic_worker_handles
            .iter()
            .find(|(_, entry)| entry.worker_name == worker_name)
            .map(|(handle, _)| handle.clone());
        if let Some(handle) = handle {
            self.retire_dynamic_worker_handle(&handle, reason, policy_violation);
            return;
        }
        if policy_violation {
            warn!(worker = worker_name, reason, "retiring dynamic worker without handle mapping");
        }
        self.retire_worker_completely(worker_name);
    }

    pub(super) async fn dynamic_worker_snapshot_cached(
        &mut self,
        source: &str,
    ) -> Option<&'static [u8]> {
        let source_hash: [u8; 32] = Sha256::digest(source.as_bytes()).into();
        if let Some(snapshot) = self.dynamic_worker_snapshots.get(&source_hash).copied() {
            return Some(snapshot);
        }
        if self.dynamic_worker_snapshot_failures.contains(&source_hash) {
            return None;
        }

        match build_worker_snapshot(self.bootstrap_snapshot, source).await {
            Ok(snapshot) => match validate_loaded_worker_runtime(snapshot) {
                Ok(()) => {
                    self.dynamic_worker_snapshots.insert(source_hash, snapshot);
                    Some(snapshot)
                }
                Err(error) => {
                    self.dynamic_worker_snapshot_failures.insert(source_hash);
                    warn!(
                        error = %error,
                        "dynamic worker snapshot validation failed; falling back to bootstrap snapshot"
                    );
                    None
                }
            },
            Err(error) => {
                self.dynamic_worker_snapshot_failures.insert(source_hash);
                warn!(
                    error = %error,
                    "dynamic worker snapshot build failed; falling back to bootstrap snapshot"
                );
                None
            }
        }
    }
}
