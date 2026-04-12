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
            host_rpc_bindings,
            reply_id,
            pending_replies,
        } = payload;
        let normalized_id = id.trim().to_string();
        if normalized_id.is_empty() {
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Create(Err(PlatformError::bad_request(
                    "dynamic worker id must not be empty",
                ))),
            );
            return;
        }
        let timeout = timeout.clamp(1, 60_000);
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
                pending_replies.finish(
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
                pending_replies.finish(
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request("dynamic host rpc binding must not be empty"),
                    )),
                );
                return;
            }
            if target_id.is_empty() {
                pending_replies.finish(
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::Create(Err(
                        PlatformError::bad_request("dynamic host rpc target_id must not be empty"),
                    )),
                );
                return;
            }
            if !seen_binding_names.insert(binding_name.clone()) {
                pending_replies.finish(
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
        let result = self
            .deploy_dynamic_internal(source, env, Vec::new(), dynamic_rpc_bindings, false)
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
            self.dynamic_worker_handles.insert(
                handle.clone(),
                DynamicWorkerHandle {
                    owner_worker: owner_worker_for_handle.clone(),
                    owner_generation,
                    binding,
                    worker_name: worker_name.clone(),
                    worker_generation,
                    timeout,
                    preferred_isolate_id: None,
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
        pending_replies.finish(
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
            pending_replies.finish(
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
            pending_replies.finish(
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
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Lookup(Ok(None)),
            );
            return;
        };
        self.dynamic_profile.record_async_reply_completion();
        pending_replies.finish(
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
        pending_replies.finish(
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
            pending_replies.finish(
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
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Delete(Ok(false)),
            );
            return;
        };
        let Some(entry) = self.dynamic_worker_handles.remove(&handle) else {
            self.dynamic_profile.record_async_reply_completion();
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Delete(Ok(false)),
            );
            return;
        };
        self.retire_worker_completely(&entry.worker_name);
        self.dynamic_profile.record_async_reply_completion();
        pending_replies.finish(
            reply_id,
            crate::ops::DynamicPendingReplyPayload::Delete(Ok(true)),
        );
    }

    pub(super) fn handle_dynamic_worker_invoke(
        &mut self,
        payload: crate::ops::DynamicWorkerInvokeEvent,
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
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::not_found(
                    "dynamic worker handle not found",
                ))),
            );
            return;
        };
        if handle_entry.owner_worker != owner_worker {
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::bad_request(
                    "dynamic worker handle owner mismatch",
                ))),
            );
            return;
        }
        if handle_entry.owner_generation != owner_generation {
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::bad_request(
                    "dynamic worker handle generation mismatch",
                ))),
            );
            return;
        }
        if handle_entry.binding != binding {
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(Err(PlatformError::bad_request(
                    "dynamic worker binding mismatch",
                ))),
            );
            return;
        }

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
        let profile = self.dynamic_profile.clone();
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
            profile.record_async_reply_completion();
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::Invoke(result),
            );
        });
    }

    pub(super) fn handle_dynamic_host_rpc_invoke(
        &mut self,
        payload: crate::ops::DynamicHostRpcInvokeEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
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
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::bad_request(
                    format!("dynamic host rpc method is blocked: {method_name}"),
                ))),
            );
            return;
        }

        let provider_id = {
            let Some(pool) = self.get_pool_mut(&caller_worker, caller_generation) else {
                pending_replies.finish(
                    reply_id,
                    crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::not_found(
                        "dynamic worker pool not found",
                    ))),
                );
                return;
            };
            pool.dynamic_rpc_bindings
                .iter()
                .find(|entry| entry.binding == binding)
                .map(|entry| entry.provider_id.clone())
        };
        let Some(provider_id) = provider_id else {
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::bad_request(
                    format!("dynamic host rpc binding not found: {binding}"),
                ))),
            );
            return;
        };
        let Some(provider) = self.host_rpc_providers.get(&provider_id).cloned() else {
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::not_found(
                    "dynamic host rpc provider not found",
                ))),
            );
            return;
        };
        if !provider.methods.contains(&method_name) {
            pending_replies.finish(
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
            pending_replies.finish(
                reply_id,
                crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::runtime(
                    "dynamic host rpc provider isolate is unavailable",
                ))),
            );
            return;
        };
        let local_queue = self
            .get_pool_mut(&provider.owner_worker, owner_pool_generation)
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == provider.owner_isolate_id)
                    .map(|isolate| {
                        (
                            isolate.dynamic_host_rpc_queue.clone(),
                            isolate.sender.clone(),
                        )
                    })
            });
        if let Some((queue, sender)) = local_queue {
            queue.enqueue(
                provider.target_id.clone(),
                method_name,
                args,
                reply_id,
                pending_replies,
            );
            let _ = sender.send(IsolateCommand::RunDynamicHostRpcTasks);
            return;
        }
        pending_replies.finish(
            reply_id,
            crate::ops::DynamicPendingReplyPayload::HostRpc(Err(PlatformError::runtime(
                "dynamic host rpc provider isolate is unavailable",
            ))),
        );
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
