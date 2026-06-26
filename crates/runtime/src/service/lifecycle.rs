use super::dispatch::estimate_pending_invoke_bytes;
use super::*;

impl WorkerManager {
    pub(crate) async fn deploy(
        &mut self,
        prepared: PreparedWorkerDeployment,
        persist: bool,
        temporary: bool,
        expires_at_ms: Option<i64>,
        enforce_temporary_transition: bool,
    ) -> Result<String> {
        let PreparedWorkerDeployment {
            worker_name,
            source,
            config,
            assets,
            server_modules,
            asset_headers,
            compiled_assets,
            bindings,
        } = prepared;
        if temporary
            && enforce_temporary_transition
            && self.current_worker_is_permanent(&worker_name)
        {
            return Err(PlatformError::conflict(
                "cannot deploy a permanent worker as temporary; redeploy without --temporary or use a new worker name",
            ));
        }
        let expires_at_ms = if temporary {
            Some(expires_at_ms.unwrap_or(self.temporary_worker_expires_at_ms()?))
        } else {
            None
        };
        let worker_source = deployed_worker_source(&source, &server_modules)?;
        if let Err(error) = self.validate_worker_cached(&worker_source).await {
            release_worker_source_modules(&worker_source);
            return Err(error);
        }
        // Keep deployed workers on the bootstrap snapshot. Distinct user-code snapshots
        // can abort V8 when multiple complex workers are instantiated in one process.
        let (snapshot, snapshot_preloaded) = (self.bootstrap_snapshot, false);
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
        let service_bindings = bindings
            .service
            .iter()
            .map(|binding| crate::ops::WorkerServiceBindingPayload {
                binding: binding.binding.clone(),
                service: binding.service.clone(),
            })
            .collect::<Vec<_>>();
        let deployment_config = Arc::new(crate::ops::WorkerDeploymentPayload {
            worker_name: worker_name.clone(),
            kv_bindings: bindings.kv.clone(),
            kv_read_cache_config: crate::ops::WorkerKvReadCacheConfigPayload {
                max_entries: self.config.kv_read_cache_max_entries,
                max_bytes: self.config.kv_read_cache_max_bytes,
                hit_ttl_ms: self.config.kv_read_cache_hit_ttl.as_millis() as u64,
                miss_ttl_ms: self.config.kv_read_cache_miss_ttl.as_millis() as u64,
            },
            memory_bindings: bindings.memory.clone(),
            dynamic_bindings: bindings.dynamic.clone(),
            dynamic_rpc_bindings: Vec::new(),
            service_bindings: service_bindings.clone(),
            dynamic_env: Vec::new(),
        });
        let request_context = RequestExecutionContext::new(RequestExecutionContextInit {
            worker_name: worker_name.clone(),
            generation,
            dynamic_bindings: bindings.dynamic.clone(),
            dynamic_rpc_bindings: Vec::new(),
            service_bindings,
            replacements: Vec::new(),
            egress_allow_hosts: Vec::new(),
            allow_cache: true,
            max_outbound_requests: None,
            dynamic_quota_state: None,
        });
        if persist {
            if let Err(error) = persist_worker_deployment(PersistWorkerDeployment {
                storage: &self.storage,
                worker_name: &worker_name,
                source: &source,
                config: &config,
                assets: &assets,
                server_modules: &server_modules,
                asset_headers: asset_headers.as_deref(),
                deployment_id: &deployment_id,
                expires_at_ms,
            })
            .await
            {
                release_worker_source_modules(&worker_source);
                return Err(error);
            }
        }
        let asset_catalog_entry = AssetCatalogEntry {
            worker_name: worker_name.clone(),
            generation,
            assets: compiled_assets.clone(),
            public: config.public,
        };
        let pool =
            WorkerPool {
                worker_name: worker_name.clone(),
                generation,
                deployment_id: deployment_id.clone(),
                internal_trace: config.internal.trace.as_ref().map(|trace| {
                    InternalTraceDestination {
                        worker: trace.worker.trim().to_string(),
                        path: normalize_trace_path(&trace.path),
                    }
                }),
                is_public: config.public,
                expires_at_ms,
                snapshot,
                snapshot_preloaded,
                source: worker_source,
                memory_bindings: bindings.memory,
                dynamic_rpc_bindings: Vec::new(),
                deployment_config,
                request_context,
                dynamic_child_policy: None,
                dynamic_quota_state: None,
                strict_request_isolation: false,
                memory_entity_leases: HashMap::new(),
                memory_shard_affinity: HashMap::new(),
                queue: PendingInvokeQueue::new(),
                isolates: Vec::new(),
                isolate_indices: HashMap::new(),
                stats: PoolStats::default(),
                queue_warn_level: 0,
            };

        let entry = self
            .workers
            .entry(worker_name.clone())
            .or_insert_with(|| WorkerEntry {
                current_generation: generation,
                pools: HashMap::new(),
            });
        entry.current_generation = generation;
        entry.pools.insert(generation, pool);
        self.asset_catalog
            .insert(worker_name.clone(), asset_catalog_entry);
        self.cleanup_drained_generations_for(&worker_name);
        info!(
            worker = %worker_name,
            generation,
            deployment_id = %deployment_id,
            temporary = expires_at_ms.is_some(),
            expires_at_ms,
            "deployed worker"
        );
        Ok(deployment_id)
    }

    fn current_worker_is_permanent(&self, worker_name: &str) -> bool {
        self.workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&entry.current_generation))
            .is_some_and(|pool| pool.expires_at_ms.is_none())
    }

    fn temporary_worker_expires_at_ms(&self) -> Result<i64> {
        let now_ms = epoch_ms_i64()?;
        let ttl_ms = i64::try_from(self.config.temporary_worker_ttl.as_millis())
            .map_err(|_| PlatformError::internal("temporary worker ttl is too large"))?;
        now_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| PlatformError::internal("temporary worker expiration overflow"))
    }

    pub(crate) async fn expire_temporary_workers(&mut self) {
        let now_ms = match epoch_ms_i64() {
            Ok(now_ms) => now_ms,
            Err(error) => {
                warn!(error = %error, "failed to read clock while expiring temporary workers");
                return;
            }
        };
        let expired = self
            .workers
            .iter()
            .filter_map(|(worker_name, entry)| {
                let pool = entry.pools.get(&entry.current_generation)?;
                let expires_at_ms = pool.expires_at_ms?;
                (expires_at_ms <= now_ms).then(|| worker_name.clone())
            })
            .collect::<Vec<_>>();

        for worker_name in expired {
            self.retire_worker_completely_with_error(
                &worker_name,
                PlatformError::not_found("temporary worker expired"),
            );
            if let Err(error) = delete_worker_deployment(&self.storage, &worker_name).await {
                warn!(
                    worker = %worker_name,
                    error = %error,
                    "failed to remove expired worker deployment from local store"
                );
            }
            info!(worker = %worker_name, "expired temporary worker");
        }
    }

    pub(crate) async fn deploy_dynamic(
        &mut self,
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
        dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    ) -> Result<DynamicDeployResult> {
        self.deploy_dynamic_internal(
            crate::ops::WorkerSource::inline(source),
            env,
            Vec::new(),
            full_dynamic_internal_policy(egress_allow_hosts),
            dynamic_rpc_bindings,
            true,
        )
        .await
    }

    pub(crate) async fn deploy_dynamic_internal(
        &mut self,
        source: crate::ops::WorkerSource,
        env: HashMap<String, String>,
        bindings: Vec<DeployBinding>,
        policy: ValidatedDynamicWorkerPolicy,
        dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
        validate_source: bool,
    ) -> Result<DynamicDeployResult> {
        let worker_name = format!("dyn-{}", Uuid::new_v4().simple());
        let dynamic_config = build_dynamic_worker_config(
            env,
            bindings,
            crate::ops::DynamicWorkerPolicy {
                egress_allow_hosts: policy.egress_allow_hosts.clone(),
                allow_host_rpc: policy.allow_host_rpc,
                allow_websocket: policy.allow_websocket,
                allow_transport: policy.allow_transport,
                allow_state_bindings: policy.allow_state_bindings,
                max_request_bytes: policy.max_request_bytes as u64,
                max_response_bytes: policy.max_response_bytes as u64,
                max_outbound_requests: policy.max_outbound_requests,
                max_concurrency: policy.max_concurrency as u64,
            },
            dynamic_rpc_bindings,
        )?;
        if validate_source {
            self.validate_worker_cached(&source).await?;
        }
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
        let dynamic_rpc_binding_names = dynamic_config
            .dynamic_rpc_bindings
            .iter()
            .map(|binding| binding.binding.clone())
            .collect::<Vec<_>>();
        let service_bindings = dynamic_config
            .bindings
            .service
            .iter()
            .map(|binding| crate::ops::WorkerServiceBindingPayload {
                binding: binding.binding.clone(),
                service: binding.service.clone(),
            })
            .collect::<Vec<_>>();
        let dynamic_quota_state = Arc::new(DynamicQuotaState::default());
        let request_context = RequestExecutionContext::new(RequestExecutionContextInit {
            worker_name: worker_name.clone(),
            generation,
            dynamic_bindings: dynamic_config.bindings.dynamic.clone(),
            dynamic_rpc_bindings: dynamic_rpc_binding_names.clone(),
            service_bindings: service_bindings.clone(),
            replacements: dynamic_config.secret_replacements,
            egress_allow_hosts: dynamic_config.egress_allow_hosts,
            allow_cache: dynamic_config.policy.allow_state_bindings,
            max_outbound_requests: Some(dynamic_config.policy.max_outbound_requests),
            dynamic_quota_state: Some(Arc::clone(&dynamic_quota_state)),
        });
        let (snapshot, snapshot_preloaded) = (self.bootstrap_snapshot, false);
        let deployment_config = Arc::new(crate::ops::WorkerDeploymentPayload {
            worker_name: worker_name.clone(),
            kv_bindings: dynamic_config.bindings.kv.clone(),
            kv_read_cache_config: crate::ops::WorkerKvReadCacheConfigPayload {
                max_entries: self.config.kv_read_cache_max_entries,
                max_bytes: self.config.kv_read_cache_max_bytes,
                hit_ttl_ms: self.config.kv_read_cache_hit_ttl.as_millis() as u64,
                miss_ttl_ms: self.config.kv_read_cache_miss_ttl.as_millis() as u64,
            },
            memory_bindings: dynamic_config.bindings.memory.clone(),
            dynamic_bindings: dynamic_config.bindings.dynamic.clone(),
            dynamic_rpc_bindings: dynamic_rpc_binding_names,
            service_bindings,
            dynamic_env: dynamic_config.dynamic_env.clone(),
        });

        let pool = WorkerPool {
            worker_name: worker_name.clone(),
            generation,
            deployment_id: deployment_id.clone(),
            internal_trace: None,
            is_public: false,
            expires_at_ms: None,
            snapshot,
            snapshot_preloaded,
            source,
            memory_bindings: dynamic_config.bindings.memory.clone(),
            dynamic_rpc_bindings: dynamic_config.dynamic_rpc_bindings.clone(),
            deployment_config,
            request_context,
            dynamic_child_policy: Some(dynamic_config.policy.clone()),
            dynamic_quota_state: Some(dynamic_quota_state),
            strict_request_isolation: false,
            memory_entity_leases: HashMap::new(),
            memory_shard_affinity: HashMap::new(),
            queue: PendingInvokeQueue::new(),
            isolates: Vec::new(),
            isolate_indices: HashMap::new(),
            stats: PoolStats::default(),
            queue_warn_level: 0,
        };

        let entry = self
            .workers
            .entry(worker_name.clone())
            .or_insert_with(|| WorkerEntry {
                current_generation: generation,
                pools: HashMap::new(),
            });
        entry.current_generation = generation;
        entry.pools.insert(generation, pool);
        self.cleanup_drained_generations_for(&worker_name);
        info!(
            worker = %worker_name,
            generation,
            deployment_id = %deployment_id,
            "deployed dynamic worker"
        );

        Ok(DynamicDeployResult {
            worker: worker_name,
            deployment_id,
            env_placeholders: dynamic_config.env_placeholders,
        })
    }

    pub(crate) fn fail_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    ) {
        let failed = self.remove_isolate_by_id(worker_name, generation, isolate_id);
        if !failed.removed {
            return;
        }
        for (request_id, reply) in failed.replies {
            self.clear_revalidation_for_request(&request_id);
            let _ = reply.send(Err(error.clone()));
        }
        if failed.was_starting {
            self.reject_queued_dynamic_invokes_for_generation(
                worker_name,
                generation,
                error.clone(),
            );
        }
        self.fail_all_streams_for_worker(worker_name, error);
    }

    pub(crate) fn track_exiting_isolate_slot(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        startup: IsolateStartup,
    ) {
        let key = IsolateSlotKey::new(worker_name, generation, isolate_id);
        if self.exiting_isolate_slots.insert(key, startup).is_some() {
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                "duplicate exiting isolate slot accounting"
            );
        }
    }

    pub(crate) fn handle_isolate_exited(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) {
        let key = IsolateSlotKey::new(worker_name, generation, isolate_id);
        let removed = if self.exiting_isolate_slots.contains_key(&key) {
            RemovedIsolate::default()
        } else {
            self.remove_isolate_by_id(worker_name, generation, isolate_id)
        };

        if let Some(startup) = self.exiting_isolate_slots.remove(&key) {
            self.global_isolate_slot_released(startup);
        } else if !removed.removed {
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                "received exit for untracked isolate"
            );
        }

        if !removed.removed {
            return;
        }

        let error = PlatformError::internal("isolate exited");
        for (request_id, reply) in removed.replies {
            self.clear_revalidation_for_request(&request_id);
            let _ = reply.send(Err(error.clone()));
        }
        if removed.was_starting {
            self.reject_queued_dynamic_invokes_for_generation(
                worker_name,
                generation,
                error.clone(),
            );
        }
        self.fail_all_streams_for_worker(worker_name, error);
    }

    pub(crate) fn mark_isolate_ready(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) {
        let was_starting;
        {
            let Some(pool) = self.get_pool_mut(worker_name, generation) else {
                return;
            };
            let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            else {
                return;
            };
            was_starting = isolate.startup.is_starting();
            isolate.startup = IsolateStartup::Ready;
            isolate.last_used_at = Instant::now();
            pool.log_stats("ready");
        }
        if was_starting {
            self.global_isolates_starting = self.global_isolates_starting.saturating_sub(1);
        }
    }

    pub(crate) fn finish_wait_until(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        request_id: &str,
        completion_token: &str,
    ) {
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                if let Some(token) = isolate.pending_wait_until.get(request_id) {
                    if token == completion_token {
                        isolate.pending_wait_until.remove(request_id);
                        if isolate.inflight_count == 0 && isolate.pending_wait_until.is_empty() {
                            isolate.last_used_at = Instant::now();
                        }
                    }
                }
            }
            pool.log_stats("wait_until_done");
        }
    }

    pub(crate) fn handle_response_start(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        status: u16,
        headers: Vec<(String, String)>,
    ) {
        let Some(registration) = self.stream_registrations.get_mut(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            return;
        }
        registration.started = true;
        if let Some(ready) = registration.ready.take() {
            if let Some(body) = registration.body_receiver.take() {
                let _ = ready.send(Ok(WorkerStreamOutput {
                    status,
                    headers,
                    body: WorkerStreamBody::new(body),
                }));
            } else {
                let _ = ready.send(Err(PlatformError::internal("stream body receiver missing")));
            }
        }
    }

    pub(crate) async fn handle_response_chunk(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        chunk: Bytes,
        event_tx: &RuntimeEventSender,
        reply: oneshot::Sender<Result<()>>,
    ) {
        let send = {
            let Some(registration) = self.stream_registrations.get_mut(request_id) else {
                let _ = reply.send(Err(PlatformError::internal(
                    "stream registration is unavailable",
                )));
                return;
            };
            if registration.worker_name != worker_name {
                let _ = reply.send(Err(PlatformError::internal(
                    "stream registration worker mismatch",
                )));
                return;
            }
            if registration.completion_token.as_deref() != Some(completion_token) {
                let _ = reply.send(Err(PlatformError::internal(
                    "stream registration completion token mismatch",
                )));
                return;
            }
            let next_bytes = registration.bytes_sent.saturating_add(chunk.len());
            if next_bytes > registration.max_bytes {
                Err(PlatformError::runtime(format!(
                    "response body exceeded max_response_body_bytes ({} bytes)",
                    registration.max_bytes
                )))
            } else {
                registration.bytes_sent = next_bytes;
                Ok(registration.body_sender.clone())
            }
        };

        let sender = match send {
            Ok(sender) => sender,
            Err(error) => {
                self.fail_stream_registration(worker_name, request_id, error.clone());
                self.cancel_invoke(worker_name.to_string(), request_id.to_string(), event_tx);
                let _ = reply.send(Err(error));
                return;
            }
        };

        match sender.send(Ok(chunk)).await {
            Ok(()) => {
                let _ = reply.send(Ok(()));
            }
            Err(_) => {
                let error = PlatformError::internal("stream response receiver closed");
                self.fail_stream_registration(worker_name, request_id, error.clone());
                self.cancel_invoke(worker_name.to_string(), request_id.to_string(), event_tx);
                let _ = reply.send(Err(error));
            }
        }
    }

    pub(crate) fn schedule_cache_revalidate(
        &mut self,
        worker_name: &str,
        generation: u64,
        request: CacheRevalidatePayload,
        event_tx: &RuntimeEventSender,
    ) {
        let Some(entry) = self.workers.get(worker_name) else {
            return;
        };
        if entry.current_generation != generation {
            return;
        }

        if request.cache_name.trim().is_empty()
            || request.method.trim().is_empty()
            || request.url.trim().is_empty()
        {
            warn!(
                worker = %worker_name,
                generation,
                "ignoring invalid cache revalidate payload"
            );
            return;
        }

        let method = request.method.trim().to_ascii_uppercase();
        if method != "GET" {
            return;
        }
        let revalidate_span = tracing::info_span!(
            "runtime.cache.revalidate_schedule",
            worker.name = %worker_name,
            worker.generation = generation,
            cache.name = %request.cache_name,
            http.method = %method,
            http.url = %request.url
        );
        set_span_parent_from_traceparent(
            &revalidate_span,
            traceparent_from_headers(&request.headers),
        );
        let _revalidate_guard = revalidate_span.enter();

        let key = cache_revalidation_key(worker_name, generation, &request);
        if !self.revalidation_keys.insert(key.clone()) {
            tracing::info!("skipping duplicate cache revalidation");
            return;
        }

        let runtime_request_id = Uuid::new_v4().to_string();
        let request_id = format!("cache-revalidate-{runtime_request_id}");
        let mut headers = request.headers.clone();
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-dd-cache-bypass-stale"))
        {
            headers.push(("x-dd-cache-bypass-stale".to_string(), "1".to_string()));
        }

        let invocation = WorkerInvocation {
            method,
            url: request.url,
            headers,
            body: Vec::new(),
            request_id,
        };
        let (reply, _receiver) = oneshot::channel();
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        let queued_bytes = estimate_pending_invoke_bytes(&invocation, false);
        let admission_error = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| self.queue_admission_error(pool, queued_bytes, false));
        if let Some(error) = admission_error {
            self.revalidation_keys.remove(&key);
            warn!(
                worker = %worker_name,
                generation,
                error = %error,
                "skipping cache revalidation because worker queue is overloaded"
            );
            return;
        }
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.queue.push_back(PendingInvoke {
                runtime_request_id: runtime_request_id.clone(),
                request: invocation,
                request_body: None,
                memory_route: None,
                memory_call: None,
                host_rpc_call: None,
                target_isolate_id: None,
                internal_origin: false,
                reply,
                reply_kind: PendingReplyKind::Normal,
                enqueued_at: Instant::now(),
                queued_bytes,
            });
            pool.update_queue_warning(&warn_thresholds);
            self.account_queued_pending(queued_bytes);
            self.revalidation_requests.insert(runtime_request_id, key);
            self.dispatch_pool(worker_name, generation, event_tx);
            tracing::info!("scheduled background cache revalidation");
        } else {
            self.revalidation_keys.remove(&key);
        }
    }

    pub(crate) fn clear_revalidation_for_request(&mut self, request_id: &str) {
        if let Some(key) = self.revalidation_requests.remove(request_id) {
            self.revalidation_keys.remove(&key);
        }
    }

    pub(crate) fn fail_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        error: PlatformError,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if let Some(ready) = registration.ready.take() {
            let _ = ready.send(Err(error));
            return;
        }
        let _ = registration.body_sender.try_send(Err(error));
    }

    pub(crate) async fn complete_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        result: Result<WorkerOutput>,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }

        match result {
            Ok(output) => {
                let WorkerOutput {
                    status,
                    headers,
                    body: output_body,
                } = output;
                if !registration.started {
                    if let Some(ready) = registration.ready.take() {
                        if let Some(body) = registration.body_receiver.take() {
                            let _ = ready.send(Ok(WorkerStreamOutput {
                                status,
                                headers,
                                body: WorkerStreamBody::new(body),
                            }));
                        } else {
                            let _ = ready
                                .send(Err(PlatformError::internal("stream body receiver missing")));
                        }
                    }
                    if !output_body.is_empty() {
                        let next_bytes = registration.bytes_sent.saturating_add(output_body.len());
                        if next_bytes > registration.max_bytes {
                            let _ = registration
                                .body_sender
                                .send(Err(PlatformError::runtime(format!(
                                    "response body exceeded max_response_body_bytes ({} bytes)",
                                    registration.max_bytes
                                ))))
                                .await;
                        } else {
                            let _ = registration
                                .body_sender
                                .send(Ok(Bytes::from(output_body)))
                                .await;
                        }
                    }
                }
            }
            Err(error) => {
                if let Some(ready) = registration.ready.take() {
                    let _ = ready.send(Err(error));
                } else {
                    let _ = registration.body_sender.send(Err(error)).await;
                }
            }
        }
    }

    pub(crate) fn fail_all_streams_for_worker(&mut self, worker_name: &str, error: PlatformError) {
        let request_ids: Vec<String> = self
            .stream_registrations
            .iter()
            .filter(|(_, registration)| registration.worker_name == worker_name)
            .map(|(request_id, _)| request_id.clone())
            .collect();

        for request_id in request_ids {
            self.fail_stream_registration(worker_name, &request_id, error.clone());
        }
    }

    pub(crate) fn retire_worker_completely(&mut self, worker_name: &str) {
        self.retire_worker_completely_with_error(
            worker_name,
            PlatformError::internal("dynamic worker was deleted"),
        );
    }

    pub(crate) fn retire_worker_completely_with_error(
        &mut self,
        worker_name: &str,
        error: PlatformError,
    ) {
        self.asset_catalog.remove(worker_name);
        let mut clear_request_ids = Vec::new();
        self.reap_owned_sessions(worker_name, None, None);
        if let Some(mut entry) = self.workers.remove(worker_name) {
            for (_, mut pool) in entry.pools.drain() {
                release_worker_source_modules(&pool.source);
                self.account_removed_pool_queue(&pool);
                while let Some(pending) = pool.queue.pop_front() {
                    self.reject_pending_invoke(worker_name, pending, error.clone());
                }
                for isolate in pool.isolates {
                    let _ = isolate.sender.try_send(IsolateCommand::Shutdown);
                    self.track_exiting_isolate_slot(
                        worker_name,
                        pool.generation,
                        isolate.id,
                        isolate.startup,
                    );
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending.reply.send(Err(error.clone()));
                    }
                }
            }
        }
        for request_id in clear_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }
        self.fail_all_streams_for_worker(worker_name, error);
        self.dynamic_worker_handles
            .retain(|_, handle| handle.worker_name != worker_name);
        let existing_handles: HashSet<String> =
            self.dynamic_worker_handles.keys().cloned().collect();
        self.dynamic_worker_ids.retain(|_, by_id| {
            by_id.retain(|_, handle| existing_handles.contains(handle));
            !by_id.is_empty()
        });
        self.host_rpc_providers
            .retain(|_, provider| provider.owner_worker != worker_name);
    }

    pub(crate) fn remove_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_idx: usize,
    ) -> RemovedIsolate {
        let mut websocket_open_session_ids = Vec::new();
        let mut transport_open_session_ids = Vec::new();
        let mut replies = Vec::new();
        let mut was_starting = false;
        let mut removed_isolate_id = None;
        let mut stale_targeted_pending = Vec::new();
        let mut stale_targeted_count = 0usize;
        let mut stale_targeted_bytes = 0usize;
        let mut removed = false;
        let mut removed_slot = None;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool.isolates.get_mut(isolate_idx) {
                was_starting = isolate.startup.is_starting();
                removed_slot = Some((isolate.id, isolate.startup));
                isolate.startup = IsolateStartup::Retiring;
            }
            if let Some(isolate) = pool.swap_remove_isolate(isolate_idx) {
                let _ = isolate.sender.try_send(IsolateCommand::Shutdown);
                removed_isolate_id = Some(isolate.id);
                pool.memory_shard_affinity
                    .retain(|_, owner_isolate_id| *owner_isolate_id != isolate.id);
                stale_targeted_pending = pool.queue.drain_target_isolate_id(isolate.id);
                stale_targeted_count = stale_targeted_pending.len();
                stale_targeted_bytes = stale_targeted_pending
                    .iter()
                    .map(|pending| pending.queued_bytes)
                    .sum::<usize>();
                replies = Vec::with_capacity(isolate.pending_replies.len());
                for (request_id, pending) in isolate.pending_replies {
                    if let Some(active_memory_lease) = pending.active_memory_lease.as_ref() {
                        pool.release_memory_entity_lease(active_memory_lease);
                    }
                    match &pending.kind {
                        PendingReplyKind::WebsocketOpen { session_id } => {
                            websocket_open_session_ids.push(session_id.clone());
                        }
                        PendingReplyKind::TransportOpen { session_id } => {
                            transport_open_session_ids.push(session_id.clone());
                        }
                        PendingReplyKind::Normal
                        | PendingReplyKind::Stream
                        | PendingReplyKind::DynamicFetch { .. }
                        | PendingReplyKind::WebsocketFrame { .. } => {}
                    }
                    replies.push((request_id, pending.reply));
                }
                removed = true;
            }
        }
        if removed {
            if let Some((isolate_id, startup)) = removed_slot {
                self.track_exiting_isolate_slot(worker_name, generation, isolate_id, startup);
            }
        }
        if let Some(isolate_id) = removed_isolate_id {
            self.reap_owned_sessions(worker_name, Some(generation), Some(isolate_id));
        }
        self.account_dequeued_many(stale_targeted_count, stale_targeted_bytes);
        for pending in stale_targeted_pending {
            self.reject_pending_invoke(
                worker_name,
                pending,
                PlatformError::runtime("target isolate is unavailable"),
            );
        }
        for session_id in websocket_open_session_ids {
            if let Some(waiter) = self.websocket_open_waiters.remove(&session_id) {
                let _ = waiter.send(Err(PlatformError::internal("isolate is unavailable")));
            }
        }
        for session_id in transport_open_session_ids {
            if let Some(waiter) = self.transport_open_waiters.remove(&session_id) {
                let _ = waiter.send(Err(PlatformError::internal("isolate is unavailable")));
            }
            self.transport_open_channels.remove(&session_id);
        }
        if removed {
            RemovedIsolate {
                removed: true,
                replies,
                was_starting,
            }
        } else {
            RemovedIsolate::default()
        }
    }

    pub(crate) fn remove_isolate_by_id(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) -> RemovedIsolate {
        let isolate_idx = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| pool.isolate_idx(isolate_id));
        if let Some(idx) = isolate_idx {
            return self.remove_isolate(worker_name, generation, idx);
        }
        RemovedIsolate::default()
    }

    pub(crate) fn reject_queued_dynamic_invokes_for_generation(
        &mut self,
        worker_name: &str,
        generation: u64,
        error: PlatformError,
    ) {
        let mut rejected = Vec::new();
        let mut rejected_count = 0usize;
        let mut rejected_bytes = 0usize;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            for pending in pool.queue.drain_matching(|pending| {
                matches!(pending.reply_kind, PendingReplyKind::DynamicFetch { .. })
            }) {
                rejected_count = rejected_count.saturating_add(1);
                rejected_bytes = rejected_bytes.saturating_add(pending.queued_bytes);
                rejected.push(pending);
            }
        }
        if rejected.is_empty() {
            return;
        }
        self.account_dequeued_many(rejected_count, rejected_bytes);
        for pending in rejected {
            self.reject_pending_invoke(worker_name, pending, error.clone());
        }
    }

    pub(crate) fn scale_down_idle(&mut self) {
        let now = Instant::now();
        let worker_names: Vec<String> = self.workers.keys().cloned().collect();
        for worker_name in worker_names {
            let generations: Vec<u64> = self
                .workers
                .get(&worker_name)
                .map(|entry| entry.pools.keys().copied().collect())
                .unwrap_or_default();
            for generation in generations {
                self.scale_down_pool(&worker_name, generation, now);
            }
            self.cleanup_drained_generations_for(&worker_name);
        }
    }

    pub(crate) fn scale_down_pool(&mut self, worker_name: &str, generation: u64, now: Instant) {
        let min_isolates = self.config.min_isolates;
        let idle_ttl = self.config.idle_ttl;
        let mut removed = Vec::new();
        let mut removed_slots = Vec::new();
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            loop {
                if pool.isolates.len() <= min_isolates {
                    break;
                }

                let candidate = pool
                    .isolates
                    .iter()
                    .enumerate()
                    .filter(|(_, isolate)| isolate.inflight_count == 0)
                    .filter(|(_, isolate)| isolate.pending_wait_until.is_empty())
                    .filter(|(_, isolate)| isolate.active_websocket_sessions == 0)
                    .filter(|(_, isolate)| isolate.active_transport_sessions == 0)
                    .filter(|(_, isolate)| now.duration_since(isolate.last_used_at) >= idle_ttl)
                    .min_by_key(|(_, isolate)| isolate.last_used_at);
                let Some((idx, _)) = candidate else {
                    break;
                };
                let isolate = pool
                    .swap_remove_isolate(idx)
                    .expect("selected isolate index must be present");
                removed_slots.push((isolate.id, isolate.startup));
                pool.stats.scale_down_count += 1;
                removed.push(isolate);
            }

            if !removed.is_empty() {
                pool.log_stats("scale_down");
            }
        }
        for (isolate_id, startup) in removed_slots {
            self.track_exiting_isolate_slot(worker_name, generation, isolate_id, startup);
        }

        for isolate in removed {
            let _ = isolate.sender.try_send(IsolateCommand::Shutdown);
            for (request_id, pending) in isolate.pending_replies {
                self.clear_revalidation_for_request(&request_id);
                let _ = pending
                    .reply
                    .send(Err(PlatformError::internal("isolate scaled down")));
            }
        }
    }

    pub(crate) fn cleanup_drained_generations_for(&mut self, worker_name: &str) {
        if self.runtime_batch_depth > 0 {
            self.pending_cleanup_workers.insert(worker_name.to_string());
            return;
        }
        let mut clear_request_ids = Vec::new();
        let mut retired_generations = HashSet::new();
        let mut exiting_slots = Vec::new();
        let live_websocket_generations: HashSet<u64> = self
            .websocket_sessions
            .values()
            .filter(|session| session.worker_name == worker_name)
            .map(|session| session.generation)
            .collect();
        let live_transport_generations: HashSet<u64> = self
            .transport_sessions
            .values()
            .filter(|session| session.worker_name == worker_name)
            .map(|session| session.generation)
            .collect();
        let drained = {
            let Some(entry) = self.workers.get(worker_name) else {
                return;
            };
            let current_generation = entry.current_generation;
            entry
                .pools
                .iter()
                .filter(|(generation, pool)| {
                    **generation != current_generation
                        && pool.is_drained()
                        && !live_websocket_generations.contains(generation)
                        && !live_transport_generations.contains(generation)
                })
                .map(|(generation, _)| *generation)
                .collect::<Vec<_>>()
        };

        for generation in drained {
            if let Some(pool) = self
                .workers
                .get_mut(worker_name)
                .and_then(|entry| entry.pools.remove(&generation))
            {
                release_worker_source_modules(&pool.source);
                self.account_removed_pool_queue(&pool);
                retired_generations.insert(generation);
                for isolate in pool.isolates {
                    let _ = isolate.sender.try_send(IsolateCommand::Shutdown);
                    exiting_slots.push((generation, isolate.id, isolate.startup));
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("worker generation retired")));
                    }
                }
                info!(worker = %pool.worker_name, generation, "retired worker generation");
            }
        }
        for request_id in clear_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }
        for (generation, isolate_id, startup) in exiting_slots {
            self.track_exiting_isolate_slot(worker_name, generation, isolate_id, startup);
        }
        if retired_generations.is_empty() {
            return;
        }
        self.dynamic_worker_handles.retain(|_, handle| {
            !(handle.owner_worker == worker_name
                && retired_generations.contains(&handle.owner_generation))
        });
        let existing_handles: HashSet<String> =
            self.dynamic_worker_handles.keys().cloned().collect();
        self.dynamic_worker_ids.retain(|key, by_id| {
            if key.owner_worker == worker_name
                && retired_generations.contains(&key.owner_generation)
            {
                return false;
            }
            by_id.retain(|_, handle| existing_handles.contains(handle));
            !by_id.is_empty()
        });
        self.host_rpc_providers.retain(|_, provider| {
            !(provider.owner_worker == worker_name
                && retired_generations.contains(&provider.owner_generation))
        });
    }

    pub(crate) fn worker_stats(&self, worker_name: &str) -> Option<WorkerStats> {
        let entry = self.workers.get(worker_name)?;
        let pool = entry.pools.get(&entry.current_generation)?;
        let mut stats = pool.stats_snapshot();
        stats.pending_memory_outbox_shards = self.pending_memory_outbox_shards.len();
        stats.max_queued_requests_per_worker = self.config.max_queued_requests_per_worker;
        stats.max_global_queued_bytes = self.config.max_global_queued_bytes;
        stats.runtime_ready_work_budget_exhausted_count =
            self.stats.ready_work_budget_exhausted_count;
        stats.runtime_max_ready_work_batch_size = self.stats.max_ready_work_batch_size;
        stats.global_isolate_budget = self.config.max_global_isolates;
        stats.global_isolates_total = self.global_isolate_slots_used;
        stats.global_isolates_starting = self.global_isolates_starting;
        stats.global_isolate_slots_available = self
            .config
            .max_global_isolates
            .saturating_sub(self.global_isolate_slots_used);
        stats.scale_up_waiting_pools = self.scale_up_request_members.len();
        stats.scale_up_budget_denied_count = self.stats.scale_up_budget_denied_count;
        stats.memory_outbox_claim_batch_count = self.stats.memory_outbox_claim_batch_count;
        stats.memory_outbox_claim_row_count = self.stats.memory_outbox_claim_row_count;
        stats.memory_outbox_saturated_batch_count = self.stats.memory_outbox_saturated_batch_count;
        stats.memory_outbox_delivery_success_count =
            self.stats.memory_outbox_delivery_success_count;
        stats.memory_outbox_delivery_retry_count = self.stats.memory_outbox_delivery_retry_count;
        stats.memory_outbox_terminal_drop_count = self.stats.memory_outbox_terminal_drop_count;
        stats.memory_outbox_ack_failure_count = self.stats.memory_outbox_ack_failure_count;
        stats.memory_outbox_channel_full_count = self.stats.memory_outbox_channel_full_count;
        stats.memory_outbox_reschedule_count = self.stats.memory_outbox_reschedule_count;
        stats.memory_outbox_worker_pending_shards = self.stats.memory_outbox_worker_pending_shards;
        stats.memory_outbox_worker_in_flight_shards =
            self.stats.memory_outbox_worker_in_flight_shards;
        stats.memory_outbox_worker_parallelism_limit =
            self.stats.memory_outbox_worker_parallelism_limit;
        stats.memory_outbox_worker_parallelism_peak =
            self.stats.memory_outbox_worker_parallelism_peak;
        stats.memory_outbox_duplicate_schedule_coalesced_count =
            self.stats.memory_outbox_duplicate_schedule_coalesced_count;
        stats.memory_outbox_task_failure_count = self.stats.memory_outbox_task_failure_count;
        stats.memory_outbox_shard_requeue_count = self.stats.memory_outbox_shard_requeue_count;
        Some(stats)
    }

    pub(crate) fn worker_debug_dump(&self, worker_name: &str) -> Option<WorkerDebugDump> {
        let entry = self.workers.get(worker_name)?;
        let pool = entry.pools.get(&entry.current_generation)?;
        let mut dump = pool.debug_dump();
        dump.memory_scheduler.max_queued_requests_per_worker =
            self.config.max_queued_requests_per_worker;
        dump.memory_scheduler.max_global_queued_bytes = self.config.max_global_queued_bytes;
        dump.memory_scheduler
            .runtime_ready_work_budget_exhausted_count =
            self.stats.ready_work_budget_exhausted_count;
        dump.memory_scheduler.runtime_max_ready_work_batch_size =
            self.stats.max_ready_work_batch_size;
        dump.memory_scheduler.global_isolate_budget = self.config.max_global_isolates;
        dump.memory_scheduler.global_isolates_total = self.global_isolate_slots_used;
        dump.memory_scheduler.global_isolates_starting = self.global_isolates_starting;
        dump.memory_scheduler.global_isolate_slots_available = self
            .config
            .max_global_isolates
            .saturating_sub(self.global_isolate_slots_used);
        dump.memory_scheduler.scale_up_waiting_pools = self.scale_up_request_members.len();
        dump.memory_scheduler.scale_up_budget_denied_count =
            self.stats.scale_up_budget_denied_count;
        dump.memory_outbox.pending_scheduled_shards = self.pending_memory_outbox_shards.len();
        dump.memory_outbox.claim_batch_count = self.stats.memory_outbox_claim_batch_count;
        dump.memory_outbox.claim_row_count = self.stats.memory_outbox_claim_row_count;
        dump.memory_outbox.saturated_batch_count = self.stats.memory_outbox_saturated_batch_count;
        dump.memory_outbox.delivery_success_count = self.stats.memory_outbox_delivery_success_count;
        dump.memory_outbox.delivery_retry_count = self.stats.memory_outbox_delivery_retry_count;
        dump.memory_outbox.terminal_drop_count = self.stats.memory_outbox_terminal_drop_count;
        dump.memory_outbox.ack_failure_count = self.stats.memory_outbox_ack_failure_count;
        dump.memory_outbox.channel_full_count = self.stats.memory_outbox_channel_full_count;
        dump.memory_outbox.reschedule_count = self.stats.memory_outbox_reschedule_count;
        dump.memory_outbox.worker_pending_shards = self.stats.memory_outbox_worker_pending_shards;
        dump.memory_outbox.worker_in_flight_shards =
            self.stats.memory_outbox_worker_in_flight_shards;
        dump.memory_outbox.worker_parallelism_limit =
            self.stats.memory_outbox_worker_parallelism_limit;
        dump.memory_outbox.worker_parallelism_peak =
            self.stats.memory_outbox_worker_parallelism_peak;
        dump.memory_outbox.duplicate_schedule_coalesced_count =
            self.stats.memory_outbox_duplicate_schedule_coalesced_count;
        dump.memory_outbox.task_failure_count = self.stats.memory_outbox_task_failure_count;
        dump.memory_outbox.shard_requeue_count = self.stats.memory_outbox_shard_requeue_count;
        Some(dump)
    }

    pub(crate) fn dynamic_debug_dump(&self) -> DynamicRuntimeDebugDump {
        let mut handles = self
            .dynamic_worker_handles
            .iter()
            .map(|(handle, entry)| DynamicHandleDebug {
                handle: handle.clone(),
                id: entry.id.clone(),
                owner_worker: entry.owner_worker.clone(),
                owner_generation: entry.owner_generation,
                binding: entry.binding.clone(),
                worker_name: entry.worker_name.clone(),
                timeout_ms: entry.timeout,
                policy_tier: entry.policy.tier.as_str().to_string(),
                egress_deny_count: entry.quota_state.egress_deny_count.load(Ordering::Relaxed),
                rpc_deny_count: entry.quota_state.rpc_deny_count.load(Ordering::Relaxed),
                quota_kill_count: entry.quota_state.quota_kill_count.load(Ordering::Relaxed),
                upgrade_deny_count: entry.quota_state.upgrade_deny_count.load(Ordering::Relaxed),
                outbound_requests: entry.quota_state.outbound_requests.load(Ordering::Relaxed),
                inflight: entry.quota_state.inflight.load(Ordering::Relaxed),
                max_concurrency: entry.policy.max_concurrency,
            })
            .collect::<Vec<_>>();
        handles.sort_by(|left, right| left.handle.cmp(&right.handle));

        let mut providers = self
            .host_rpc_providers
            .iter()
            .map(|(provider_id, provider)| {
                let mut methods = provider.methods.iter().cloned().collect::<Vec<_>>();
                methods.sort();
                HostRpcProviderDebug {
                    provider_id: provider_id.clone(),
                    owner_worker: provider.owner_worker.clone(),
                    owner_generation: provider.owner_generation,
                    owner_isolate_id: provider.owner_isolate_id,
                    target_id: provider.target_id.clone(),
                    methods,
                }
            })
            .collect::<Vec<_>>();
        providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));

        DynamicRuntimeDebugDump { handles, providers }
    }

    pub(crate) fn log_dynamic_timeout_diagnostic(&self, diagnostic: DynamicTimeoutDiagnostic) {
        let owner_dump = self.worker_debug_dump(&diagnostic.owner_worker);
        let target_dump = self.worker_debug_dump(&diagnostic.target_worker);
        let handle_summary = self
            .dynamic_worker_handles
            .get(&diagnostic.handle)
            .map(|handle| {
                (
                    handle.owner_worker.clone(),
                    handle.owner_generation,
                    handle.binding.clone(),
                    handle.worker_name.clone(),
                    handle.timeout,
                )
            });
        let mut provider_summaries = self
            .host_rpc_providers
            .iter()
            .filter(|(_, provider)| {
                provider.owner_worker == diagnostic.owner_worker
                    && provider.owner_generation == diagnostic.owner_generation
            })
            .map(|(provider_id, provider)| {
                let mut methods = provider.methods.iter().cloned().collect::<Vec<_>>();
                methods.sort();
                (
                    provider_id.clone(),
                    provider.owner_isolate_id,
                    provider.target_id.clone(),
                    methods,
                )
            })
            .collect::<Vec<_>>();
        provider_summaries.sort_by(|left, right| left.0.cmp(&right.0));
        warn!(
            stage = diagnostic.stage,
            owner_worker = %diagnostic.owner_worker,
            owner_generation = diagnostic.owner_generation,
            binding = %diagnostic.binding,
            handle = %diagnostic.handle,
            target_worker = %diagnostic.target_worker,
            target_isolate_id = diagnostic.target_isolate_id,
            target_generation = diagnostic.target_generation,
            provider_id = ?diagnostic.provider_id,
            provider_owner_isolate_id = diagnostic.provider_owner_isolate_id,
            provider_target_id = ?diagnostic.provider_target_id,
            timeout_ms = diagnostic.timeout_ms,
            handle_summary = ?handle_summary,
            host_rpc_providers = ?provider_summaries,
            owner_dump = ?owner_dump,
            target_dump = ?target_dump,
            "dynamic invoke timeout diagnostic"
        );
    }
}

fn deployed_worker_source(
    source: &str,
    server_modules: &[DeployServerModule],
) -> Result<crate::ops::WorkerSource> {
    if server_modules.is_empty() {
        return Ok(crate::ops::WorkerSource::inline(source.to_string()));
    }
    let (graph_id, entrypoint) = crate::dynamic_modules::register_server_module_graph(
        "worker.js",
        source.to_string(),
        server_modules.to_vec(),
    )?;
    Ok(crate::ops::WorkerSource::DynamicModule {
        graph_id,
        entrypoint,
    })
}

fn release_worker_source_modules(source: &crate::ops::WorkerSource) {
    if let crate::ops::WorkerSource::DynamicModule { graph_id, .. } = source {
        crate::dynamic_modules::release_dynamic_module_graph(graph_id);
    }
}
