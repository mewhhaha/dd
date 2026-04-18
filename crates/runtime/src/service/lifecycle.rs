use super::*;

impl WorkerManager {
    pub(crate) async fn deploy(
        &mut self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
        persist: bool,
    ) -> Result<String> {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return Err(PlatformError::bad_request("Worker name must not be empty"));
        }
        let bindings = extract_bindings(&config)?;
        let compiled_assets = compile_asset_bundle(&assets, asset_headers.as_deref())?;
        self.validate_worker_cached(&source).await?;
        let has_dynamic_bindings = !bindings.dynamic.is_empty();
        let (snapshot, snapshot_preloaded) = if has_dynamic_bindings {
            (self.bootstrap_snapshot, false)
        } else {
            let worker_snapshot = build_worker_snapshot(self.bootstrap_snapshot, &source).await?;
            validate_loaded_worker_runtime(worker_snapshot)?;
            (worker_snapshot, true)
        };
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
        let worker_name_json = Arc::<str>::from(
            crate::json::to_string(&worker_name)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let kv_bindings_json = Arc::<str>::from(
            crate::json::to_string(&bindings.kv)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let memory_bindings_json = Arc::<str>::from(
            crate::json::to_string(&bindings.memory)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let dynamic_bindings_json = Arc::<str>::from(
            crate::json::to_string(&bindings.dynamic)
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let dynamic_rpc_bindings_json = Arc::<str>::from(
            crate::json::to_string(&Vec::<String>::new())
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let dynamic_env_json = Arc::<str>::from(
            crate::json::to_string(&Vec::<(String, String)>::new())
                .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        let kv_read_cache_config_json = Arc::<str>::from(
            crate::json::to_string(&KvReadCacheConfigPayload {
                max_entries: self.config.kv_read_cache_max_entries,
                max_bytes: self.config.kv_read_cache_max_bytes,
                hit_ttl_ms: self.config.kv_read_cache_hit_ttl.as_millis() as u64,
                miss_ttl_ms: self.config.kv_read_cache_miss_ttl.as_millis() as u64,
            })
            .map_err(|error| PlatformError::internal(error.to_string()))?,
        );
        if persist {
            persist_worker_deployment(
                &self.storage,
                &worker_name,
                &source,
                &config,
                &assets,
                asset_headers.as_deref(),
                &deployment_id,
            )
            .await?;
        }
        let pool =
            WorkerPool {
                worker_name: worker_name.clone(),
                worker_name_json,
                generation,
                deployment_id: deployment_id.clone(),
                internal_trace: config.internal.trace.as_ref().map(|trace| {
                    InternalTraceDestination {
                        worker: trace.worker.trim().to_string(),
                        path: normalize_trace_path(&trace.path),
                    }
                }),
                is_public: config.public,
                snapshot,
                snapshot_preloaded,
                source: Arc::<str>::from(source.clone()),
                kv_bindings_json,
                kv_read_cache_config_json,
                memory_bindings: bindings.memory,
                memory_bindings_json,
                dynamic_bindings: bindings.dynamic,
                dynamic_bindings_json,
                dynamic_rpc_bindings: Vec::new(),
                dynamic_rpc_bindings_json,
                dynamic_env_json,
                secret_replacements: Vec::new(),
                egress_allow_hosts: Vec::new(),
                assets: compiled_assets,
                strict_request_isolation: false,
                queue: VecDeque::new(),
                isolates: Vec::new(),
                memory_owners: HashMap::new(),
                memory_inflight: HashMap::new(),
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
        info!(worker = %worker_name, generation, deployment_id = %deployment_id, "deployed worker");
        Ok(deployment_id)
    }

    pub(crate) async fn deploy_dynamic(
        &mut self,
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
        dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    ) -> Result<DynamicDeployResult> {
        self.deploy_dynamic_internal(source, env, egress_allow_hosts, dynamic_rpc_bindings, true)
            .await
    }

    pub(crate) async fn deploy_dynamic_internal(
        &mut self,
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
        dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
        validate_source: bool,
    ) -> Result<DynamicDeployResult> {
        let worker_name = format!("dyn-{}", Uuid::new_v4().simple());
        let dynamic_config =
            build_dynamic_worker_config(env, egress_allow_hosts, dynamic_rpc_bindings)?;
        if validate_source {
            self.validate_worker_cached(&source).await?;
        }
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();
        let dynamic_binding_names = dynamic_config
            .dynamic_rpc_bindings
            .iter()
            .map(|binding| binding.binding.clone())
            .collect::<Vec<_>>();
        let (snapshot, snapshot_preloaded) =
            match self.dynamic_worker_snapshot_cached(&source).await {
                Some(snapshot) => (snapshot, true),
                None => (self.bootstrap_snapshot, false),
            };
        let kv_read_cache_config_json = Arc::<str>::from(
            crate::json::to_string(&KvReadCacheConfigPayload {
                max_entries: self.config.kv_read_cache_max_entries,
                max_bytes: self.config.kv_read_cache_max_bytes,
                hit_ttl_ms: self.config.kv_read_cache_hit_ttl.as_millis() as u64,
                miss_ttl_ms: self.config.kv_read_cache_miss_ttl.as_millis() as u64,
            })
            .map_err(|error| PlatformError::internal(error.to_string()))?,
        );

        let pool = WorkerPool {
            worker_name: worker_name.clone(),
            worker_name_json: Arc::<str>::from(
                crate::json::to_string(&worker_name)
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            generation,
            deployment_id: deployment_id.clone(),
            internal_trace: None,
            is_public: false,
            snapshot,
            snapshot_preloaded,
            source: Arc::<str>::from(source),
            kv_bindings_json: Arc::<str>::from(
                crate::json::to_string(&Vec::<String>::new())
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            kv_read_cache_config_json,
            memory_bindings: Vec::new(),
            memory_bindings_json: Arc::<str>::from(
                crate::json::to_string(&Vec::<String>::new())
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            dynamic_bindings: Vec::new(),
            dynamic_bindings_json: Arc::<str>::from(
                crate::json::to_string(&Vec::<String>::new())
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            dynamic_rpc_bindings: dynamic_config.dynamic_rpc_bindings.clone(),
            dynamic_rpc_bindings_json: Arc::<str>::from(
                crate::json::to_string(&dynamic_binding_names)
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            dynamic_env_json: Arc::<str>::from(
                crate::json::to_string(&dynamic_config.dynamic_env)
                    .map_err(|error| PlatformError::internal(error.to_string()))?,
            ),
            secret_replacements: dynamic_config.secret_replacements.clone(),
            egress_allow_hosts: dynamic_config.egress_allow_hosts.clone(),
            assets: AssetBundle::default(),
            strict_request_isolation: false,
            queue: VecDeque::new(),
            isolates: Vec::new(),
            memory_owners: HashMap::new(),
            memory_inflight: HashMap::new(),
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
        for (request_id, reply) in failed {
            self.clear_revalidation_for_request(&request_id);
            let _ = reply.send(Err(error.clone()));
        }
        self.fail_all_streams_for_worker(worker_name, error);
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
                    body,
                }));
            } else {
                let _ = ready.send(Err(PlatformError::internal("stream body receiver missing")));
            }
        }
    }

    pub(crate) fn handle_response_chunk(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        chunk: Vec<u8>,
    ) {
        let Some(registration) = self.stream_registrations.get(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            return;
        }
        let _ = registration.body_sender.send(Ok(chunk));
    }

    pub(crate) fn schedule_cache_revalidate(
        &mut self,
        worker_name: &str,
        generation: u64,
        payload: String,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let Some(entry) = self.workers.get(worker_name) else {
            return;
        };
        if entry.current_generation != generation {
            return;
        }

        let request = match decode_cache_revalidate_payload(payload) {
            Ok(request) => request,
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    error = %error,
                    "ignoring invalid cache revalidate payload"
                );
                return;
            }
        };

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
            traceparent_from_headers(&request.headers).as_deref(),
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
            });
            pool.update_queue_warning(&warn_thresholds);
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
            let _ = ready.send(Err(error.clone()));
            return;
        }
        let _ = registration.body_sender.send(Err(error));
    }

    pub(crate) fn complete_stream_registration(
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
                if !registration.started {
                    if let Some(ready) = registration.ready.take() {
                        if let Some(body) = registration.body_receiver.take() {
                            let _ = ready.send(Ok(WorkerStreamOutput {
                                status: output.status,
                                headers: output.headers.clone(),
                                body,
                            }));
                        } else {
                            let _ = ready
                                .send(Err(PlatformError::internal("stream body receiver missing")));
                        }
                    }
                    if !output.body.is_empty() {
                        let _ = registration.body_sender.send(Ok(output.body));
                    }
                }
            }
            Err(error) => {
                if let Some(ready) = registration.ready.take() {
                    let _ = ready.send(Err(error.clone()));
                } else {
                    let _ = registration.body_sender.send(Err(error));
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
        let mut clear_request_ids = Vec::new();
        self.reap_owned_sessions(worker_name, None, None);
        if let Some(mut entry) = self.workers.remove(worker_name) {
            for (_, pool) in entry.pools.drain() {
                for isolate in pool.isolates {
                    let _ = isolate.sender.send(IsolateCommand::Shutdown);
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("dynamic worker was deleted")));
                    }
                }
            }
        }
        for request_id in clear_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }
        self.fail_all_streams_for_worker(
            worker_name,
            PlatformError::internal("dynamic worker was deleted"),
        );
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
    ) -> Vec<(String, oneshot::Sender<Result<WorkerOutput>>)> {
        let mut websocket_open_session_ids = Vec::new();
        let mut transport_open_session_ids = Vec::new();
        let mut replies = Vec::new();
        let mut removed_isolate_id = None;
        let mut removed = false;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if isolate_idx < pool.isolates.len() {
                let isolate = pool.isolates.swap_remove(isolate_idx);
                let _ = isolate.sender.send(IsolateCommand::Shutdown);
                removed_isolate_id = Some(isolate.id);
                pool.memory_owners
                    .retain(|_, owner_id| *owner_id != isolate.id);
                replies = Vec::with_capacity(isolate.pending_replies.len());
                for (request_id, pending) in isolate.pending_replies {
                    if let Some(memory_key) = pending.memory_key.as_deref() {
                        decrement_memory_inflight(&mut pool.memory_inflight, memory_key);
                    }
                    match &pending.kind {
                        PendingReplyKind::WebsocketOpen { session_id } => {
                            websocket_open_session_ids.push(session_id.clone());
                        }
                        PendingReplyKind::TransportOpen { session_id } => {
                            transport_open_session_ids.push(session_id.clone());
                        }
                        PendingReplyKind::Normal
                        | PendingReplyKind::DynamicInvoke { .. }
                        | PendingReplyKind::DynamicFetch { .. }
                        | PendingReplyKind::WebsocketFrame { .. } => {}
                    }
                    replies.push((request_id, pending.reply));
                }
                removed = true;
            }
        }
        if let Some(isolate_id) = removed_isolate_id {
            self.reap_owned_sessions(worker_name, Some(generation), Some(isolate_id));
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
            replies
        } else {
            Vec::new()
        }
    }

    pub(crate) fn remove_isolate_by_id(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) -> Vec<(String, oneshot::Sender<Result<WorkerOutput>>)> {
        let isolate_idx = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .position(|isolate| isolate.id == isolate_id)
            });
        if let Some(idx) = isolate_idx {
            return self.remove_isolate(worker_name, generation, idx);
        }
        Vec::new()
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
                let isolate = pool.isolates.swap_remove(idx);
                pool.stats.scale_down_count += 1;
                pool.memory_owners
                    .retain(|_, owner_id| *owner_id != isolate.id);
                for pending in isolate.pending_replies.values() {
                    if let Some(memory_key) = pending.memory_key.as_deref() {
                        decrement_memory_inflight(&mut pool.memory_inflight, memory_key);
                    }
                }
                removed.push(isolate);
            }

            if !removed.is_empty() {
                pool.log_stats("scale_down");
            }
        }

        for isolate in removed {
            let _ = isolate.sender.send(IsolateCommand::Shutdown);
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
                retired_generations.insert(generation);
                for isolate in pool.isolates {
                    let _ = isolate.sender.send(IsolateCommand::Shutdown);
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
        Some(pool.stats_snapshot())
    }

    pub(crate) fn worker_debug_dump(&self, worker_name: &str) -> Option<WorkerDebugDump> {
        let entry = self.workers.get(worker_name)?;
        let pool = entry.pools.get(&entry.current_generation)?;
        Some(pool.debug_dump())
    }

    pub(crate) fn dynamic_debug_dump(&self) -> DynamicRuntimeDebugDump {
        let mut handles = self
            .dynamic_worker_handles
            .iter()
            .map(|(handle, entry)| DynamicHandleDebug {
                handle: handle.clone(),
                owner_worker: entry.owner_worker.clone(),
                owner_generation: entry.owner_generation,
                binding: entry.binding.clone(),
                worker_name: entry.worker_name.clone(),
                timeout_ms: entry.timeout,
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

        DynamicRuntimeDebugDump {
            handles,
            providers,
            snapshot_cache_entries: self.dynamic_worker_snapshots.len(),
            snapshot_cache_failures: self.dynamic_worker_snapshot_failures.len(),
        }
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
