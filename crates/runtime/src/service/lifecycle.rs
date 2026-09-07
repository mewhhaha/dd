use super::dispatch::estimate_pending_invoke_bytes;
use super::*;

impl WorkerManager {
    pub(crate) fn deploy(
        &mut self,
        prepared: PreparedWorkerDeployment,
        deployment_id: String,
        expires_at_ms: Option<i64>,
    ) -> Result<String> {
        let PreparedWorkerDeployment {
            worker_name,
            source,
            config,
            server_modules,
            compiled_assets,
            bindings,
            ..
        } = prepared;
        let worker_source =
            deployed_worker_source(&self.module_registry, &source, &server_modules)?;
        let (snapshot, snapshot_preloaded) = (self.bootstrap_snapshot, false);
        let generation = self.next_generation;
        self.next_generation += 1;
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
            memory_bindings: bindings.memory.clone(),
            service_bindings: service_bindings.clone(),
        });
        let request_context = RequestExecutionContext::new(RequestExecutionContextInit {
            worker_name: worker_name.clone(),
            generation,
            service_bindings,
            egress_allow_hosts: config.egress_allow_hosts.clone(),
        });
        let asset_catalog_entry = AssetCatalogEntry {
            worker_name: worker_name.clone(),
            generation,
            assets: compiled_assets.clone(),
            public: config.public,
            cache_enabled: config.cache.enabled,
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
                retired_at: None,
                snapshot,
                snapshot_preloaded,
                source: worker_source,
                memory_bindings: bindings.memory,
                deployment_config,
                request_context,
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
        if let Some(previous) = entry.pools.get_mut(&entry.current_generation) {
            previous.retired_at = Some(Instant::now());
        }
        entry.current_generation = generation;
        entry.pools.insert(generation, pool);
        self.asset_catalog
            .insert(worker_name.clone(), asset_catalog_entry);
        let retiring_sessions = self
            .websocket_sessions
            .iter()
            .filter(|(_, session)| {
                session.worker_name == worker_name && session.generation != generation
            })
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        for session_id in retiring_sessions {
            self.websocket_close_signals.insert(
                session_id.clone(),
                SocketCloseEvent {
                    code: 1012,
                    reason: "worker redeployed".to_string(),
                },
            );
            self.flush_pending_websocket_frame_replies(&session_id);
            self.notify_websocket_frame_waiters(&session_id);
        }
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

    pub(super) fn current_worker_is_permanent(&self, worker_name: &str) -> bool {
        self.workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&entry.current_generation))
            .is_some_and(|pool| pool.expires_at_ms.is_none())
    }

    pub(crate) fn expire_temporary_workers(&mut self) {
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
                (expires_at_ms <= now_ms).then(|| (worker_name.clone(), pool.deployment_id.clone()))
            })
            .collect::<Vec<_>>();

        for (worker_name, deployment_id) in expired {
            self.retire_worker_completely_with_error(
                &worker_name,
                PlatformError::not_found("temporary worker expired"),
            );
            info!(worker = %worker_name, "expired temporary worker");
            let control_store = self.control_store.clone();
            tokio::spawn(async move {
                if let Err(error) = control_store
                    .deactivate_deployment(&worker_name, &deployment_id)
                    .await
                {
                    warn!(worker = %worker_name, deployment_id, error = %error,
                        "failed to remove expired deployment from local store");
                }
            });
        }
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
            self.global_isolate_slot_released(&key, startup);
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
        self.fail_all_streams_for_worker(worker_name, error);
    }

    pub(crate) fn mark_isolate_ready(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) {
        let slot_starting;
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
            slot_starting = Arc::clone(&isolate.slot_starting);
            isolate.startup = IsolateStartup::Ready;
            isolate.last_used_at = Instant::now();
            pool.log_stats("ready");
        }
        self.admission.isolate_ready(&slot_starting);
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
                && let Some(token) = isolate.pending_wait_until.get(request_id)
                && token == completion_token
            {
                isolate.pending_wait_until.remove(request_id);
                if isolate.inflight_count == 0 && isolate.pending_wait_until.is_empty() {
                    isolate.last_used_at = Instant::now();
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
            if let Some((body, completion)) = registration.body_receiver.take() {
                let _ = ready.send(Ok(WorkerStreamOutput {
                    status,
                    headers,
                    body: WorkerStreamBody::new(body, completion),
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

        match sender.try_send(chunk) {
            Ok(()) => {
                let _ = reply.send(Ok(()));
            }
            Err(mpsc::error::TrySendError::Full(chunk)) => {
                // The producer waits for this acknowledgment before emitting another chunk.
                let task = tokio::spawn(async move {
                    let result = sender
                        .send(chunk)
                        .await
                        .map_err(|_| PlatformError::internal("stream response receiver closed"));
                    let _ = reply.send(result);
                });
                self.stream_registrations
                    .get_mut(request_id)
                    .expect("validated stream registration must exist")
                    .pending_send = Some(task);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
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
        let revalidate_span = tracing::debug_span!(
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
            tracing::debug!("skipping duplicate cache revalidation");
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
            .and_then(|pool| self.queue_admission_error(pool, false));
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
        let queue_admission = match self.admission.reserve_queue(queued_bytes, false) {
            Ok(admission) => Some(admission),
            Err(error) => {
                self.revalidation_keys.remove(&key);
                warn!(error = %error, "cache revalidation queue is overloaded");
                return;
            }
        };
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.queue.push_back(PendingInvoke {
                queue_admission,
                runtime_request_id: runtime_request_id.clone(),
                request: invocation,
                request_body: None,
                memory_route: None,
                memory_call: None,
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
            tracing::debug!("scheduled background cache revalidation");
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
        if let Some(completion) = registration.completion.take() {
            let _ = completion.send(Err(error));
        }
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

        let completion_result = match result {
            Ok(_) if registration.started => Ok(()),
            Ok(_) => Err(PlatformError::internal(format!(
                "worker {worker_name} completed streamed request {request_id} without response headers"
            ))),
            Err(error) => Err(error),
        };
        if let Some(ready) = registration.ready.take() {
            let error = completion_result
                .as_ref()
                .expect_err("unstarted stream must fail")
                .clone();
            let _ = ready.send(Err(error));
        }
        if let Some(completion) = registration.completion.take() {
            let _ = completion.send(completion_result);
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
                release_worker_source_modules(&self.module_registry, &pool.source);
                self.account_removed_pool_queue(&pool);
                while let Some(pending) = pool.queue.pop_front() {
                    self.reject_pending_invoke(worker_name, pending, error.clone());
                }
                for isolate in pool.isolates {
                    isolate.request_shutdown();
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
    }

    pub(crate) fn remove_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_idx: usize,
    ) -> RemovedIsolate {
        let mut websocket_open_session_ids = Vec::new();
        let mut replies = Vec::new();
        let mut removed_isolate_id = None;
        let mut stale_targeted_pending = Vec::new();
        let mut stale_targeted_count = 0usize;
        let mut stale_targeted_bytes = 0usize;
        let mut removed = false;
        let mut removed_slot = None;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool.isolates.get_mut(isolate_idx) {
                removed_slot = Some((isolate.id, isolate.startup));
                isolate.startup = IsolateStartup::Retiring;
            }
            if let Some(isolate) = pool.swap_remove_isolate(isolate_idx) {
                isolate.request_shutdown();
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
                        PendingReplyKind::Normal
                        | PendingReplyKind::Stream
                        | PendingReplyKind::WebsocketFrame { .. } => {}
                    }
                    replies.push((request_id, pending.reply));
                }
                removed = true;
            }
        }
        if removed && let Some((isolate_id, startup)) = removed_slot {
            self.track_exiting_isolate_slot(worker_name, generation, isolate_id, startup);
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
        if removed {
            RemovedIsolate {
                removed: true,
                replies,
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

    pub(super) fn oldest_idle_isolate(&self) -> Option<(Instant, String, u64, u64)> {
        self.workers
            .iter()
            .flat_map(|(worker_name, entry)| {
                entry.pools.iter().flat_map(move |(generation, pool)| {
                    pool.isolates.iter().filter_map(move |isolate| {
                        if !pool.queue.is_empty()
                            || !isolate.startup.is_ready()
                            || isolate.inflight_count != 0
                            || !isolate.pending_replies.is_empty()
                            || !isolate.pending_wait_until.is_empty()
                            || isolate.active_websocket_sessions != 0
                            || !isolate.request_control_inbox.is_empty()
                        {
                            return None;
                        }
                        Some((
                            isolate.last_used_at,
                            worker_name.clone(),
                            *generation,
                            isolate.id,
                        ))
                    })
                })
            })
            .min_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
                    .then_with(|| left.2.cmp(&right.2))
                    .then_with(|| left.3.cmp(&right.3))
            })
    }

    pub(crate) fn retire_lru_idle_isolate_for_budget(
        &mut self,
        requesting_worker_name: &str,
        requesting_generation: u64,
    ) -> bool {
        if self.regular_isolate_slots_used() < self.config.max_global_isolates {
            return false;
        }
        let candidate = self.oldest_idle_isolate();
        if !candidate
            .as_ref()
            .is_some_and(|candidate| self.admission.claim_idle_isolate(candidate))
        {
            return false;
        }
        let Some((_last_used_at, worker_name, generation, isolate_id)) = candidate else {
            return false;
        };
        let isolate_idx = self
            .workers
            .get(&worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| pool.isolate_idx(isolate_id));
        let Some(isolate_idx) = isolate_idx else {
            return false;
        };
        if let Some(pool) = self.get_pool_mut(&worker_name, generation) {
            pool.stats.scale_down_count = pool.stats.scale_down_count.saturating_add(1);
            pool.log_stats("budget_pressure");
        }
        let removed = self.remove_isolate(&worker_name, generation, isolate_idx);
        debug_assert!(removed.removed);
        debug_assert!(removed.replies.is_empty());
        tracing::debug!(
            worker = %worker_name,
            generation,
            isolate_id,
            requesting_worker = %requesting_worker_name,
            requesting_generation,
            "retired idle isolate under global budget pressure"
        );
        removed.removed
    }

    pub(crate) fn scale_down_pool(&mut self, worker_name: &str, generation: u64, now: Instant) {
        let min_isolates = self.config.min_isolates;
        let idle_ttl = self.config.idle_ttl;
        let internal_rescue_isolate_ids = self
            .internal_rescue_isolate_slots
            .iter()
            .filter(|key| key.worker_name == worker_name && key.generation == generation)
            .map(|key| key.isolate_id)
            .collect::<HashSet<_>>();
        let mut removed = Vec::new();
        let mut removed_slots = Vec::new();
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            loop {
                let above_minimum = pool.isolates.len() > min_isolates;
                let candidate = pool
                    .isolates
                    .iter()
                    .enumerate()
                    .filter(|(_, isolate)| {
                        above_minimum || internal_rescue_isolate_ids.contains(&isolate.id)
                    })
                    .filter(|(_, isolate)| isolate.inflight_count == 0)
                    .filter(|(_, isolate)| isolate.pending_wait_until.is_empty())
                    .filter(|(_, isolate)| isolate.active_websocket_sessions == 0)
                    .filter(|(_, isolate)| now.duration_since(isolate.last_used_at) >= idle_ttl)
                    .min_by_key(|(_, isolate)| {
                        (
                            !internal_rescue_isolate_ids.contains(&isolate.id),
                            isolate.last_used_at,
                        )
                    });
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
            isolate.request_shutdown();
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
        let mut exiting_slots = Vec::new();
        let retirement_limit = self.config.request_wall_timeout;
        let retirement_expired = self
            .workers
            .get(worker_name)
            .map(|entry| {
                entry
                    .pools
                    .iter()
                    .filter(|(_, pool)| {
                        pool.retired_at
                            .is_some_and(|retired_at| retired_at.elapsed() >= retirement_limit)
                    })
                    .map(|(generation, _)| *generation)
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        for generation in &retirement_expired {
            self.reap_owned_sessions(worker_name, Some(*generation), None);
        }
        let live_websocket_generations: HashSet<u64> = self
            .websocket_sessions
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
                        && ((pool.is_drained() && !live_websocket_generations.contains(generation))
                            || retirement_expired.contains(generation))
                })
                .map(|(generation, _)| *generation)
                .collect::<Vec<_>>()
        };

        for generation in drained {
            if let Some(mut pool) = self
                .workers
                .get_mut(worker_name)
                .and_then(|entry| entry.pools.remove(&generation))
            {
                release_worker_source_modules(&self.module_registry, &pool.source);
                self.account_removed_pool_queue(&pool);
                while let Some(pending) = pool.queue.pop_front() {
                    self.reject_pending_invoke(worker_name, pending, PlatformError::runtime(
                        format!("worker {worker_name} generation {generation} exceeded its retirement deadline"),
                    ));
                }
                for isolate in pool.isolates {
                    isolate.request_shutdown();
                    exiting_slots.push((generation, isolate.id, isolate.startup));
                    for (request_id, pending) in isolate.pending_replies {
                        self.fail_stream_registration(
                            worker_name,
                            &request_id,
                            PlatformError::runtime(format!(
                                "worker {worker_name} generation {generation} retired"
                            )),
                        );
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
    }

    pub(crate) fn worker_stats(&self, worker_name: &str) -> Option<WorkerStats> {
        let admission = self.admission.snapshot();
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
        stats.global_isolates_total = admission.regular + admission.rescue;
        stats.global_isolates_starting = admission.starting;
        stats.global_internal_rescue_isolates = admission.rescue;
        stats.global_isolate_slots_available = self
            .config
            .max_global_isolates
            .saturating_sub(admission.regular);
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
        let admission = self.admission.snapshot();
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
        dump.memory_scheduler.global_isolates_total = admission.regular + admission.rescue;
        dump.memory_scheduler.global_isolates_starting = admission.starting;
        dump.memory_scheduler.global_internal_rescue_isolates = admission.rescue;
        dump.memory_scheduler.global_isolate_slots_available = self
            .config
            .max_global_isolates
            .saturating_sub(admission.regular);
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
}

pub(super) fn deployed_worker_source(
    module_registry: &crate::module_registry::ModuleRegistry,
    source: &str,
    server_modules: &[DeployServerModule],
) -> Result<crate::ops::WorkerSource> {
    if server_modules.is_empty() {
        return Ok(crate::ops::WorkerSource::inline(source.to_string()));
    }
    let (graph_id, entrypoint) = module_registry.register_server_module_graph(
        "worker.js",
        source.to_string(),
        server_modules.to_vec(),
    )?;
    Ok(crate::ops::WorkerSource::Module {
        graph_id,
        entrypoint,
    })
}

fn release_worker_source_modules(
    module_registry: &crate::module_registry::ModuleRegistry,
    source: &crate::ops::WorkerSource,
) {
    if let crate::ops::WorkerSource::Module { graph_id, .. } = source {
        module_registry.release(graph_id);
    }
}
