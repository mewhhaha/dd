use super::*;

const PRE_CANCELED_TTL: Duration = Duration::from_secs(60);
const MAX_PRE_CANCELED_REQUESTS_PER_WORKER: usize = 1024;
const RESPONSE_STREAM_CHANNEL_CAPACITY: usize = 16;

fn take_pre_canceled_request(
    pre_canceled: &mut HashMap<String, HashMap<String, Instant>>,
    worker_name: &str,
    runtime_request_id: &str,
    now: Instant,
) -> bool {
    let Some(request_ids) = pre_canceled.get_mut(worker_name) else {
        return false;
    };
    request_ids.retain(|_, expires_at| *expires_at > now);
    let matched = request_ids.remove(runtime_request_id).is_some();
    if request_ids.is_empty() {
        pre_canceled.remove(worker_name);
    }
    matched
}

fn remember_pre_canceled_request(
    pre_canceled: &mut HashMap<String, HashMap<String, Instant>>,
    worker_name: String,
    runtime_request_id: String,
    now: Instant,
) {
    let request_ids = pre_canceled.entry(worker_name).or_default();
    request_ids.retain(|_, expires_at| *expires_at > now);
    if request_ids.len() >= MAX_PRE_CANCELED_REQUESTS_PER_WORKER
        && !request_ids.contains_key(&runtime_request_id)
    {
        let oldest_id = request_ids
            .iter()
            .min_by_key(|(_, expires_at)| **expires_at)
            .map(|(request_id, _)| request_id.clone());
        if let Some(request_id) = oldest_id {
            request_ids.remove(&request_id);
        }
    }
    request_ids.insert(runtime_request_id, now + PRE_CANCELED_TTL);
}

impl WorkerManager {
    pub(super) fn account_queued_pending(&mut self, queued_bytes: usize) {
        self.queue_counters.requests = self.queue_counters.requests.saturating_add(1);
        self.queue_counters.bytes = self.queue_counters.bytes.saturating_add(queued_bytes);
    }

    pub(super) fn account_dequeued_pending(&mut self, pending: &PendingInvoke) {
        self.queue_counters.requests = self.queue_counters.requests.saturating_sub(1);
        self.queue_counters.bytes = self
            .queue_counters
            .bytes
            .saturating_sub(pending.queued_bytes);
    }

    pub(super) fn account_dequeued_many(&mut self, count: usize, bytes: usize) {
        self.queue_counters.requests = self.queue_counters.requests.saturating_sub(count);
        self.queue_counters.bytes = self.queue_counters.bytes.saturating_sub(bytes);
    }

    pub(super) fn account_removed_pool_queue(&mut self, pool: &WorkerPool) {
        self.account_dequeued_many(pool.queue.len(), pool.queue.queued_bytes());
    }

    fn note_queue_expiry_candidate(&mut self, enqueued_at: Instant) {
        let expires_at = enqueued_at + self.config.max_queue_wait;
        self.next_queue_expiry_at = Some(match self.next_queue_expiry_at {
            Some(current) => current.min(expires_at),
            None => expires_at,
        });
    }

    pub(crate) fn register_stream(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
    ) {
        let (body_sender, body_receiver) = mpsc::channel(RESPONSE_STREAM_CHANNEL_CAPACITY);
        self.stream_registrations.insert(
            runtime_request_id,
            StreamRegistration {
                worker_name,
                completion_token: None,
                ready: Some(ready),
                body_sender,
                body_receiver: Some(body_receiver),
                started: false,
                bytes_sent: 0,
                max_bytes: self.config.max_response_body_bytes,
            },
        );
    }

    pub(crate) fn enqueue_invoke(
        &mut self,
        invoke: EnqueueInvokeRequest,
        event_tx: &RuntimeEventSender,
    ) {
        let EnqueueInvokeRequest {
            worker_name,
            runtime_request_id,
            request,
            request_body,
            mut memory_route,
            memory_call,
            host_rpc_call,
            target_isolate_id,
            target_generation,
            internal_origin,
            reply,
            reply_kind,
        } = invoke;
        let worker_name = worker_name.trim().to_string();
        if take_pre_canceled_request(
            &mut self.pre_canceled,
            &worker_name,
            &runtime_request_id,
            Instant::now(),
        ) {
            let _ = reply.send(Err(PlatformError::runtime("request was aborted")));
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
            return;
        }
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        if request.body.len() > self.config.max_request_body_bytes {
            let error = PlatformError::bad_request(format!(
                "request body exceeded max_request_body_bytes ({} bytes)",
                self.config.max_request_body_bytes
            ));
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        }
        let queued_bytes = estimate_pending_invoke_bytes(&request, request_body.is_some());
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

        let admission_error = entry
            .pools
            .get(&generation)
            .and_then(|pool| self.queue_admission_error(pool, queued_bytes, internal_origin));
        if let Some(error) = admission_error {
            self.reject_not_queued_invoke(
                &worker_name,
                &runtime_request_id,
                reply,
                &reply_kind,
                error,
            );
            return;
        }

        if let Some(route) = memory_route.as_mut() {
            if route.shard_index.is_none() {
                route.shard_index = Some(
                    self.memory_store
                        .shard_index_for_key(&route.binding, &route.key),
                );
            }
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
            let enqueued_at = Instant::now();
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
                enqueued_at,
                queued_bytes,
            });
            pool.update_queue_warning(&warn_thresholds);
            self.account_queued_pending(queued_bytes);
            self.note_queue_expiry_candidate(enqueued_at);
        } else {
            let error = PlatformError::not_found("Worker not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        }

        self.dispatch_pool(&worker_name, generation, event_tx);
    }

    pub(super) fn queue_admission_error(
        &self,
        pool: &WorkerPool,
        queued_bytes: usize,
        internal_origin: bool,
    ) -> Option<PlatformError> {
        let per_worker_limit = self.config.max_queued_requests_per_worker
            + if internal_origin {
                self.config.reserved_internal_queued_requests_per_worker
            } else {
                0
            };
        if pool.queue.len() >= per_worker_limit {
            return Some(PlatformError::overloaded(format!(
                "worker queue is full (max {per_worker_limit} queued requests)"
            )));
        }

        let global_limit = self.config.max_global_queued_requests
            + if internal_origin {
                self.config.reserved_internal_queued_requests_per_worker
            } else {
                0
            };
        if self.queue_counters.requests >= global_limit {
            return Some(PlatformError::overloaded(format!(
                "runtime queue is full (max {global_limit} queued requests)"
            )));
        }

        if self.queue_counters.bytes.saturating_add(queued_bytes)
            > self.config.max_global_queued_bytes
        {
            return Some(PlatformError::overloaded(format!(
                "runtime queue byte budget is full (max {} bytes)",
                self.config.max_global_queued_bytes
            )));
        }

        None
    }

    fn reject_not_queued_invoke(
        &mut self,
        worker_name: &str,
        runtime_request_id: &str,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        reply_kind: &PendingReplyKind,
        error: PlatformError,
    ) {
        match reply_kind {
            PendingReplyKind::WebsocketOpen { session_id } => {
                if let Some(waiter) = self.websocket_open_waiters.remove(session_id) {
                    let _ = waiter.send(Err(error.clone()));
                }
            }
            PendingReplyKind::TransportOpen { session_id } => {
                if let Some(waiter) = self.transport_open_waiters.remove(session_id) {
                    let _ = waiter.send(Err(error.clone()));
                }
                self.transport_open_channels.remove(session_id);
            }
            PendingReplyKind::Normal
            | PendingReplyKind::Stream
            | PendingReplyKind::DynamicFetch { .. }
            | PendingReplyKind::WebsocketFrame { .. } => {}
        }
        self.fail_stream_registration(worker_name, runtime_request_id, error.clone());
        let _ = reply.send(Err(error));
    }

    pub(crate) fn expire_queued_requests(&mut self) {
        if self.queue_counters.requests == 0 {
            self.next_queue_expiry_at = None;
            return;
        }
        let max_queue_wait = self.config.max_queue_wait;
        let now = Instant::now();
        if self
            .next_queue_expiry_at
            .is_some_and(|next_expiry_at| now < next_expiry_at)
        {
            return;
        }
        let mut expired = Vec::new();
        let mut expired_count = 0usize;
        let mut expired_bytes = 0usize;
        let mut next_queue_expiry_at: Option<Instant> = None;

        for (worker_name, entry) in &mut self.workers {
            for pool in entry.pools.values_mut() {
                for pending in pool.queue.drain_expired(now, max_queue_wait) {
                    expired_count = expired_count.saturating_add(1);
                    expired_bytes = expired_bytes.saturating_add(pending.queued_bytes);
                    expired.push((worker_name.clone(), pending));
                }
                if let Some(expires_at) = pool.queue.next_expiry_at(max_queue_wait) {
                    next_queue_expiry_at = Some(match next_queue_expiry_at {
                        Some(current) => current.min(expires_at),
                        None => expires_at,
                    });
                }
            }
        }
        self.next_queue_expiry_at = next_queue_expiry_at;
        self.account_dequeued_many(expired_count, expired_bytes);

        for (worker_name, pending) in expired {
            let runtime_request_id = pending.runtime_request_id.clone();
            warn!(
                worker = %worker_name,
                runtime_request_id = %runtime_request_id,
                max_queue_wait_ms = max_queue_wait.as_millis() as u64,
                "dropping request after queue wait limit"
            );
            self.reject_pending_invoke(
                &worker_name,
                pending,
                PlatformError::overloaded("request exceeded queue wait limit"),
            );
        }
    }

    pub(crate) fn expire_inflight_requests(&mut self, event_tx: &RuntimeEventSender) {
        let request_wall_timeout = self.config.request_wall_timeout;
        let now = Instant::now();
        let mut expired_isolates = Vec::new();

        for (worker_name, entry) in &self.workers {
            for (generation, pool) in &entry.pools {
                for isolate in &pool.isolates {
                    let expired_request_ids = isolate
                        .pending_replies
                        .iter()
                        .filter(|(_, pending)| {
                            now.duration_since(pending.dispatched_at) >= request_wall_timeout
                        })
                        .map(|(request_id, _)| request_id.clone())
                        .collect::<Vec<_>>();
                    if expired_request_ids.is_empty() {
                        continue;
                    }
                    let v8_handle = isolate
                        .v8_handle
                        .lock()
                        .expect("v8 handle mutex poisoned")
                        .clone();
                    expired_isolates.push((
                        worker_name.clone(),
                        *generation,
                        isolate.id,
                        expired_request_ids,
                        v8_handle,
                    ));
                }
            }
        }

        for (worker_name, generation, isolate_id, expired_request_ids, v8_handle) in
            expired_isolates
        {
            let timeout_ms = request_wall_timeout.as_millis() as u64;
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                request_wall_timeout_ms = timeout_ms,
                expired_requests = %expired_request_ids.join(","),
                "terminating isolate after request wall-time limit"
            );
            if let Some(v8_handle) = v8_handle {
                let _ = v8_handle.terminate_execution();
            }
            self.fail_isolate(
                &worker_name,
                generation,
                isolate_id,
                PlatformError::runtime(format!(
                    "request exceeded wall-time limit of {timeout_ms}ms; isolate retired"
                )),
            );
            self.dispatch_pool(&worker_name, generation, event_tx);
            self.cleanup_drained_generations_for(&worker_name);
        }
    }

    pub(crate) fn expire_starting_isolates(&mut self, event_tx: &RuntimeEventSender) {
        let startup_timeout = self.config.isolate_startup_timeout;
        let now = Instant::now();
        let mut expired_isolates = Vec::new();

        for (worker_name, entry) in &self.workers {
            for (generation, pool) in &entry.pools {
                for isolate in &pool.isolates {
                    if !isolate.startup.timed_out(now, startup_timeout) {
                        continue;
                    }
                    let v8_handle = isolate
                        .v8_handle
                        .lock()
                        .expect("v8 handle mutex poisoned")
                        .clone();
                    expired_isolates.push((
                        worker_name.clone(),
                        *generation,
                        isolate.id,
                        v8_handle,
                    ));
                }
            }
        }

        for (worker_name, generation, isolate_id, v8_handle) in expired_isolates {
            let timeout_ms = startup_timeout.as_millis() as u64;
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                isolate_startup_timeout_ms = timeout_ms,
                "terminating isolate after startup timeout"
            );
            if let Some(v8_handle) = v8_handle {
                let _ = v8_handle.terminate_execution();
            }
            self.fail_isolate(
                &worker_name,
                generation,
                isolate_id,
                PlatformError::runtime(format!(
                    "isolate exceeded startup timeout of {timeout_ms}ms; isolate retired"
                )),
            );
            self.dispatch_pool(&worker_name, generation, event_tx);
            self.cleanup_drained_generations_for(&worker_name);
        }
    }

    pub(super) fn reject_pending_invoke(
        &mut self,
        worker_name: &str,
        pending: PendingInvoke,
        error: PlatformError,
    ) {
        let PendingInvoke {
            runtime_request_id,
            reply,
            reply_kind,
            ..
        } = pending;
        self.clear_revalidation_for_request(&runtime_request_id);
        self.reject_not_queued_invoke(worker_name, &runtime_request_id, reply, &reply_kind, error);
    }

    pub(crate) fn try_dispatch_direct_dynamic_fetch(
        &mut self,
        dispatch: DirectDynamicFetchRequest,
    ) -> DirectDynamicFetchDispatch {
        let DirectDynamicFetchRequest {
            worker_name,
            generation,
            target_isolate_id,
            runtime_request_id,
            request,
            reply,
            handle,
        } = dispatch;
        if request.body.len() > self.config.max_request_body_bytes {
            let _ = reply.send(Err(PlatformError::bad_request(format!(
                "request body exceeded max_request_body_bytes ({} bytes)",
                self.config.max_request_body_bytes
            ))));
            return DirectDynamicFetchDispatch::Dispatched;
        }
        let config_max_inflight = self.config.max_inflight_per_isolate;
        let dispatch_result = {
            let Some(pool) = self.get_pool_mut(&worker_name, generation) else {
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
            let Some(isolate_idx) = pool.isolate_idx(target_isolate_id) else {
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

            let counted_reuse = pool.isolates[isolate_idx].served_requests > 0;
            if counted_reuse {
                pool.stats.reuse_count += 1;
            }

            let completion_token = next_runtime_token("done");
            let dispatched_at = Instant::now();
            let completion_meta = Some(PendingReplyMeta {
                method: request.method.clone(),
                url: request.url.clone(),
                traceparent: None,
                user_request_id: request.request_id.clone(),
            });
            let command = pool.build_execute_command(BuildExecuteCommand {
                runtime_request_id: runtime_request_id.clone(),
                completion_token: completion_token.clone(),
                request,
                request_body: None,
                stream_response: false,
                memory_call: None,
                host_rpc_call: None,
                memory_route: None,
                dispatched_at,
                profile_memory_atomic: false,
            });

            let isolate = &mut pool.isolates[isolate_idx];
            isolate.served_requests += 1;
            isolate.inflight_count += 1;
            isolate.pending_replies.insert(
                runtime_request_id.clone(),
                PendingReply {
                    completion_token,
                    canceled: false,
                    memory_key: None,
                    active_memory_lease: None,
                    memory_outbox_shard: None,
                    internal_origin: true,
                    reply,
                    completion_meta,
                    kind: PendingReplyKind::DynamicFetch { handle },
                    dispatched_at,
                },
            );

            if isolate.sender.try_send(command).is_ok() {
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
        let failed = self.remove_isolate(&worker_name, generation, isolate_idx);
        for (request_id, reply) in failed.replies {
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
        event_tx: &RuntimeEventSender,
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
        let route = MemoryRoute::new(
            decoded.binding.trim().to_string(),
            decoded.key.trim().to_string(),
        );
        if route.binding.is_empty() || route.key.is_empty() {
            let _ = payload.reply.send(Err(PlatformError::bad_request(
                "memory binding/key must not be empty",
            )));
            return;
        }
        if matches!(
            &memory_call,
            MemoryExecutionCall::Method { name, .. } if name == MEMORY_ATOMIC_METHOD
        ) {
            self.memory_store.record_profile(
                MemoryProfileMetricKind::RuntimeAtomicInvokeEventWait,
                duration_us(payload.created_at.elapsed()),
                1,
            );
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
                    target_isolate_id = Some(payload.caller_isolate_id);
                }
            }
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            EnqueueInvokeRequest {
                worker_name: decoded.worker_name,
                runtime_request_id,
                request,
                request_body: None,
                memory_route: Some(route),
                memory_call: Some(memory_call),
                host_rpc_call: None,
                target_isolate_id,
                target_generation,
                internal_origin: false,
                reply: reply_tx,
                reply_kind: PendingReplyKind::Normal,
            },
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
        event_tx: &RuntimeEventSender,
    ) {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return;
        }

        let mut touched_generations = Vec::new();
        let mut abort_commands = Vec::new();
        let mut retire_isolates = Vec::new();
        let mut matched = false;
        let mut cleared_request_ids = Vec::new();
        let mut websocket_waiters_to_abort = Vec::new();
        let mut dequeued_count = 0usize;
        let mut dequeued_bytes = 0usize;

        if let Some(entry) = self.workers.get_mut(&worker_name) {
            for (generation, pool) in &mut entry.pools {
                let mut generation_touched = false;

                if let Some(pending) = pool.queue.remove_by_runtime_request_id(&runtime_request_id)
                {
                    dequeued_count = dequeued_count.saturating_add(1);
                    dequeued_bytes = dequeued_bytes.saturating_add(pending.queued_bytes);
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

                for isolate in &mut pool.isolates {
                    if let Some(pending_reply) = isolate.pending_replies.get(&runtime_request_id) {
                        if let PendingReplyKind::WebsocketOpen { session_id } = &pending_reply.kind
                        {
                            websocket_waiters_to_abort.push(session_id.clone());
                        }
                        if matches!(&pending_reply.kind, PendingReplyKind::Stream) {
                            let stream_request_ids = isolate
                                .pending_replies
                                .iter()
                                .filter(|(_, pending)| {
                                    matches!(&pending.kind, PendingReplyKind::Stream)
                                })
                                .map(|(request_id, _)| request_id.clone())
                                .collect::<Vec<_>>();
                            retire_isolates.push((*generation, isolate.id, stream_request_ids));
                        } else {
                            if let Some(pending_reply) =
                                isolate.pending_replies.get_mut(&runtime_request_id)
                            {
                                pending_reply.canceled = true;
                            }
                            abort_commands.push((*generation, isolate.id, isolate.sender.clone()));
                        }
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
        self.account_dequeued_many(dequeued_count, dequeued_bytes);

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
                .try_send(IsolateCommand::Abort {
                    runtime_request_id: runtime_request_id.clone(),
                })
                .is_err()
            {
                let failed = self.remove_isolate_by_id(&worker_name, generation, isolate_id);
                for (request_id, reply) in failed.replies {
                    self.clear_revalidation_for_request(&request_id);
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
            }
        }

        for (generation, isolate_id, stream_request_ids) in retire_isolates {
            for request_id in stream_request_ids {
                self.fail_stream_registration(
                    &worker_name,
                    &request_id,
                    PlatformError::runtime("request was aborted"),
                );
            }
            let failed = self.remove_isolate_by_id(&worker_name, generation, isolate_id);
            for (request_id, reply) in failed.replies {
                self.clear_revalidation_for_request(&request_id);
                let _ = reply.send(Err(PlatformError::runtime(
                    "streaming request was aborted; isolate retired",
                )));
            }
        }

        touched_generations.sort_unstable();
        touched_generations.dedup();
        for generation in touched_generations {
            self.dispatch_pool(&worker_name, generation, event_tx);
        }
        if !matched {
            remember_pre_canceled_request(
                &mut self.pre_canceled,
                worker_name.clone(),
                runtime_request_id.clone(),
                Instant::now(),
            );
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
        event_tx: &RuntimeEventSender,
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
                let allow_memory_atomic_overflow = pool.isolates.len() >= max_isolates;
                let selection = select_dispatch_candidate(
                    pool,
                    max_inflight,
                    pool.strict_request_isolation,
                    allow_memory_atomic_overflow,
                );
                let spawn_needed = selection.is_none()
                    && !pool.has_dispatch_capacity(max_inflight)
                    && !pool.has_starting_isolate()
                    && pool.isolates.len() < max_isolates;
                (selection, spawn_needed)
            };

            if spawn_needed {
                if let Err(error) = self.spawn_isolate(worker_name, generation, event_tx.clone()) {
                    let pending = self
                        .get_pool_mut(worker_name, generation)
                        .and_then(|pool| pool.queue.pop_front());
                    if let Some(pending) = pending {
                        self.account_dequeued_pending(&pending);
                        let _ = pending.reply.send(Err(error));
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
                DispatchSelection::DropStaleTarget { queue_key } => {
                    let stale = self
                        .get_pool_mut(worker_name, generation)
                        .and_then(|pool| pool.queue.remove(queue_key));
                    if let Some(stale) = stale {
                        self.account_dequeued_pending(&stale);
                        let _ = stale
                            .reply
                            .send(Err(PlatformError::runtime("target isolate is unavailable")));
                    }
                    continue;
                }
            };
            let isolate_idx = candidate.isolate_idx;
            let Some(pending_invoke) = self
                .get_pool_mut(worker_name, generation)
                .and_then(|pool| pool.queue.remove(candidate.queue_key))
            else {
                return;
            };
            self.account_dequeued_pending(&pending_invoke);

            let runtime_request_id = pending_invoke.runtime_request_id.clone();
            let internal_origin = pending_invoke.internal_origin;
            let pending_memory_key = pending_invoke
                .memory_route
                .as_ref()
                .map(|route| route.owner_key.clone());
            let active_memory_entity = pending_memory_key.as_ref().filter(|_| {
                matches!(
                    pending_invoke.memory_call.as_ref(),
                    Some(MemoryExecutionCall::Method { name, .. }) if name == MEMORY_ATOMIC_METHOD
                )
            });
            let profile_memory_atomic = active_memory_entity.is_some();
            let memory_outbox_shard = pending_invoke
                .memory_route
                .as_ref()
                .and_then(|route| route.shard_index);
            if profile_memory_atomic {
                self.memory_store.record_profile(
                    MemoryProfileMetricKind::RuntimeAtomicQueueWait,
                    duration_us(pending_invoke.enqueued_at.elapsed()),
                    1,
                );
            }
            let info_tracing_enabled = tracing::enabled!(Level::INFO);
            let needs_completion_meta = info_tracing_enabled
                || self
                    .get_pool_mut(worker_name, generation)
                    .and_then(|pool| pool.internal_trace.as_ref())
                    .is_some();
            let traceparent = if needs_completion_meta {
                traceparent_from_headers(&pending_invoke.request.headers).map(str::to_string)
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
            let dispatched_at = Instant::now();
            if let Some(registration) = self.stream_registrations.get_mut(&runtime_request_id) {
                if registration.worker_name == worker_name {
                    registration.completion_token = Some(completion_token.clone());
                }
            }
            let stream_response = self.stream_registrations.contains_key(&runtime_request_id);
            let mut send_failed = false;
            let active_memory_epoch = if active_memory_entity.is_some() {
                self.next_memory_entity_epoch =
                    self.next_memory_entity_epoch.saturating_add(1).max(1);
                Some(self.next_memory_entity_epoch)
            } else {
                None
            };
            if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                if isolate_idx >= pool.isolates.len() {
                    continue;
                }

                let should_count_reuse = pool.isolates[isolate_idx].served_requests > 0;
                if should_count_reuse {
                    pool.stats.reuse_count += 1;
                }
                let pending_kind = pending_invoke.reply_kind.clone();
                let completion_meta = Some(PendingReplyMeta {
                    method: pending_invoke.request.method.clone(),
                    url: pending_invoke.request.url.clone(),
                    traceparent: traceparent.clone(),
                    user_request_id: pending_invoke.request.request_id.clone(),
                });
                let active_memory_lease = active_memory_entity.as_ref().map(|owner_key| {
                    let owner_isolate_id = pool.isolates[isolate_idx].id;
                    if let Some(shard_index) = memory_outbox_shard {
                        pool.memory_shard_affinity
                            .insert(shard_index, owner_isolate_id);
                    }
                    pool.acquire_memory_entity_lease(
                        (*owner_key).clone(),
                        owner_isolate_id,
                        active_memory_epoch.expect("memory epoch must be allocated"),
                    )
                });
                let active_memory_route_epoch = active_memory_lease
                    .as_ref()
                    .map(|lease| i64::try_from(lease.epoch).unwrap_or(i64::MAX));
                let memory_route = match (pending_invoke.memory_route, active_memory_route_epoch) {
                    (Some(route), Some(epoch)) => Some(route.with_owner_epoch(epoch)),
                    (route, _) => route,
                };
                let command = pool.build_execute_command(BuildExecuteCommand {
                    runtime_request_id: runtime_request_id.clone(),
                    completion_token: completion_token.clone(),
                    request: pending_invoke.request,
                    request_body: pending_invoke.request_body,
                    stream_response,
                    memory_call: pending_invoke.memory_call,
                    host_rpc_call: pending_invoke.host_rpc_call,
                    memory_route,
                    dispatched_at,
                    profile_memory_atomic,
                });
                let isolate = &mut pool.isolates[isolate_idx];
                isolate.served_requests += 1;
                isolate.inflight_count += 1;
                isolate.pending_replies.insert(
                    runtime_request_id.clone(),
                    PendingReply {
                        completion_token,
                        canceled: false,
                        memory_key: pending_memory_key,
                        active_memory_lease: active_memory_lease.clone(),
                        memory_outbox_shard,
                        internal_origin,
                        reply: pending_reply
                            .take()
                            .expect("pending reply must exist before dispatch"),
                        completion_meta,
                        kind: pending_kind,
                        dispatched_at,
                    },
                );
                if isolate.sender.try_send(command).is_err() {
                    isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                    pending_reply = isolate
                        .pending_replies
                        .remove(&runtime_request_id)
                        .map(|pending| pending.reply);
                    if let Some(lease) = &active_memory_lease {
                        pool.release_memory_entity_lease(lease);
                    }
                    send_failed = true;
                }
            }

            if send_failed {
                let failed = self.remove_isolate(worker_name, generation, isolate_idx);
                self.clear_revalidation_for_request(&runtime_request_id);
                if let Some(reply) = pending_reply.take() {
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
                for (request_id, reply) in failed.replies {
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
        event_tx: RuntimeEventSender,
    ) -> Result<()> {
        let (snapshot, snapshot_preloaded, source, deployment_config) = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .map(|pool| {
                (
                    pool.snapshot,
                    pool.snapshot_preloaded,
                    pool.source.clone(),
                    Arc::clone(&pool.deployment_config),
                )
            })
            .ok_or_else(|| PlatformError::not_found("Worker not found"))?;
        let allow_code_generation = self.config.debug_code_generation;
        let isolate_id = self.next_isolate_id;
        self.next_isolate_id += 1;
        let kv_store = self.kv_store.clone();
        let memory_store = self.memory_store.clone();
        let cache_store = self.cache_store.clone();
        let dynamic_profile = self.dynamic_profile.clone();
        let open_handle_registry = self.open_handle_registry.clone();
        let execution_limits = crate::ops::RuntimeExecutionLimits {
            max_request_body_bytes: self.config.max_request_body_bytes,
            max_isolate_heap_bytes: self.config.max_isolate_heap_bytes,
        };
        let isolate = spawn_isolate_thread(IsolateThreadStart {
            snapshot,
            snapshot_preloaded,
            source,
            deployment_config,
            allow_code_generation,
            kv_store,
            memory_store,
            cache_store,
            open_handle_registry,
            dynamic_profile,
            execution_limits,
            runtime_fast_sender: self.runtime_fast_sender.clone(),
            worker_name: worker_name.to_string(),
            generation,
            isolate_id,
            event_tx: event_tx.clone(),
        })?;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.stats.spawn_count += 1;
            pool.push_isolate(isolate);
            pool.log_stats("spawn");
            Ok(())
        } else {
            Err(PlatformError::internal("worker pool missing"))
        }
    }

    pub(crate) async fn finish_request(
        &mut self,
        finish: FinishRequest,
        event_tx: &RuntimeEventSender,
    ) {
        let FinishRequest {
            worker_name,
            generation,
            isolate_id,
            request_id,
            completion_token,
            finished_at,
            wait_until_count,
            mut result,
        } = finish;
        let completion_started_at = Instant::now();
        let worker_name = worker_name.as_str();
        let request_id = request_id.as_str();
        let completion_token = completion_token.as_str();
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
        let mut memory_outbox_shard = None;
        let mut atomic_execution_duration = None;
        let mut atomic_completion_wait = None;
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
                let mut active_memory_lease = None;
                if let Some(pending) = isolate.pending_replies.remove(request_id) {
                    canceled = pending.canceled;
                    internal_origin = pending.internal_origin;
                    active_memory_lease = pending.active_memory_lease.clone();
                    memory_outbox_shard = pending.memory_outbox_shard;
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
                    if pending.active_memory_lease.is_some() {
                        atomic_execution_duration =
                            finished_at.checked_duration_since(pending.dispatched_at);
                        atomic_completion_wait =
                            completion_started_at.checked_duration_since(finished_at);
                    }
                    pending_kind = pending.kind;
                    reply = Some(pending.reply);
                }
                if let Some(active_memory_lease) = active_memory_lease {
                    pool.release_memory_entity_lease(&active_memory_lease);
                }
            }
            pool.log_stats("complete");
        }
        if let Some(execution_duration) = atomic_execution_duration {
            self.memory_store.record_profile(
                MemoryProfileMetricKind::RuntimeAtomicExecution,
                duration_us(execution_duration),
                1,
            );
        }
        if let Some(completion_wait) = atomic_completion_wait {
            self.memory_store.record_profile(
                MemoryProfileMetricKind::RuntimeAtomicCompletionWait,
                duration_us(completion_wait),
                1,
            );
        }
        if clear_revalidation {
            self.clear_revalidation_for_request(request_id);
        }
        if !stream_registered {
            if let Ok(output) = &result {
                if output.body.len() > self.config.max_response_body_bytes {
                    result = Err(PlatformError::runtime(format!(
                        "response body exceeded max_response_body_bytes ({} bytes)",
                        self.config.max_response_body_bytes
                    )));
                }
            }
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

        let stream_consumes_result =
            stream_registered && matches!(&pending_kind, PendingReplyKind::Stream);
        let mut stream_result = if stream_registered && !stream_consumes_result {
            Some(result.clone())
        } else {
            None
        };
        let trace_result = if trace_destination.is_some() {
            Some(match &result {
                Ok(output) => TraceResultMeta {
                    status: Some(output.status),
                    error: None,
                },
                Err(error) => TraceResultMeta {
                    status: None,
                    error: Some(error.to_string()),
                },
            })
        } else {
            None
        };

        if stream_consumes_result {
            stream_result = Some(result);
            if canceled {
                if info_tracing_enabled {
                    info!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropped completion for canceled request"
                    );
                }
            } else if info_tracing_enabled {
                tracing::info!("request completion delivered");
            }
        } else if !canceled {
            match pending_kind {
                PendingReplyKind::Normal | PendingReplyKind::Stream => {
                    if let Some(reply) = reply {
                        let _ = reply.send(result);
                    }
                }
                PendingReplyKind::DynamicFetch { handle } => {
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
                    self.complete_websocket_frame(
                        session_id,
                        reply,
                        result,
                        memory_outbox_shard.is_some(),
                    );
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
        if let Some(shard_index) = memory_outbox_shard {
            self.schedule_memory_outbox_drain_shard(shard_index, event_tx);
        }
        if let (Some(trace_destination), Some(trace_result)) = (trace_destination, trace_result) {
            self.enqueue_trace_forward(
                TraceForwardRequest {
                    worker_name: worker_name.to_string(),
                    generation,
                    request_method,
                    request_url,
                    runtime_request_id: request_id.to_string(),
                    user_request_id,
                    result: trace_result,
                    execution_ms: execution_ms.unwrap_or_default(),
                    wait_until_count,
                    internal_origin,
                    trace_destination: Some(trace_destination),
                },
                event_tx,
            );
        }
        if let Some(stream_result) = stream_result {
            self.complete_stream_registration(
                worker_name,
                request_id,
                completion_token,
                stream_result,
            )
            .await;
        }
    }

    pub(crate) fn enqueue_trace_forward(
        &mut self,
        forward: TraceForwardRequest,
        event_tx: &RuntimeEventSender,
    ) {
        let TraceForwardRequest {
            worker_name,
            generation,
            request_method,
            request_url,
            runtime_request_id,
            user_request_id,
            result,
            execution_ms,
            wait_until_count,
            internal_origin,
            trace_destination,
        } = forward;
        let worker_name = worker_name.as_str();
        let request_method = request_method.as_str();
        let request_url = request_url.as_str();
        let runtime_request_id = runtime_request_id.as_str();
        let user_request_id = user_request_id.as_str();
        let Some(trace_destination) = trace_destination else {
            return;
        };
        if internal_origin {
            return;
        }

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
            status: result.status,
            ok: result.status.is_some(),
            error: result.error,
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
            EnqueueInvokeRequest {
                worker_name: trace_destination.worker,
                runtime_request_id: Uuid::new_v4().to_string(),
                request: trace_request,
                request_body: None,
                memory_route: None,
                memory_call: None,
                host_rpc_call: None,
                target_isolate_id: None,
                target_generation: None,
                internal_origin: true,
                reply,
                reply_kind: PendingReplyKind::Normal,
            },
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

pub(super) fn estimate_pending_invoke_bytes(
    request: &WorkerInvocation,
    has_streaming_body: bool,
) -> usize {
    let headers = request
        .headers
        .iter()
        .map(|(name, value)| name.len().saturating_add(value.len()))
        .sum::<usize>();
    request
        .method
        .len()
        .saturating_add(request.url.len())
        .saturating_add(request.request_id.len())
        .saturating_add(request.body.len())
        .saturating_add(headers)
        .saturating_add(if has_streaming_body { 1024 } else { 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_canceled_request_is_taken_once_before_ttl() {
        let now = Instant::now();
        let mut pre_canceled = HashMap::new();

        remember_pre_canceled_request(
            &mut pre_canceled,
            "worker".to_string(),
            "request".to_string(),
            now,
        );

        assert!(take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request",
            now + Duration::from_secs(1)
        ));
        assert!(!take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request",
            now + Duration::from_secs(1)
        ));
        assert!(pre_canceled.is_empty());
    }

    #[test]
    fn pre_canceled_requests_expire_and_are_capped() {
        let now = Instant::now();
        let mut pre_canceled = HashMap::new();

        remember_pre_canceled_request(
            &mut pre_canceled,
            "worker".to_string(),
            "expired".to_string(),
            now,
        );
        assert!(!take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "expired",
            now + PRE_CANCELED_TTL + Duration::from_millis(1)
        ));
        assert!(pre_canceled.is_empty());

        for index in 0..(MAX_PRE_CANCELED_REQUESTS_PER_WORKER + 10) {
            remember_pre_canceled_request(
                &mut pre_canceled,
                "worker".to_string(),
                format!("request-{index}"),
                now + Duration::from_millis(index as u64),
            );
        }

        assert_eq!(
            pre_canceled.get("worker").expect("worker tombstones").len(),
            MAX_PRE_CANCELED_REQUESTS_PER_WORKER
        );
        assert!(!take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request-0",
            now + Duration::from_secs(1)
        ));
        assert!(take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request-1024",
            now + Duration::from_secs(1)
        ));
    }
}
