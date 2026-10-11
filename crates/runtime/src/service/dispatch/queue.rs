//! Queue accounting, admission, expiry, and cancellation.

use super::*;

impl WorkerManager {
    pub(in crate::service) fn account_queued_pending(&mut self, queued_bytes: usize) {
        self.queue_counters.requests = self.queue_counters.requests.saturating_add(1);
        self.queue_counters.bytes = self.queue_counters.bytes.saturating_add(queued_bytes);
    }

    pub(super) fn account_dequeued_pending(&mut self, pending: &PendingInvoke) {
        if let Some(admission) = &pending.queue_admission {
            admission.release();
        }
        self.queue_counters.requests = self.queue_counters.requests.saturating_sub(1);
        self.queue_counters.bytes = self
            .queue_counters
            .bytes
            .saturating_sub(pending.queued_bytes);
    }

    pub(in crate::service) fn account_dequeued_many(&mut self, count: usize, bytes: usize) {
        self.queue_counters.requests = self.queue_counters.requests.saturating_sub(count);
        self.queue_counters.bytes = self.queue_counters.bytes.saturating_sub(bytes);
    }

    pub(in crate::service) fn account_removed_pool_queue(&mut self, pool: &WorkerPool) {
        self.account_dequeued_many(pool.queue.len(), pool.queue.queued_bytes());
    }

    fn note_queue_expiry_candidate(&mut self, enqueued_at: Instant) {
        let expires_at = enqueued_at + self.config.max_queue_wait;
        self.next_queue_expiry_at = Some(match self.next_queue_expiry_at {
            Some(current) => current.min(expires_at),
            None => expires_at,
        });
    }

    pub(crate) fn enqueue_invoke(
        &mut self,
        invoke: EnqueueInvokeRequest,
        event_tx: &RuntimeEventSender,
    ) {
        let EnqueueInvokeRequest {
            mut queue_admission,
            worker_name,
            runtime_request_id,
            request,
            request_body,
            mut memory_route,
            memory_call,
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
        let queued_bytes = estimate_pending_invoke_bytes(&request, request_body.is_some())
            .saturating_add(
                memory_call
                    .as_ref()
                    .map_or(0, MemoryExecutionCall::queued_bytes),
            );
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
            .and_then(|pool| self.queue_admission_error(pool, internal_origin));
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

        let admission = match queue_admission.as_mut() {
            Some(admission) => admission.resize(queued_bytes),
            None => self
                .admission
                .reserve_queue(queued_bytes, internal_origin)
                .map(|admission| {
                    queue_admission = Some(admission);
                }),
        };
        if let Err(error) = admission {
            self.reject_not_queued_invoke(
                &worker_name,
                &runtime_request_id,
                reply,
                &reply_kind,
                error,
            );
            return;
        }

        if let Some(route) = memory_route.as_mut()
            && route.shard_index.is_none()
        {
            route.shard_index = Some(self.memory_store.shard_index_for_key(
                &crate::memory::worker_namespace(&worker_name, &route.binding),
                &route.key,
            ));
        }
        if let Some(pool) = self.get_pool_mut(&worker_name, generation) {
            if let Some(route) = &memory_route
                && !pool
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
            let enqueued_at = Instant::now();
            pool.queue.push_back(PendingInvoke {
                queue_admission,
                runtime_request_id,
                request,
                request_body,
                memory_route,
                memory_call,
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

    pub(in crate::service) fn queue_admission_error(
        &self,
        pool: &WorkerPool,
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

        None
    }

    fn reject_not_queued_invoke(
        &mut self,
        worker_name: &str,
        runtime_request_id: &str,
        reply: InvocationReply,
        reply_kind: &PendingReplyKind,
        error: PlatformError,
    ) {
        match reply_kind {
            PendingReplyKind::WebsocketOpen { session_id } => {
                if let Some(waiter) = self.websocket_open_waiters.remove(session_id) {
                    let _ = waiter.send(Err(error.clone()));
                }
            }
            PendingReplyKind::Normal
            | PendingReplyKind::Stream
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
                        .chain(
                            isolate
                                .pending_wait_until
                                .iter()
                                .filter(|(_, pending)| {
                                    now.duration_since(pending.completed_at) >= request_wall_timeout
                                })
                                .map(|(request_id, _)| format!("{request_id}:waitUntil")),
                        )
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

    pub(in crate::service) fn reject_pending_invoke(
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
        let mut aborted_stream = false;
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
                    // The isolate aborts the request in place: its signal
                    // fires, its body reads stop and its completion is
                    // dropped. Concurrent requests on the isolate carry on;
                    // work that ignores the abort meets the wall-time limit.
                    if let Some(pending_reply) =
                        isolate.pending_replies.get_mut(&runtime_request_id)
                    {
                        if let PendingReplyKind::WebsocketOpen { session_id } = &pending_reply.kind
                        {
                            websocket_waiters_to_abort.push(session_id.clone());
                        }
                        if matches!(&pending_reply.kind, PendingReplyKind::Stream) {
                            aborted_stream = true;
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

        if aborted_stream {
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
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
}

pub(in crate::service) fn estimate_pending_invoke_bytes(
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
