//! Pool dispatch and isolate scaling.

use super::*;

impl WorkerManager {
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
        let max_isolates = self.config.max_isolates_per_worker;
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
                let internal_request_needs_capacity =
                    pool.queue.has_internal_request() && !pool.has_dispatch_capacity(max_inflight);
                let external_request_needs_capacity = pool.queue.has_external_request()
                    && !pool.has_regular_dispatch_capacity(max_inflight);
                let spawn_needed = selection.is_none()
                    && (internal_request_needs_capacity || external_request_needs_capacity)
                    && !pool.has_starting_isolate()
                    && (pool
                        .isolates
                        .iter()
                        .filter(|isolate| !isolate.internal_rescue)
                        .count()
                        < max_isolates
                        || internal_request_needs_capacity);
                (selection, spawn_needed)
            };

            if spawn_needed {
                self.request_scale_up(worker_name, generation);
                self.process_scale_up_requests(event_tx);
                return;
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
            let active_memory_entity = pending_memory_key
                .as_ref()
                .filter(|_| pending_invoke.memory_call.is_some());
            let profile_memory_atomic = active_memory_entity.is_some();
            let memory_outbox_shard = pending_invoke
                .memory_route
                .as_ref()
                .and_then(|route| route.shard_index);
            if profile_memory_atomic {
                self.memory_store.record_profile(
                    MemoryProfileMetricKind::RuntimeSocketQueueWait,
                    duration_us(pending_invoke.enqueued_at.elapsed()),
                    1,
                );
            }
            let debug_tracing_enabled = tracing::enabled!(Level::DEBUG);
            let needs_completion_meta = debug_tracing_enabled
                || self
                    .get_pool_mut(worker_name, generation)
                    .and_then(|pool| pool.internal_trace.as_ref())
                    .is_some();
            let traceparent = if needs_completion_meta {
                traceparent_from_headers(&pending_invoke.request.headers).map(str::to_string)
            } else {
                None
            };
            let dispatch_span = if debug_tracing_enabled {
                let queue_wait_ms = pending_invoke.enqueued_at.elapsed().as_millis() as u64;
                Some(tracing::debug_span!(
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
            if let Some(registration) = self.stream_registrations.get_mut(&runtime_request_id)
                && registration.worker_name == worker_name
            {
                registration.completion_token = Some(completion_token.clone());
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

    fn spawn_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        event_tx: RuntimeEventSender,
        spawn_policy: IsolateSpawnPolicy,
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
        let Some(admission) = self.admission.reserve_isolate(&self.config, spawn_policy) else {
            self.stats.scale_up_budget_denied_count =
                self.stats.scale_up_budget_denied_count.saturating_add(1);
            return Err(PlatformError::overloaded(format!(
                "runtime isolate budget is exhausted (max {} global isolates)",
                self.config.max_global_isolates
            )));
        };
        let reservation = admission.reservation;
        let policy = crate::engine::IsolatePolicy::for_config(&self.config);
        let isolate_id = self.next_isolate_id;
        self.next_isolate_id += 1;
        let kv_store = self.kv_store.clone();
        let memory_store = self.memory_store.clone();
        let cache_store = self.cache_store.clone();
        let open_handle_registry = self.open_handle_registry.clone();
        let execution_limits = crate::ops::RuntimeExecutionLimits {
            max_request_body_bytes: self.config.max_request_body_bytes,
            max_response_body_bytes: self.config.max_response_body_bytes,
            max_isolate_heap_bytes: self.config.max_isolate_heap_bytes,
            max_buffered_response_bytes: self.config.max_buffered_response_bytes,
            response_byte_budget: Arc::clone(&self.response_byte_budget),
        };
        let mut isolate = match spawn_isolate_thread(IsolateThreadStart {
            admission,
            snapshot,
            snapshot_preloaded,
            source,
            module_registry: self.module_registry.clone(),
            deployment_config,
            policy,
            console: self.console.clone(),
            inspector: self.inspector.clone(),
            kv_store,
            memory_store,
            cache_store,
            open_handle_registry,
            execution_limits,
            runtime_fast_sender: self.runtime_fast_sender.clone(),
            worker_name: worker_name.to_string(),
            generation,
            isolate_id,
            event_tx: event_tx.clone(),
            thread_tracker: self.isolate_thread_tracker.clone(),
        }) {
            Ok(isolate) => isolate,
            Err(error) => {
                return Err(error);
            }
        };
        if matches!(reservation, IsolateSlotReservation::InternalRescue) {
            isolate.internal_rescue = true;
            self.internal_rescue_isolate_slots
                .insert(IsolateSlotKey::new(worker_name, generation, isolate.id));
        }
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.stats.spawn_count += 1;
            pool.push_isolate(isolate);
            pool.log_stats("spawn");
            Ok(())
        } else {
            isolate.request_shutdown();
            self.track_exiting_isolate_slot(worker_name, generation, isolate.id, isolate.startup);
            Err(PlatformError::internal("worker pool missing"))
        }
    }

    fn request_scale_up(&mut self, worker_name: &str, generation: u64) {
        let key = (worker_name.to_string(), generation);
        if self.scale_up_request_members.insert(key.clone()) {
            self.scale_up_requests.push_back(key);
        }
    }

    pub(crate) fn process_scale_up_requests(&mut self, event_tx: &RuntimeEventSender) {
        let mut remaining = self.scale_up_requests.len();
        while remaining > 0 && !self.scale_up_requests.is_empty() {
            remaining -= 1;
            let Some((worker_name, generation)) = self.scale_up_requests.pop_front() else {
                break;
            };
            self.scale_up_request_members
                .remove(&(worker_name.clone(), generation));
            if !self.pool_needs_scale_up(&worker_name, generation) {
                continue;
            }
            let allow_internal_rescue =
                self.internal_rescue_available_for_pool(&worker_name, generation);
            if self.regular_isolate_slots_used() >= self.config.max_global_isolates
                && !allow_internal_rescue
            {
                self.request_scale_up(&worker_name, generation);
                let exiting_regular_slots = self
                    .exiting_isolate_slots
                    .keys()
                    .filter(|key| !self.internal_rescue_isolate_slots.contains(*key))
                    .count();
                let live_slots = self
                    .regular_isolate_slots_used()
                    .saturating_sub(exiting_regular_slots);
                if live_slots >= self.config.max_global_isolates {
                    self.admission
                        .reclaim_idle
                        .send_replace(Some((worker_name.clone(), generation)));
                }
                continue;
            }
            let worker_regular_slots = self
                .workers
                .get(&worker_name)
                .and_then(|entry| entry.pools.get(&generation))
                .map(|pool| {
                    pool.isolates
                        .iter()
                        .filter(|isolate| !isolate.internal_rescue)
                        .count()
                })
                .unwrap_or_default();
            let spawn_policy = if allow_internal_rescue
                && worker_regular_slots >= self.config.max_isolates_per_worker
            {
                IsolateSpawnPolicy::InternalRescueOnly
            } else if allow_internal_rescue {
                IsolateSpawnPolicy::AllowInternalRescue
            } else {
                IsolateSpawnPolicy::WithinGlobalBudget
            };
            match self.spawn_isolate(&worker_name, generation, event_tx.clone(), spawn_policy) {
                Ok(()) => {
                    self.dispatch_pool(&worker_name, generation, event_tx);
                    if self.pool_needs_scale_up(&worker_name, generation) {
                        self.request_scale_up(&worker_name, generation);
                    }
                }
                Err(error) if error.kind() == ErrorKind::Overloaded => {
                    self.request_scale_up(&worker_name, generation);
                    continue;
                }
                Err(error) => {
                    let pending = self
                        .get_pool_mut(&worker_name, generation)
                        .and_then(|pool| pool.queue.pop_front());
                    if let Some(pending) = pending {
                        self.account_dequeued_pending(&pending);
                        let _ = pending.reply.send(Err(error));
                    }
                }
            }
        }
        if !self.scale_up_requests.is_empty()
            && self.regular_isolate_slots_used() >= self.config.max_global_isolates
        {
            self.stats.scale_up_budget_denied_count =
                self.stats.scale_up_budget_denied_count.saturating_add(1);
        }
    }

    fn pool_needs_scale_up(&mut self, worker_name: &str, generation: u64) -> bool {
        let max_inflight = self.config.max_inflight_per_isolate;
        let max_isolates = self.config.max_isolates_per_worker;
        let Some(pool) = self.get_pool_mut(worker_name, generation) else {
            return false;
        };
        let max_inflight = if pool.strict_request_isolation {
            1
        } else {
            max_inflight
        };
        let internal_request_needs_capacity =
            pool.queue.has_internal_request() && !pool.has_dispatch_capacity(max_inflight);
        let external_request_needs_capacity =
            pool.queue.has_external_request() && !pool.has_regular_dispatch_capacity(max_inflight);
        !pool.queue.is_empty()
            && (internal_request_needs_capacity || external_request_needs_capacity)
            && !pool.has_starting_isolate()
            && (pool
                .isolates
                .iter()
                .filter(|isolate| !isolate.internal_rescue)
                .count()
                < max_isolates
                || internal_request_needs_capacity)
    }

    fn internal_rescue_available_for_pool(&self, worker_name: &str, generation: u64) -> bool {
        self.admission.snapshot().rescue < self.config.max_global_isolates
            && self
                .workers
                .get(worker_name)
                .and_then(|entry| entry.pools.get(&generation))
                .is_some_and(|pool| pool.queue.has_internal_request())
    }

    pub(in crate::service) fn regular_isolate_slots_used(&self) -> usize {
        self.admission.snapshot().regular
    }

    pub(crate) fn global_isolate_slot_released(
        &mut self,
        key: &IsolateSlotKey,
        _startup: IsolateStartup,
    ) {
        self.internal_rescue_isolate_slots.remove(key);
    }
}
