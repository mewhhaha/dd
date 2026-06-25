use super::*;

impl WorkerPool {
    pub(super) fn debug_dump(&self) -> WorkerDebugDump {
        let (memory_max_shard_depth, memory_median_shard_depth) =
            self.queue.memory_shard_depth_summary();
        let stale_memory_affinity_entries = self
            .memory_shard_affinity
            .values()
            .filter(|isolate_id| !self.isolate_indices.contains_key(isolate_id))
            .count();
        let blocked_owner_queues = self
            .queue
            .blocked_memory_owner_queues(&self.memory_entity_leases);
        let memory_scheduler = MemorySchedulerDebug {
            queued: self.queue.lane_depths().memory,
            active_leases: self.memory_entity_leases.len(),
            active_shards: self.queue.active_memory_shards(),
            max_shard_depth: memory_max_shard_depth,
            median_shard_depth: memory_median_shard_depth,
            owner_queues: self.queue.memory_owner_queues(),
            blocked_owner_queues,
            affinity_entries: self.memory_shard_affinity.len(),
            stale_affinity_entries: stale_memory_affinity_entries,
            oldest_queue_ms: self.queue.oldest_queue_age_ms(Instant::now()),
            queued_bytes: self.queue.queued_bytes(),
            max_queued_requests_per_worker: 0,
            max_global_queued_bytes: 0,
            top_shards: self.queue.memory_shard_debug(
                &self.memory_entity_leases,
                &self.memory_shard_affinity,
                &self.isolate_indices,
                8,
            ),
            affinity_hit_count: self.stats.memory_affinity_hit_count,
            affinity_miss_no_mapping_count: self.stats.memory_affinity_miss_no_mapping_count,
            affinity_miss_stale_count: self.stats.memory_affinity_miss_stale_count,
            affinity_miss_saturated_count: self.stats.memory_affinity_miss_saturated_count,
            least_loaded_fallback_count: self.stats.memory_least_loaded_fallback_count,
            atomic_overflow_dispatch_count: self.stats.memory_atomic_overflow_dispatch_count,
            candidate_rejected_owner_lease_count: self
                .stats
                .memory_candidate_rejected_owner_lease_count,
            candidate_rejected_isolate_state_count: self
                .stats
                .memory_candidate_rejected_isolate_state_count,
            candidate_heads_inspected_count: self.stats.memory_candidate_heads_inspected_count,
            dispatch_no_ready_candidate_count: self.stats.memory_dispatch_no_ready_candidate_count,
            runtime_ready_work_budget_exhausted_count: 0,
            runtime_max_ready_work_batch_size: 0,
            global_isolate_budget: 0,
            global_isolates_total: 0,
            global_isolates_starting: 0,
            global_isolate_slots_available: 0,
            scale_up_waiting_pools: 0,
            scale_up_budget_denied_count: 0,
        };
        let queued_requests = self
            .queue
            .iter()
            .map(|pending| WorkerDebugRequest {
                runtime_request_id: pending.runtime_request_id.clone(),
                user_request_id: pending.request.request_id.clone(),
                method: pending.request.method.clone(),
                url: pending.request.url.clone(),
                memory_key: pending
                    .memory_route
                    .as_ref()
                    .map(|route| route.owner_key.clone()),
                target_isolate_id: pending.target_isolate_id,
                internal_origin: pending.internal_origin,
                reply_kind: pending.reply_kind.label().to_string(),
                host_rpc_target_id: pending
                    .host_rpc_call
                    .as_ref()
                    .map(|call| call.target_id.clone()),
                host_rpc_method: pending
                    .host_rpc_call
                    .as_ref()
                    .map(|call| call.method.clone()),
            })
            .collect::<Vec<_>>();

        let isolates = self
            .isolates
            .iter()
            .map(|isolate| {
                let mut pending_requests = isolate
                    .pending_replies
                    .iter()
                    .map(|(runtime_request_id, pending)| {
                        let meta = pending.completion_meta.as_ref();
                        WorkerDebugRequest {
                            runtime_request_id: runtime_request_id.clone(),
                            user_request_id: meta
                                .map(|meta| meta.user_request_id.clone())
                                .unwrap_or_default(),
                            method: meta.map(|meta| meta.method.clone()).unwrap_or_default(),
                            url: meta.map(|meta| meta.url.clone()).unwrap_or_default(),
                            memory_key: pending.memory_key.clone(),
                            target_isolate_id: Some(isolate.id),
                            internal_origin: pending.internal_origin,
                            reply_kind: pending.kind.label().to_string(),
                            host_rpc_target_id: None,
                            host_rpc_method: None,
                        }
                    })
                    .collect::<Vec<_>>();
                pending_requests
                    .sort_by(|left, right| left.runtime_request_id.cmp(&right.runtime_request_id));
                WorkerDebugIsolate {
                    id: isolate.id,
                    inflight_count: isolate.inflight_count,
                    pending_wait_until: isolate.pending_wait_until.len(),
                    active_websocket_sessions: isolate.active_websocket_sessions,
                    active_transport_sessions: isolate.active_transport_sessions,
                    pending_requests,
                }
            })
            .collect::<Vec<_>>();

        WorkerDebugDump {
            generation: self.generation,
            queued: self.queue.len(),
            isolates,
            queued_requests,
            memory_scheduler,
            memory_outbox: MemoryOutboxDebug::default(),
        }
    }

    pub(super) fn log_stats(&self, event: &str) {
        if !tracing::enabled!(Level::INFO) {
            return;
        }
        let snapshot = self.stats_snapshot();
        info!(
            worker = %self.worker_name,
            generation = snapshot.generation,
            public = snapshot.public,
            deployment_id = %self.deployment_id,
            queued = snapshot.queued,
            busy = snapshot.busy,
            inflight_total = snapshot.inflight_total,
            wait_until_total = snapshot.wait_until_total,
            isolates_total = snapshot.isolates_total,
            spawn_count = snapshot.spawn_count,
            reuse_count = snapshot.reuse_count,
            scale_down_count = snapshot.scale_down_count,
            memory_lane_queued = snapshot.memory_lane_queued,
            memory_active_shards = snapshot.memory_active_shards,
            memory_owner_queues = snapshot.memory_owner_queues,
            memory_blocked_owner_queues = snapshot.memory_blocked_owner_queues,
            active_memory_leases = snapshot.active_memory_leases,
            oldest_queue_ms = snapshot.oldest_queue_ms,
            queued_bytes = snapshot.queued_bytes,
            event,
            "worker pool stats"
        );
    }
}
