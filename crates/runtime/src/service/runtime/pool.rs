//! Worker pool execution, capacity, and activity statistics.

use super::*;

impl WorkerPool {
    pub(in crate::service) fn build_execute_command(
        &self,
        command: BuildExecuteCommand,
    ) -> IsolateCommand {
        let BuildExecuteCommand {
            runtime_request_id,
            completion_token,
            request,
            request_body,
            stream_response,
            memory_call,
            memory_route,
            dispatched_at,
            profile_memory_atomic,
        } = command;
        IsolateCommand::Execute {
            runtime_request_id,
            completion_token,
            request_context: Box::new(self.request_context.clone()),
            request: Box::new(request),
            request_body,
            stream_response,
            memory_call: Box::new(memory_call),
            memory_route: Box::new(memory_route),
            dispatched_at,
            profile_memory_atomic,
        }
    }

    pub(in crate::service) fn has_dispatch_capacity(&self, max_inflight: usize) -> bool {
        self.isolates.iter().any(|isolate| {
            isolate.startup.is_ready()
                && isolate.inflight_count < max_inflight
                && (!self.strict_request_isolation || isolate.pending_wait_until.is_empty())
        })
    }

    pub(in crate::service) fn has_regular_dispatch_capacity(&self, max_inflight: usize) -> bool {
        self.isolates.iter().any(|isolate| {
            !isolate.internal_rescue
                && isolate.startup.is_ready()
                && isolate.inflight_count < max_inflight
                && (!self.strict_request_isolation || isolate.pending_wait_until.is_empty())
        })
    }

    pub(in crate::service) fn has_starting_isolate(&self) -> bool {
        self.isolates
            .iter()
            .any(|isolate| isolate.startup.is_starting())
    }

    pub(in crate::service) fn is_drained(&self) -> bool {
        self.queue.is_empty() && self.activity_snapshot().is_idle()
    }

    pub(in crate::service) fn update_queue_warning(&mut self, thresholds: &[usize]) {
        let queue_len = self.queue.len();
        let level = thresholds
            .iter()
            .take_while(|threshold| queue_len >= **threshold)
            .count();
        if level > self.queue_warn_level {
            warn!(
                worker = %self.worker_name,
                generation = self.generation,
                queued = queue_len,
                "worker queue depth crossed warning threshold"
            );
        }
        self.queue_warn_level = level;
    }

    pub(in crate::service) fn stats_snapshot(&self) -> WorkerStats {
        let activity = self.activity_snapshot();
        let lane_depths = self.queue.lane_depths();
        let (memory_max_shard_depth, memory_median_shard_depth) =
            self.queue.memory_shard_depth_summary();
        let stale_memory_affinity_entries = self
            .memory_shard_affinity
            .values()
            .filter(|isolate_id| !self.isolate_indices.contains_key(isolate_id))
            .count();
        WorkerStats {
            generation: self.generation,
            public: self.is_public,
            temporary: self.expires_at_ms.is_some(),
            expires_at_ms: self.expires_at_ms,
            queued: self.queue.len(),
            busy: activity.busy,
            inflight_total: activity.inflight_total,
            wait_until_total: activity.wait_until_total,
            isolates_total: self.isolates.len(),
            spawn_count: self.stats.spawn_count,
            reuse_count: self.stats.reuse_count,
            scale_down_count: self.stats.scale_down_count,
            targeted_lane_queued: lane_depths.targeted,
            memory_lane_queued: lane_depths.memory,
            general_lane_queued: lane_depths.general,
            memory_active_shards: self.queue.active_memory_shards(),
            memory_max_shard_depth,
            memory_median_shard_depth,
            memory_owner_queues: self.queue.memory_owner_queues(),
            memory_blocked_owner_queues: self
                .queue
                .blocked_memory_owner_queues(&self.memory_entity_leases),
            active_memory_leases: self.memory_entity_leases.len(),
            oldest_queue_ms: self.queue.oldest_queue_age_ms(Instant::now()),
            queued_bytes: self.queue.queued_bytes(),
            max_queued_requests_per_worker: 0,
            max_global_queued_bytes: 0,
            memory_affinity_entries: self.memory_shard_affinity.len(),
            stale_memory_affinity_entries,
            pending_memory_outbox_shards: 0,
            memory_affinity_hit_count: self.stats.memory_affinity_hit_count,
            memory_affinity_miss_no_mapping_count: self.stats.memory_affinity_miss_no_mapping_count,
            memory_affinity_miss_stale_count: self.stats.memory_affinity_miss_stale_count,
            memory_affinity_miss_saturated_count: self.stats.memory_affinity_miss_saturated_count,
            memory_least_loaded_fallback_count: self.stats.memory_least_loaded_fallback_count,
            memory_atomic_overflow_dispatch_count: self.stats.memory_atomic_overflow_dispatch_count,
            memory_candidate_rejected_owner_lease_count: self
                .stats
                .memory_candidate_rejected_owner_lease_count,
            memory_candidate_rejected_isolate_state_count: self
                .stats
                .memory_candidate_rejected_isolate_state_count,
            memory_candidate_heads_inspected_count: self
                .stats
                .memory_candidate_heads_inspected_count,
            memory_dispatch_no_ready_candidate_count: self
                .stats
                .memory_dispatch_no_ready_candidate_count,
            runtime_ready_work_budget_exhausted_count: 0,
            runtime_max_ready_work_batch_size: 0,
            global_isolate_budget: 0,
            global_isolates_total: 0,
            global_isolates_starting: 0,
            global_internal_rescue_isolates: 0,
            global_isolate_slots_available: 0,
            scale_up_waiting_pools: 0,
            scale_up_budget_denied_count: 0,
            memory_outbox_claim_batch_count: 0,
            memory_outbox_claim_row_count: 0,
            memory_outbox_saturated_batch_count: 0,
            memory_outbox_delivery_success_count: 0,
            memory_outbox_delivery_retry_count: 0,
            memory_outbox_terminal_drop_count: 0,
            memory_outbox_ack_failure_count: 0,
            memory_outbox_channel_full_count: 0,
            memory_outbox_reschedule_count: 0,
            memory_outbox_worker_pending_shards: 0,
            memory_outbox_worker_in_flight_shards: 0,
            memory_outbox_worker_parallelism_limit: 0,
            memory_outbox_worker_parallelism_peak: 0,
            memory_outbox_duplicate_schedule_coalesced_count: 0,
            memory_outbox_task_failure_count: 0,
            memory_outbox_shard_requeue_count: 0,
        }
    }

    fn activity_snapshot(&self) -> PoolActivity {
        let mut activity = PoolActivity::default();
        for isolate in &self.isolates {
            let wait_until = isolate.pending_wait_until.len();
            if isolate.inflight_count > 0 || wait_until > 0 || isolate.active_websocket_sessions > 0
            {
                activity.busy += 1;
            }
            activity.inflight_total += isolate.inflight_count;
            activity.wait_until_total += wait_until;
            activity.active_websocket_total += isolate.active_websocket_sessions;
        }
        activity
    }
}
