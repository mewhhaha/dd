//! Fair dispatch selection across isolate and memory queues.

use super::*;

pub(in crate::service) fn select_dispatch_candidate(
    pool: &mut WorkerPool,
    max_inflight: usize,
    require_wait_until_idle: bool,
    allow_memory_atomic_overflow: bool,
) -> Option<DispatchSelection> {
    let mut attempt_stats = DispatchAttemptStats::default();

    let selection = loop {
        let candidate = {
            let isolates = &pool.isolates;
            let isolate_indices = &pool.isolate_indices;
            let memory_entity_leases = &pool.memory_entity_leases;
            let memory_shard_affinity = &pool.memory_shard_affinity;
            pool.queue.find_fair_map(
                [
                    PendingQueueLane::Targeted,
                    PendingQueueLane::Memory,
                    PendingQueueLane::General,
                ],
                |queue_key, pending| {
                    let Some(target_isolate_id) = pending.target_isolate_id else {
                        let eligibility = IsolateEligibility::for_pending(pending);
                        if pending.memory_route.is_none() {
                            return least_loaded_isolate_idx(
                                isolates,
                                max_inflight,
                                require_wait_until_idle,
                                eligibility,
                            )
                            .map(|isolate_idx| {
                                DispatchReadiness::Ready(DispatchSelection::Dispatch(
                                    DispatchCandidate {
                                        queue_key,
                                        isolate_idx,
                                    },
                                ))
                            });
                        }

                        attempt_stats.memory_candidate_heads_inspected_count = attempt_stats
                            .memory_candidate_heads_inspected_count
                            .saturating_add(1);
                        if memory_atomic_route_is_active(memory_entity_leases, pending) {
                            attempt_stats.memory_candidate_rejected_owner_lease_count =
                                attempt_stats
                                    .memory_candidate_rejected_owner_lease_count
                                    .saturating_add(1);
                            return Some(DispatchReadiness::OwnerBlocked(queue_key));
                        }

                        let mut isolate_idx = None;
                        if memory_route_needs_ordering(pending) {
                            match memory_shard_affinity_outcome(
                                isolates,
                                isolate_indices,
                                memory_shard_affinity,
                                pending,
                                max_inflight,
                                require_wait_until_idle,
                                eligibility,
                            ) {
                                MemoryAffinityOutcome::Hit(idx) => {
                                    attempt_stats.memory_affinity_hit_count =
                                        attempt_stats.memory_affinity_hit_count.saturating_add(1);
                                    isolate_idx = Some(idx);
                                }
                                MemoryAffinityOutcome::MissNoMapping => {
                                    attempt_stats.memory_affinity_miss_no_mapping_count =
                                        attempt_stats
                                            .memory_affinity_miss_no_mapping_count
                                            .saturating_add(1);
                                }
                                MemoryAffinityOutcome::MissStale => {
                                    attempt_stats.memory_affinity_miss_stale_count = attempt_stats
                                        .memory_affinity_miss_stale_count
                                        .saturating_add(1);
                                }
                                MemoryAffinityOutcome::MissSaturated => {
                                    attempt_stats.memory_affinity_miss_saturated_count =
                                        attempt_stats
                                            .memory_affinity_miss_saturated_count
                                            .saturating_add(1);
                                }
                            }
                        }
                        if isolate_idx.is_none() {
                            isolate_idx = least_loaded_isolate_idx(
                                isolates,
                                max_inflight,
                                require_wait_until_idle,
                                eligibility,
                            );
                            if isolate_idx.is_some() && memory_route_needs_ordering(pending) {
                                attempt_stats.memory_least_loaded_fallback_count = attempt_stats
                                    .memory_least_loaded_fallback_count
                                    .saturating_add(1);
                            }
                        }
                        if isolate_idx.is_none()
                            && allow_memory_atomic_overflow
                            && memory_route_needs_ordering(pending)
                        {
                            isolate_idx = least_loaded_isolate_any_idx(isolates, eligibility);
                            if isolate_idx.is_some() {
                                attempt_stats.memory_atomic_overflow_dispatch_count = attempt_stats
                                    .memory_atomic_overflow_dispatch_count
                                    .saturating_add(1);
                            }
                        }
                        if isolate_idx.is_none() {
                            attempt_stats.memory_candidate_rejected_isolate_state_count =
                                attempt_stats
                                    .memory_candidate_rejected_isolate_state_count
                                    .saturating_add(1);
                        }
                        return isolate_idx.map(|isolate_idx| {
                            DispatchReadiness::Ready(DispatchSelection::Dispatch(
                                DispatchCandidate {
                                    queue_key,
                                    isolate_idx,
                                },
                            ))
                        });
                    };

                    if let Some(isolate_idx) =
                        target_isolate_idx(isolate_indices, target_isolate_id)
                    {
                        let isolate = &isolates[isolate_idx];
                        debug_assert!(pending.memory_route.is_none());
                        if isolate.startup.is_ready()
                            && IsolateEligibility::for_pending(pending).accepts(isolate)
                            && isolate.inflight_count < max_inflight
                            && (!require_wait_until_idle || isolate.pending_wait_until.is_empty())
                        {
                            Some(DispatchReadiness::Ready(DispatchSelection::Dispatch(
                                DispatchCandidate {
                                    queue_key,
                                    isolate_idx,
                                },
                            )))
                        } else {
                            attempt_stats.memory_candidate_rejected_isolate_state_count =
                                attempt_stats
                                    .memory_candidate_rejected_isolate_state_count
                                    .saturating_add(1);
                            None
                        }
                    } else {
                        Some(DispatchReadiness::Ready(
                            DispatchSelection::DropStaleTarget { queue_key },
                        ))
                    }
                },
            )
        };
        match candidate {
            Some(DispatchReadiness::Ready(selection)) => break Some(selection),
            Some(DispatchReadiness::OwnerBlocked(queue_key)) => {
                pool.queue.mark_memory_owner_blocked(&queue_key);
            }
            None => break None,
        }
    };
    if selection.is_none() && pool.queue.lane_depths().memory > 0 {
        attempt_stats.memory_dispatch_no_ready_candidate_count = attempt_stats
            .memory_dispatch_no_ready_candidate_count
            .saturating_add(1);
    }
    pool.stats.record_dispatch_attempt(attempt_stats);
    selection
}
