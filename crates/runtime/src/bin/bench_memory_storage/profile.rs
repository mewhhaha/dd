use super::*;

pub(super) async fn take_profile(
    service: &RuntimeService,
    worker_name: &str,
) -> Result<MemoryProfileSnapshot, String> {
    let output = service
        .invoke(worker_name.to_string(), invocation("/__profile", 0, 1))
        .await
        .map_err(|error| error.to_string())?;
    let body = String::from_utf8(output.body).map_err(|error| error.to_string())?;
    let profile: MemoryProfileEnvelope =
        serde_json::from_str(&body).map_err(|error| error.to_string())?;
    if !profile.ok {
        return Err(if profile.error.is_empty() {
            "memory profile collection failed".to_string()
        } else {
            profile.error
        });
    }
    Ok(profile.snapshot.unwrap_or_default())
}

pub(super) fn print_profile(profile: &MemoryProfileSnapshot) {
    if !profile.enabled {
        return;
    }
    println!(
        "profile-memory js_read={:.2}ms js_commit={:.2}ms hydrate_full={:.2}ms hydrate_keys={:.2}ms cache hit={} miss={} stale={}",
        metric_mean_ms(&profile.js_read_only_total),
        metric_mean_ms(&profile.js_txn_commit),
        metric_mean_ms(&profile.js_hydrate_full),
        metric_mean_ms(&profile.js_hydrate_keys),
        profile.js_cache_hit.calls,
        profile.js_cache_miss.calls,
        profile.js_cache_stale.calls,
    );
    println!(
        "profile-memory-op read={:.2}ms snapshot={:.2}ms version={:.2}ms apply={:.2}ms store_read={:.2}ms store_snapshot={:.2}ms store_keys={:.2}ms store_version={:.2}ms store_apply={:.2}ms store_validate={:.2}ms store_write={:.2}ms db_cache_hit={} db_cache_miss={} db_cache_eviction={} snapshot_cache_hit={} snapshot_cache_miss={} snapshot_cache_eviction={}",
        metric_mean_ms(&profile.op_read),
        metric_mean_ms(&profile.op_snapshot),
        metric_mean_ms(&profile.op_version_if_newer),
        metric_mean_ms(&profile.op_apply_batch),
        metric_mean_ms(&profile.store_read),
        metric_mean_ms(&profile.store_snapshot),
        metric_mean_ms(&profile.store_snapshot_keys),
        metric_mean_ms(&profile.store_version_if_newer),
        metric_mean_ms(&profile.store_apply_batch),
        metric_mean_ms(&profile.store_apply_batch_validate),
        metric_mean_ms(&profile.store_apply_batch_write),
        profile.store_database_cache_hit.calls,
        profile.store_database_cache_miss.calls,
        profile.store_database_cache_eviction.total_items,
        profile.store_snapshot_cache_hit.calls,
        profile.store_snapshot_cache_miss.calls,
        profile.store_snapshot_cache_eviction.total_items,
    );
    println!(
        "profile-memory-atomic event_wait={:.2}ms queue_wait={:.2}ms dispatch_wait={:.2}ms execution={:.2}ms completion_wait={:.2}ms outbox_drain={:.2}ms outbox_items={}",
        metric_mean_ms(&profile.runtime_atomic_invoke_event_wait),
        metric_mean_ms(&profile.runtime_atomic_queue_wait),
        metric_mean_ms(&profile.runtime_atomic_dispatch_wait),
        metric_mean_ms(&profile.runtime_atomic_execution),
        metric_mean_ms(&profile.runtime_atomic_completion_wait),
        metric_mean_ms(&profile.runtime_atomic_outbox_drain),
        profile.runtime_atomic_outbox_drain.total_items,
    );
}

pub(super) fn print_scheduler_stats(stats: &WorkerStats) {
    println!(
        "profile-memory-scheduler queued={} active_shards={} max_shard_depth={} median_shard_depth={} owner_queues={} blocked_owner_queues={} active_leases={} queued_bytes={} max_worker_queue={} max_global_queue_bytes={} affinity_hit={} affinity_miss_no_mapping={} affinity_miss_stale={} affinity_miss_saturated={} fallback={} overflow={} lease_reject={} isolate_reject={} heads_inspected={} no_ready={} ready_budget_exhausted={} max_ready_batch={} pending_outbox_shards={} outbox_claim_batches={} outbox_claim_rows={} outbox_saturated_batches={} outbox_success={} outbox_retry={} outbox_terminal_drop={} outbox_ack_failure={} outbox_channel_full={} outbox_reschedule={}",
        stats.memory_lane_queued,
        stats.memory_active_shards,
        stats.memory_max_shard_depth,
        stats.memory_median_shard_depth,
        stats.memory_owner_queues,
        stats.memory_blocked_owner_queues,
        stats.active_memory_leases,
        stats.queued_bytes,
        stats.max_queued_requests_per_worker,
        stats.max_global_queued_bytes,
        stats.memory_affinity_hit_count,
        stats.memory_affinity_miss_no_mapping_count,
        stats.memory_affinity_miss_stale_count,
        stats.memory_affinity_miss_saturated_count,
        stats.memory_least_loaded_fallback_count,
        stats.memory_atomic_overflow_dispatch_count,
        stats.memory_candidate_rejected_owner_lease_count,
        stats.memory_candidate_rejected_isolate_state_count,
        stats.memory_candidate_heads_inspected_count,
        stats.memory_dispatch_no_ready_candidate_count,
        stats.runtime_ready_work_budget_exhausted_count,
        stats.runtime_max_ready_work_batch_size,
        stats.pending_memory_outbox_shards,
        stats.memory_outbox_claim_batch_count,
        stats.memory_outbox_claim_row_count,
        stats.memory_outbox_saturated_batch_count,
        stats.memory_outbox_delivery_success_count,
        stats.memory_outbox_delivery_retry_count,
        stats.memory_outbox_terminal_drop_count,
        stats.memory_outbox_ack_failure_count,
        stats.memory_outbox_channel_full_count,
        stats.memory_outbox_reschedule_count,
    );
}

pub(super) fn metric_mean_ms(metric: &MemoryProfileMetric) -> f64 {
    if metric.calls == 0 {
        0.0
    } else {
        metric.total_us as f64 / metric.calls as f64 / 1000.0
    }
}
