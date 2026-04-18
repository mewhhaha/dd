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
        "profile-memory js_read={:.2}ms js_commit={:.2}ms js_blind_commit={:.2}ms js_validate={:.2}ms freshness={:.2}ms hydrate_full={:.2}ms hydrate_keys={:.2}ms cache hit={} miss={} stale={}",
        metric_mean_ms(&profile.js_read_only_total),
        metric_mean_ms(&profile.js_txn_commit),
        metric_mean_ms(&profile.js_txn_blind_commit),
        metric_mean_ms(&profile.js_txn_validate),
        metric_mean_ms(&profile.js_freshness_check),
        metric_mean_ms(&profile.js_hydrate_full),
        metric_mean_ms(&profile.js_hydrate_keys),
        profile.js_cache_hit.calls,
        profile.js_cache_miss.calls,
        profile.js_cache_stale.calls,
    );
    println!(
        "profile-memory-op read={:.2}ms snapshot={:.2}ms version={:.2}ms validate={:.2}ms apply={:.2}ms blind_apply={:.2}ms store_read={:.2}ms store_snapshot={:.2}ms store_keys={:.2}ms store_version={:.2}ms store_apply={:.2}ms store_validate={:.2}ms store_write={:.2}ms store_blind_apply={:.2}ms store_blind_write={:.2}ms",
        metric_mean_ms(&profile.op_read),
        metric_mean_ms(&profile.op_snapshot),
        metric_mean_ms(&profile.op_version_if_newer),
        metric_mean_ms(&profile.op_validate_reads),
        metric_mean_ms(&profile.op_apply_batch),
        metric_mean_ms(&profile.op_apply_blind_batch),
        metric_mean_ms(&profile.store_read),
        metric_mean_ms(&profile.store_snapshot),
        metric_mean_ms(&profile.store_snapshot_keys),
        metric_mean_ms(&profile.store_version_if_newer),
        metric_mean_ms(&profile.store_apply_batch),
        metric_mean_ms(&profile.store_apply_batch_validate),
        metric_mean_ms(&profile.store_apply_batch_write),
        metric_mean_ms(&profile.store_apply_blind_batch),
        metric_mean_ms(&profile.store_apply_blind_batch_write),
    );
    println!(
        "profile-memory-direct enqueue={:.2}ms await={:.2}ms queue_load={:.2}ms queue_flush={:.2}ms queue_delete={:.2}ms waiter_complete={:.2}ms enqueue_calls={} await_calls={} flush_calls={}",
        metric_mean_ms(&profile.store_direct_enqueue),
        metric_mean_ms(&profile.store_direct_await),
        metric_mean_ms(&profile.store_direct_queue_load),
        metric_mean_ms(&profile.store_direct_queue_flush),
        metric_mean_ms(&profile.store_direct_queue_delete),
        metric_mean_ms(&profile.store_direct_waiter_complete),
        profile.store_direct_enqueue.calls,
        profile.store_direct_await.calls,
        profile.store_direct_queue_flush.calls,
    );
}

pub(super) fn metric_mean_ms(metric: &MemoryProfileMetric) -> f64 {
    if metric.calls == 0 {
        0.0
    } else {
        metric.total_us as f64 / metric.calls as f64 / 1000.0
    }
}
