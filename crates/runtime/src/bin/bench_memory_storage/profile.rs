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
        "profile-memory-op read={:.2}ms snapshot={:.2}ms version={:.2}ms apply={:.2}ms store_read={:.2}ms store_snapshot={:.2}ms store_keys={:.2}ms store_version={:.2}ms store_apply={:.2}ms store_validate={:.2}ms store_write={:.2}ms",
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

pub(super) fn metric_mean_ms(metric: &MemoryProfileMetric) -> f64 {
    if metric.calls == 0 {
        0.0
    } else {
        metric.total_us as f64 / metric.calls as f64 / 1000.0
    }
}
