use super::*;

pub(super) async fn status_response(state: &AppState) -> ApiResult<Response<ResponseBody>> {
    let runtime = state.runtime.admin_snapshot().await;
    let trace_exporter = trace_exporter_status();
    let ready = state.operations.is_ready() && runtime.readiness.ready;
    let restoration_failures = runtime
        .restore_failures
        .iter()
        .map(|failure| failure.error.clone())
        .collect::<Vec<_>>();
    Ok(json_response(
        StatusCode::OK,
        &serde_json::json!({
            "ok": true,
            "ready": ready,
            "draining": state.operations.is_draining(),
            "shutting_down": state.operations.is_shutting_down(),
            "active_requests": state.operations.active_requests(),
            "active_deployments": runtime.active_deployments,
            "restoration_failures": restoration_failures,
            "runtime": runtime,
            "trace_exporter": trace_exporter,
        }),
    )?)
}

pub(super) async fn metrics_response(state: &AppState) -> ApiResult<Response<ResponseBody>> {
    let runtime = state.runtime.admin_snapshot().await;
    let mut body = String::with_capacity(8 * 1024);
    metric_help(
        &mut body,
        "dd_ready",
        "Whether the server accepts worker requests.",
    );
    metric(
        &mut body,
        "dd_ready",
        u8::from(state.operations.is_ready() && runtime.readiness.ready),
    );
    metric_help(
        &mut body,
        "dd_draining",
        "Whether the server is in maintenance drain mode.",
    );
    metric(
        &mut body,
        "dd_draining",
        u8::from(state.operations.is_draining()),
    );
    metric_help(
        &mut body,
        "dd_active_requests",
        "HTTP requests whose response bodies are still active.",
    );
    metric(
        &mut body,
        "dd_active_requests",
        state.operations.active_requests(),
    );
    metric_help(
        &mut body,
        "dd_runtime_active_deployments",
        "Active worker deployments.",
    );
    metric(
        &mut body,
        "dd_runtime_active_deployments",
        runtime.active_deployments,
    );
    metric_help(
        &mut body,
        "dd_runtime_restore_failures",
        "Persisted workers that failed restoration.",
    );
    metric(
        &mut body,
        "dd_runtime_restore_failures",
        runtime.restore_failures.len(),
    );
    metric_help(
        &mut body,
        "dd_runtime_healthy",
        "Whether the runtime coordinator is accepting commands.",
    );
    metric(
        &mut body,
        "dd_runtime_healthy",
        u8::from(runtime.readiness.runtime_ready),
    );
    metric_help(
        &mut body,
        "dd_storage_healthy",
        "Whether control, KV, cache, and memory storage health checks pass.",
    );
    metric(
        &mut body,
        "dd_storage_healthy",
        u8::from(runtime.readiness.storage_ready),
    );
    metric_help(
        &mut body,
        "dd_migrations_ready",
        "Whether all required control and storage migrations are current.",
    );
    metric(
        &mut body,
        "dd_migrations_ready",
        u8::from(runtime.readiness.migrations_ready),
    );
    counter_help(
        &mut body,
        "dd_storage_retries_total",
        "Turso busy or snapshot-conflict retries.",
    );
    metric(
        &mut body,
        "dd_storage_retries_total",
        runtime.storage_retry_count,
    );
    counter_help(
        &mut body,
        "dd_cache_recency_flush_failures_total",
        "Failed cache recency flush attempts.",
    );
    metric(
        &mut body,
        "dd_cache_recency_flush_failures_total",
        runtime.cache_flush_failure_count,
    );
    metric_help(
        &mut body,
        "dd_cache_pending_recency_touches",
        "Cache recency updates waiting to be persisted.",
    );
    metric(
        &mut body,
        "dd_cache_pending_recency_touches",
        runtime.cache_pending_recency_touches,
    );
    counter_help(
        &mut body,
        "dd_memory_snapshot_cache_hits_total",
        "Memory snapshot cache hits.",
    );
    metric(
        &mut body,
        "dd_memory_snapshot_cache_hits_total",
        runtime.memory_snapshot_cache_hits,
    );
    counter_help(
        &mut body,
        "dd_memory_snapshot_cache_misses_total",
        "Memory snapshot cache misses.",
    );
    metric(
        &mut body,
        "dd_memory_snapshot_cache_misses_total",
        runtime.memory_snapshot_cache_misses,
    );
    counter_help(
        &mut body,
        "dd_memory_snapshot_cache_evictions_total",
        "Memory snapshot cache entries evicted by capacity or idle limits.",
    );
    metric(
        &mut body,
        "dd_memory_snapshot_cache_evictions_total",
        runtime.memory_snapshot_cache_evictions,
    );

    metric_help(
        &mut body,
        "dd_runtime_worker_queued_requests",
        "Queued worker requests.",
    );
    metric_help(
        &mut body,
        "dd_runtime_worker_oldest_queue_milliseconds",
        "Age of the oldest queued worker request.",
    );
    metric_help(
        &mut body,
        "dd_runtime_worker_inflight_requests",
        "Worker requests currently executing.",
    );
    metric_help(
        &mut body,
        "dd_runtime_worker_isolates",
        "Live worker isolates.",
    );
    metric_help(
        &mut body,
        "dd_runtime_worker_outbox_pending_shards",
        "Memory outbox shards awaiting delivery.",
    );
    metric_help(
        &mut body,
        "dd_runtime_worker_outbox_lag_shards",
        "Outbox lag proxy measured as scheduled or in-flight memory shards.",
    );
    counter_help(
        &mut body,
        "dd_runtime_worker_outbox_delivery_retries_total",
        "Memory outbox delivery retries.",
    );
    counter_help(
        &mut body,
        "dd_runtime_worker_isolate_budget_denials_total",
        "Worker scale-up attempts denied by the global isolate budget.",
    );
    counter_help(
        &mut body,
        "dd_runtime_worker_isolate_spawns_total",
        "Worker isolates started.",
    );
    counter_help(
        &mut body,
        "dd_runtime_worker_isolate_reuses_total",
        "Requests dispatched to an isolate that had already served a request.",
    );
    counter_help(
        &mut body,
        "dd_runtime_worker_isolate_scale_downs_total",
        "Worker isolates retired by idle or budget pressure.",
    );
    metric_help(
        &mut body,
        "dd_runtime_worker_memory_max_shard_depth",
        "Largest queued memory shard for a worker.",
    );
    metric_help(
        &mut body,
        "dd_runtime_worker_memory_blocked_owner_queues",
        "Memory owner queues blocked by an active entity lease.",
    );
    for worker in &runtime.workers {
        let label = prometheus_label(&worker.name);
        worker_metric(
            &mut body,
            "dd_runtime_worker_queued_requests",
            &label,
            worker.stats.queued,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_oldest_queue_milliseconds",
            &label,
            worker.stats.oldest_queue_ms,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_inflight_requests",
            &label,
            worker.stats.inflight_total,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_isolates",
            &label,
            worker.stats.isolates_total,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_outbox_pending_shards",
            &label,
            worker
                .stats
                .pending_memory_outbox_shards
                .max(worker.stats.memory_outbox_worker_pending_shards),
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_outbox_lag_shards",
            &label,
            worker.outbox_lag_shards,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_outbox_delivery_retries_total",
            &label,
            worker.stats.memory_outbox_delivery_retry_count,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_isolate_budget_denials_total",
            &label,
            worker.stats.scale_up_budget_denied_count,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_isolate_spawns_total",
            &label,
            worker.stats.spawn_count,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_isolate_reuses_total",
            &label,
            worker.stats.reuse_count,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_isolate_scale_downs_total",
            &label,
            worker.stats.scale_down_count,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_memory_max_shard_depth",
            &label,
            worker.stats.memory_max_shard_depth,
        );
        worker_metric(
            &mut body,
            "dd_runtime_worker_memory_blocked_owner_queues",
            &label,
            worker.stats.memory_blocked_owner_queues,
        );
    }

    let global = runtime.workers.first().map(|worker| &worker.stats);
    metric_help(
        &mut body,
        "dd_runtime_global_isolates",
        "Allocated runtime isolates across all workers.",
    );
    metric(
        &mut body,
        "dd_runtime_global_isolates",
        global.map_or(0, |stats| stats.global_isolates_total),
    );
    metric_help(
        &mut body,
        "dd_runtime_internal_rescue_isolates",
        "Temporary isolates above the global budget serving internal dependencies.",
    );
    metric(
        &mut body,
        "dd_runtime_internal_rescue_isolates",
        global.map_or(0, |stats| stats.global_internal_rescue_isolates),
    );
    metric_help(
        &mut body,
        "dd_runtime_global_isolate_budget",
        "Configured global isolate budget.",
    );
    metric(
        &mut body,
        "dd_runtime_global_isolate_budget",
        global.map_or(0, |stats| stats.global_isolate_budget),
    );
    let trace = trace_exporter_status();
    body.push_str("# HELP dd_trace_exporter_state Trace exporter configuration state.\n");
    body.push_str("# TYPE dd_trace_exporter_state gauge\n");
    body.push_str("dd_trace_exporter_state{state=\"");
    body.push_str(trace.state);
    body.push_str("\"} 1\n");
    counter_help(
        &mut body,
        "dd_trace_export_successes_total",
        "Successfully exported OTLP trace batches.",
    );
    metric(
        &mut body,
        "dd_trace_export_successes_total",
        trace.export_successes,
    );
    counter_help(
        &mut body,
        "dd_trace_export_failures_total",
        "Failed OTLP trace batch exports.",
    );
    metric(
        &mut body,
        "dd_trace_export_failures_total",
        trace.export_failures,
    );
    metric_help(
        &mut body,
        "dd_trace_exporter_verified",
        "Whether an OTLP collector acknowledgement has been observed.",
    );
    metric(
        &mut body,
        "dd_trace_exporter_verified",
        u8::from(trace.verified),
    );

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(full_body(body))
        .map_err(|error| PlatformError::internal(error.to_string()).into())
}

pub(super) async fn checkpoint_response(state: &AppState) -> ApiResult<Response<ResponseBody>> {
    if !state.operations.is_draining() {
        return Err(
            PlatformError::conflict("checkpoint requires the service to be drained").into(),
        );
    }
    if state.operations.active_requests() != 0 {
        return Err(
            PlatformError::conflict("checkpoint requires all active requests to finish").into(),
        );
    }
    if !state.runtime.is_quiescent().await {
        return Err(PlatformError::conflict(
            "checkpoint requires runtime requests, sessions, waitUntil tasks, and outbox work to drain",
        )
        .into());
    }
    let checkpoint = state.runtime.checkpoint().await?;
    Ok(json_response(
        StatusCode::OK,
        &serde_json::json!({"ok": true, "checkpoint": checkpoint}),
    )?)
}

#[derive(serde::Serialize)]
struct TraceExporterStatus {
    compiled: bool,
    configured: bool,
    enabled: bool,
    state: &'static str,
    verified: bool,
    export_successes: u64,
    export_failures: u64,
}

impl From<crate::trace_health::TraceExporterHealth> for TraceExporterStatus {
    fn from(health: crate::trace_health::TraceExporterHealth) -> Self {
        Self {
            compiled: health.compiled,
            configured: health.configured,
            enabled: health.enabled,
            state: health.state,
            verified: health.verified,
            export_successes: health.export_successes,
            export_failures: health.export_failures,
        }
    }
}

fn trace_exporter_status() -> TraceExporterStatus {
    let health = crate::trace_exporter_health();
    TraceExporterStatus::from(health)
}

fn metric_help(body: &mut String, name: &str, help: &str) {
    metric_metadata(body, name, help, "gauge");
}

fn counter_help(body: &mut String, name: &str, help: &str) {
    metric_metadata(body, name, help, "counter");
}

fn metric_metadata(body: &mut String, name: &str, help: &str, metric_type: &str) {
    body.push_str("# HELP ");
    body.push_str(name);
    body.push(' ');
    body.push_str(help);
    body.push('\n');
    body.push_str("# TYPE ");
    body.push_str(name);
    body.push(' ');
    body.push_str(metric_type);
    body.push('\n');
}

fn metric(body: &mut String, name: &str, value: impl std::fmt::Display) {
    body.push_str(name);
    body.push(' ');
    use std::fmt::Write as _;
    let _ = writeln!(body, "{value}");
}

fn worker_metric(body: &mut String, name: &str, worker: &str, value: impl std::fmt::Display) {
    use std::fmt::Write as _;
    let _ = writeln!(body, "{name}{{worker=\"{worker}\"}} {value}");
}

fn prometheus_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
