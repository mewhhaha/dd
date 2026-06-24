use super::*;

pub(super) async fn print_profile_on_failure(
    service: &RuntimeService,
    worker_name: &str,
    label: &str,
) {
    match timeout(Duration::from_secs(2), take_profile(service, worker_name)).await {
        Err(_) => {
            println!(
                "bench-profile label={} outcome=error message=profile collection timed out",
                label
            );
        }
        Ok(Err(error)) => {
            println!(
                "bench-profile label={} outcome=error message={}",
                label, error
            );
        }
        Ok(Ok(profile)) => {
            println!("bench-profile label={} outcome=error", label);
            print_profile(&profile);
        }
    }
}

pub(super) async fn print_debug_dump_on_failure(
    service: &RuntimeService,
    worker_name: &str,
    label: &str,
) {
    match timeout(
        Duration::from_secs(2),
        service.debug_dump(worker_name.to_string()),
    )
    .await
    {
        Err(_) => {
            println!(
                "bench-dump label={} outcome=error message=debug dump timed out",
                label
            );
        }
        Ok(None) => {
            println!(
                "bench-dump label={} outcome=error message=debug dump unavailable",
                label
            );
        }
        Ok(Some(dump)) => print_debug_dump(label, &dump),
    }
}

pub(super) fn print_debug_dump(label: &str, dump: &WorkerDebugDump) {
    let pending_requests = dump
        .isolates
        .iter()
        .flat_map(|isolate| isolate.pending_requests.iter())
        .take(8)
        .map(|request| {
            format!(
                "{}:{}:{}:{} target={:?}",
                request.runtime_request_id,
                request.method,
                request.user_request_id,
                request.url,
                request.target_isolate_id
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let queued_requests = dump
        .queued_requests
        .iter()
        .take(8)
        .map(|request| {
            format!(
                "{}:{}:{}:{} target={:?}",
                request.runtime_request_id,
                request.method,
                request.user_request_id,
                request.url,
                request.target_isolate_id
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let isolate_summary = dump
        .isolates
        .iter()
        .map(|isolate| {
            format!(
                "{}:inflight={},wait_until={},pending={}",
                isolate.id,
                isolate.inflight_count,
                isolate.pending_wait_until,
                isolate.pending_requests.len()
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let top_shards = dump
        .memory_scheduler
        .top_shards
        .iter()
        .map(|shard| {
            format!(
                "{}:queued={},ready={},blocked={},affinity={:?},stale={}",
                shard.shard_index,
                shard.queued,
                shard.ready_owners,
                shard.blocked_owners,
                shard.affinity_isolate_id,
                shard.affinity_stale
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    println!(
        "bench-dump label={} outcome=ok generation={} queued={} isolates={} memory_queued={} memory_active_shards={} memory_owner_queues={} memory_blocked_owner_queues={} active_memory_leases={} memory_affinity_entries={} stale_memory_affinity_entries={} oldest_queue_ms={} pending_memory_outbox_shards={} top_memory_shards={} queued_requests={} pending_requests={}",
        label,
        dump.generation,
        dump.queued,
        isolate_summary,
        dump.memory_scheduler.queued,
        dump.memory_scheduler.active_shards,
        dump.memory_scheduler.owner_queues,
        dump.memory_scheduler.blocked_owner_queues,
        dump.memory_scheduler.active_leases,
        dump.memory_scheduler.affinity_entries,
        dump.memory_scheduler.stale_affinity_entries,
        dump.memory_scheduler.oldest_queue_ms,
        dump.memory_outbox.pending_scheduled_shards,
        top_shards,
        queued_requests,
        pending_requests,
    );
}
