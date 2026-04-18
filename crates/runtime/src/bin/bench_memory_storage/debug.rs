use super::*;

pub(super) async fn print_profile_on_failure(service: &RuntimeService, worker_name: &str, label: &str) {
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

pub(super) async fn print_debug_dump_on_failure(service: &RuntimeService, worker_name: &str, label: &str) {
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
                "{}:{}:{}:{} memory={:?} target={:?}",
                request.runtime_request_id,
                request.method,
                request.user_request_id,
                request.url,
                request.memory_key,
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
                "{}:{}:{}:{} memory={:?} target={:?}",
                request.runtime_request_id,
                request.method,
                request.user_request_id,
                request.url,
                request.memory_key,
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
    println!(
        "bench-dump label={} outcome=ok generation={} queued={} isolates={} owners={:?} memory_inflight={:?} queued_requests={} pending_requests={}",
        label,
        dump.generation,
        dump.queued,
        isolate_summary,
        dump.memory_owners.iter().take(8).collect::<Vec<_>>(),
        dump.memory_inflight.iter().take(8).collect::<Vec<_>>(),
        queued_requests,
        pending_requests,
    );
}
