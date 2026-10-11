//! Request scheduling and completion.

use super::*;

mod completion;
mod pool;
mod queue;

pub(super) use queue::estimate_pending_invoke_bytes;

const PRE_CANCELED_TTL: Duration = Duration::from_secs(60);
const MAX_PRE_CANCELED_REQUESTS_PER_WORKER: usize = 1024;
const RESPONSE_STREAM_CHANNEL_CAPACITY: usize = 16;

fn take_pre_canceled_request(
    pre_canceled: &mut HashMap<String, HashMap<String, Instant>>,
    worker_name: &str,
    runtime_request_id: &str,
    now: Instant,
) -> bool {
    let Some(request_ids) = pre_canceled.get_mut(worker_name) else {
        return false;
    };
    request_ids.retain(|_, expires_at| *expires_at > now);
    let matched = request_ids.remove(runtime_request_id).is_some();
    if request_ids.is_empty() {
        pre_canceled.remove(worker_name);
    }
    matched
}

fn remember_pre_canceled_request(
    pre_canceled: &mut HashMap<String, HashMap<String, Instant>>,
    worker_name: String,
    runtime_request_id: String,
    now: Instant,
) {
    let request_ids = pre_canceled.entry(worker_name).or_default();
    request_ids.retain(|_, expires_at| *expires_at > now);
    if request_ids.len() >= MAX_PRE_CANCELED_REQUESTS_PER_WORKER
        && !request_ids.contains_key(&runtime_request_id)
    {
        let oldest_id = request_ids
            .iter()
            .min_by_key(|(_, expires_at)| **expires_at)
            .map(|(request_id, _)| request_id.clone());
        if let Some(request_id) = oldest_id {
            request_ids.remove(&request_id);
        }
    }
    request_ids.insert(runtime_request_id, now + PRE_CANCELED_TTL);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_canceled_request_is_taken_once_before_ttl() {
        let now = Instant::now();
        let mut pre_canceled = HashMap::new();

        remember_pre_canceled_request(
            &mut pre_canceled,
            "worker".to_string(),
            "request".to_string(),
            now,
        );

        assert!(take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request",
            now + Duration::from_secs(1)
        ));
        assert!(!take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request",
            now + Duration::from_secs(1)
        ));
        assert!(pre_canceled.is_empty());
    }

    #[test]
    fn pre_canceled_requests_expire_and_are_capped() {
        let now = Instant::now();
        let mut pre_canceled = HashMap::new();

        remember_pre_canceled_request(
            &mut pre_canceled,
            "worker".to_string(),
            "expired".to_string(),
            now,
        );
        assert!(!take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "expired",
            now + PRE_CANCELED_TTL + Duration::from_millis(1)
        ));
        assert!(pre_canceled.is_empty());

        for index in 0..(MAX_PRE_CANCELED_REQUESTS_PER_WORKER + 10) {
            remember_pre_canceled_request(
                &mut pre_canceled,
                "worker".to_string(),
                format!("request-{index}"),
                now + Duration::from_millis(index as u64),
            );
        }

        assert_eq!(
            pre_canceled.get("worker").expect("worker tombstones").len(),
            MAX_PRE_CANCELED_REQUESTS_PER_WORKER
        );
        assert!(!take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request-0",
            now + Duration::from_secs(1)
        ));
        assert!(take_pre_canceled_request(
            &mut pre_canceled,
            "worker",
            "request-1024",
            now + Duration::from_secs(1)
        ));
    }
}
