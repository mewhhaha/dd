//! Pending queue accounting and scheduling tests.

use super::*;

fn pending_invoke(
    request_id: &str,
    target_isolate_id: Option<u64>,
    memory_route: Option<MemoryRoute>,
) -> PendingInvoke {
    let (reply, _) = oneshot::channel::<Result<WorkerOutput>>();
    PendingInvoke {
        queue_admission: None,
        runtime_request_id: request_id.to_string(),
        request: WorkerInvocation {
            method: "GET".to_string(),
            url: "http://worker/".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: request_id.to_string(),
        },
        request_body: None,
        memory_route,
        memory_call: None,
        target_isolate_id,
        reply_kind: PendingReplyKind::Normal,
        internal_origin: false,
        reply: reply.into(),
        enqueued_at: Instant::now(),
        queued_bytes: 1,
    }
}

fn memory_route_on_shard(key: &str, shard_index: usize) -> MemoryRoute {
    let mut route = MemoryRoute::new("MEM".to_string(), key.to_string());
    route.shard_index = Some(shard_index);
    route
}

fn assert_queue_accounting(queue: &PendingInvokeQueue) {
    let actual_count = queue.iter().count();
    let actual_bytes = queue
        .iter()
        .map(|pending| pending.queued_bytes)
        .sum::<usize>();
    assert_eq!(queue.len(), actual_count);
    assert_eq!(
        queue.internal_queued,
        queue
            .iter()
            .filter(|pending| pending.internal_origin)
            .count()
    );
    assert_eq!(queue.queued_bytes(), actual_bytes);
    assert_eq!(
        queue.is_empty(),
        actual_count == 0,
        "is_empty should follow actual queued count"
    );
    for shard in queue.memory_shards.values() {
        let shard_count = shard.iter().count();
        let shard_bytes = shard
            .iter()
            .map(|pending| pending.queued_bytes)
            .sum::<usize>();
        assert_eq!(shard.len(), shard_count);
        assert_eq!(shard.queued_bytes, shard_bytes);
        assert_eq!(shard.is_empty(), shard_count == 0);
    }
}

#[test]
fn pending_invoke_queue_tracks_internal_and_external_work() {
    let mut queue = PendingInvokeQueue::new();
    let mut internal = pending_invoke("internal", None, None);
    internal.internal_origin = true;
    queue.push_back(internal);
    queue.push_back(pending_invoke("external", None, None));

    assert!(queue.has_internal_request());
    assert!(queue.has_external_request());
    queue
        .pop_front()
        .expect("internal request should be present");
    assert!(!queue.has_internal_request());
    assert!(queue.has_external_request());
    assert_queue_accounting(&queue);
}

#[test]
fn pending_invoke_queue_lanes_preserve_global_fifo_pop_order() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke("general", None, None));
    queue.push_back(pending_invoke(
        "memory",
        None,
        Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
    ));
    queue.push_back(pending_invoke("targeted", Some(7), None));

    assert_eq!(queue.general.len(), 1);
    assert_eq!(
        queue
            .memory_shards
            .get(&0)
            .map(MemoryShardQueue::len)
            .unwrap_or_default(),
        1
    );
    assert_eq!(queue.targeted.len(), 1);
    assert_queue_accounting(&queue);
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("general".to_string())
    );
    assert_queue_accounting(&queue);
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("memory".to_string())
    );
    assert_queue_accounting(&queue);
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("targeted".to_string())
    );
    assert!(queue.is_empty());
    assert_queue_accounting(&queue);
}

#[test]
fn pending_invoke_queue_key_removal_skips_blocked_older_work() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke(
        "blocked-memory",
        None,
        Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
    ));
    queue.push_back(pending_invoke("ready-general", None, None));

    let key = queue
        .find_oldest_map(
            [PendingQueueLane::Memory, PendingQueueLane::General],
            |key, pending| (pending.runtime_request_id == "ready-general").then_some(key),
        )
        .expect("ready general work should be selectable");
    assert_eq!(
        queue.remove(key).map(|pending| pending.runtime_request_id),
        Some("ready-general".to_string())
    );
    assert_queue_accounting(&queue);
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("blocked-memory".to_string())
    );
    assert!(queue.is_empty());
    assert_queue_accounting(&queue);
}

#[test]
fn pending_invoke_queue_stores_memory_work_in_shard_lanes() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke(
        "memory-shard-7",
        None,
        Some(memory_route_on_shard("room-a", 7)),
    ));
    queue.push_back(pending_invoke(
        "memory-shard-3",
        None,
        Some(memory_route_on_shard("room-b", 3)),
    ));
    queue.push_back(pending_invoke("general", None, None));

    assert_eq!(queue.memory_shards.len(), 2);
    assert_queue_accounting(&queue);
    assert_eq!(
        queue
            .memory_shards
            .get(&7)
            .map(MemoryShardQueue::len)
            .unwrap_or_default(),
        1
    );
    assert_eq!(
        queue
            .memory_shards
            .get(&3)
            .map(MemoryShardQueue::len)
            .unwrap_or_default(),
        1
    );

    let key = queue
        .find_oldest_map([PendingQueueLane::Memory], |key, pending| {
            (pending.runtime_request_id == "memory-shard-3").then_some(key)
        })
        .expect("memory shard work should be selectable");
    assert_eq!(key.memory_shard_index, Some(3));
    assert_eq!(
        queue.remove(key).map(|pending| pending.runtime_request_id),
        Some("memory-shard-3".to_string())
    );
    assert!(!queue.memory_shards.contains_key(&3));
    assert!(queue.memory_shards.contains_key(&7));
    assert_queue_accounting(&queue);
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("memory-shard-7".to_string())
    );
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("general".to_string())
    );
    assert!(queue.memory_shards.is_empty());
    assert!(queue.is_empty());
    assert_queue_accounting(&queue);
}

#[test]
fn pending_invoke_queue_memory_lookup_can_bypass_blocked_owner_in_same_shard() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke(
        "owner-a-head",
        None,
        Some(memory_route_on_shard("room-a", 3)),
    ));
    queue.push_back(pending_invoke(
        "owner-a-tail",
        None,
        Some(memory_route_on_shard("room-a", 3)),
    ));
    queue.push_back(pending_invoke(
        "owner-b-head",
        None,
        Some(memory_route_on_shard("room-b", 3)),
    ));
    queue.push_back(pending_invoke(
        "memory-shard-7-head",
        None,
        Some(memory_route_on_shard("room-c", 7)),
    ));

    let owner_b = queue
        .find_oldest_map([PendingQueueLane::Memory], |key, pending| {
            (pending.runtime_request_id == "owner-b-head").then_some(key)
        })
        .expect("independent owner behind blocked owner should be selectable");
    assert_eq!(owner_b.memory_shard_index, Some(3));
    assert_eq!(
        queue
            .remove(owner_b)
            .map(|pending| pending.runtime_request_id),
        Some("owner-b-head".to_string())
    );

    let owner_a_tail = queue.find_oldest_map([PendingQueueLane::Memory], |key, pending| {
        (pending.runtime_request_id == "owner-a-tail").then_some(key)
    });
    assert!(
        owner_a_tail.is_none(),
        "later same-owner work must not overtake its owner head"
    );

    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("owner-a-head".to_string())
    );
    let exposed_tail = queue
        .find_oldest_map([PendingQueueLane::Memory], |key, pending| {
            (pending.runtime_request_id == "owner-a-tail").then_some(key)
        })
        .expect("tail becomes selectable after the shard head is removed");
    assert_eq!(exposed_tail.memory_shard_index, Some(3));
}

#[test]
fn pending_invoke_queue_fair_memory_lookup_rotates_between_owners_in_one_shard() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke(
        "owner-a-1",
        None,
        Some(memory_route_on_shard("room-a", 3)),
    ));
    queue.push_back(pending_invoke(
        "owner-a-2",
        None,
        Some(memory_route_on_shard("room-a", 3)),
    ));
    queue.push_back(pending_invoke(
        "owner-b-1",
        None,
        Some(memory_route_on_shard("room-b", 3)),
    ));

    let first = queue
        .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
        .expect("first owner should be selectable");
    assert_eq!(
        queue
            .remove(first)
            .map(|pending| pending.runtime_request_id),
        Some("owner-a-1".to_string())
    );

    let second = queue
        .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
        .expect("second owner should rotate ahead of hot owner backlog");
    assert_eq!(
        queue
            .remove(second)
            .map(|pending| pending.runtime_request_id),
        Some("owner-b-1".to_string())
    );

    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("owner-a-2".to_string())
    );
    assert!(queue.is_empty());
}

#[test]
fn pending_invoke_queue_blocked_memory_owner_is_skipped_until_woken() {
    let mut queue = PendingInvokeQueue::new();
    let route_a = memory_route_on_shard("room-a", 3);
    let owner_a = route_a.owner_key.clone();
    queue.push_back(pending_invoke("owner-a-1", None, Some(route_a)));
    queue.push_back(pending_invoke(
        "owner-b-1",
        None,
        Some(memory_route_on_shard("room-b", 3)),
    ));

    let blocked = queue
        .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
        .expect("owner a should be the first memory head");
    queue.mark_memory_owner_blocked(&blocked);

    let mut inspected = 0usize;
    let ready = queue
        .find_fair_map([PendingQueueLane::Memory], |key, pending| {
            inspected = inspected.saturating_add(1);
            Some((key, pending.runtime_request_id.clone()))
        })
        .expect("owner b should remain dispatchable");
    assert_eq!(ready.1, "owner-b-1");
    assert_eq!(inspected, 1);
    assert_eq!(
        queue
            .remove(ready.0)
            .map(|pending| pending.runtime_request_id),
        Some("owner-b-1".to_string())
    );

    assert!(queue.mark_memory_owner_ready(&owner_a));
    assert!(!queue.mark_memory_owner_ready(&owner_a));
    let woken = queue
        .find_fair_map([PendingQueueLane::Memory], |key, pending| {
            Some((key, pending.runtime_request_id.clone()))
        })
        .expect("woken owner should be dispatchable again");
    assert_eq!(woken.1, "owner-a-1");
}

#[test]
fn pending_invoke_queue_final_memory_owner_removal_clears_ready_and_blocked_state() {
    let mut queue = PendingInvokeQueue::new();
    let route = memory_route_on_shard("room-a", 3);
    let owner_key = route.owner_key.clone();
    queue.push_back(pending_invoke("owner-a-1", None, Some(route)));

    let key = queue
        .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
        .expect("memory owner should be selectable");
    queue.mark_memory_owner_blocked(&key);
    assert_eq!(queue.blocked_memory_owner_queues(&HashMap::new()), 1);

    assert_eq!(
        queue.remove(key).map(|pending| pending.runtime_request_id),
        Some("owner-a-1".to_string())
    );
    assert_eq!(queue.blocked_memory_owner_queues(&HashMap::new()), 0);
    assert!(!queue.mark_memory_owner_ready(&owner_key));
    assert!(queue.memory_shards.is_empty());
}

#[test]
fn pending_invoke_queue_fair_memory_lookup_rotates_across_shard_heads() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke(
        "hot-1",
        None,
        Some(memory_route_on_shard("hot-a", 1)),
    ));
    queue.push_back(pending_invoke(
        "hot-2",
        None,
        Some(memory_route_on_shard("hot-b", 1)),
    ));
    queue.push_back(pending_invoke(
        "cold-1",
        None,
        Some(memory_route_on_shard("cold-a", 2)),
    ));

    let first_key = queue
        .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
        .expect("first shard head should be selectable");
    assert_eq!(
        queue
            .remove(first_key)
            .map(|pending| pending.runtime_request_id),
        Some("hot-1".to_string())
    );

    let second_key = queue
        .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
        .expect("cold shard head should be selected before hot shard backlog");
    assert_eq!(second_key.memory_shard_index, Some(2));
    assert_eq!(
        queue
            .remove(second_key)
            .map(|pending| pending.runtime_request_id),
        Some("cold-1".to_string())
    );

    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("hot-2".to_string())
    );
    assert!(queue.is_empty());
}

#[test]
fn pending_invoke_queue_keys_are_stable_after_older_removals() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke("first", None, None));
    queue.push_back(pending_invoke("second", None, None));
    queue.push_back(pending_invoke("third", None, None));

    let third_key = queue
        .find_oldest_map([PendingQueueLane::General], |key, pending| {
            (pending.runtime_request_id == "third").then_some(key)
        })
        .expect("third should be indexed by a stable queue key");

    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("first".to_string())
    );
    assert_eq!(
        queue
            .remove(third_key)
            .map(|pending| pending.runtime_request_id),
        Some("third".to_string())
    );
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("second".to_string())
    );
    assert!(queue.is_empty());
}

#[test]
fn pending_invoke_queue_removes_by_runtime_request_id_without_predicate_scan() {
    let mut queue = PendingInvokeQueue::new();
    queue.push_back(pending_invoke("first", None, None));
    queue.push_back(pending_invoke(
        "memory",
        None,
        Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
    ));
    queue.push_back(pending_invoke("last", None, None));

    assert_eq!(
        queue
            .remove_by_runtime_request_id("memory")
            .map(|pending| pending.runtime_request_id),
        Some("memory".to_string())
    );
    assert!(queue.remove_by_runtime_request_id("memory").is_none());
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("first".to_string())
    );
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("last".to_string())
    );
    assert!(queue.is_empty());
}

#[test]
fn pending_invoke_queue_keeps_target_and_memory_indexes_coherent() {
    let mut queue = PendingInvokeQueue::new();
    let memory_route = MemoryRoute::new("MEM".to_string(), "room".to_string());
    let memory_owner_key = memory_route.owner_key.clone();

    queue.push_back(pending_invoke("general", None, None));
    queue.push_back(pending_invoke("targeted", Some(7), None));
    queue.push_back(pending_invoke("memory", None, Some(memory_route.clone())));
    queue.push_back(pending_invoke(
        "targeted-memory",
        Some(7),
        Some(memory_route),
    ));

    assert_eq!(queue.indexed_target_count(7), 2);
    assert_eq!(queue.indexed_memory_count(&memory_owner_key), 2);

    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("general".to_string())
    );
    assert_eq!(queue.indexed_target_count(7), 2);
    assert_eq!(queue.indexed_memory_count(&memory_owner_key), 2);

    assert_eq!(
        queue
            .remove_by_runtime_request_id("targeted")
            .map(|pending| pending.runtime_request_id),
        Some("targeted".to_string())
    );
    assert_eq!(queue.indexed_target_count(7), 1);
    assert_eq!(queue.indexed_memory_count(&memory_owner_key), 2);

    let targeted_memory_key = queue
        .find_oldest_map([PendingQueueLane::Targeted], |key, pending| {
            (pending.runtime_request_id == "targeted-memory").then_some(key)
        })
        .expect("targeted memory work should be indexed");
    assert_eq!(
        queue
            .remove(targeted_memory_key)
            .map(|pending| pending.runtime_request_id),
        Some("targeted-memory".to_string())
    );
    assert_eq!(queue.indexed_target_count(7), 0);
    assert_eq!(queue.indexed_memory_count(&memory_owner_key), 1);

    let drained = queue.pop_front().into_iter().collect::<Vec<_>>();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].runtime_request_id, "memory");
    assert_queue_accounting(&queue);
    assert_eq!(queue.indexed_target_count(7), 0);
    assert_eq!(queue.indexed_memory_count(&memory_owner_key), 0);
    assert!(queue.is_empty());
}

#[test]
fn pending_invoke_queue_reports_bounded_memory_scheduler_debug_without_owner_keys() {
    let mut queue = PendingInvokeQueue::new();
    let mut hot_route = MemoryRoute::new("MEM".to_string(), "hot".to_string());
    hot_route.shard_index = Some(2);
    let mut cold_route = MemoryRoute::new("MEM".to_string(), "cold".to_string());
    cold_route.shard_index = Some(3);
    let hot_owner_key = hot_route.owner_key.clone();
    queue.push_back(pending_invoke("general", None, None));
    queue.push_back(pending_invoke("hot-a", None, Some(hot_route.clone())));
    queue.push_back(pending_invoke("hot-b", None, Some(hot_route)));
    queue.push_back(pending_invoke("cold", None, Some(cold_route)));

    let lane_depths = queue.lane_depths();
    assert_eq!(lane_depths.general, 1);
    assert_eq!(lane_depths.memory, 3);
    assert_eq!(queue.active_memory_shards(), 2);
    assert_eq!(queue.memory_shard_depth_summary(), (2, 2));
    assert_eq!(queue.memory_owner_queues(), 2);

    let mut leases = HashMap::new();
    leases.insert(
        hot_owner_key.clone(),
        MemoryEntityLease {
            owner_key: hot_owner_key.clone(),
            epoch: 1,
            owner_isolate_id: 10,
        },
    );
    assert_eq!(queue.blocked_memory_owner_queues(&leases), 1);

    let mut affinity = HashMap::new();
    affinity.insert(2, 10);
    affinity.insert(3, 99);
    let isolate_indices = HashMap::from([(10, 0)]);
    let shards = queue.memory_shard_debug(&leases, &affinity, &isolate_indices, 1);
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].shard_index, 2);
    assert_eq!(shards[0].queued, 2);
    assert_eq!(shards[0].blocked_owners, 1);
    assert_eq!(shards[0].ready_owners, 0);
    assert_eq!(shards[0].affinity_isolate_id, Some(10));
    assert!(!shards[0].affinity_stale);
    assert!(format!("{:?}", shards).contains("shard_index"));
    assert!(!format!("{:?}", shards).contains(&hot_owner_key));
}

#[test]
fn pending_invoke_queue_drains_targeted_work_by_isolate_index() {
    let mut queue = PendingInvokeQueue::new();
    let memory_route = MemoryRoute::new("MEM".to_string(), "room".to_string());
    let memory_owner_key = memory_route.owner_key.clone();

    queue.push_back(pending_invoke("general", None, None));
    queue.push_back(pending_invoke("targeted-a", Some(7), None));
    queue.push_back(pending_invoke(
        "targeted-memory",
        Some(7),
        Some(memory_route),
    ));
    queue.push_back(pending_invoke("targeted-b", Some(8), None));

    let drained = queue.drain_target_isolate_id(7);
    let drained_ids = drained
        .iter()
        .map(|pending| pending.runtime_request_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(drained_ids, vec!["targeted-a", "targeted-memory"]);
    assert_queue_accounting(&queue);
    assert_eq!(queue.indexed_target_count(7), 0);
    assert_eq!(queue.indexed_target_count(8), 1);
    assert_eq!(queue.indexed_memory_count(&memory_owner_key), 0);
    assert!(queue.remove_by_runtime_request_id("targeted-a").is_none());
    assert!(
        queue
            .remove_by_runtime_request_id("targeted-memory")
            .is_none()
    );
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("general".to_string())
    );
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("targeted-b".to_string())
    );
    assert!(queue.is_empty());
    assert_queue_accounting(&queue);
}

#[test]
fn pending_invoke_queue_drains_expired_work_by_enqueued_index() {
    let mut queue = PendingInvokeQueue::new();
    let now = Instant::now();
    let max_wait = Duration::from_millis(50);
    let mut old_targeted = pending_invoke("old-targeted", Some(7), None);
    old_targeted.enqueued_at = now - Duration::from_millis(75);
    let mut fresh_memory = pending_invoke(
        "fresh-memory",
        None,
        Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
    );
    fresh_memory.enqueued_at = now - Duration::from_millis(10);
    let memory_owner_key = fresh_memory
        .memory_route
        .as_ref()
        .expect("memory route")
        .owner_key
        .clone();
    let mut old_general = pending_invoke("old-general", None, None);
    old_general.enqueued_at = now - Duration::from_millis(55);

    queue.push_back(old_targeted);
    queue.push_back(fresh_memory);
    queue.push_back(old_general);
    assert_eq!(queue.indexed_enqueued_count(), 3);

    let drained = queue.drain_expired(now, max_wait);
    let drained_ids = drained
        .iter()
        .map(|pending| pending.runtime_request_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(drained_ids, vec!["old-targeted", "old-general"]);
    assert_queue_accounting(&queue);
    assert_eq!(queue.indexed_target_count(7), 0);
    assert_eq!(queue.indexed_memory_count(&memory_owner_key), 1);
    assert_eq!(queue.indexed_enqueued_count(), 1);
    assert_eq!(
        queue.next_expiry_at(max_wait),
        Some(now + Duration::from_millis(40))
    );
    assert!(queue.remove_by_runtime_request_id("old-targeted").is_none());
    assert!(queue.remove_by_runtime_request_id("old-general").is_none());
    assert_eq!(
        queue.pop_front().map(|pending| pending.runtime_request_id),
        Some("fresh-memory".to_string())
    );
    assert!(queue.is_empty());
    assert_eq!(queue.indexed_enqueued_count(), 0);
    assert_queue_accounting(&queue);
}

#[test]
fn memory_entity_lease_matching_is_epoch_and_owner_checked() {
    let first = MemoryEntityLease {
        owner_key: "MEM\u{1f}room".to_string(),
        epoch: 1,
        owner_isolate_id: 7,
    };
    let newer = MemoryEntityLease {
        owner_key: first.owner_key.clone(),
        epoch: 2,
        owner_isolate_id: 7,
    };
    let migrated = MemoryEntityLease {
        owner_key: first.owner_key.clone(),
        epoch: 1,
        owner_isolate_id: 8,
    };
    assert!(first.matches(&first));
    assert!(!newer.matches(&first));
    assert!(!migrated.matches(&first));
}
