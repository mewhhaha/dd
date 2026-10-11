//! The isolate pool: reusing isolates, growing with backlog within the global
//! isolate budget, and retiring idle isolates when the budget is short or the
//! pool idles.
use super::*;

#[tokio::test]
#[serial]
async fn reuse_preserves_state() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("counter".to_string(), counter_worker())
        .await
        .expect("deploy should succeed");

    let one = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("first invoke should succeed");
    let two = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("second invoke should succeed");

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn scales_up_with_backlog() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 4,
        max_isolates_per_worker: 4,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow".to_string(), slow_worker())
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..12 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = format!("req-{idx}");
            svc.invoke("slow".to_string(), req).await
        }));
    }

    timeout(Duration::from_secs(10), async {
        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }
    })
    .await
    .expect("single worker requests should finish");

    let stats = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert!(stats.spawn_count > 1);
    assert!(stats.isolates_total <= 4);
    assert_eq!(stats.global_isolate_budget, 4);
    assert!(stats.global_isolates_total <= 4);
}

#[tokio::test]
#[serial]
async fn single_worker_can_grow_to_global_isolate_budget() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates_per_worker: 8,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow".to_string(), slow_worker())
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for idx in 0..12 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = format!("budget-req-{idx}");
            svc.invoke("slow".to_string(), req).await
        }));
    }

    timeout(Duration::from_secs(10), async {
        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }
    })
    .await
    .expect("single worker requests should finish");

    let stats = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert_eq!(stats.global_isolate_budget, 2);
    assert!(stats.spawn_count > 1);
    assert!(stats.isolates_total <= 2);
    assert!(stats.global_isolates_total <= 2);
    assert_eq!(
        stats.global_isolate_slots_available,
        2usize.saturating_sub(stats.global_isolates_total)
    );
}

#[tokio::test]
#[serial]
async fn active_workers_share_small_global_isolate_budget() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates_per_worker: 4,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow-a".to_string(), slow_worker())
        .await
        .expect("deploy a should succeed");
    service
        .deploy("slow-b".to_string(), slow_worker())
        .await
        .expect("deploy b should succeed");

    let mut tasks = Vec::new();
    for worker in ["slow-a", "slow-b"] {
        for idx in 0..4 {
            let svc = service.clone();
            let worker = worker.to_string();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = format!("{worker}-req-{idx}");
                svc.invoke(worker, req).await
            }));
        }
    }

    for task in tasks {
        task.await.expect("join").expect("invoke should succeed");
    }

    let a = service.stats("slow-a".to_string()).await.expect("stats a");
    let b = service.stats("slow-b".to_string()).await.expect("stats b");
    assert!(a.spawn_count >= 1);
    assert!(b.spawn_count >= 1);
    assert!(a.isolates_total + b.isolates_total <= 2);
    assert_eq!(a.global_isolates_total, b.global_isolates_total);
    assert!(a.global_isolates_total <= 2);
}

#[test]
fn runtime_default_global_isolate_budget_has_two_slot_floor() {
    let config = RuntimeConfig::default();
    assert!(config.max_global_isolates >= 2);
    assert_eq!(config.max_isolates_per_worker, config.max_global_isolates);
    assert_eq!(config.min_isolates, 0);
}

#[tokio::test]
#[serial]
async fn cold_routes_retire_lru_idle_isolates_without_waiting_for_idle_ttl() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(60 * 60),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let workers = ["cold-a", "cold-b", "cold-c", "cold-d"];
    for worker in workers {
        service
            .deploy(worker.to_string(), counter_worker())
            .await
            .expect("deploy should succeed");
    }

    for (index, worker) in workers.into_iter().enumerate() {
        let mut request = test_invocation();
        request.request_id = format!("cold-route-{index}");
        let output = timeout(
            Duration::from_secs(2),
            service.invoke(worker.to_string(), request),
        )
        .await
        .expect("cold route must not wait for the one-hour idle TTL")
        .expect("cold route should succeed");
        assert_eq!(output.status, 200);
        sleep(Duration::from_millis(20)).await;
    }

    let first = service
        .stats("cold-a".to_string())
        .await
        .expect("first worker stats should exist");
    let second = service
        .stats("cold-b".to_string())
        .await
        .expect("second worker stats should exist");
    let last = service
        .stats("cold-d".to_string())
        .await
        .expect("last worker stats should exist");
    assert_eq!(first.isolates_total, 0, "oldest idle isolate should retire");
    assert_eq!(
        second.isolates_total, 0,
        "next-oldest idle isolate should retire"
    );
    assert_eq!(last.global_isolate_budget, 2);
    assert!(last.global_isolates_total <= 2);
}

#[tokio::test]
#[serial]
async fn budget_pressure_never_retires_an_isolate_with_an_active_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(60 * 60),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy("active-a".to_string(), slow_worker())
        .await
        .expect("first deploy should succeed");
    service
        .deploy("active-b".to_string(), counter_worker())
        .await
        .expect("second deploy should succeed");

    let first_service = service.clone();
    let first = tokio::spawn(async move {
        first_service
            .invoke("active-a".to_string(), test_invocation())
            .await
    });
    sleep(Duration::from_millis(10)).await;
    let second = timeout(
        Duration::from_secs(2),
        service.invoke("active-b".to_string(), test_invocation()),
    );

    let (first, second) = tokio::join!(first, second);
    assert_eq!(
        String::from_utf8(
            first
                .expect("first task should join")
                .expect("active request must not be evicted")
                .body,
        )
        .expect("utf8"),
        "ok"
    );
    assert!(
        second
            .expect("second route should receive the slot after completion")
            .is_ok()
    );
}

#[tokio::test]
#[serial]
async fn budget_pressure_waits_for_wait_until_before_retiring_an_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(60 * 60),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "wait-until-a".to_string(),
            r#"
export default {
  async fetch(_request, _env, ctx) {
    ctx.waitUntil(__dd_internals.core.ops.op_time_sleep(150));
    return new Response("queued");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("waitUntil worker should deploy");
    service
        .deploy("wait-until-b".to_string(), counter_worker())
        .await
        .expect("second worker should deploy");

    let first = service
        .invoke("wait-until-a".to_string(), test_invocation())
        .await
        .expect("first response should succeed");
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "queued");

    let started = Instant::now();
    timeout(
        Duration::from_secs(2),
        service.invoke("wait-until-b".to_string(), test_invocation()),
    )
    .await
    .expect("second worker should receive the slot after waitUntil")
    .expect("second request should succeed");
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "waitUntil isolate was retired before its background work completed"
    );
}

#[tokio::test]
#[serial]
async fn scales_down_when_idle() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 3,
        max_isolates_per_worker: 3,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_millis(200),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("slow".to_string(), slow_worker())
        .await
        .expect("deploy should succeed");

    for idx in 0..6 {
        let mut req = test_invocation();
        req.request_id = format!("req-{idx}");
        service
            .invoke("slow".to_string(), req)
            .await
            .expect("invoke should succeed");
    }

    let before = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert!(before.isolates_total > 0);

    timeout(Duration::from_secs(3), async {
        loop {
            let stats = service.stats("slow".to_string()).await.expect("stats");
            if stats.isolates_total == 0 && stats.global_isolates_total == 0 {
                assert_eq!(stats.global_isolates_total, 0);
                assert_eq!(stats.global_isolate_slots_available, 3);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("isolates should scale down to zero");
}

#[tokio::test]
#[serial]
async fn single_isolate_allows_multiple_inflight_requests() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("io".to_string(), io_wait_worker())
        .await
        .expect("deploy should succeed");

    let started = Instant::now();
    let mut tasks = Vec::new();
    for idx in 0..2 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = format!("io-{idx}");
            svc.invoke("io".to_string(), req).await
        }));
    }

    for task in tasks {
        task.await.expect("join").expect("invoke should succeed");
    }
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_millis(260),
        "expected multiplexed inflight execution, elapsed={elapsed:?}"
    );
}
