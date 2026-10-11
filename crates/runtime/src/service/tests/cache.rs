//! The Cache API: the default cache, named caches kept apart per worker, and the
//! capacity they share.
use super::*;

#[tokio::test]
#[serial]
async fn cache_default_reuses_response() {
    let service = test_service(test_pool_config(1, 1, 1)).await;

    service
        .deploy("cache".to_string(), cache_worker("default", "cache"))
        .await
        .expect("deploy should succeed");

    let one = service
        .invoke(
            "cache".to_string(),
            test_invocation_with_path("/", "cache-one"),
        )
        .await
        .expect("first invoke should succeed");
    let two = service
        .invoke(
            "cache".to_string(),
            test_invocation_with_path("/", "cache-two"),
        )
        .await
        .expect("second invoke should succeed");

    assert_eq!(String::from_utf8(one.body).expect("utf8"), "cache:1");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "cache:1");
}

#[tokio::test]
#[serial]
async fn cache_names_are_isolated_by_worker() {
    let service = test_service(serial_runtime_config()).await;

    service
        .deploy("cache-owner-a".to_string(), cache_worker("shared", "A"))
        .await
        .expect("deploy a should succeed");
    service
        .deploy("cache-owner-b".to_string(), cache_worker("shared", "B"))
        .await
        .expect("deploy b should succeed");

    let a = service
        .invoke(
            "cache-owner-a".to_string(),
            test_invocation_with_path("/", "cache-owner-a-1"),
        )
        .await
        .expect("worker a should succeed");
    let b = service
        .invoke(
            "cache-owner-b".to_string(),
            test_invocation_with_path("/", "cache-owner-b-1"),
        )
        .await
        .expect("worker b should succeed");

    assert_eq!(String::from_utf8(a.body).expect("utf8"), "A:1");
    assert_eq!(String::from_utf8(b.body).expect("utf8"), "B:1");
}

#[tokio::test]
#[serial]
async fn named_caches_share_global_capacity_budget() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        cache_max_entries: 1,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("worker-a".to_string(), cache_worker("cache-a", "A"))
        .await
        .expect("deploy a should succeed");
    service
        .deploy("worker-b".to_string(), cache_worker("cache-b", "B"))
        .await
        .expect("deploy b should succeed");

    let a1 = service
        .invoke(
            "worker-a".to_string(),
            test_invocation_with_path("/", "a-1"),
        )
        .await
        .expect("a1 should succeed");
    let b1 = service
        .invoke(
            "worker-b".to_string(),
            test_invocation_with_path("/", "b-1"),
        )
        .await
        .expect("b1 should succeed");
    let a2 = service
        .invoke(
            "worker-a".to_string(),
            test_invocation_with_path("/", "a-2"),
        )
        .await
        .expect("a2 should succeed");

    assert_eq!(String::from_utf8(a1.body).expect("utf8"), "A:1");
    assert_eq!(String::from_utf8(b1.body).expect("utf8"), "B:1");
    assert_eq!(String::from_utf8(a2.body).expect("utf8"), "A:2");
}
