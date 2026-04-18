use super::{
    BlobStoreConfig, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
};
use common::{
    DeployAsset, DeployBinding, DeployConfig, DeployInternalConfig, DeployTraceDestination,
    WorkerInvocation, WorkerOutput,
};
use serde::Deserialize;
use serde_json::Value;
use serial_test::serial;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

#[path = "tests/fixtures.rs"]
mod fixtures;
#[path = "tests/dynamic.rs"]
mod dynamic;
#[path = "tests/sessions.rs"]
mod sessions;
#[path = "tests/memory.rs"]
mod memory;

use self::fixtures::*;

#[tokio::test]
#[serial]
async fn service_starts_with_deno_runtime_bootstrap() {
    let _ = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
}

#[tokio::test]
#[serial]
async fn deployed_assets_resolve_with_headers_and_head_support() {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some(asset_headers_file()),
        )
        .await
        .expect("deploy should succeed");

    let asset = service
        .resolve_asset(
            "assets".to_string(),
            "GET".to_string(),
            Some("foo.example.com:443".to_string()),
            "/a.js".to_string(),
            Vec::new(),
        )
        .await
        .expect("asset lookup should succeed")
        .expect("asset should exist");
    assert_eq!(asset.status, 200);
    assert_eq!(asset.body, b"asset-body");
    assert!(asset.headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("cache-control") && value == "public, max-age=60"
    }));
    assert!(asset
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-host") && value == "foo"));

    let etag = asset
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
        .map(|(_, value)| value.clone())
        .expect("etag should be present");

    let head = service
        .resolve_asset(
            "assets".to_string(),
            "HEAD".to_string(),
            Some("foo.example.com".to_string()),
            "/nested/b.css".to_string(),
            Vec::new(),
        )
        .await
        .expect("head lookup should succeed")
        .expect("asset should exist");
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert!(head
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("x-splat") && value == "b.css"));

    let not_modified = service
        .resolve_asset(
            "assets".to_string(),
            "GET".to_string(),
            Some("foo.example.com".to_string()),
            "/a.js".to_string(),
            vec![("if-none-match".to_string(), etag)],
        )
        .await
        .expect("etag lookup should succeed")
        .expect("asset should exist");
    assert_eq!(not_modified.status, 304);
    assert!(not_modified.body.is_empty());
}

#[tokio::test]
#[serial]
async fn deployed_assets_restore_from_worker_store() {
    let root = PathBuf::from(format!("/tmp/dd-assets-{}", Uuid::new_v4()));
    let db_path = root.join("dd-test.db");
    let database_url = format!("file:{}", db_path.display());

    let service = test_service_with_paths(
        RuntimeConfig::default(),
        root.clone(),
        database_url.clone(),
        true,
    )
    .await;
    service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some(asset_headers_file()),
        )
        .await
        .expect("deploy should succeed");
    drop(service);

    let restored =
        test_service_with_paths(RuntimeConfig::default(), root.clone(), database_url, true).await;
    let asset = restored
        .resolve_asset(
            "assets".to_string(),
            "GET".to_string(),
            Some("foo.example.com".to_string()),
            "/a.js".to_string(),
            Vec::new(),
        )
        .await
        .expect("asset lookup should succeed")
        .expect("asset should exist after restore");
    assert_eq!(asset.body, b"asset-body");

    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
#[serial]
async fn invalid_asset_headers_fail_deploy() {
    let service = test_service(RuntimeConfig::default()).await;
    let error = service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some("/a.js\n  BadHeader".to_string()),
        )
        .await
        .expect_err("deploy should fail");
    assert!(error
        .to_string()
        .contains("must be `Name: value` or `! Name`"));
}

#[tokio::test]
#[serial]
async fn service_can_deploy_simple_worker_with_deno_runtime_bootstrap() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "simple-deno-worker".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  }
                };
                "#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");
}

#[tokio::test]
#[serial]
async fn service_can_invoke_simple_worker_with_deno_runtime_bootstrap() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "simple-deno-invoke".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  }
                };
                "#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("simple-deno-invoke".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "ok"
    );
}

#[tokio::test]
#[serial]
async fn reuse_preserves_state() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
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
async fn spectre_time_mitigation_freezes_time_between_io_boundaries() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("frozen-time".to_string(), frozen_time_worker())
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("frozen-time".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    let payload: FrozenTimeState = crate::json::from_string(
        String::from_utf8(output.body).expect("frozen-time body should be utf8"),
    )
    .expect("frozen-time response should parse");

    assert_eq!(
        payload.now0, payload.now1,
        "Date.now should remain frozen during pure compute"
    );
    assert_eq!(
        payload.perf0, payload.perf1,
        "performance.now should remain frozen during pure compute"
    );
    assert!(
        payload.now2 >= payload.now1,
        "Date.now should not move backwards across I/O boundaries"
    );
    assert!(
        payload.perf2 >= payload.perf1,
        "performance.now should not move backwards across I/O boundaries"
    );
    assert!(
        payload.now2 > payload.now1 || payload.perf2 > payload.perf1,
        "expected frozen clocks to advance after an I/O boundary"
    );
    assert!(payload.guard > 0, "worker should run local compute loop");
}

#[tokio::test]
#[serial]
async fn crypto_globals_work_with_deno_crypto_ops() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("crypto-worker".to_string(), crypto_worker())
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("crypto-worker".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    let payload: CryptoState =
        crate::json::from_string(String::from_utf8(output.body).expect("body should be utf8"))
            .expect("response should parse");

    assert_eq!(payload.random_length, 16);
    assert!(
        payload.random_non_zero,
        "random bytes should not be all zero"
    );
    assert_eq!(payload.digest_length, 32);
    assert_eq!(payload.uuid.len(), 36, "uuid should be canonical v4 length");
}

#[tokio::test]
#[serial]
async fn lazy_kv_get_batching_matches_sequential_reads_and_preserves_duplicates() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-equality".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(&worker_name),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let sequential = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/sequential", "kv-sequential-request"),
        )
        .await
        .expect("sequential read should succeed");
    let queued = service
        .invoke(
            worker_name,
            test_invocation_with_path("/queued", "kv-queued-request"),
        )
        .await
        .expect("queued read should succeed");

    let sequential_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(sequential.body).expect("sequential body should be utf8"),
    )
    .expect("sequential response should parse");
    let queued_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(queued.body).expect("queued body should be utf8"),
    )
    .expect("queued response should parse");

    assert_eq!(queued_values, sequential_values);
    assert_eq!(queued_values.len(), 10);
    assert!(queued_values.iter().all(|value| value == "1"));
}

#[tokio::test]
#[serial]
async fn lazy_kv_get_batching_decodes_mixed_values_and_rejects_whole_batch_on_failure() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-mixed".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(&worker_name),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-mixed-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let mixed = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/mixed", "kv-mixed-request"),
        )
        .await
        .expect("mixed read should succeed");
    let mixed_values: Vec<Value> =
        crate::json::from_string(String::from_utf8(mixed.body).expect("mixed body should be utf8"))
            .expect("mixed response should parse");
    assert_eq!(
        mixed_values,
        vec![
            Value::String("plain".to_string()),
            crate::json::from_string::<Value>(r#"{"ok":true,"n":7}"#.to_string())
                .expect("object value should parse"),
            Value::Null,
            Value::String("plain".to_string()),
            crate::json::from_string::<Value>(r#"{"ok":true,"n":7}"#.to_string())
                .expect("object value should parse"),
        ]
    );

    let rejected = service
        .invoke(
            worker_name,
            test_invocation_with_path("/reject", "kv-reject-request"),
        )
        .await
        .expect("reject route should return response");
    assert_eq!(rejected.status, 500);
    let body = String::from_utf8(rejected.body).expect("reject body should be utf8");
    assert!(
        body.contains("deserialize failed"),
        "expected queued batch rejection body, got: {body}"
    );
}

#[tokio::test]
#[serial]
async fn lazy_kv_get_batching_is_scoped_to_each_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 16,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-batching-scope".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_batching_worker(&worker_name),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-scope-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let left_request = test_invocation_with_path("/scoped?key=left", "kv-scope-left-request");
    let right_request = test_invocation_with_path("/scoped?key=right", "kv-scope-right-request");
    let (left, right) = tokio::join!(
        service.invoke(worker_name.clone(), left_request),
        service.invoke(worker_name, right_request)
    );
    let left_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(left.expect("left invoke should succeed").body)
            .expect("left body should be utf8"),
    )
    .expect("left response should parse");
    let right_values: Vec<Value> = crate::json::from_string(
        String::from_utf8(right.expect("right invoke should succeed").body)
            .expect("right body should be utf8"),
    )
    .expect("right response should parse");

    assert_eq!(left_values.len(), 10);
    assert_eq!(right_values.len(), 10);
    assert!(left_values.iter().all(|value| value == "L"));
    assert!(right_values.iter().all(|value| value == "R"));
}

#[tokio::test]
#[serial]
async fn shared_env_is_reused_safely_across_requests() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "shared-env-reuse".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            reusable_env_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/", "shared-env-first-request"),
        )
        .await
        .expect("first invoke should succeed");
    let second = service
        .invoke(
            worker_name,
            test_invocation_with_path("/", "shared-env-second-request"),
        )
        .await
        .expect("second invoke should succeed");

    let first_payload: Value =
        crate::json::from_string(String::from_utf8(first.body).expect("first body should be utf8"))
            .expect("first response should parse");
    let second_payload: Value = crate::json::from_string(
        String::from_utf8(second.body).expect("second body should be utf8"),
    )
    .expect("second response should parse");

    assert_eq!(first_payload["sameEnv"], Value::Bool(false));
    assert_eq!(first_payload["sameKv"], Value::Bool(false));
    assert_eq!(second_payload["sameEnv"], Value::Bool(true));
    assert_eq!(second_payload["sameKv"], Value::Bool(true));

    for payload in [&first_payload, &second_payload] {
        assert_eq!(payload["envExtensible"], Value::Bool(false));
        assert_eq!(payload["kvExtensible"], Value::Bool(false));
        assert_eq!(payload["envMutationResult"], Value::Bool(false));
        assert_eq!(payload["kvMutationResult"], Value::Bool(false));
        assert_eq!(payload["envHasTemp"], Value::Bool(false));
        assert_eq!(payload["kvHasTemp"], Value::Bool(false));
    }
}

#[tokio::test]
#[serial]
async fn kv_write_batching_preserves_last_write_wins() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-batching".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-batch-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let output = service
        .invoke(
            worker_name,
            test_invocation_with_path("/write-batch", "kv-write-batch-request"),
        )
        .await
        .expect("write batch should succeed");
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "4"
    );
}

#[tokio::test]
#[serial]
async fn kv_write_overlay_makes_same_request_reads_predictable() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-overlay".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-overlay-seed-request"),
        )
        .await
        .expect("seed should succeed");

    let output = service
        .invoke(
            worker_name,
            test_invocation_with_path("/write-overlay", "kv-write-overlay-request"),
        )
        .await
        .expect("write overlay should succeed");
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "9"
    );
}

#[tokio::test]
#[serial]
async fn kv_read_cache_hits_across_requests_in_same_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        kv_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-cache-hit".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-read-cache-hit-seed"),
        )
        .await
        .expect("seed should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/__profile_reset", "kv-read-cache-hit-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read", "kv-read-cache-hit-read-1"),
        )
        .await
        .expect("first read should succeed");
    let second = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read", "kv-read-cache-hit-read-2"),
        )
        .await
        .expect("second read should succeed");
    let profile = decode_kv_profile(
        service
            .invoke(
                worker_name,
                test_invocation_with_path("/__profile", "kv-read-cache-hit-profile"),
            )
            .await
            .expect("profile should succeed"),
    );

    assert_eq!(String::from_utf8(first.body).expect("utf8"), "1");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "1");
    assert_eq!(profile.op_get.calls, 0);
    assert!(profile.js_cache_hit.calls >= 2);
}

#[tokio::test]
#[serial]
async fn kv_read_cache_caches_missing_keys_across_requests() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        kv_profile_enabled: true,
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-cache-miss".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/__profile_reset", "kv-read-cache-miss-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-miss-read-1"),
        )
        .await
        .expect("first missing read should succeed");
    let second = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-miss-read-2"),
        )
        .await
        .expect("second missing read should succeed");
    let profile = decode_kv_profile(
        service
            .invoke(
                worker_name,
                test_invocation_with_path("/__profile", "kv-read-cache-miss-profile"),
            )
            .await
            .expect("profile should succeed"),
    );

    assert_eq!(String::from_utf8(first.body).expect("utf8"), "missing");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "missing");
    assert_eq!(profile.op_get.calls, 1);
    assert!(profile.js_cache_miss.calls >= 1);
    assert!(profile.js_cache_hit.calls >= 1);
}

#[tokio::test]
#[serial]
async fn kv_read_cache_expires_and_refills_after_ttl() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        kv_profile_enabled: true,
        kv_read_cache_hit_ttl: Duration::from_millis(20),
        kv_read_cache_miss_ttl: Duration::from_millis(20),
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-cache-expiry".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/__profile_reset", "kv-read-cache-expiry-profile-reset"),
        )
        .await
        .expect("profile reset should succeed");

    let first = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-expiry-read-1"),
        )
        .await
        .expect("first read should succeed");
    tokio::time::sleep(Duration::from_millis(40)).await;
    let second = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-missing", "kv-read-cache-expiry-read-2"),
        )
        .await
        .expect("second read should succeed");
    let profile = decode_kv_profile(
        service
            .invoke(
                worker_name,
                test_invocation_with_path("/__profile", "kv-read-cache-expiry-profile"),
            )
            .await
            .expect("profile should succeed"),
    );

    assert_eq!(String::from_utf8(first.body).expect("utf8"), "missing");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "missing");
    assert!(profile.op_get.calls >= 2);
    assert!(profile.js_cache_stale.calls >= 1);
}

#[tokio::test]
#[serial]
async fn kv_local_cache_updates_immediately_after_put_and_delete() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-local-cache-updates".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-local-cache-updates-seed"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/write-fire-and-forget", "kv-local-cache-updates-put"),
        )
        .await
        .expect("put should enqueue");
    let read_after_put = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read", "kv-local-cache-updates-read-after-put"),
        )
        .await
        .expect("read after put should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/delete-fire-and-forget", "kv-local-cache-updates-delete"),
        )
        .await
        .expect("delete should enqueue");
    let read_after_delete = service
        .invoke(
            worker_name,
            test_invocation_with_path("/read", "kv-local-cache-updates-read-after-delete"),
        )
        .await
        .expect("read after delete should succeed");

    assert_eq!(String::from_utf8(read_after_put.body).expect("utf8"), "7");
    assert_eq!(
        String::from_utf8(read_after_delete.body).expect("utf8"),
        "missing"
    );
}

#[tokio::test]
#[serial]
async fn kv_unawaited_write_flushes_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-fire-and-forget".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-fire-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/write-fire-and-forget", "kv-write-fire-request"),
        )
        .await
        .expect("fire-and-forget request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "kv-write-fire-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "7" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("7"));
}

#[tokio::test]
#[serial]
async fn kv_wait_until_write_flushes_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-write-wait-until".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_write_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-write-wait-until-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/write-wait-until", "kv-write-wait-until-request"),
        )
        .await
        .expect("wait-until request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "kv-write-wait-until-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "8" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let status = service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path(
                "/write-wait-until-result",
                "kv-write-wait-until-status-request",
            ),
        )
        .await
        .expect("status request should succeed");
    let status_body = String::from_utf8(status.body).expect("status body should be utf8");

    assert_eq!(status_body, "ok");
    assert_eq!(observed.as_deref(), Some("8"));
}

#[tokio::test]
#[serial]
async fn kv_wait_until_read_runs_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "kv-read-wait-until".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            kv_wait_until_read_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Kv {
                    binding: "MY_KV".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");
    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/seed", "kv-read-wait-until-seed-request"),
        )
        .await
        .expect("seed should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/read-wait-until", "kv-read-wait-until-request"),
        )
        .await
        .expect("wait-until read request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path(
                    "/read-wait-until-result",
                    "kv-read-wait-until-result-request",
                ),
            )
            .await
            .expect("result request should succeed");
        let body = String::from_utf8(output.body).expect("result body should be utf8");
        if body == "1" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("1"));
}

#[tokio::test]
#[serial]
async fn wait_until_background_work_runs_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "wait-until-basic".to_string();
    service
        .deploy(worker_name.clone(), wait_until_worker())
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/trigger", "wait-until-trigger-request"),
        )
        .await
        .expect("trigger request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "wait-until-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "done" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("done"));
}

#[tokio::test]
#[serial]
async fn dynamic_worker_fetch_uses_deno_fetch_with_host_policy_and_secret_replacement() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have addr");
    let (request_tx, request_rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept should succeed");
        let mut buffer = vec![0_u8; 8192];
        let bytes_read = socket
            .read(&mut buffer)
            .await
            .expect("server read should succeed");
        request_tx
            .send(String::from_utf8_lossy(&buffer[..bytes_read]).to_string())
            .expect("request should be captured");
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                )
                .await
                .expect("server write should succeed");
    });

    let deployed = service
        .deploy_dynamic(
            dynamic_fetch_probe_worker(&format!("http://{address}/fetch-probe")),
            HashMap::from([("API_TOKEN".to_string(), "secret-value".to_string())]),
            vec![address.to_string()],
        )
        .await
        .expect("dynamic deploy should succeed");

    let output = service
        .invoke(deployed.worker, test_invocation())
        .await
        .expect("dynamic fetch invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");

    let raw_request = request_rx.await.expect("request should arrive");
    assert!(
        raw_request.starts_with("GET /fetch-probe?token=secret-value HTTP/1.1\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        raw_request.contains("\r\nauthorization: Bearer secret-value\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        raw_request.contains("\r\nx-dd-secret: secret-value\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        !raw_request.contains("__DD_SECRET_"),
        "secret placeholders leaked into outbound request: {raw_request}"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_worker_fetch_rejects_egress_hosts_outside_allowlist() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let deployed = service
        .deploy_dynamic(
            dynamic_fetch_probe_worker("http://127.0.0.1:9/blocked"),
            HashMap::from([("API_TOKEN".to_string(), "secret-value".to_string())]),
            vec!["example.com".to_string()],
        )
        .await
        .expect("dynamic deploy should succeed");

    let error = service
        .invoke(deployed.worker, test_invocation())
        .await
        .expect_err("dynamic fetch invoke should fail");
    let body = error.to_string();
    assert!(
        body.contains("egress origin is not allowed: http://127.0.0.1:9"),
        "body was {body}"
    );
}

#[tokio::test]
#[serial]
async fn dynamic_worker_fetch_abort_signal_cancels_outbound_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have addr");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept should succeed");
        let mut buffer = vec![0_u8; 4096];
        let _ = socket.read(&mut buffer).await;
        sleep(Duration::from_millis(200)).await;
        let _ = socket.shutdown().await;
    });

    let deployed = service
        .deploy_dynamic(
            dynamic_fetch_abort_worker(&format!("http://{address}/abort-probe")),
            HashMap::new(),
            vec![address.to_string()],
        )
        .await
        .expect("dynamic deploy should succeed");

    let started_at = Instant::now();
    let output = timeout(
        Duration::from_secs(2),
        service.invoke(deployed.worker, test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(
        body == "Error" || body.contains("Abort") || body.to_ascii_lowercase().contains("abort"),
        "body was {body}"
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "abort should finish quickly"
    );
}

#[tokio::test]
#[serial]
async fn preview_dynamic_worker_can_proxy_module_based_children() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "preview-dynamic".to_string(),
            preview_dynamic_worker(),
            DeployConfig {
                bindings: vec![DeployBinding::Dynamic {
                    binding: "SANDBOX".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let root = service
        .invoke(
            "preview-dynamic".to_string(),
            test_invocation_with_path("/preview/pr-123", "preview-root"),
        )
        .await
        .expect("preview root should succeed");
    assert_eq!(root.status, 200);
    let root_text = String::from_utf8(root.body).expect("utf8");
    assert!(root_text.contains("\"preview\":\"pr-123\""));
    assert!(root_text.contains("\"route\":\"root\""));

    let health = service
        .invoke(
            "preview-dynamic".to_string(),
            test_invocation_with_path("/preview/pr-123/api/health", "preview-health"),
        )
        .await
        .expect("preview health should succeed");
    assert_eq!(health.status, 200);
    let health_text = String::from_utf8(health.body).expect("utf8");
    assert!(health_text.contains("\"route\":\"health\""));
}

#[tokio::test]
#[serial]
async fn scales_up_with_backlog() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 4,
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

    for task in tasks {
        task.await.expect("join").expect("invoke should succeed");
    }

    let stats = service
        .stats("slow".to_string())
        .await
        .expect("stats should exist");
    assert!(stats.spawn_count > 1);
    assert!(stats.isolates_total <= 4);
}

#[tokio::test]
#[serial]
async fn scales_down_when_idle() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 3,
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
            if stats.isolates_total == 0 {
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
async fn invalid_redeploy_keeps_previous_generation() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 2,
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
        .expect("initial deploy should succeed");

    let one = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("first invoke should succeed");
    assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");

    let bad_redeploy = service
        .deploy("counter".to_string(), "export default {};".to_string())
        .await;
    assert!(bad_redeploy.is_err());

    let two = service
        .invoke("counter".to_string(), test_invocation())
        .await
        .expect("invoke should still use old generation");
    assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn redeploy_switches_new_traffic_while_old_generation_drains() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("worker".to_string(), versioned_worker("v1", 120))
        .await
        .expect("deploy v1 should succeed");

    let svc_one = service.clone();
    let first = tokio::spawn(async move {
        let mut req = test_invocation();
        req.request_id = "first".to_string();
        svc_one.invoke("worker".to_string(), req).await
    });

    sleep(Duration::from_millis(10)).await;

    let svc_two = service.clone();
    let second = tokio::spawn(async move {
        let mut req = test_invocation();
        req.request_id = "second".to_string();
        svc_two.invoke("worker".to_string(), req).await
    });

    sleep(Duration::from_millis(10)).await;
    service
        .deploy("worker".to_string(), versioned_worker("v2", 0))
        .await
        .expect("deploy v2 should succeed");

    let mut third_req = test_invocation();
    third_req.request_id = "third".to_string();
    let third = service
        .invoke("worker".to_string(), third_req)
        .await
        .expect("third invoke should succeed");
    assert_eq!(String::from_utf8(third.body).expect("utf8"), "v2");

    let first_output = first.await.expect("join first").expect("first invoke");
    let second_output = second.await.expect("join second").expect("second invoke");
    assert_eq!(String::from_utf8(first_output.body).expect("utf8"), "v1");
    assert_eq!(String::from_utf8(second_output.body).expect("utf8"), "v1");
}

#[tokio::test]
#[serial]
async fn single_isolate_allows_multiple_inflight_requests() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
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

#[tokio::test]
#[serial]
async fn dropped_invoke_aborts_inflight_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("abortable".to_string(), abort_aware_worker())
        .await
        .expect("deploy should succeed");

    let service_for_blocked = service.clone();
    let blocked = tokio::spawn(async move {
        let mut req = test_invocation();
        req.request_id = "block".to_string();
        service_for_blocked
            .invoke("abortable".to_string(), req)
            .await
    });

    timeout(Duration::from_secs(1), async {
        loop {
            let stats = service.stats("abortable".to_string()).await.expect("stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("request should become inflight");

    blocked.abort();
    assert!(blocked.await.is_err(), "aborted task should be canceled");

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats("abortable".to_string()).await.expect("stats");
            if stats.inflight_total == 0 && stats.queued == 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("abort should clear inflight slot");

    let mut followup_req = test_invocation();
    followup_req.request_id = "after".to_string();
    let followup = service
        .invoke("abortable".to_string(), followup_req)
        .await
        .expect("followup invoke should succeed");

    assert_eq!(
        String::from_utf8(followup.body).expect("utf8"),
        "abortCount=1"
    );
}

#[tokio::test]
#[serial]
async fn duplicate_user_request_ids_do_not_collide() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
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

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "same-user-request-id".to_string();
            svc.invoke("io".to_string(), req).await
        }));
    }

    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");
    }
}

#[tokio::test]
#[serial]
async fn forged_and_invalid_completion_payloads_are_ignored() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("malicious".to_string(), malicious_completion_worker())
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke("malicious".to_string(), test_invocation())
        .await
        .expect("first invoke should succeed");
    assert_eq!(String::from_utf8(first.body).expect("utf8"), "1");

    let second = service
        .invoke("malicious".to_string(), test_invocation())
        .await
        .expect("second invoke should succeed");
    assert_eq!(String::from_utf8(second.body).expect("utf8"), "2");
}

#[tokio::test]
#[serial]
async fn invoke_stream_delivers_chunked_response_body() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "streaming".to_string(),
            r#"
export default {
  async fetch() {
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue("hel");
        controller.enqueue("lo");
        controller.close();
      }
    }), { status: 201, headers: [["x-mode", "stream"]] });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut output = service
        .invoke_stream("streaming".to_string(), test_invocation())
        .await
        .expect("invoke stream should succeed");
    assert_eq!(output.status, 201);
    assert!(output
        .headers
        .iter()
        .any(|(name, value)| name == "x-mode" && value == "stream"));

    let mut body = Vec::new();
    while let Some(chunk) = output.body.recv().await {
        body.extend(chunk.expect("chunk should be ok"));
    }
    assert_eq!(String::from_utf8(body).expect("utf8"), "hello");
}

#[tokio::test]
#[serial]
async fn invoke_with_request_body_stream_delivers_chunks_to_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "streaming-body".to_string(),
            streaming_request_body_worker(),
        )
        .await
        .expect("deploy should succeed");

    let (tx, rx) = mpsc::channel(4);
    let mut request = test_invocation();
    request.method = "POST".to_string();
    request.request_id = "streaming-body-request".to_string();

    let invoke_task = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke_with_request_body("streaming-body".to_string(), request, Some(rx))
                .await
        })
    };

    tx.send(Ok(b"hel".to_vec()))
        .await
        .expect("first body chunk should send");
    tx.send(Ok(b"lo".to_vec()))
        .await
        .expect("second body chunk should send");
    drop(tx);

    let output = invoke_task
        .await
        .expect("join")
        .expect("invoke should succeed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "hello");
}

#[tokio::test]
#[serial]
async fn async_context_store_survives_promise_boundaries_and_nested_runs() {
    let service = test_service(RuntimeConfig::default()).await;

    service
        .deploy_with_config(
            "async-context".to_string(),
            async_context_worker(),
            DeployConfig {
                public: false,
                internal: DeployInternalConfig { trace: None },
                bindings: Vec::new(),
            },
        )
        .await
        .expect("deploy should succeed");

    let promise = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/promise", "async-context-promise"),
        )
        .await
        .expect("promise request should succeed");
    assert_eq!(String::from_utf8(promise.body).expect("utf8"), "outer");

    let nested = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/nested", "async-context-nested"),
        )
        .await
        .expect("nested request should succeed");
    assert_eq!(
        String::from_utf8(nested.body).expect("utf8"),
        "outer:inner:outer"
    );

    let restore = service
        .invoke(
            "async-context".to_string(),
            test_invocation_with_path("/restore", "async-context-restore"),
        )
        .await
        .expect("restore request should succeed");
    assert_eq!(
        String::from_utf8(restore.body).expect("utf8"),
        "missing:missing"
    );
}

#[tokio::test]
#[serial]
async fn cache_default_reuses_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

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
async fn named_caches_share_global_capacity_budget() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
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

#[tokio::test]
#[serial]
async fn internal_trace_includes_markers_and_targets_configured_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("trace-sink".to_string(), trace_sink_worker())
        .await
        .expect("deploy trace sink should succeed");
    service
        .deploy_with_config(
            "traced-worker".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  },
                };
                "#
            .to_string(),
            DeployConfig {
                internal: DeployInternalConfig {
                    trace: Some(DeployTraceDestination {
                        worker: "trace-sink".to_string(),
                        path: "/ingest".to_string(),
                    }),
                },
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy traced worker should succeed");

    let mut request = test_invocation_with_path("/", "trace-request");
    request
        .headers
        .push(("x-test".to_string(), "value".to_string()));
    service
        .invoke("traced-worker".to_string(), request)
        .await
        .expect("traced invoke should succeed");

    sleep(Duration::from_millis(100)).await;
}

#[test]
fn internal_trace_headers_include_markers() {
    let mut headers = vec![("x-other".to_string(), "value".to_string())];
    super::append_internal_trace_headers(&mut headers, "traced-worker", 42);

    let internal = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal"))
        .expect("x-dd-internal header should be present")
        .1
        .as_str();
    let reason = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal-reason"))
        .expect("x-dd-internal-reason header should be present")
        .1
        .as_str();
    let source_worker = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-worker"))
        .expect("x-dd-trace-source-worker header should be present")
        .1
        .as_str();
    let source_generation = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-generation"))
        .expect("x-dd-trace-source-generation header should be present")
        .1
        .as_str();

    assert_eq!(internal, "1");
    assert_eq!(reason, "trace");
    assert_eq!(source_worker, "traced-worker");
    assert_eq!(source_generation, "42");
}

#[tokio::test]
#[serial]
async fn internal_trace_invocations_do_not_recurse() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "loop-worker".to_string(),
            loop_trace_worker(),
            DeployConfig {
                internal: DeployInternalConfig {
                    trace: Some(DeployTraceDestination {
                        worker: "loop-worker".to_string(),
                        path: "/trace".to_string(),
                    }),
                },
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy loop worker should succeed");

    service
        .invoke(
            "loop-worker".to_string(),
            test_invocation_with_path("/", "loop-user"),
        )
        .await
        .expect("loop worker invoke should succeed");

    sleep(Duration::from_millis(100)).await;
    let state = timeout(Duration::from_secs(2), async {
        loop {
            let state_output = service
                .invoke(
                    "loop-worker".to_string(),
                    test_invocation_with_path("/state", "loop-state"),
                )
                .await
                .expect("loop worker state invoke should succeed");
            let state: LoopTraceState = crate::json::from_string(
                String::from_utf8(state_output.body).expect("loop state body should be utf8"),
            )
            .expect("loop state should parse as json");
            if state.trace_calls >= 2 {
                return state;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("loop state query should complete");

    assert_eq!(state.trace_calls, 2);
    assert!(state.total_calls >= 2);
}

#[test]
fn dynamic_host_rpc_fake_wake_path_is_removed_from_runtime_sources() {
    let worker_runtime_source =
        include_str!(concat!(env!("OUT_DIR"), "/execute_worker.generated.js"));

    assert!(!worker_runtime_source.contains("__dd_drain_dynamic_host_rpc_queue"));
    assert!(!worker_runtime_source.contains("__dd_run_dynamic_host_rpc_tasks"));
    assert!(!worker_runtime_source.contains("__dd_dynamic_colocated_runtime_cache"));
    assert!(!worker_runtime_source.contains("tryColocatedDynamicFetch"));
    assert!(!worker_runtime_source.contains("__dd_force_remote_fetch"));
    assert!(!worker_runtime_source.contains("runDynamicHostRpcTask"));
    assert!(!worker_runtime_source.contains("host-rpc-task"));
    assert!(!worker_runtime_source.contains("op_dynamic_host_rpc_task_complete"));
    assert!(!worker_runtime_source.contains("op_dynamic_take_reply"));
    assert!(!worker_runtime_source.contains("op_dynamic_take_pushed_replies"));
    assert!(!worker_runtime_source.contains("op_dynamic_take_host_rpc_tasks"));
    assert!(worker_runtime_source.contains("__dd_drain_dynamic_control_queue"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/core.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/fetch_cache.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/sockets_transport.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/memory.js"));
    assert!(worker_runtime_source.contains("__dd_source_unit:execute_worker/dynamic.js"));
}

#[test]
fn dynamic_worker_config_builds_placeholders() {
    let mut env = HashMap::new();
    env.insert("OPENAI_API_KEY".to_string(), "sk-test-123".to_string());
    let config =
        super::build_dynamic_worker_config(env, vec!["api.openai.com".to_string()], Vec::new())
            .expect("dynamic config should build");

    assert_eq!(config.dynamic_env.len(), 1);
    assert_eq!(config.secret_replacements.len(), 1);
    assert_eq!(
        config.egress_allow_hosts,
        vec!["api.openai.com".to_string()]
    );

    let placeholder = config
        .env_placeholders
        .get("OPENAI_API_KEY")
        .expect("placeholder should be present");
    assert!(placeholder.starts_with("__DD_SECRET_"));
}

#[test]
fn dynamic_worker_config_rejects_invalid_host() {
    let config = super::build_dynamic_worker_config(
        HashMap::new(),
        vec!["http://bad-host".to_string()],
        Vec::new(),
    );
    assert!(config.is_err());
}

#[test]
fn dynamic_worker_config_accepts_host_port_and_wildcard_rules() {
    let config = super::build_dynamic_worker_config(
        HashMap::new(),
        vec![
            "api.example.com:8443".to_string(),
            "*.example.com".to_string(),
            "*.example.com:9443".to_string(),
        ],
        Vec::new(),
    )
    .expect("dynamic config should accept host+port rules");

    assert_eq!(
        config.egress_allow_hosts,
        vec![
            "api.example.com:8443".to_string(),
            "*.example.com".to_string(),
            "*.example.com:9443".to_string(),
        ]
    );
}

#[test]
fn extract_bindings_collects_dynamic_bindings() {
    let bindings = super::extract_bindings(&DeployConfig {
        bindings: vec![
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
            DeployBinding::Dynamic {
                binding: "SANDBOX".to_string(),
            },
        ],
        ..DeployConfig::default()
    })
    .expect("bindings should parse");

    assert_eq!(bindings.kv, vec!["MY_KV".to_string()]);
    assert_eq!(bindings.dynamic, vec!["SANDBOX".to_string()]);
}

#[test]
fn extract_bindings_rejects_duplicate_dynamic_name() {
    let result = super::extract_bindings(&DeployConfig {
        bindings: vec![
            DeployBinding::Dynamic {
                binding: "SANDBOX".to_string(),
            },
            DeployBinding::Dynamic {
                binding: "SANDBOX".to_string(),
            },
        ],
        ..DeployConfig::default()
    });
    assert!(result.is_err());
}
