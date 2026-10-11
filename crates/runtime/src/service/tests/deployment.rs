//! Deploying workers: server modules, temporary workers, deployments that fail
//! to start, and redeploys that move traffic from one generation to the next.
use super::*;

#[tokio::test]
#[serial]
async fn deployed_worker_imports_server_module_assets() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "server-modules".to_string();
    let encode = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);

    service
        .deploy_with_bundle_config_lifecycle_and_server_modules(
            worker.clone(),
            r#"
import config from "./config.json" with { type: "json" };
import message from "./message.txt" with { type: "text" };
import payload from "./payload.bin" with { type: "bytes" };
import wasmModule from "./empty.wasm";

function byteValues(value) {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  if (value instanceof ArrayBuffer) {
    return Array.from(new Uint8Array(value));
  }
  return Array.from(new Uint8Array(value.buffer));
}

export default {
  async fetch() {
    const dynamic = await import("./dynamic.json", { with: { type: "json" } });
    return Response.json({
      config,
      message,
      payload: byteValues(payload),
      dynamic: dynamic.default,
      wasm: wasmModule instanceof WebAssembly.Module,
    });
  },
};
"#
            .to_string(),
            DeployConfig::default(),
            Vec::new(),
            vec![
                DeployServerModule {
                    path: "config.json".to_string(),
                    kind: DeployServerModuleKind::Json,
                    content_base64: encode(br#"{"answer":42}"#),
                },
                DeployServerModule {
                    path: "message.txt".to_string(),
                    kind: DeployServerModuleKind::Text,
                    content_base64: encode(b"hello"),
                },
                DeployServerModule {
                    path: "payload.bin".to_string(),
                    kind: DeployServerModuleKind::Data,
                    content_base64: encode(&[1, 2, 3, 4]),
                },
                DeployServerModule {
                    path: "dynamic.json".to_string(),
                    kind: DeployServerModuleKind::Json,
                    content_base64: encode(br#"{"loaded":true}"#),
                },
                DeployServerModule {
                    path: "empty.wasm".to_string(),
                    kind: DeployServerModuleKind::CompiledWasm,
                    content_base64: encode(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]),
                },
            ],
            None,
            false,
        )
        .await
        .expect("deploy should succeed");

    let output = timeout(
        Duration::from_secs(2),
        service.invoke(worker, test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("request should succeed");
    assert_eq!(output.status, 200);
    let body: Value = serde_json::from_slice(&output.body).expect("body should be json");
    assert_eq!(body["config"]["answer"], 42);
    assert_eq!(body["message"], "hello");
    assert_eq!(body["payload"], serde_json::json!([1, 2, 3, 4]));
    assert_eq!(body["dynamic"]["loaded"], true);
    assert_eq!(body["wasm"], true);
}

#[tokio::test]
#[serial]
async fn temporary_worker_redeploy_refreshes_and_normal_deploy_makes_permanent() {
    let service = test_service(RuntimeConfig {
        scale_tick: Duration::from_millis(20),
        temporary_worker_ttl: Duration::from_secs(60),
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("temp-one", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("temporary deploy should succeed");
    let first = service
        .stats("preview".to_string())
        .await
        .expect("temporary worker stats");
    assert!(first.temporary);
    let first_expires_at = first.expires_at_ms.expect("temporary expiration");

    sleep(Duration::from_millis(5)).await;
    service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("temp-two", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("temporary redeploy should refresh expiration");
    let refreshed = service
        .stats("preview".to_string())
        .await
        .expect("refreshed temporary worker stats");
    assert!(refreshed.expires_at_ms.expect("refreshed expiration") > first_expires_at);

    service
        .deploy_with_bundle_config(
            "preview".to_string(),
            versioned_worker("permanent", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("normal deploy should make worker permanent");
    let permanent = service
        .stats("preview".to_string())
        .await
        .expect("permanent worker stats");
    assert!(!permanent.temporary);
    assert_eq!(permanent.expires_at_ms, None);

    let error = service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("should-not-replace", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect_err("temporary deploy over permanent worker should fail");
    assert_eq!(error.kind(), ErrorKind::Conflict);

    let output = service
        .invoke(
            "preview".to_string(),
            test_invocation_with_path("/", "temporary-permanent-check"),
        )
        .await
        .expect("permanent worker should remain deployed");
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "permanent");
}

#[tokio::test]
#[serial]
async fn temporary_worker_expires_and_is_not_restored_from_store() {
    let store = TestStoreDir::new("dd-temp-workers");
    let config = RuntimeConfig {
        scale_tick: Duration::from_secs(1),
        temporary_worker_ttl: Duration::from_millis(250),
        ..RuntimeConfig::default()
    };

    let service = test_service_with_store(config.clone(), store.clone(), true).await;
    service
        .deploy_temporary_with_bundle_config(
            "preview".to_string(),
            versioned_worker("temp", 0),
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("temporary deploy should succeed");
    assert!(service.stats("preview".to_string()).await.is_some());
    service.shutdown().await.expect("service should shut down");
    drop(service);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let restored = test_service_with_store(config, store.clone(), true).await;
    assert!(restored.stats("preview".to_string()).await.is_none());
}

#[tokio::test]
#[serial]
async fn temporary_worker_expires_while_runtime_is_running() {
    let service = test_service(RuntimeConfig {
        scale_tick: Duration::from_millis(10),
        temporary_worker_ttl: Duration::from_millis(30),
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy_temporary_with_bundle_config(
            "preview-live".to_string(),
            versioned_worker("temp", 0),
            DeployConfig::default(),
            test_assets(),
            None,
        )
        .await
        .expect("temporary deploy should succeed");
    assert!(
        service
            .resolve_asset("preview-live", "GET", None, "/a.js", &[],)
            .expect("asset lookup before expiry should succeed")
            .is_some()
    );

    timeout(Duration::from_secs(2), async {
        loop {
            if service.stats("preview-live".to_string()).await.is_none() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("temporary worker should expire");
    assert!(
        service
            .resolve_asset("preview-live", "GET", None, "/a.js", &[],)
            .expect("asset lookup after expiry should succeed")
            .is_none()
    );
}

#[tokio::test]
#[serial]
async fn service_deploys_a_simple_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "simple-worker".to_string(),
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
async fn looping_deployment_times_out_while_previous_generation_serves_requests() {
    let service = test_service(RuntimeConfig {
        isolate_startup_timeout: Duration::from_secs(1),
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy("healthy".to_string(), versioned_worker("healthy", 0))
        .await
        .expect("initial deployment");
    service
        .invoke("healthy".to_string(), test_invocation())
        .await
        .expect("warm isolate");

    let deploying_service = service.clone();
    let deployment = tokio::spawn(async move {
        deploying_service
            .deploy(
                "healthy".to_string(),
                "for (;;) {} export default { fetch() { return new Response('unreachable'); } }"
                    .to_string(),
            )
            .await
    });
    sleep(Duration::from_millis(100)).await;
    let output = timeout(
        Duration::from_millis(500),
        service.invoke("healthy".to_string(), test_invocation()),
    )
    .await
    .expect("validation must not block the scheduler")
    .expect("healthy request");
    assert_eq!(output.body, b"healthy");
    let error = timeout(Duration::from_secs(3), deployment)
        .await
        .expect("bounded startup")
        .expect("deployment task")
        .expect_err("looping deployment must fail");
    assert!(error.to_string().contains("startup exceeded"), "{error}");
    let output = service
        .invoke("healthy".to_string(), test_invocation())
        .await
        .expect("old deployment");
    assert_eq!(output.body, b"healthy");
    service
        .shutdown()
        .await
        .expect("shutdown after terminated validation");
}

#[tokio::test]
#[serial]
async fn unresolved_deployment_startup_is_bounded() {
    let service = test_service(RuntimeConfig {
        isolate_startup_timeout: Duration::from_secs(1),
        ..RuntimeConfig::default()
    })
    .await;
    let result = timeout(Duration::from_secs(3), service.deploy("unresolved".to_string(),
        "await new Promise(() => {}); export default { fetch() { return new Response('never'); } }".to_string()
    )).await.expect("unresolved startup must finish");
    assert!(result.is_err());
    service
        .deploy("recovery".to_string(), counter_worker())
        .await
        .expect("validator slot released");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
#[serial]
async fn deployment_heap_exhaustion_preserves_the_server() {
    let service = test_service(RuntimeConfig {
        max_isolate_heap_bytes: 32 * 1024 * 1024,
        isolate_startup_timeout: Duration::from_secs(3),
        ..RuntimeConfig::default()
    })
    .await;
    let result = timeout(Duration::from_secs(5), service.deploy("exhausted".to_string(),
        "const retained = []; for (;;) retained.push(new Array(100000).fill(42)); export default {};".to_string()
    )).await.expect("heap exhaustion must terminate validation");
    assert!(result.is_err());
    service
        .deploy("healthy".to_string(), versioned_worker("survived", 0))
        .await
        .expect("subsequent deployment");
    let output = service
        .invoke("healthy".to_string(), test_invocation())
        .await
        .expect("subsequent request");
    assert_eq!(output.body, b"survived");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
#[serial]
async fn invalid_redeploy_keeps_previous_generation() {
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
        max_isolates_per_worker: 1,
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
