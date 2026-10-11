//! Health, maintenance, metrics and private authentication.
use super::fixtures::TestState;
use crate::handlers::{handle_private_request, handle_public_request};
use bytes::Bytes;
use common::{
    DeployCacheConfig, DeployConfig, DeployRequest, DeployTokenCapabilities,
    DeployTokenDeleteResponse, DeployTokenGetResponse, DeployTokenListResponse,
    DeployTokenMintRequest,
};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn readiness_fails_during_maintenance_drain_while_liveness_stays_healthy() {
    let state = TestState::new("example.com").await;

    let ready = Request::builder()
        .method("GET")
        .uri("/readyz")
        .body(Empty::<Bytes>::new())
        .expect("ready request");
    assert_eq!(
        handle_private_request(state.app(), ready).await.status(),
        StatusCode::OK
    );

    let drain = Request::builder()
        .method("POST")
        .uri("/v1/admin/drain")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("drain request");
    assert_eq!(
        handle_private_request(state.app(), drain).await.status(),
        StatusCode::OK
    );

    let ready = Request::builder()
        .method("GET")
        .uri("/readyz")
        .body(Empty::<Bytes>::new())
        .expect("ready request");
    assert_eq!(
        handle_private_request(state.app(), ready).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let health = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(Empty::<Bytes>::new())
        .expect("health request");
    assert_eq!(
        handle_private_request(state.app(), health).await.status(),
        StatusCode::OK
    );

    let blocked = Request::builder()
        .method("GET")
        .uri("/")
        .header("host", "missing.example.com")
        .body(Empty::<Bytes>::new())
        .expect("blocked request");
    let blocked = handle_public_request(state.app(), blocked).await;
    assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        blocked.headers().get("retry-after").expect("retry-after"),
        "1"
    );
    let body = blocked
        .into_body()
        .collect()
        .await
        .expect("error body")
        .to_bytes();
    let error: common::ErrorBody = serde_json::from_slice(&body).expect("error json");
    assert_eq!(error.error, "service is draining");
    assert_eq!(error.code, "overloaded");
    assert!(error.retryable);

    let resume = Request::builder()
        .method("POST")
        .uri("/v1/admin/resume")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("resume request");
    assert_eq!(
        handle_private_request(state.app(), resume).await.status(),
        StatusCode::OK
    );
    assert!(state.app().operations.is_ready());
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_status_metrics_and_checkpoint_are_authenticated_and_operational() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy(
            "observed".to_string(),
            "export default { fetch() { return new Response('ok'); } }".to_string(),
        )
        .await
        .expect("deploy observed worker");

    let unauthorized = Request::builder()
        .method("GET")
        .uri("/v1/admin/status")
        .body(Empty::<Bytes>::new())
        .expect("status request");
    assert_eq!(
        handle_private_request(state.app(), unauthorized)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let status = Request::builder()
        .method("GET")
        .uri("/v1/admin/status")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("status request");
    let status = handle_private_request(state.app(), status).await;
    assert_eq!(status.status(), StatusCode::OK);
    let status_body = status
        .into_body()
        .collect()
        .await
        .expect("status body")
        .to_bytes();
    let status: serde_json::Value = serde_json::from_slice(&status_body).expect("status json");
    assert_eq!(status["runtime"]["active_deployments"], 1);
    assert_eq!(status["runtime"]["workers"][0]["name"], "observed");
    assert!(status["runtime"]["storage_retry_count"].is_number());
    assert!(status["trace_exporter"]["state"].is_string());

    let metrics = Request::builder()
        .method("GET")
        .uri("/metrics")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("metrics request");
    let metrics = handle_private_request(state.app(), metrics).await;
    assert_eq!(metrics.status(), StatusCode::OK);
    assert_eq!(
        metrics
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let metrics = String::from_utf8(
        metrics
            .into_body()
            .collect()
            .await
            .expect("metrics body")
            .to_bytes()
            .to_vec(),
    )
    .expect("metrics utf8");
    assert!(metrics.contains("dd_runtime_active_deployments 1"));
    assert!(metrics.contains("dd_runtime_worker_isolates{worker=\"observed\"}"));
    assert!(metrics.contains("dd_runtime_worker_isolate_spawns_total{worker=\"observed\"}"));
    assert!(metrics.contains("dd_runtime_worker_memory_max_shard_depth{worker=\"observed\"}"));
    assert!(metrics.contains("dd_storage_retries_total"));
    assert!(metrics.contains("dd_memory_snapshot_cache_hits_total"));
    assert!(metrics.contains("dd_runtime_internal_rescue_isolates"));

    let restore_failure: common::Result<()> =
        Err(common::PlatformError::runtime("restore fixture failed"));
    state
        .app()
        .runtime
        .control_store()
        .record_restore_result("observed", Some("fixture-deployment"), &restore_failure)
        .await
        .expect("record restore failure");
    let not_ready = Request::builder()
        .method("GET")
        .uri("/readyz")
        .body(Empty::<Bytes>::new())
        .expect("ready request");
    let not_ready = handle_private_request(state.app(), not_ready).await;
    assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let not_ready_body = not_ready
        .into_body()
        .collect()
        .await
        .expect("ready body")
        .to_bytes();
    let not_ready: serde_json::Value = serde_json::from_slice(&not_ready_body).expect("ready json");
    assert_eq!(not_ready["worker_restoration_ready"], false);
    assert_eq!(not_ready["restore_failure_count"], 1);
    state
        .app()
        .runtime
        .control_store()
        .record_restore_result("observed", Some("fixture-deployment"), &Ok(()))
        .await
        .expect("clear restore failure");

    let before_drain = Request::builder()
        .method("POST")
        .uri("/v1/admin/checkpoint")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("checkpoint request");
    assert_eq!(
        handle_private_request(state.app(), before_drain)
            .await
            .status(),
        StatusCode::CONFLICT
    );

    let drain = Request::builder()
        .method("POST")
        .uri("/v1/admin/drain")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("drain request");
    assert_eq!(
        handle_private_request(state.app(), drain).await.status(),
        StatusCode::OK
    );
    let checkpoint = Request::builder()
        .method("POST")
        .uri("/v1/admin/checkpoint")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("checkpoint request");
    let checkpoint = handle_private_request(state.app(), checkpoint).await;
    assert_eq!(checkpoint.status(), StatusCode::OK);
    let checkpoint_body = checkpoint
        .into_body()
        .collect()
        .await
        .expect("checkpoint body")
        .to_bytes();
    let checkpoint: serde_json::Value =
        serde_json::from_slice(&checkpoint_body).expect("checkpoint json");
    assert_eq!(
        checkpoint["checkpoint"]["state_shards"],
        storage::state::STATE_SHARDS
    );
    assert_eq!(checkpoint["checkpoint"]["cache"], true);

    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn drain_waits_for_deployment_after_http_client_disconnects() {
    let state = TestState::new("example.com").await;
    let payload = DeployRequest {
        name: "drain-deploy".into(),
        source: "await new Promise(resolve => setTimeout(resolve, 400)); export default { fetch() { return new Response('ok'); } };".into(),
        config: DeployConfig { cache: DeployCacheConfig { enabled: true }, ..DeployConfig::default() },
        assets: Vec::new(), server_modules: Vec::new(), asset_headers: None, temporary: false,
    };
    let deploying = tokio::spawn({
        let app = state.app();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/deploy")
            .header("authorization", "Bearer test-private-token")
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&payload).unwrap(),
            )))
            .unwrap();
        async move { handle_private_request(app, request).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while state.state.runtime.active_deployment_operations() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("deployment is admitted");
    deploying.abort();
    assert!(deploying.await.unwrap_err().is_cancelled());
    assert_eq!(state.state.operations.active_requests(), 0);
    assert_eq!(state.state.operations.active_control_operations(), 1);
    assert!(!state.state.runtime.is_quiescent().await);
    state.state.operations.begin_drain();
    let admin_request = |path| {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer test-private-token")
            .body(Empty::<Bytes>::new())
            .unwrap()
    };
    let checkpoint =
        handle_private_request(state.app(), admin_request("/v1/admin/checkpoint")).await;
    assert_eq!(checkpoint.status(), StatusCode::CONFLICT);
    let drain = handle_private_request(state.app(), admin_request("/v1/admin/drain")).await;
    assert_eq!(drain.status(), StatusCode::OK);
    let body = drain.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["drained"], true);
    assert_eq!(body["active_control_operations"], 0);
    assert!(
        state
            .state
            .runtime
            .front_cache_namespace("drain-deploy")
            .is_some()
    );
    assert!(
        state
            .state
            .runtime
            .control_store()
            .active_deployments()
            .await
            .unwrap()
            .iter()
            .any(|deployment| deployment.worker == "drain-deploy")
    );
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_admin_lists_reads_and_deletes_tokens() {
    let state = TestState::new("example.com").await;
    let minted = state
        .app()
        .deploy_tokens
        .mint(DeployTokenMintRequest {
            name: Some("github".to_string()),
            capabilities: DeployTokenCapabilities {
                workers: vec!["echo".to_string()],
                allow_public: true,
                allow_any_bindings: true,
                ..DeployTokenCapabilities::default()
            },
            ..DeployTokenMintRequest::default()
        })
        .await
        .expect("mint");
    assert_eq!(minted.id, "github");
    assert_eq!(minted.name.as_deref(), Some("github"));

    let request = Request::builder()
        .method("GET")
        .uri("/v1/admin/tokens")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = handle_private_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let list: DeployTokenListResponse = serde_json::from_slice(&body).expect("list");
    assert_eq!(list.tokens.len(), 1);
    assert_eq!(list.tokens[0].id, minted.id);
    assert_eq!(list.tokens[0].uses, 0);

    let request = Request::builder()
        .method("GET")
        .uri(format!("/v1/admin/tokens/{}", minted.id))
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = handle_private_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let get: DeployTokenGetResponse = serde_json::from_slice(&body).expect("get");
    assert_eq!(get.token.name.as_deref(), Some("github"));

    let request = Request::builder()
        .method("DELETE")
        .uri(format!("/v1/admin/tokens/{}", minted.id))
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = handle_private_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let deleted: DeployTokenDeleteResponse = serde_json::from_slice(&body).expect("delete");
    assert_eq!(deleted.id, minted.id);

    let deploy = DeployRequest {
        name: "echo".to_string(),
        source: "export default { async fetch() { return new Response('bad'); } }".to_string(),
        config: DeployConfig {
            public: true,
            cache: Default::default(),
            ..DeployConfig::default()
        },
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let request = Request::builder()
        .method("POST")
        .uri("/v1/deploy")
        .header("host", "example.com")
        .header("authorization", format!("Bearer {}", minted.token))
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(
            serde_json::to_vec(&deploy).expect("deploy json"),
        )))
        .expect("request");
    let response = handle_public_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_routes_reject_missing_bearer_token() {
    let state = TestState::new("example.com").await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/deploy")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = handle_private_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get("www-authenticate")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer")
    );
    state.shutdown().await;
}
