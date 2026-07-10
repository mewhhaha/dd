use crate::deploy_tokens::DeployTokenStore;
use crate::handlers::{
    deploy_worker, handle_private_request, handle_public_request, invoke_worker_private,
    invoke_worker_public,
};
use crate::state::AppState;
use bytes::Bytes;
use common::{
    DeployAsset, DeployBinding, DeployCacheConfig, DeployConfig, DeployRequest,
    DeployTokenCapabilities, DeployTokenDeleteResponse, DeployTokenGetResponse,
    DeployTokenListResponse, DeployTokenMintRequest, DeployTokenMintResponse,
    DeploymentInspectResponse, DeploymentListResponse, ErrorKind, RollbackRequest,
    RollbackResponse, UndeployResponse, WorkerInvocation, WorkerNameRequest,
};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::Frame;
#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::trace::TracerProvider as _;
#[cfg(feature = "otel")]
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::SdkTracerProvider;
use runtime::{RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use serial_test::serial;
use std::convert::Infallible;
use std::path::PathBuf;
#[cfg(feature = "otel")]
use tracing_subscriber::prelude::*;
use uuid::Uuid;

struct TestState {
    state: AppState,
    store_dir: PathBuf,
}

impl TestState {
    async fn new(public_base_domain: &str) -> Self {
        let store_dir = PathBuf::from(format!("./target/test-store-api-{}", Uuid::new_v4()));
        let storage = RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            database_url: format!("file:{}/dd-test.db", store_dir.display()),
            worker_store_enabled: true,
            ..RuntimeStorageConfig::default()
        };
        let runtime = RuntimeService::start_with_service_config(RuntimeServiceConfig {
            runtime: Default::default(),
            storage,
        })
        .await
        .expect("runtime");
        let legacy_token_path = store_dir.join("tokens.json");
        let deploy_tokens =
            DeployTokenStore::from_control_store(runtime.control_store(), Some(&legacy_token_path))
                .await
                .expect("token store");
        let state = AppState::new(
            runtime,
            deploy_tokens,
            1024 * 1024,
            public_base_domain.to_string(),
            Some("test-private-token".to_string()),
            None,
            None,
        );
        Self { state, store_dir }
    }

    fn app(&self) -> AppState {
        self.state.clone()
    }

    async fn shutdown(self) {
        self.state
            .runtime
            .shutdown()
            .await
            .expect("runtime shutdown");
        let _ = tokio::fs::remove_dir_all(self.store_dir).await;
    }
}

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
    assert!(metrics.contains("dd_storage_retries_total"));

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
    assert_eq!(checkpoint["checkpoint"]["kv"], true);
    assert_eq!(checkpoint["checkpoint"]["cache"], true);

    state.shutdown().await;
}

fn test_assets() -> Vec<DeployAsset> {
    vec![DeployAsset {
        path: "/a.js".to_string(),
        content_base64: "YXNzZXQtYm9keQ==".to_string(),
    }]
}

#[cfg(feature = "otel")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn info_tracing_is_concurrency_safe_and_preserves_traceparent() {
    const TRACE_ID: &str = "0123456789abcdef0123456789abcdef";
    const TRACEPARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

    global::set_text_map_propagator(TraceContextPropagator::new());
    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("dd-api-concurrency-test");
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .expect("test tracing subscriber should initialize once");
    global::set_tracer_provider(provider.clone());

    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "traced".to_string(),
            r#"
export default {
  async fetch() {
    await Promise.resolve();
    return new Response("traced");
  },
};
"#
            .to_string(),
            DeployConfig {
                public: true,
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy");

    let mut requests = tokio::task::JoinSet::new();
    for request_index in 0..32 {
        let app = state.app();
        requests.spawn(async move {
            let request = Request::builder()
                .method("GET")
                .uri(format!("/request-{request_index}"))
                .header("host", "traced.example.com")
                .header("traceparent", TRACEPARENT)
                .body(Empty::<Bytes>::new())
                .expect("request");
            let response = invoke_worker_public(app, request, None)
                .await
                .expect("invoke");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get("x-dd-trace-id")
                    .expect("trace id response header"),
                TRACE_ID
            );
            assert_eq!(
                response
                    .into_body()
                    .collect()
                    .await
                    .expect("body")
                    .to_bytes(),
                Bytes::from_static(b"traced")
            );
        });
    }
    while let Some(result) = requests.join_next().await {
        result.expect("concurrent traced request should not panic");
    }

    state.shutdown().await;
    provider.shutdown().expect("shutdown tracing provider");
}

#[tokio::test]
#[serial]
async fn opt_in_front_cache_bypasses_worker_after_first_response() {
    let state = TestState::new("example.com").await;
    deploy_worker(
        state.app(),
        DeployRequest {
            name: "cached".to_string(),
            source: r#"
let count = 0;
export default {
  async fetch() {
    count += 1;
    return new Response(String(count), {
      headers: { "cache-control": "public, max-age=60, stale-while-revalidate=30" },
    });
  },
};
"#
            .to_string(),
            config: DeployConfig {
                public: true,
                cache: DeployCacheConfig { enabled: true },
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        },
    )
    .await
    .expect("deploy");

    let invoke = || {
        Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "cached.example.com")
            .body(Empty::<Bytes>::new())
            .expect("request")
    };
    let first = invoke_worker_public(state.app(), invoke(), None)
        .await
        .expect("first invoke");
    assert_eq!(first.headers().get("x-dd-cache").expect("cache"), "MISS");
    assert_eq!(
        first.into_body().collect().await.expect("body").to_bytes(),
        Bytes::from_static(b"1")
    );

    let second = invoke_worker_public(state.app(), invoke(), None)
        .await
        .expect("second invoke");
    assert_eq!(second.headers().get("x-dd-cache").expect("cache"), "HIT");
    assert_eq!(
        second.into_body().collect().await.expect("body").to_bytes(),
        Bytes::from_static(b"1")
    );
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_listener_requires_deploy_token() {
    let state = TestState::new("example.com").await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/deploy")
        .header("host", "echo.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = handle_public_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_admin_mints_scoped_deploy_token_for_public_deploy() {
    let state = TestState::new("example.com").await;
    let mint = DeployTokenMintRequest {
        name: Some("ci".to_string()),
        max_uses: Some(1),
        capabilities: DeployTokenCapabilities {
            workers: vec!["echo".to_string()],
            allow_public: true,
            bindings: vec![DeployBinding::Memory {
                binding: "ROOM".to_string(),
            }],
            max_source_bytes: Some(1024),
            max_assets: Some(0),
            ..DeployTokenCapabilities::default()
        },
        ..DeployTokenMintRequest::default()
    };
    let request = Request::builder()
        .method("POST")
        .uri("/v1/admin/tokens")
        .header("authorization", "Bearer test-private-token")
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(Bytes::from(
            serde_json::to_vec(&mint).expect("mint json"),
        )))
        .expect("request");
    let response = handle_private_request(state.app(), request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let minted: DeployTokenMintResponse = serde_json::from_slice(&body).expect("minted token");
    assert!(minted.ok);
    assert_eq!(minted.id, "ci");
    assert_eq!(minted.name.as_deref(), Some("ci"));

    let deploy = DeployRequest {
        name: "echo".to_string(),
        source: "export default { async fetch() { return new Response('public-ok'); } }"
            .to_string(),
        config: DeployConfig {
            public: true,
            cache: Default::default(),
            bindings: vec![DeployBinding::Memory {
                binding: "ROOM".to_string(),
            }],
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
    assert_eq!(response.status(), StatusCode::OK);

    let request = Request::builder()
        .method("GET")
        .uri("/")
        .header("host", "echo.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("invoke");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"public-ok");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_deploy_token_rejects_unscoped_binding() {
    let state = TestState::new("example.com").await;
    let minted = state
        .app()
        .deploy_tokens
        .mint(DeployTokenMintRequest {
            capabilities: DeployTokenCapabilities {
                workers: vec!["echo".to_string()],
                allow_public: true,
                bindings: vec![DeployBinding::Memory {
                    binding: "ROOM".to_string(),
                }],
                ..DeployTokenCapabilities::default()
            },
            ..DeployTokenMintRequest::default()
        })
        .await
        .expect("mint");
    let deploy = DeployRequest {
        name: "echo".to_string(),
        source: "export default { async fetch() { return new Response('bad'); } }".to_string(),
        config: DeployConfig {
            public: true,
            cache: Default::default(),
            bindings: vec![DeployBinding::Kv {
                binding: "ROOM".to_string(),
            }],
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
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_deployment_history_rolls_back_and_undeploys() {
    let state = TestState::new("example.com").await;
    let deploy = |source: &str| DeployRequest {
        name: "history-name".to_string(),
        source: format!(
            "export default {{ async fetch() {{ return new Response({source:?}); }} }}"
        ),
        config: DeployConfig::default(),
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let first = deploy_worker(state.app(), deploy("first"))
        .await
        .expect("first deploy");
    let second = deploy_worker(state.app(), deploy("second"))
        .await
        .expect("second deploy");

    let list = Request::builder()
        .method("GET")
        .uri("/v1/admin/deployments?worker=history%2Dname")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("list request");
    let list = handle_private_request(state.app(), list).await;
    assert_eq!(list.status(), StatusCode::OK);
    let list: DeploymentListResponse = serde_json::from_slice(
        &list
            .into_body()
            .collect()
            .await
            .expect("list body")
            .to_bytes(),
    )
    .expect("deployment list");
    assert_eq!(list.deployments.len(), 2);
    assert!(
        list.deployments.iter().any(
            |deployment| deployment.deployment_id == second.deployment_id && deployment.active
        )
    );

    let inspect = Request::builder()
        .method("GET")
        .uri(format!("/v1/admin/deployment?id={}", first.deployment_id))
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("inspect request");
    let inspect = handle_private_request(state.app(), inspect).await;
    assert_eq!(inspect.status(), StatusCode::OK);
    let inspect: DeploymentInspectResponse = serde_json::from_slice(
        &inspect
            .into_body()
            .collect()
            .await
            .expect("inspect body")
            .to_bytes(),
    )
    .expect("deployment inspect");
    assert_eq!(
        inspect.deployment.summary.deployment_id,
        first.deployment_id
    );
    assert!(inspect.deployment.source.contains("first"));

    let rollback = Request::builder()
        .method("POST")
        .uri("/v1/admin/rollback")
        .header("authorization", "Bearer test-private-token")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&RollbackRequest {
                worker: "history-name".to_string(),
                deployment_id: first.deployment_id.clone(),
            })
            .expect("rollback json"),
        )))
        .expect("rollback request");
    let rollback = handle_private_request(state.app(), rollback).await;
    assert_eq!(rollback.status(), StatusCode::OK);
    let rollback: RollbackResponse = serde_json::from_slice(
        &rollback
            .into_body()
            .collect()
            .await
            .expect("rollback body")
            .to_bytes(),
    )
    .expect("rollback response");
    assert_eq!(rollback.deployment_id, first.deployment_id);

    let output = state
        .app()
        .runtime
        .invoke(
            "history-name".to_string(),
            WorkerInvocation {
                method: "GET".to_string(),
                url: "http://history-name/".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                request_id: "history-after-rollback".to_string(),
            },
        )
        .await
        .expect("invoke rollback");
    assert_eq!(output.body, b"first");

    let undeploy = Request::builder()
        .method("POST")
        .uri("/v1/admin/undeploy")
        .header("authorization", "Bearer test-private-token")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&WorkerNameRequest {
                worker: "history-name".to_string(),
            })
            .expect("undeploy json"),
        )))
        .expect("undeploy request");
    let undeploy = handle_private_request(state.app(), undeploy).await;
    assert_eq!(undeploy.status(), StatusCode::OK);
    let undeploy: UndeployResponse = serde_json::from_slice(
        &undeploy
            .into_body()
            .collect()
            .await
            .expect("undeploy body")
            .to_bytes(),
    )
    .expect("undeploy response");
    assert_eq!(undeploy.worker, "history-name");
    assert!(
        state
            .app()
            .runtime
            .stats("history-name".to_string())
            .await
            .is_none()
    );
    assert!(
        state
            .app()
            .runtime
            .deployments(Some("history-name"))
            .await
            .expect("history remains inspectable")
            .iter()
            .all(|deployment| !deployment.active)
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
async fn private_deploy_and_invoke_succeeds() {
    let state = TestState::new("example.com").await;
    let deploy = DeployRequest {
        name: "echo".to_string(),
        source: "export default { async fetch() { return new Response('ok'); } }".to_string(),
        config: DeployConfig {
            public: false,
            cache: Default::default(),
            bindings: vec![],
            ..Default::default()
        },
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let response = deploy_worker(state.app(), deploy).await.expect("deploy");
    assert!(response.ok);

    let request = Request::builder()
        .method("GET")
        .uri("/v1/invoke/echo")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_private(state.app(), request, None)
        .await
        .expect("invoke");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"ok");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_get_with_declared_body_is_rejected() {
    let state = TestState::new("example.com").await;
    let request = Request::builder()
        .method("GET")
        .uri("/v1/invoke/echo")
        .header("authorization", "Bearer test-private-token")
        .header("content-length", "4")
        .body(Full::new(Bytes::from_static(b"body")))
        .expect("request");
    let error = invoke_worker_private(state.app(), request, None)
        .await
        .expect_err("GET body should fail");

    assert_eq!(error.0.kind(), ErrorKind::BadRequest);
    assert_eq!(error.0.to_string(), "GET request bodies are not supported");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_get_with_undeclared_streamed_body_is_rejected() {
    let state = TestState::new("example.com").await;
    let stream = futures_util::stream::iter([Ok::<_, Infallible>(Frame::data(
        Bytes::from_static(b"body"),
    ))]);
    let request = Request::builder()
        .method("GET")
        .uri("/v1/invoke/echo")
        .header("authorization", "Bearer test-private-token")
        .body(StreamBody::new(stream))
        .expect("request");
    let error = invoke_worker_private(state.app(), request, None)
        .await
        .expect_err("GET body should fail");

    assert_eq!(error.0.kind(), ErrorKind::BadRequest);
    assert_eq!(error.0.to_string(), "GET request bodies are not supported");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_host_invoke_routes_by_subdomain() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "echo".to_string(),
            "export default { async fetch() { return new Response('host-ok'); } }".to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                bindings: vec![],
                ..Default::default()
            },
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/")
        .header("host", "echo.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("invoke");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"host-ok");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_router_allows_worker_paths_near_reserved_prefixes() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "echo".to_string(),
            "export default { async fetch(request) { return new Response(new URL(request.url).pathname); } }"
                .to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                bindings: vec![],
                ..Default::default()
            },
        )
        .await
        .expect("deploy");

    for path in [
        "/v1/deployment-status",
        "/v1/administer",
        "/v1/dynamic-page",
        "/v1/invoker",
    ] {
        let request = Request::builder()
            .method("GET")
            .uri(path)
            .header("host", "echo.example.com")
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = handle_public_request(state.app(), request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body.as_ref(), path.as_bytes());
    }
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_host_invoke_ignores_spoofed_forwarded_request_url() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "echo".to_string(),
            "export default { async fetch(request) { return new Response(request.url); } }"
                .to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                bindings: vec![],
                ..Default::default()
            },
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/rooms/test?x=1")
        .header("host", "echo.example.com")
        .header("x-forwarded-host", "echo.wdyt.chat")
        .header("x-forwarded-proto", "https")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("invoke");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"https://echo.example.com/rooms/test?x=1");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_host_invoke_rejects_private_worker_assets() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_bundle_config(
            "private-worker".to_string(),
            "export default { async fetch() { return new Response('private-ok'); } }".to_string(),
            DeployConfig {
                public: false,
                cache: Default::default(),
                bindings: vec![],
                ..Default::default()
            },
            test_assets(),
            None,
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/a.js")
        .header("host", "private-worker.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let error = invoke_worker_public(state.app(), request, None)
        .await
        .expect_err("private worker should not be public");
    assert_eq!(error.0.kind(), ErrorKind::NotFound);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_invoke_serves_asset_before_worker_code() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_bundle_config(
            "assets".to_string(),
            "export default { async fetch() { return new Response('worker-fallback'); } }"
                .to_string(),
            DeployConfig::default(),
            test_assets(),
            Some("/a.js\n  Cache-Control: public, max-age=60\n".to_string()),
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/v1/invoke/assets/a.js")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_private(state.app(), request, None)
        .await
        .expect("invoke");
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body.as_ref(), b"asset-body");
    assert_eq!(
        headers
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("public, max-age=60")
    );

    let fallback_request = Request::builder()
        .method("GET")
        .uri("/v1/invoke/assets/missing")
        .header("authorization", "Bearer test-private-token")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let fallback = invoke_worker_private(state.app(), fallback_request, None)
        .await
        .expect("invoke fallback");
    let fallback_body = fallback
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(fallback_body.as_ref(), b"worker-fallback");
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_host_invoke_serves_assets_for_public_workers() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_bundle_config(
            "assets".to_string(),
            "export default { async fetch() { return new Response('worker-fallback'); } }"
                .to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                ..DeployConfig::default()
            },
            test_assets(),
            None,
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("HEAD")
        .uri("/a.js")
        .header("host", "assets.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("invoke");
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert!(body.is_empty());
    assert_eq!(
        headers
            .get("content-length")
            .and_then(|value| value.to_str().ok()),
        Some("10")
    );
    let stats = state
        .app()
        .runtime
        .stats("assets".to_string())
        .await
        .expect("stats");
    assert_eq!(stats.spawn_count, 0);
    assert_eq!(stats.isolates_total, 0);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_host_asset_miss_falls_back_to_worker() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_bundle_config(
            "assets".to_string(),
            "export default { async fetch(request) { return new Response(new URL(request.url).pathname); } }"
                .to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                ..DeployConfig::default()
            },
            test_assets(),
            None,
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/missing")
        .header("host", "assets.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("invoke");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();

    assert_eq!(body.as_ref(), b"/missing");
    let stats = state
        .app()
        .runtime
        .stats("assets".to_string())
        .await
        .expect("stats");
    assert_eq!(stats.spawn_count, 1);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn public_host_asset_catalog_swaps_on_redeploy_without_isolate_work() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_bundle_config(
            "assets".to_string(),
            "export default { async fetch() { return new Response('old-worker'); } }".to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                ..DeployConfig::default()
            },
            vec![DeployAsset {
                path: "/a.js".to_string(),
                content_base64: "b2xkLWFzc2V0".to_string(),
            }],
            None,
        )
        .await
        .expect("first deploy");
    let first_resolution = state
        .app()
        .runtime
        .resolve_public_route_asset("assets", "GET", Some("assets.example.com"), "/a.js", &[])
        .expect("first catalog resolution should succeed");
    assert_eq!(first_resolution.generation, Some(1));

    let request = Request::builder()
        .method("GET")
        .uri("/a.js")
        .header("host", "assets.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("first invoke");
    let first_body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(first_body.as_ref(), b"old-asset");

    state
        .app()
        .runtime
        .deploy_with_bundle_config(
            "assets".to_string(),
            "export default { async fetch() { return new Response('new-worker'); } }".to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                ..DeployConfig::default()
            },
            vec![DeployAsset {
                path: "/a.js".to_string(),
                content_base64: "bmV3LWFzc2V0".to_string(),
            }],
            None,
        )
        .await
        .expect("second deploy");
    let second_resolution = state
        .app()
        .runtime
        .resolve_public_route_asset("assets", "GET", Some("assets.example.com"), "/a.js", &[])
        .expect("second catalog resolution should succeed");
    assert_eq!(second_resolution.generation, Some(2));

    let request = Request::builder()
        .method("GET")
        .uri("/a.js")
        .header("host", "assets.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect("second invoke");
    let second_body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(second_body.as_ref(), b"new-asset");

    let stats = state
        .app()
        .runtime
        .stats("assets".to_string())
        .await
        .expect("stats");
    assert_eq!(stats.spawn_count, 0);
    assert_eq!(stats.isolates_total, 0);
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn private_websocket_route_rejects_non_memory_upgrade() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "echo".to_string(),
            "export default { async fetch() { return new Response('ok'); } }".to_string(),
            DeployConfig {
                public: false,
                cache: Default::default(),
                bindings: vec![],
                ..Default::default()
            },
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/v1/invoke/echo")
        .header("authorization", "Bearer test-private-token")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(Empty::<Bytes>::new())
        .expect("request");

    let response = handle_private_request(state.app(), request).await.status();
    assert_eq!(response, StatusCode::BAD_REQUEST);
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

#[tokio::test]
#[serial]
async fn public_websocket_route_rejects_non_memory_upgrade() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "echo".to_string(),
            "export default { async fetch() { return new Response('ok'); } }".to_string(),
            DeployConfig {
                public: true,
                cache: Default::default(),
                bindings: vec![],
                ..Default::default()
            },
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/")
        .header("host", "echo.example.com")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(Empty::<Bytes>::new())
        .expect("request");

    let response = handle_public_request(state.app(), request).await.status();
    assert_eq!(response, StatusCode::BAD_REQUEST);
    state.shutdown().await;
}
