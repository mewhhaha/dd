use crate::handlers::{
    deploy_worker, handle_private_request, handle_public_request, invoke_worker_private,
    invoke_worker_public,
};
use crate::state::AppState;
use bytes::Bytes;
use common::{DeployAsset, DeployConfig, DeployRequest, ErrorKind};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use runtime::{RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use serial_test::serial;
use std::path::PathBuf;
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
            worker_store_enabled: false,
            ..RuntimeStorageConfig::default()
        };
        let runtime = RuntimeService::start_with_service_config(RuntimeServiceConfig {
            runtime: Default::default(),
            storage,
        })
        .await
        .expect("runtime");
        let state = AppState::new(
            runtime,
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

fn test_assets() -> Vec<DeployAsset> {
    vec![DeployAsset {
        path: "/a.js".to_string(),
        content_base64: "YXNzZXQtYm9keQ==".to_string(),
    }]
}

#[tokio::test]
#[serial]
async fn public_listener_blocks_deploy_path() {
    let state = TestState::new("example.com").await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/deploy")
        .header("host", "echo.example.com")
        .body(Empty::<Bytes>::new())
        .expect("request");
    let response = invoke_worker_public(state.app(), request, None)
        .await
        .expect_err("blocked");
    assert_eq!(response.0.kind(), ErrorKind::NotFound);
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
            bindings: vec![],
            ..Default::default()
        },
        assets: Vec::new(),
        asset_headers: None,
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
async fn public_host_invoke_rejects_private_worker() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "private-worker".to_string(),
            "export default { async fetch() { return new Response('private-ok'); } }".to_string(),
            DeployConfig {
                public: false,
                bindings: vec![],
                ..Default::default()
            },
        )
        .await
        .expect("deploy");

    let request = Request::builder()
        .method("GET")
        .uri("/")
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
