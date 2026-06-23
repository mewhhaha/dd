use crate::deploy_tokens::DeployTokenStore;
use crate::handlers::{
    deploy_worker, handle_private_request, handle_public_request, invoke_worker_private,
    invoke_worker_public,
};
use crate::state::AppState;
use bytes::Bytes;
use common::{
    DeployAsset, DeployBinding, DeployConfig, DeployRequest, DeployTokenCapabilities,
    DeployTokenDeleteResponse, DeployTokenGetResponse, DeployTokenListResponse,
    DeployTokenMintRequest, DeployTokenMintResponse, ErrorKind,
};
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
        let deploy_tokens = DeployTokenStore::load(store_dir.join("tokens.json"))
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

fn test_assets() -> Vec<DeployAsset> {
    vec![DeployAsset {
        path: "/a.js".to_string(),
        content_base64: "YXNzZXQtYm9keQ==".to_string(),
    }]
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
            bindings: vec![DeployBinding::Memory {
                binding: "ROOM".to_string(),
            }],
            ..DeployConfig::default()
        },
        assets: Vec::new(),
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
            bindings: vec![DeployBinding::Kv {
                binding: "ROOM".to_string(),
            }],
            ..DeployConfig::default()
        },
        assets: Vec::new(),
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
            ..DeployConfig::default()
        },
        assets: Vec::new(),
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
            bindings: vec![],
            ..Default::default()
        },
        assets: Vec::new(),
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
