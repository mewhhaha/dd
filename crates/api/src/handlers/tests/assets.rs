//! Asset access, worker fallback and catalog replacement.
use super::fixtures::{TestState, test_assets};
use crate::handlers::{invoke_worker_private, invoke_worker_public};
use bytes::Bytes;
use common::{DeployAsset, DeployConfig, ErrorKind};
use http::Request;
use http_body_util::{BodyExt, Empty};
use serial_test::serial;

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
                egress_allow_hosts: Vec::new(),
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
