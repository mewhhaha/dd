//! Public host and path routing.
use super::fixtures::TestState;
use crate::handlers::{handle_public_request, invoke_worker_public};
use bytes::Bytes;
use common::DeployConfig;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use serial_test::serial;

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
                egress_allow_hosts: Vec::new(),
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
                egress_allow_hosts: Vec::new(),
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
                egress_allow_hosts: Vec::new(),
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
