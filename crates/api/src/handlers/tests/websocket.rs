//! Upgrade routing for workers without a memory socket handler.
use super::fixtures::TestState;
use crate::handlers::{handle_private_request, handle_public_request};
use bytes::Bytes;
use common::DeployConfig;
use http::{Request, StatusCode};
use http_body_util::Empty;
use serial_test::serial;

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
                egress_allow_hosts: Vec::new(),
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
async fn public_websocket_route_rejects_non_memory_upgrade() {
    let state = TestState::new("example.com").await;
    state
        .app()
        .runtime
        .deploy_with_config(
            "echo".to_string(),
            "export default { async fetch() { return new Response('ok'); } }".to_string(),
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
