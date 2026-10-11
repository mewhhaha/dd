//! Request-body admission for worker invocations.
use super::fixtures::TestState;
use crate::handlers::invoke_worker_private;
use bytes::Bytes;
use common::ErrorKind;
use http::Request;
use http_body_util::{Full, StreamBody};
use hyper::body::Frame;
use serial_test::serial;
use std::convert::Infallible;

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
