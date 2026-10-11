//! Concurrent tracing and traceparent propagation.
use super::fixtures::TestState;
use crate::handlers::invoke_worker_public;
use bytes::Bytes;
use common::DeployConfig;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Empty};
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serial_test::serial;
use tracing_subscriber::prelude::*;

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
