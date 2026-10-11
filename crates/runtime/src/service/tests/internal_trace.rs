//! Internal trace: what a deployment's configured trace worker receives, and
//! the markers that keep trace requests from being traced themselves.
use super::*;

#[tokio::test]
#[serial]
async fn internal_trace_includes_markers_and_targets_configured_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("trace-sink".to_string(), trace_sink_worker())
        .await
        .expect("deploy trace sink should succeed");
    service
        .deploy_with_config(
            "traced-worker".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  },
                };
                "#
            .to_string(),
            DeployConfig {
                internal: DeployInternalConfig {
                    trace: Some(DeployTraceDestination {
                        worker: "trace-sink".to_string(),
                        path: "/ingest".to_string(),
                    }),
                },
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy traced worker should succeed");

    let mut request = test_invocation_with_path("/", "trace-request");
    request
        .headers
        .push(("x-test".to_string(), "value".to_string()));
    service
        .invoke("traced-worker".to_string(), request)
        .await
        .expect("traced invoke should succeed");

    sleep(Duration::from_millis(100)).await;
}

#[test]
fn internal_trace_headers_include_markers() {
    let mut headers = vec![("x-other".to_string(), "value".to_string())];
    crate::service::append_internal_trace_headers(&mut headers, "traced-worker", 42);

    let internal = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal"))
        .expect("x-dd-internal header should be present")
        .1
        .as_str();
    let reason = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-internal-reason"))
        .expect("x-dd-internal-reason header should be present")
        .1
        .as_str();
    let source_worker = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-worker"))
        .expect("x-dd-trace-source-worker header should be present")
        .1
        .as_str();
    let source_generation = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("x-dd-trace-source-generation"))
        .expect("x-dd-trace-source-generation header should be present")
        .1
        .as_str();

    assert_eq!(internal, "1");
    assert_eq!(reason, "trace");
    assert_eq!(source_worker, "traced-worker");
    assert_eq!(source_generation, "42");
}

#[tokio::test]
#[serial]
async fn internal_trace_invocations_do_not_recurse() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy_with_config(
            "loop-worker".to_string(),
            loop_trace_worker(),
            DeployConfig {
                internal: DeployInternalConfig {
                    trace: Some(DeployTraceDestination {
                        worker: "loop-worker".to_string(),
                        path: "/trace".to_string(),
                    }),
                },
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy loop worker should succeed");

    service
        .invoke(
            "loop-worker".to_string(),
            test_invocation_with_path("/", "loop-user"),
        )
        .await
        .expect("loop worker invoke should succeed");

    sleep(Duration::from_millis(100)).await;
    let state = timeout(Duration::from_secs(2), async {
        loop {
            let state_output = service
                .invoke(
                    "loop-worker".to_string(),
                    test_invocation_with_path("/state", "loop-state"),
                )
                .await
                .expect("loop worker state invoke should succeed");
            let state: LoopTraceState = crate::json::from_string(
                String::from_utf8(state_output.body).expect("loop state body should be utf8"),
            )
            .expect("loop state should parse as json");
            if state.trace_calls >= 2 {
                return state;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("loop state query should complete");

    assert_eq!(state.trace_calls, 2);
    assert!(state.total_calls >= 2);
}
