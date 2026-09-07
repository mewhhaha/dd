use super::*;

#[tokio::test]
#[serial]
async fn async_reply_immediate_completion_resumes_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("async-reply-probe".to_string(), async_reply_probe_worker())
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "async-reply-probe",
        test_invocation_with_path("/async/immediate", "reply-immediate"),
        "async reply immediate",
    )
    .await;
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "immediate");
}

#[tokio::test]
#[serial]
async fn async_reply_delayed_completion_resumes_after_yield() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("async-reply-probe".to_string(), async_reply_probe_worker())
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "async-reply-probe",
        test_invocation_with_path("/async/delayed", "reply-delayed"),
        "async reply delayed",
    )
    .await;
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "delayed");
}

#[tokio::test]
#[serial]
async fn async_reply_timeout_surfaces_explicit_error() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("async-reply-probe".to_string(), async_reply_probe_worker())
        .await
        .expect("deploy should succeed");

    let output = invoke_with_timeout_and_dump(
        &service,
        "async-reply-probe",
        test_invocation_with_path("/async/timeout", "reply-timeout"),
        "async reply timeout",
    )
    .await;
    assert_eq!(output.status, 504);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        "test async reply timed out after 25ms"
    );
}
