use super::*;

#[tokio::test]
#[serial]
async fn async_reply_immediate_completion_resumes_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
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
        max_isolates_per_worker: 1,
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
        max_isolates_per_worker: 1,
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

#[tokio::test]
#[serial]
async fn overlapping_requests_keep_independent_request_context_handles() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 2,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "overlap-context".to_string(),
            r#"
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const label = url.searchParams.get("label");
    const before = {
      id: __dd_internals.dd.runtimeRequestId?.(),
      handle: __dd_internals.dd.runtimeRequestContextHandle?.(),
    };
    await ctx.sleep(Number(url.searchParams.get("delay") ?? "0"));
    const after = {
      id: __dd_internals.dd.runtimeRequestId?.(),
      handle: __dd_internals.dd.runtimeRequestContextHandle?.(),
    };
    return Response.json({ label, before, after });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let slow = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "overlap-context".to_string(),
                    test_invocation_with_path("/?label=slow&delay=50", "overlap-slow"),
                )
                .await
        })
    };
    let fast = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke(
                    "overlap-context".to_string(),
                    test_invocation_with_path("/?label=fast&delay=1", "overlap-fast"),
                )
                .await
        })
    };

    let slow = slow
        .await
        .expect("slow task should join")
        .expect("slow request should succeed");
    let fast = fast
        .await
        .expect("fast task should join")
        .expect("fast request should succeed");
    let slow: Value = serde_json::from_slice(&slow.body).expect("slow response should be json");
    let fast: Value = serde_json::from_slice(&fast.body).expect("fast response should be json");

    assert_eq!(slow["label"], "slow");
    assert_eq!(fast["label"], "fast");
    assert_eq!(slow["before"]["id"], slow["after"]["id"]);
    assert_eq!(fast["before"]["id"], fast["after"]["id"]);
    assert_eq!(slow["before"]["handle"], slow["after"]["handle"]);
    assert_eq!(fast["before"]["handle"], fast["after"]["handle"]);
    assert_ne!(slow["before"]["id"], fast["before"]["id"]);
    assert_ne!(slow["before"]["handle"], fast["before"]["handle"]);
}

#[tokio::test]
#[serial]
async fn service_invokes_a_simple_worker() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy(
            "simple-invoke".to_string(),
            r#"
                export default {
                  async fetch() {
                    return new Response("ok");
                  }
                };
                "#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let output = service
        .invoke("simple-invoke".to_string(), test_invocation())
        .await
        .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("body should be utf8"),
        "ok"
    );
}

/// The global scope is no EventTarget, so nothing can handle an exception
/// that `reportError` or a throwing event listener reports: it fails the
/// request with that exception, as an unhandled rejection does.
#[tokio::test]
#[serial]
async fn reported_exceptions_fail_the_request_with_their_own_error() {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy(
            "reporter".to_string(),
            r#"
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/listener") {
      const target = new EventTarget();
      target.addEventListener("ping", () => { throw new Error("the listener threw"); });
      target.dispatchEvent(new Event("ping"));
    } else {
      reportError(new Error("the worker reported this"));
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
    return new Response("the exception went unreported");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    for (path, message) in [
        ("/report", "the worker reported this"),
        ("/listener", "the listener threw"),
    ] {
        let error = service
            .invoke(
                "reporter".to_string(),
                test_invocation_with_path(path, "reporter"),
            )
            .await
            .expect_err("a reported exception should fail the request");
        assert!(error.to_string().contains(message), "{path}: {error}");
    }
}

#[tokio::test]
#[serial]
async fn wait_until_background_work_runs_after_response() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 8,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "wait-until-basic".to_string();
    service
        .deploy(worker_name.clone(), wait_until_worker())
        .await
        .expect("deploy should succeed");

    service
        .invoke(
            worker_name.clone(),
            test_invocation_with_path("/trigger", "wait-until-trigger-request"),
        )
        .await
        .expect("trigger request should succeed");

    let mut observed = None;
    for _ in 0..20 {
        let output = service
            .invoke(
                worker_name.clone(),
                test_invocation_with_path("/read", "wait-until-read-request"),
            )
            .await
            .expect("read request should succeed");
        let body = String::from_utf8(output.body).expect("read body should be utf8");
        if body == "done" {
            observed = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(observed.as_deref(), Some("done"));
}

#[tokio::test]
#[serial]
async fn dropped_invoke_aborts_inflight_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    service
        .deploy("abortable".to_string(), abort_aware_worker())
        .await
        .expect("deploy should succeed");

    let service_for_blocked = service.clone();
    let blocked = tokio::spawn(async move {
        let mut req = test_invocation();
        req.request_id = "block".to_string();
        service_for_blocked
            .invoke("abortable".to_string(), req)
            .await
    });

    timeout(Duration::from_secs(1), async {
        loop {
            let stats = service.stats("abortable".to_string()).await.expect("stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("request should become inflight");

    blocked.abort();
    assert!(blocked.await.is_err(), "aborted task should be canceled");

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats("abortable".to_string()).await.expect("stats");
            if stats.inflight_total == 0 && stats.queued == 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("abort should clear inflight slot");

    let mut followup_req = test_invocation();
    followup_req.request_id = "after".to_string();
    let followup = service
        .invoke("abortable".to_string(), followup_req)
        .await
        .expect("followup invoke should succeed");

    assert_eq!(
        String::from_utf8(followup.body).expect("utf8"),
        "abortCount=1"
    );
}

#[tokio::test]
#[serial]
async fn duplicate_user_request_ids_do_not_collide() {
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
        .deploy("io".to_string(), io_wait_worker())
        .await
        .expect("deploy should succeed");

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let svc = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "same-user-request-id".to_string();
            svc.invoke("io".to_string(), req).await
        }));
    }

    for task in tasks {
        let output = task.await.expect("join").expect("invoke should succeed");
        assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");
    }
}
