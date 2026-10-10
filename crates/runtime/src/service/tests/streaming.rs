use super::*;

#[tokio::test]
#[serial]
async fn buffered_response_limit_cancels_endless_producer_before_completion() {
    let service = test_service(RuntimeConfig {
        max_isolates: 1,
        max_response_body_bytes: 4,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "buffered-endless".into(),
            r#"
let canceled = false;
let pulls = 0;
export default {
  fetch(request) {
    if (new URL(request.url).pathname === '/state') {
      return new Response(canceled ? 'yes' : 'no', { headers: { 'x-pulls': String(pulls) } });
    }
    return new Response(new ReadableStream({
      pull(controller) { pulls++; controller.enqueue(new Uint8Array(3)); },
      cancel() { canceled = true; },
    }));
  },
};
"#
            .into(),
        )
        .await
        .expect("endless producer deploys");
    let error = timeout(
        Duration::from_secs(2),
        service.invoke("buffered-endless".into(), test_invocation()),
    )
    .await
    .expect("size bound rejects before request wall timeout")
    .expect_err("oversized response fails");
    assert!(
        error.to_string().contains("max_response_body_bytes"),
        "{error}"
    );
    let state = service
        .invoke(
            "buffered-endless".into(),
            test_invocation_with_path("/state", "after-body-limit"),
        )
        .await
        .expect("isolate remains usable");
    assert_eq!(state.body, b"yes");
    let pulls: usize = state
        .headers
        .iter()
        .find(|(name, _)| name == "x-pulls")
        .expect("pull count is returned")
        .1
        .parse()
        .unwrap();
    assert!(
        pulls <= 3,
        "producer must stop on its second consumed chunk: {pulls}"
    );
    service.shutdown().await.expect("runtime shuts down");
}

#[tokio::test]
#[serial]
async fn an_unread_response_does_not_block_other_workers_or_cancellation() {
    let service = test_service(RuntimeConfig {
        max_isolates: 1,
        max_global_isolates: 2,
        max_buffered_response_bytes: 2 * 1024 * 1024,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "slow".into(),
            r#"
export default {
  fetch() {
    return new Response(new ReadableStream({
      pull(controller) { controller.enqueue(new Uint8Array(4096)); }
    }));
  }
};
"#
            .into(),
        )
        .await
        .expect("slow worker should deploy");
    service
        .deploy(
            "healthy".into(),
            "export default { fetch() { return new Response('healthy'); } };".into(),
        )
        .await
        .expect("healthy worker should deploy");
    let unread = service
        .invoke_stream("slow".into(), test_invocation())
        .await
        .expect("stream should start");
    sleep(Duration::from_millis(100)).await;
    let healthy = timeout(
        Duration::from_secs(2),
        service.invoke("healthy".into(), test_invocation()),
    )
    .await
    .expect("an unread stream must not block another worker")
    .expect("healthy request should succeed");
    assert_eq!(healthy.body, b"healthy");
    let stats = timeout(Duration::from_secs(1), service.stats("slow".into()))
        .await
        .expect("stats must remain responsive")
        .expect("slow worker stats should exist");
    assert_eq!(stats.inflight_total, 1);
    drop(unread);
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service
                .stats("slow".into())
                .await
                .expect("slow worker stats should exist");
            if stats.inflight_total == 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dropping a blocked stream must cancel its producer");
}

#[tokio::test]
#[serial]
async fn response_byte_budget_bounds_production_and_cancels_waiting_copies() {
    let service = test_service(RuntimeConfig {
        max_isolates: 1,
        max_buffered_response_bytes: 4096,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "budget".into(),
            r#"
let produced = 0;
export default {
  fetch(request) {
    if (new URL(request.url).pathname === "/produced") return Response.json(produced);
    return new Response(new ReadableStream({
      pull(controller) { produced++; controller.enqueue(new Uint8Array(1024)); }
    }));
  }
};
"#
            .into(),
        )
        .await
        .expect("worker should deploy");
    let unread = service
        .invoke_stream("budget".into(), test_invocation())
        .await
        .expect("stream should start");
    sleep(Duration::from_millis(100)).await;
    let output = timeout(
        Duration::from_secs(2),
        service.invoke(
            "budget".into(),
            test_invocation_with_path("/produced", "progress"),
        ),
    )
    .await
    .expect("byte admission must not block the scheduler")
    .expect("progress should succeed");
    let produced: usize = serde_json::from_slice(&output.body).expect("counter must be JSON");
    assert!(
        (4..=6).contains(&produced),
        "4096 retained bytes plus producer prefetch must bound 1024-byte chunks, got {produced}"
    );
    drop(unread);
    timeout(Duration::from_secs(2), async {
        loop {
            if service
                .stats("budget".into())
                .await
                .expect("stats")
                .inflight_total
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancel must wake a producer waiting for byte permits");
    let mut next = service
        .invoke_stream("budget".into(), test_invocation())
        .await
        .expect("next stream should start");
    let chunk = timeout(Duration::from_secs(2), next.body.recv())
        .await
        .expect("canceled stream must release its byte permits")
        .expect("next stream must produce a chunk")
        .expect("chunk must succeed");
    assert_eq!(chunk.len(), 1024);
}

#[tokio::test]
#[serial]
async fn a_full_response_queue_delivers_its_terminal_error_without_blocking_stats() {
    let service = test_service(RuntimeConfig {
        max_isolates: 1,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "error".into(),
            r#"
export default {
  fetch() {
    let next = 0;
    return new Response(new ReadableStream({
      pull(controller) {
        if (next === 16) { controller.error(new Error("terminal failure")); return; }
        controller.enqueue(new Uint8Array([next++]));
      }
    }));
  }
};
"#
            .into(),
        )
        .await
        .expect("worker should deploy");
    let mut output = service
        .invoke_stream("error".into(), test_invocation())
        .await
        .expect("stream should start");
    timeout(Duration::from_secs(2), async {
        loop {
            if service
                .stats("error".into())
                .await
                .expect("stats")
                .inflight_total
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("terminal error must finish even while the body queue is full");
    for expected in 0..16_u8 {
        let chunk = output
            .body
            .recv()
            .await
            .expect("buffered chunk")
            .expect("buffered bytes precede error");
        assert_eq!(chunk.as_ref(), &[expected]);
    }
    let error = output
        .body
        .recv()
        .await
        .expect("terminal error must be delivered")
        .expect_err("body must fail");
    assert!(
        error.to_string().contains("terminal failure"),
        "original stream error must survive: {error}"
    );
    assert!(output.body.recv().await.is_none());
}

#[tokio::test]
#[serial]
async fn dropping_a_completed_undrained_body_releases_the_shared_byte_budget() {
    let service = test_service(RuntimeConfig {
        max_isolates: 1,
        max_buffered_response_bytes: 4,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "completed".into(),
            "export default { fetch() { return new Response('done'); } };".into(),
        )
        .await
        .expect("worker should deploy");
    let completed = service
        .invoke_stream("completed".into(), test_invocation())
        .await
        .expect("first stream should start");
    timeout(Duration::from_secs(2), async {
        loop {
            if service
                .stats("completed".into())
                .await
                .expect("stats")
                .inflight_total
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first producer should finish before the body is read");
    let mut waiting = service
        .invoke_stream("completed".into(), test_invocation())
        .await
        .expect("second response headers should arrive");
    assert!(
        timeout(Duration::from_millis(50), waiting.body.recv())
            .await
            .is_err(),
        "completed but retained bytes must still consume the budget"
    );
    drop(completed);
    let chunk = timeout(Duration::from_secs(2), waiting.body.recv())
        .await
        .expect("dropping completed body must release permits")
        .expect("second body chunk")
        .expect("second body must succeed");
    assert_eq!(chunk.as_ref(), b"done");
    drop(chunk);
    assert!(waiting.body.recv().await.is_none());
}

#[tokio::test]
#[serial]
async fn aborting_one_stream_leaves_concurrent_streams_and_the_isolate_alone() {
    let service = test_service(RuntimeConfig {
        max_isolates: 1,
        max_inflight_per_isolate: 4,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "streams".into(),
            r#"
let canceled = false;
export default {
  fetch(request) {
    const { pathname } = new URL(request.url);
    if (pathname === "/state") {
      return new Response(canceled ? "canceled" : "running");
    }
    if (pathname === "/idle") {
      // One chunk, then nothing: a producer waiting for events.
      return new Response(new ReadableStream({
        start(controller) { controller.enqueue(new TextEncoder().encode("idle")); },
        cancel() { canceled = true; },
      }));
    }
    let sent = 0;
    return new Response(new ReadableStream({
      async pull(controller) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        controller.enqueue(new TextEncoder().encode(String(sent)));
        if (++sent === 3) controller.close();
      },
    }));
  },
};
"#
            .into(),
        )
        .await
        .expect("streaming worker deploys");

    let mut idle = service
        .invoke_stream("streams".into(), test_invocation_with_path("/idle", "idle"))
        .await
        .expect("idle stream starts");
    let first = timeout(Duration::from_secs(2), idle.body.recv())
        .await
        .expect("idle stream sends its first chunk")
        .expect("idle stream is open")
        .expect("first chunk is ok");
    assert_eq!(first.as_ref(), b"idle");
    let mut sibling = service
        .invoke_stream(
            "streams".into(),
            test_invocation_with_path("/count", "sibling"),
        )
        .await
        .expect("sibling stream starts");

    drop(idle);

    let mut body = Vec::new();
    while let Some(chunk) = timeout(Duration::from_secs(2), sibling.body.recv())
        .await
        .expect("sibling keeps streaming")
    {
        body.extend_from_slice(&chunk.expect("sibling chunk is ok"));
    }
    assert_eq!(body, b"012");

    timeout(Duration::from_secs(2), async {
        loop {
            let state = service
                .invoke(
                    "streams".into(),
                    test_invocation_with_path("/state", "state"),
                )
                .await
                .expect("state request succeeds");
            if state.body == b"canceled" {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the aborted stream's producer is cancelled");
    let stats = service
        .stats("streams".into())
        .await
        .expect("worker stats exist");
    assert_eq!(stats.isolates_total, 1, "the isolate is not retired");
    service.shutdown().await.expect("runtime shuts down");
}
