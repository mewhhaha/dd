use super::*;

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
