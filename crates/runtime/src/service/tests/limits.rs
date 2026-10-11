//! Per-worker request limits: queue depth and wait, wall time, and request and
//! response body size.
use super::*;

#[tokio::test]
#[serial]
async fn worker_queue_rejects_when_per_worker_limit_is_full() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, held) = tokio::sync::oneshot::channel();
    let gate = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            let length = socket.read(&mut buffer).await.unwrap();
            assert!(length > 0, "gate request ended before its headers");
            request.extend_from_slice(&buffer[..length]);
            assert!(request.len() <= 16 * 1024);
        }
        held.await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        max_queued_requests_per_worker: 1,
        max_global_queued_requests: 16,
        max_global_queued_bytes: 1024 * 1024,
        max_queue_wait: Duration::from_secs(5),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "queue-limit".to_string();
    // The first request holds the only isolate until the gate is released, so
    // the queue stays full for as long as the assertions need.
    let source = format!(
        "const gateUrl = {};\n{}",
        serde_json::to_string(&format!("http://{address}/gate")).unwrap(),
        r#"
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/one") await fetch(gateUrl);
    return new Response(new URL(request.url).pathname);
  },
};
"#,
    );
    service
        .deploy_with_config(
            worker.clone(),
            source,
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{address}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy should succeed");

    let first_service = service.clone();
    let first_worker = worker.clone();
    let first = tokio::spawn(async move {
        first_service
            .invoke(first_worker, test_invocation_with_path("/one", "queue-one"))
            .await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first request should dispatch");

    let second_service = service.clone();
    let second_worker = worker.clone();
    let second = tokio::spawn(async move {
        second_service
            .invoke(
                second_worker,
                test_invocation_with_path("/two", "queue-two"),
            )
            .await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.queued == 1 && stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("second request should occupy the queue");

    let error = service
        .invoke(
            worker.clone(),
            test_invocation_with_path("/three", "queue-three"),
        )
        .await
        .expect_err("third request should be rejected");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    assert!(error.to_string().contains("worker queue is full"));

    release.send(()).expect("release the occupied isolate");
    gate.await.expect("queue gate should complete");

    timeout(Duration::from_secs(3), first)
        .await
        .expect("first join")
        .expect("first request should complete")
        .expect("first request should succeed");
    timeout(Duration::from_secs(3), second)
        .await
        .expect("second join")
        .expect("second request should complete")
        .expect("second request should succeed");
}

#[tokio::test]
#[serial]
async fn worker_queue_rejects_when_global_limit_is_full_across_workers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, held) = tokio::sync::oneshot::channel();
    let gate = tokio::spawn(async move {
        let mut sockets = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let length = socket.read(&mut buffer).await.unwrap();
                assert!(length > 0, "gate request ended before its headers");
                request.extend_from_slice(&buffer[..length]);
                assert!(request.len() <= 16 * 1024);
            }
            sockets.push(socket);
        }
        held.await.unwrap();
        for mut socket in sockets {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
        }
    });
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 2,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        max_queued_requests_per_worker: 8,
        max_global_queued_requests: 1,
        max_global_queued_bytes: 1024 * 1024,
        max_queue_wait: Duration::from_secs(5),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(20),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker_a = "global-queue-a".to_string();
    let worker_b = "global-queue-b".to_string();
    let source = format!(
        "const gateUrl = {};\n{}",
        serde_json::to_string(&format!("http://{address}/gate")).unwrap(),
        r#"
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/one") await fetch(gateUrl);
    return new Response(new URL(request.url).pathname);
  },
};
"#,
    );
    let config = DeployConfig {
        egress_allow_hosts: vec![format!("private:{address}")],
        ..DeployConfig::default()
    };
    service
        .deploy_with_config(worker_a.clone(), source.clone(), config.clone())
        .await
        .expect("worker a deploy should succeed");
    service
        .deploy_with_config(worker_b.clone(), source, config)
        .await
        .expect("worker b deploy should succeed");

    let first_a_service = service.clone();
    let first_a_worker = worker_a.clone();
    let first_a = tokio::spawn(async move {
        first_a_service
            .invoke(
                first_a_worker,
                test_invocation_with_path("/one", "global-a-one"),
            )
            .await
    });
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service
                .stats(worker_a.clone())
                .await
                .expect("worker a stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker a first request should dispatch");

    let first_b_service = service.clone();
    let first_b_worker = worker_b.clone();
    let first_b = tokio::spawn(async move {
        first_b_service
            .invoke(
                first_b_worker,
                test_invocation_with_path("/one", "global-b-one"),
            )
            .await
    });
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service
                .stats(worker_b.clone())
                .await
                .expect("worker b stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker b first request should dispatch");

    let queued_a_service = service.clone();
    let queued_a_worker = worker_a.clone();
    let queued_a = tokio::spawn(async move {
        queued_a_service
            .invoke(
                queued_a_worker,
                test_invocation_with_path("/two", "global-a-two"),
            )
            .await
    });
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service
                .stats(worker_a.clone())
                .await
                .expect("worker a stats");
            if stats.queued == 1 && stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker a second request should occupy the only global queue slot");

    let error = service
        .invoke(
            worker_b.clone(),
            test_invocation_with_path("/two", "global-b-two"),
        )
        .await
        .expect_err("worker b queued request should hit the global queue limit");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    assert!(error.to_string().contains("runtime queue is full"));

    release.send(()).expect("release the occupied isolates");
    gate.await.expect("queue gate should complete");

    timeout(Duration::from_secs(3), first_a)
        .await
        .expect("worker a first join")
        .expect("worker a first request should complete")
        .expect("worker a first request should succeed");
    timeout(Duration::from_secs(3), queued_a)
        .await
        .expect("worker a queued join")
        .expect("worker a queued request should complete")
        .expect("worker a queued request should succeed");
    timeout(Duration::from_secs(3), first_b)
        .await
        .expect("worker b first join")
        .expect("worker b first request should complete")
        .expect("worker b first request should succeed");
}

#[tokio::test]
#[serial]
async fn worker_queue_expires_requests_after_queue_wait_limit() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        max_queued_requests_per_worker: 8,
        max_global_queued_requests: 16,
        max_global_queued_bytes: 1024 * 1024,
        max_queue_wait: Duration::from_millis(50),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "queue-timeout".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === "/one") {
      await new Promise(resolve => {
        request.signal.addEventListener("abort", resolve, { once: true });
        if (request.signal.aborted) resolve();
      });
    }
    return new Response(new URL(request.url).pathname);
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    // Isolate startup must finish before exercising the short queue deadline.
    if let Err(error) = service
        .invoke(
            worker.clone(),
            test_invocation_with_path("/warm", "queue-wait-warm"),
        )
        .await
    {
        assert_eq!(error.kind(), ErrorKind::Overloaded);
        assert!(error.to_string().contains("queue wait limit"));
    }
    timeout(Duration::from_secs(5), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.isolates_total == 1 && stats.global_isolates_starting == 0 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("queue timeout fixture should be ready");

    let first_service = service.clone();
    let first_worker = worker.clone();
    let first = tokio::spawn(async move {
        first_service
            .invoke(
                first_worker,
                test_invocation_with_path("/one", "queue-wait-one"),
            )
            .await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats(worker.clone()).await.expect("stats");
            if stats.inflight_total == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first request should dispatch");

    let queued_service = service.clone();
    let queued_worker = worker.clone();
    let queued = tokio::spawn(async move {
        queued_service
            .invoke(
                queued_worker,
                test_invocation_with_path("/queued", "queue-wait-two"),
            )
            .await
    });

    let error = timeout(Duration::from_secs(2), queued)
        .await
        .expect("queued request should complete")
        .expect("queued join")
        .expect_err("queued request should expire");
    assert_eq!(error.kind(), ErrorKind::Overloaded);
    assert!(error.to_string().contains("queue wait limit"));

    first.abort();
    assert!(
        timeout(Duration::from_secs(3), first)
            .await
            .expect("first request cancellation should complete")
            .expect_err("first task was canceled")
            .is_cancelled()
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn worker_request_wall_timeout_retires_cooperative_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        request_wall_timeout: Duration::from_millis(50),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "cooperative-wall-timeout".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  async fetch() {
    await __dd_internals.core.ops.op_time_sleep(500);
    return new Response("late");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let error = timeout(
        Duration::from_secs(2),
        service.invoke(worker.clone(), test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect_err("request should exceed wall timeout");
    assert!(
        error.to_string().contains("wall-time limit"),
        "unexpected error: {error}"
    );

    wait_for_isolate_total(&service, &worker, 0).await;
}

#[tokio::test]
#[serial]
async fn worker_request_wall_timeout_interrupts_cpu_bound_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        request_wall_timeout: Duration::from_millis(50),
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    let worker = "cpu-wall-timeout".to_string();
    service
        .deploy(
            worker.clone(),
            r#"
export default {
  fetch() {
    while (true) {}
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let started_at = Instant::now();
    let error = timeout(
        Duration::from_secs(2),
        service.invoke(worker.clone(), test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect_err("request should exceed wall timeout");
    assert!(
        error.to_string().contains("wall-time limit"),
        "unexpected error: {error}"
    );
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "cpu-bound request took too long to interrupt"
    );

    wait_for_isolate_total(&service, &worker, 0).await;
}

#[tokio::test]
#[serial]
async fn buffered_request_body_limit_rejects_before_dispatch() {
    let service = test_service(RuntimeConfig {
        max_request_body_bytes: 3,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "request-limit".to_string(),
            r#"
export default {
  async fetch(request) {
    return new Response(await request.text());
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut request = test_invocation();
    request.method = "POST".to_string();
    request.body = b"four".to_vec();
    let error = service
        .invoke("request-limit".to_string(), request)
        .await
        .expect_err("oversized request should fail");
    assert!(
        error.to_string().contains("max_request_body_bytes"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn streamed_request_body_limit_fails_during_read() {
    let service = test_service(RuntimeConfig {
        max_request_body_bytes: 3,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "streamed-request-limit".to_string(),
            r#"
export default {
  async fetch(request) {
    return new Response(await request.text());
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let (tx, rx) = mpsc::channel(4);
    tx.send(Ok(Bytes::from_static(b"ab")))
        .await
        .expect("send first chunk");
    tx.send(Ok(Bytes::from_static(b"cd")))
        .await
        .expect("send second chunk");
    drop(tx);

    let mut request = test_invocation();
    request.method = "POST".to_string();
    let error = service
        .invoke_with_request_body("streamed-request-limit".to_string(), request, Some(rx))
        .await
        .expect_err("oversized streamed request should fail");
    assert!(
        error.to_string().contains("max_request_body_bytes"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn buffered_response_body_limit_rejects_completion() {
    let service = test_service(RuntimeConfig {
        max_response_body_bytes: 4,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "response-limit".to_string(),
            r#"
export default {
  fetch() {
    return new Response("hello");
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let error = service
        .invoke("response-limit".to_string(), test_invocation())
        .await
        .expect_err("oversized response should fail");
    assert!(
        error.to_string().contains("max_response_body_bytes"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[serial]
async fn streamed_response_body_limit_fails_stream_and_retires_isolate() {
    let service = test_service(RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 1,
        max_response_body_bytes: 5,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        queue_warn_thresholds: vec![1],
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "streamed-response-limit".to_string(),
            r#"
export default {
  fetch(request) {
    if (new URL(request.url).pathname === "/ok") {
      return new Response("ok");
    }
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue("abc");
        controller.enqueue("def");
        controller.close();
      }
    }));
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let mut output = service
        .invoke_stream(
            "streamed-response-limit".to_string(),
            test_invocation_with_path("/stream", "stream-limit"),
        )
        .await
        .expect("stream should start");
    let first = timeout(Duration::from_secs(2), output.body.recv())
        .await
        .expect("first chunk should arrive")
        .expect("body should still be open")
        .expect("first chunk should be ok");
    assert_eq!(String::from_utf8(first.to_vec()).expect("utf8"), "abc");

    let error = timeout(Duration::from_secs(2), output.body.recv())
        .await
        .expect("limit error should arrive")
        .expect("body should report limit error")
        .expect_err("second chunk should exceed response limit");
    assert!(
        error.to_string().contains("max_response_body_bytes"),
        "unexpected error: {error}"
    );

    let followup = timeout(
        Duration::from_secs(3),
        service.invoke(
            "streamed-response-limit".to_string(),
            test_invocation_with_path("/ok", "stream-limit-followup"),
        ),
    )
    .await
    .expect("stream limit should retire the isolate and allow a followup")
    .expect("followup invoke should succeed");
    assert_eq!(String::from_utf8(followup.body).expect("utf8"), "ok");
}
