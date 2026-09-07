use super::*;

#[tokio::test]
#[serial]
async fn static_deployment_rejects_malformed_egress_hosts() {
    let service = test_service(RuntimeConfig::default()).await;
    let error = service
        .deploy_with_config(
            "invalid-egress".to_string(),
            "export default { fetch() { return new Response('ok'); } };".to_string(),
            DeployConfig {
                egress_allow_hosts: vec!["https://api.example.com/path".to_string()],
                ..DeployConfig::default()
            },
        )
        .await
        .expect_err("deployment must reject a URL where an egress host is required");
    assert_eq!(error.kind(), ErrorKind::BadRequest);
    assert!(error.to_string().contains("https://api.example.com/path"));
}

#[tokio::test]
#[serial]
async fn static_worker_fetch_uses_allowlist_and_preserves_headers() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have addr");
    let (request_tx, request_rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept should succeed");
        let mut buffer = vec![0_u8; 8192];
        let bytes_read = socket
            .read(&mut buffer)
            .await
            .expect("server read should succeed");
        request_tx
            .send(String::from_utf8_lossy(&buffer[..bytes_read]).to_string())
            .expect("request should be captured");
        socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                )
                .await
                .expect("server write should succeed");
    });

    let worker_name = "outbound-fetch".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            fetch_probe_worker(&format!("http://{address}/fetch-probe")),
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{address}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("static deploy should succeed");

    let output = service
        .invoke(worker_name, test_invocation())
        .await
        .expect("outbound fetch invoke should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");

    let raw_request = request_rx.await.expect("request should arrive");
    assert!(
        raw_request.starts_with("GET /fetch-probe?token=secret-value HTTP/1.1\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        raw_request.contains("\r\nauthorization: Bearer secret-value\r\n"),
        "raw request was {raw_request}"
    );
    assert!(
        raw_request.contains("\r\nx-dd-secret: secret-value\r\n"),
        "raw request was {raw_request}"
    );
}

#[tokio::test]
#[serial]
async fn static_worker_fetch_revalidates_redirect_and_strips_cross_origin_credentials() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let destination = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("destination listener should bind");
    let destination_address = destination.local_addr().expect("destination address");
    let (request_tx, request_rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut socket, _) = destination.accept().await.expect("destination accept");
        let mut buffer = vec![0_u8; 8192];
        let bytes_read = socket.read(&mut buffer).await.expect("destination read");
        request_tx
            .send(String::from_utf8_lossy(&buffer[..bytes_read]).to_string())
            .expect("request should be captured");
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
            )
            .await
            .expect("destination write");
    });

    let redirect = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("redirect listener should bind");
    let redirect_address = redirect.local_addr().expect("redirect address");
    tokio::spawn(async move {
        let (mut socket, _) = redirect.accept().await.expect("redirect accept");
        let mut buffer = vec![0_u8; 4096];
        let _ = socket.read(&mut buffer).await.expect("redirect read");
        let response = format!(
            "HTTP/1.1 302 Found\r\nlocation: http://{destination_address}/final\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("redirect write");
    });

    let worker_name = "outbound-fetch".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            fetch_probe_worker(&format!("http://{redirect_address}/start")),
            DeployConfig {
                egress_allow_hosts: vec![
                    format!("private:{redirect_address}"),
                    format!("private:{destination_address}"),
                ],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("static deploy should succeed");

    let output = service
        .invoke(worker_name, test_invocation())
        .await
        .expect("redirected fetch should succeed");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");

    let redirected_request = request_rx.await.expect("redirected request should arrive");
    assert!(redirected_request.starts_with("GET /final HTTP/1.1\r\n"));
    assert!(
        !redirected_request
            .to_ascii_lowercase()
            .contains("\r\nauthorization:"),
        "cross-origin authorization leaked: {redirected_request}"
    );
}

#[tokio::test]
#[serial]
async fn static_worker_fetch_rejects_egress_hosts_outside_allowlist() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let worker_name = "outbound-fetch".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            fetch_probe_worker("http://127.0.0.1:9/blocked"),
            DeployConfig {
                egress_allow_hosts: vec!["example.com".to_string()],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("static deploy should succeed");

    let error = service
        .invoke(worker_name, test_invocation())
        .await
        .expect_err("outbound fetch invoke should fail");
    let body = error.to_string();
    assert!(
        body.contains("egress origin is not allowed: http://127.0.0.1:9"),
        "body was {body}"
    );
}

#[tokio::test]
#[serial]
async fn static_worker_fetch_abort_signal_cancels_outbound_request() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 2,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(5),
        scale_tick: Duration::from_millis(50),
        queue_warn_thresholds: vec![10],
        ..RuntimeConfig::default()
    })
    .await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have addr");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept should succeed");
        let mut buffer = vec![0_u8; 4096];
        let _ = socket.read(&mut buffer).await;
        sleep(Duration::from_millis(200)).await;
        let _ = socket.shutdown().await;
    });

    let worker_name = "outbound-fetch".to_string();
    service
        .deploy_with_config(
            worker_name.clone(),
            fetch_abort_worker(&format!("http://{address}/abort-probe")),
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{address}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("static deploy should succeed");

    let started_at = Instant::now();
    let output = timeout(
        Duration::from_secs(2),
        service.invoke(worker_name, test_invocation()),
    )
    .await
    .expect("invoke should not hang")
    .expect("invoke should succeed");
    assert_eq!(output.status, 200);
    let body = String::from_utf8(output.body).expect("utf8");
    assert!(
        body == "Error" || body.contains("Abort") || body.to_ascii_lowercase().contains("abort"),
        "body was {body}"
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "abort should finish quickly"
    );
}

fn fetch_probe_worker(url: &str) -> String {
    format!(
        r#"
export default {{
  async fetch(_request, env) {{
    const response = await fetch("{url}?token=" + encodeURIComponent("secret-value"), {{
      headers: {{
        "authorization": "Bearer " + "secret-value",
        "x-dd-secret": "secret-value",
      }},
    }});
    return new Response(await response.text(), {{
      status: response.status,
      headers: response.headers,
    }});
  }},
}};
"#
    )
}

fn fetch_abort_worker(url: &str) -> String {
    format!(
        r#"
export default {{
  async fetch() {{
    const controller = new AbortController();
    setTimeout(() => controller.abort(new Error("stop")), 25);
    try {{
      await fetch("{url}", {{
        signal: controller.signal,
        headers: {{
          "x-abort-test": "true",
        }},
      }});
      return new Response("unexpected-success", {{ status: 500 }});
    }} catch (error) {{
      return new Response(String(error?.name ?? error));
    }}
  }},
}};
"#
    )
}
