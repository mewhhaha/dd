use super::*;

async fn host_fetch_worker(
    address: std::net::SocketAddr,
    source: &str,
    body_limit: usize,
) -> TestRuntime {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_request_body_bytes: body_limit,
        request_wall_timeout: Duration::from_secs(5),
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy_with_config(
            "host-fetch-contract".into(),
            format!(
                "const target = {};\n{source}",
                serde_json::to_string(&format!("http://{address}/echo")).unwrap()
            ),
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{address}")],
                ..DeployConfig::default()
            },
        )
        .await
        .unwrap();
    service
}

async fn echo_requests(listener: TcpListener, count: usize) {
    for _ in 0..count {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 2048];
        let header_end = loop {
            let length = socket.read(&mut buffer).await.unwrap();
            assert!(length > 0, "request ended before its headers");
            request.extend_from_slice(&buffer[..length]);
            assert!(request.len() <= 16 * 1024);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
        let header = |name: &str| {
            headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.trim().to_string())
        };
        let body_length = header("content-length").unwrap().parse::<usize>().unwrap();
        assert!(body_length <= 4096);
        while request.len() < header_end + body_length {
            let length = socket.read(&mut buffer).await.unwrap();
            assert!(length > 0, "request ended before its body");
            request.extend_from_slice(&buffer[..length]);
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "method": headers.split_whitespace().next().unwrap(),
            "contentType": header("content-type"),
            "body": String::from_utf8(request[header_end..header_end + body_length].to_vec()).unwrap(),
        })).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    }
}

#[tokio::test]
#[serial]
async fn host_fetch_encodes_native_body_types_and_honors_request_init_overrides() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        timeout(Duration::from_secs(5), echo_requests(listener, 5))
            .await
            .unwrap();
    });
    let service = host_fetch_worker(
        address,
        r#"
export default {
  async fetch() {
    const form = new FormData();
    form.set('message', 'form-value');
    form.set('file', new Blob(['file-value'], { type: 'text/plain' }), 'file.txt');
    const bodies = [
      form,
      new Blob(['blob-value'], { type: 'application/x-probe' }),
      new URLSearchParams({ message: 'space value' }),
      new ReadableStream({ start(controller) {
        controller.enqueue(new TextEncoder().encode('stream-'));
        controller.enqueue(new TextEncoder().encode('value'));
        controller.close();
      } }),
    ];
    const results = [];
    for (const body of bodies) {
      results.push(await (await fetch(target, { method: 'POST', body })).json());
    }
    const input = new Request(target, { method: 'POST', body: new ReadableStream({
      pull() { return new Promise(() => {}); },
    }) });
    results.push(await (await fetch(input, { body: 'replacement' })).json());
    return Response.json(results);
  },
};
"#,
        4096,
    )
    .await;
    let output = timeout(
        Duration::from_secs(5),
        service.invoke("host-fetch-contract".into(), test_invocation()),
    )
    .await
    .expect("native body conversion must finish")
    .unwrap();
    let results: Value = serde_json::from_slice(&output.body).unwrap();
    let results = results.as_array().unwrap();
    assert_eq!(results.len(), 5);
    assert!(results.iter().all(|item| item["method"] == "POST"));
    assert!(
        results[0]["contentType"]
            .as_str()
            .unwrap()
            .starts_with("multipart/form-data; boundary=")
    );
    let form = results[0]["body"].as_str().unwrap();
    assert!(form.contains("name=\"message\"") && form.contains("form-value"));
    assert!(form.contains("filename=\"file.txt\"") && form.contains("file-value"));
    assert_eq!(results[1]["body"], "blob-value");
    assert_eq!(results[1]["contentType"], "application/x-probe");
    assert_eq!(results[2]["body"], "message=space+value");
    assert_eq!(
        results[2]["contentType"],
        "application/x-www-form-urlencoded;charset=UTF-8"
    );
    assert_eq!(results[3]["body"], "stream-value");
    assert_eq!(results[3]["contentType"], Value::Null);
    assert_eq!(results[4]["body"], "replacement");
    server.await.unwrap();
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn host_fetch_cancels_bounded_body_reads_and_preserves_abort_reasons() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = host_fetch_worker(listener.local_addr().unwrap(), r#"
export default {
  async fetch() {
    const reasons = [null, 73, { marker: 'custom-abort' }];
    const preserved = [];
    for (const reason of reasons) {
      const controller = new AbortController();
      controller.abort(reason);
      try { await fetch(target, { signal: controller.signal }); preserved.push(false); }
      catch (error) { preserved.push(error === reason); }
    }
    let preCanceled = false;
    const pre = new AbortController();
    pre.abort('before-body');
    try { await fetch(target, { method: 'POST', signal: pre.signal, body: new ReadableStream({
      cancel(reason) { preCanceled = reason === 'before-body'; },
    }) }); } catch (error) { if (error !== 'before-body') throw error; }

    let canceled = false;
    const controller = new AbortController();
    const reason = { marker: 'blocked-body-abort' };
    const blocked = new ReadableStream({
      pull() { return new Promise(() => {}); },
      cancel(value) { canceled = value === reason; return new Promise(() => {}); },
    });
    setTimeout(() => controller.abort(reason), 10);
    let blockedReason = false;
    try { await fetch(target, { method: 'POST', body: blocked, signal: controller.signal }); }
    catch (error) { blockedReason = error === reason; }

    let oversizedCanceled = false;
    let pulls = 0;
    let oversized = false;
    try { await fetch(target, { method: 'POST', body: new ReadableStream({
      pull(controller) { pulls++; controller.enqueue(new Uint8Array(24)); },
      cancel() { oversizedCanceled = true; return new Promise(() => {}); },
    }) }); } catch (error) { oversized = error.message.includes('max_request_body_bytes'); }
    let getRejected = false;
    try { await fetch(target, { body: 'invalid GET body' }); }
    catch (error) { getRejected = error instanceof TypeError; }
    return Response.json({ preserved, preCanceled, canceled, blockedReason, oversizedCanceled, oversized, pulls, getRejected });
  },
};
"#, 32).await;
    let output = timeout(
        Duration::from_secs(2),
        service.invoke("host-fetch-contract".into(), test_invocation()),
    )
    .await
    .expect("abort must not await an unresolved producer cancellation")
    .unwrap();
    let result: Value = serde_json::from_slice(&output.body).unwrap();
    assert_eq!(result["preserved"], serde_json::json!([true, true, true]));
    for field in [
        "preCanceled",
        "canceled",
        "blockedReason",
        "oversizedCanceled",
        "oversized",
        "getRejected",
    ] {
        assert_eq!(result[field], true, "{field}: {result}");
    }
    assert!(
        result["pulls"].as_u64().unwrap() <= 3,
        "oversized producer kept running: {result}"
    );
    assert!(
        timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "invalid or aborted input must never reach the network"
    );
    service.shutdown().await.unwrap();
}
