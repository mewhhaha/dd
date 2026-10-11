use super::*;

async fn host_fetch_worker(
    address: std::net::SocketAddr,
    source: &str,
    body_limit: usize,
) -> TestRuntime {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates_per_worker: 1,
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

#[tokio::test]
#[serial]
async fn host_fetch_encodes_native_body_types_and_honors_request_init_overrides() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // The limit only stops a lost request from hanging the suite. It also
    // covers service start and deploy, which a loaded machine can stretch
    // past a few seconds.
    let server = tokio::spawn(async move {
        timeout(
            Duration::from_secs(30),
            echo_requests(listener, 5, ("contentType", "content-type"), true),
        )
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

#[tokio::test]
#[serial]
async fn host_fetch_is_single_permanent_request_scoped_wrapper() {
    let service = test_service(test_pool_config(1, 1, 1)).await;

    service
        .deploy(
            "fetch-wrapper".to_string(),
            r#"
let firstFetch;
let firstHostFetch;
let firstRequestIdGetter;
let firstRequestContextHandleGetter;
let firstSyncBoundary;
let firstCacheBypassGetter;

export default {
  async fetch() {
    const current = globalThis.fetch;
    const hostFetch = __dd_internals.dd.hostFetch;
    const requestIdGetter = __dd_internals.dd.runtimeRequestId;
    const requestContextHandleGetter = __dd_internals.dd.runtimeRequestContextHandle;
    const syncBoundary = __dd_internals.dd.syncTimeBoundary;
    const cacheBypassGetter = __dd_internals.dd.cacheBypassStale;
    const first = firstFetch === undefined;
    if (first) {
      firstFetch = current;
      firstHostFetch = hostFetch;
      firstRequestIdGetter = requestIdGetter;
      firstRequestContextHandleGetter = requestContextHandleGetter;
      firstSyncBoundary = syncBoundary;
      firstCacheBypassGetter = cacheBypassGetter;
    }
    return Response.json({
      first,
      sameAsFirst: current === firstFetch,
      hostFetchSameAsFirst: hostFetch === firstHostFetch,
      requestIdGetterSameAsFirst: requestIdGetter === firstRequestIdGetter,
      requestContextHandleGetterSameAsFirst: requestContextHandleGetter === firstRequestContextHandleGetter,
      syncBoundarySameAsFirst: syncBoundary === firstSyncBoundary,
      cacheBypassGetterSameAsFirst: cacheBypassGetter === firstCacheBypassGetter,
      cacheBypassGetterAvailable: typeof cacheBypassGetter === "function",
      cacheBypassValue: Boolean(cacheBypassGetter?.()),
      rawAvailable: typeof globalThis.__dd_raw_host_fetch === "function",
      rawInstalled: current === globalThis.__dd_raw_host_fetch,
      // The global stays the web layer's fetch, which calls the request-scoped
      // host fetch; the runtime writes no worker-visible global per request.
      wrapperInstalled: current === __dd_internals.dd.web.fetch
        && typeof hostFetch === "function",
    });
  },
};
"#
            .to_string(),
        )
        .await
        .expect("deploy should succeed");

    let first = service
        .invoke(
            "fetch-wrapper".to_string(),
            test_invocation_with_path("/", "fetch-wrapper-first"),
        )
        .await
        .expect("first invoke should succeed");
    let second = service
        .invoke(
            "fetch-wrapper".to_string(),
            test_invocation_with_path("/", "fetch-wrapper-second"),
        )
        .await
        .expect("second invoke should succeed");

    let first: Value = serde_json::from_slice(&first.body).expect("first body should be json");
    let second: Value = serde_json::from_slice(&second.body).expect("second body should be json");

    assert_eq!(first["first"], true);
    assert_eq!(first["sameAsFirst"], true);
    assert_eq!(first["hostFetchSameAsFirst"], true);
    assert_eq!(first["requestIdGetterSameAsFirst"], true);
    assert_eq!(first["requestContextHandleGetterSameAsFirst"], true);
    assert_eq!(first["syncBoundarySameAsFirst"], true);
    assert_eq!(first["cacheBypassGetterSameAsFirst"], true);
    assert_eq!(first["cacheBypassGetterAvailable"], true);
    assert_eq!(first["cacheBypassValue"], false);
    assert_eq!(first["rawAvailable"], false);
    assert_eq!(first["rawInstalled"], false);
    assert_eq!(first["wrapperInstalled"], true);
    assert_eq!(second["first"], false);
    assert_eq!(second["sameAsFirst"], true);
    assert_eq!(second["hostFetchSameAsFirst"], true);
    assert_eq!(second["requestIdGetterSameAsFirst"], true);
    assert_eq!(second["requestContextHandleGetterSameAsFirst"], true);
    assert_eq!(second["syncBoundarySameAsFirst"], true);
    assert_eq!(second["cacheBypassGetterSameAsFirst"], true);
    assert_eq!(second["cacheBypassGetterAvailable"], true);
    assert_eq!(second["cacheBypassValue"], false);
    assert_eq!(second["rawAvailable"], false);
    assert_eq!(second["rawInstalled"], false);
    assert_eq!(second["wrapperInstalled"], true);
}
