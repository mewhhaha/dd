use super::*;

async fn service_fetch_workers(child: &str, parent: &str) -> TestRuntime {
    let service = test_service(RuntimeConfig {
        max_global_isolates: 2,
        max_isolates: 1,
        max_inflight_per_isolate: 2,
        max_response_body_bytes: 1024,
        max_request_body_bytes: 1024,
        request_wall_timeout: Duration::from_secs(5),
        scale_tick: Duration::from_millis(10),
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy("fetch-child".into(), child.into())
        .await
        .unwrap();
    service
        .deploy_with_config(
            "fetch-parent".into(),
            parent.into(),
            DeployConfig {
                bindings: vec![DeployBinding::Service {
                    binding: "CHILD".into(),
                    service: "fetch-child".into(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .unwrap();
    service
}

#[tokio::test]
#[serial]
async fn service_fetch_preserves_null_body_statuses_and_cancels_head_producers() {
    let service = service_fetch_workers(
        r#"
let headCanceled = false;
export default {
  fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === '/state') return Response.json({ headCanceled });
    if (request.method === 'HEAD') {
      return new Response(new ReadableStream({
        pull(controller) { controller.enqueue(new Uint8Array(4096)); },
        cancel() { headCanceled = true; },
      }), { headers: { 'x-child': 'head' } });
    }
    return new Response(null, { status: Number(path.slice(1)), headers: { 'x-child': path.slice(1) } });
  },
};
"#,
        r#"
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    const input = new Request('http://child' + path, { method: path === '/head' ? 'HEAD' : 'GET' });
    const response = await env.CHILD.fetch(input);
    return Response.json({ status: response.status, nullBody: response.body === null, body: await response.text(), child: response.headers.get('x-child') });
  },
};
"#,
    ).await;
    for status in [204, 205, 304] {
        let output = service
            .invoke(
                "fetch-parent".into(),
                test_invocation_with_path(&format!("/{status}"), "null-body"),
            )
            .await
            .unwrap();
        let response: Value = serde_json::from_slice(&output.body).unwrap();
        assert_eq!(
            response,
            serde_json::json!({ "status": status, "nullBody": true, "body": "", "child": status.to_string() })
        );
    }
    let output = timeout(
        Duration::from_secs(2),
        service.invoke(
            "fetch-parent".into(),
            test_invocation_with_path("/head", "head"),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&output.body).unwrap(),
        serde_json::json!({ "status": 200, "nullBody": true, "body": "", "child": "head" })
    );
    let state = service
        .invoke(
            "fetch-child".into(),
            test_invocation_with_path("/state", "head-state"),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&state.body).unwrap(),
        serde_json::json!({ "headCanceled": true })
    );
    service.shutdown().await.unwrap();
}

const ABORT_CHILD: &str = r#"
let calls = 0;
let aborted = 0;
export default {
  async fetch(request) {
    if (new URL(request.url).pathname === '/state') return Response.json({ calls, aborted });
    calls++;
    await new Promise((resolve, reject) => {
      const abort = () => { aborted++; reject(request.signal.reason); };
      request.signal.addEventListener('abort', abort, { once: true });
      if (request.signal.aborted) abort();
    });
    return new Response('unreachable');
  },
};
"#;

async fn wait_for_child_state(service: &RuntimeService, calls: usize, aborted: usize) {
    timeout(Duration::from_secs(2), async {
        loop {
            let stats = service.stats("fetch-child".into()).await.unwrap();
            // Wait for the canceled request to release its dispatch slot.
            if stats.inflight_total == 0 {
                let output = service
                    .invoke(
                        "fetch-child".into(),
                        test_invocation_with_path("/state", "abort-state"),
                    )
                    .await
                    .unwrap();
                let state: Value = serde_json::from_slice(&output.body).unwrap();
                if state == serde_json::json!({ "calls": calls, "aborted": aborted }) {
                    break;
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child cancellation releases its scope and capacity before wall timeout");
}

#[tokio::test]
#[serial]
async fn service_fetch_rejects_pre_aborted_signal_without_dispatch() {
    let service = service_fetch_workers(ABORT_CHILD, r#"
export default {
  async fetch(_request, env) {
    const control = new AbortController();
    control.abort(new Error('pre-abort-marker'));
    try { await env.CHILD.fetch(new Request('http://child/hold', { signal: control.signal })); return new Response('unexpected'); }
    catch (error) { return new Response(String(error)); }
  },
};
"#).await;
    let output = service
        .invoke("fetch-parent".into(), test_invocation())
        .await
        .unwrap();
    assert!(
        String::from_utf8(output.body)
            .unwrap()
            .contains("pre-abort-marker")
    );
    wait_for_child_state(&service, 0, 0).await;
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn service_fetch_midflight_abort_cancels_child_and_releases_capacity() {
    let service = service_fetch_workers(
        ABORT_CHILD,
        r#"
let control;
export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === '/abort') {
      control.abort(new Error('mid-abort-marker'));
      return new Response('aborted');
    }
    control = new AbortController();
    const pending = env.CHILD.fetch('http://child/hold', { signal: control.signal });
    try { await pending; return new Response('unexpected'); }
    catch (error) { return new Response(String(error)); }
  },
};
"#,
    )
    .await;
    let invoke = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke("fetch-parent".into(), test_invocation())
                .await
        })
    };
    wait_for_child_inflight(&service).await;
    service
        .invoke(
            "fetch-parent".into(),
            test_invocation_with_path("/abort", "abort-child"),
        )
        .await
        .unwrap();
    let output = timeout(Duration::from_secs(2), invoke)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8(output.body)
            .unwrap()
            .contains("mid-abort-marker")
    );
    wait_for_child_state(&service, 1, 1).await;
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn dropping_parent_request_cancels_its_service_child() {
    let service = service_fetch_workers(
        ABORT_CHILD,
        r#"
export default { async fetch(_request, env) { return env.CHILD.fetch('http://child/hold'); } };
"#,
    )
    .await;
    let invoke = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke("fetch-parent".into(), test_invocation())
                .await
        })
    };
    wait_for_child_inflight(&service).await;
    invoke.abort();
    let _ = invoke.await;
    wait_for_child_state(&service, 1, 1).await;
    service.shutdown().await.unwrap();
}

async fn wait_for_child_inflight(service: &RuntimeService) {
    timeout(Duration::from_secs(2), async {
        loop {
            if service
                .stats("fetch-child".into())
                .await
                .unwrap()
                .inflight_total
                == 1
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[serial]
async fn service_fetch_abort_cancels_pending_request_body_before_dispatch() {
    let service = service_fetch_workers(
        ABORT_CHILD,
        r#"
let control;
let bodyPulling = false;
let bodyCanceled = false;
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === '/state') return Response.json({ bodyPulling, bodyCanceled });
    if (path === '/abort') { control.abort(new Error('body-abort-marker')); return new Response('aborted'); }
    control = new AbortController();
    const body = new ReadableStream({
      pull() { bodyPulling = true; return new Promise(() => {}); },
      cancel() { bodyCanceled = true; return new Promise(() => {}); },
    });
    try { await env.CHILD.fetch('http://child/hold', { method: 'POST', body, signal: control.signal }); return new Response('unexpected'); }
    catch (error) { return new Response(String(error)); }
  },
};
"#,
    ).await;
    let invoke = {
        let service = service.clone();
        tokio::spawn(async move {
            service
                .invoke("fetch-parent".into(), test_invocation())
                .await
        })
    };
    timeout(Duration::from_secs(2), async {
        loop {
            let state = service
                .invoke(
                    "fetch-parent".into(),
                    test_invocation_with_path("/state", "body-state"),
                )
                .await
                .unwrap();
            if serde_json::from_slice::<Value>(&state.body).unwrap()["bodyPulling"] == true {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    service
        .invoke(
            "fetch-parent".into(),
            test_invocation_with_path("/abort", "abort-body"),
        )
        .await
        .unwrap();
    let output = timeout(Duration::from_secs(2), invoke)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8(output.body)
            .unwrap()
            .contains("body-abort-marker")
    );
    let state = service
        .invoke(
            "fetch-parent".into(),
            test_invocation_with_path("/state", "body-canceled-state"),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&state.body).unwrap(),
        serde_json::json!({ "bodyPulling": true, "bodyCanceled": true })
    );
    wait_for_child_state(&service, 0, 0).await;
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn service_fetch_rejects_oversized_request_body_and_cancels_producer() {
    let service = service_fetch_workers(
        ABORT_CHILD,
        r#"
let canceled = false;
export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === '/state') return Response.json({ canceled });
    const body = new ReadableStream({
      pull(controller) { controller.enqueue(new Uint8Array(2048)); },
      cancel() { canceled = true; },
    });
    try { await env.CHILD.fetch('http://child/hold', { method: 'POST', body }); return new Response('unexpected'); }
    catch (error) { return new Response(String(error)); }
  },
};
"#,
    ).await;
    // The producer never closes. The configured per-request cap must end it.
    let output = timeout(
        Duration::from_secs(2),
        service.invoke("fetch-parent".into(), test_invocation()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        String::from_utf8(output.body)
            .unwrap()
            .contains("max_request_body_bytes")
    );
    let state = service
        .invoke(
            "fetch-parent".into(),
            test_invocation_with_path("/state", "body-limit-state"),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&state.body).unwrap(),
        serde_json::json!({ "canceled": true })
    );
    wait_for_child_state(&service, 0, 0).await;
    service.shutdown().await.unwrap();
}
