use super::*;
use crate::{InspectorMode, InspectorSession, InspectorTarget};
use serde_json::json;

fn send(session: &InspectorSession, id: u32, method: &str, params: Value) {
    session.send(json!({ "id": id, "method": method, "params": params }).to_string());
}

/// Reads protocol messages until one matches, skipping the rest.
async fn next(session: &mut InspectorSession, matches: impl Fn(&Value) -> bool) -> Value {
    timeout(Duration::from_secs(10), async {
        loop {
            let message = session.recv().await.expect("inspector session open");
            let message: Value = serde_json::from_str(&message).expect("protocol JSON");
            if matches(&message) {
                return message;
            }
        }
    })
    .await
    .expect("expected protocol message")
}

async fn single_target(service: &RuntimeService) -> InspectorTarget {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let targets = service.inspector_targets();
        if let [target] = targets.as_slice() {
            return target.clone();
        }
        assert!(targets.len() <= 1, "one isolate, one target: {targets:?}");
        assert!(Instant::now() < deadline, "no inspector target appeared");
        sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
#[serial]
async fn devtools_sees_worker_console_calls_at_their_call_site() {
    let service = test_service(RuntimeConfig {
        inspector: InspectorMode::On,
        max_isolates: 1,
        ..RuntimeConfig::default()
    })
    .await;
    let mut console = service.subscribe_console();
    service
        .deploy(
            "inspected".into(),
            r#"let calls = 0;
export default {
  async fetch() {
    console.log("call", ++calls, { answer: 42 });
    console.trace("traced");
    return new Response(console.log.name);
  },
};
"#
            .into(),
        )
        .await
        .expect("worker deploys");
    let first = service
        .invoke("inspected".into(), test_invocation_with_path("/", "first"))
        .await
        .expect("first request");
    assert_eq!(first.body, b"log");

    let target = single_target(&service).await;
    assert_eq!(target.worker, "inspected");
    let mut session = target.connect();
    send(&session, 1, "Runtime.enable", json!({}));
    next(&mut session, |message| message["id"] == 1).await;
    service
        .invoke("inspected".into(), test_invocation_with_path("/", "second"))
        .await
        .expect("second request");

    let logged = next(&mut session, |message| {
        message["method"] == "Runtime.consoleAPICalled"
    })
    .await;
    let logged = &logged["params"];
    // The first request's call happened before the session: it is not replayed.
    assert_eq!(logged["type"], "log");
    assert_eq!(logged["args"][1]["value"], 2);
    assert_eq!(logged["args"][2]["type"], "object");
    let top = &logged["stackTrace"]["callFrames"][0];
    assert_eq!(top["url"], crate::assets::WORKER_SPECIFIER);
    assert_eq!(top["lineNumber"], 3);
    let traced = next(&mut session, |message| {
        message["method"] == "Runtime.consoleAPICalled"
    })
    .await;
    assert_eq!(traced["params"]["type"], "trace");

    // dd's own console output is unchanged, its trace included.
    let mut lines = Vec::new();
    while let Ok(Ok(line)) = timeout(Duration::from_millis(500), console.recv()).await {
        lines.push(line.message);
    }
    assert!(
        lines.contains(&"call 2 { answer: 42 }".to_string()),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|line| line.starts_with("Trace: traced\n")
            && line.contains(crate::assets::WORKER_SPECIFIER)),
        "{lines:?}"
    );
}

#[tokio::test]
#[serial]
async fn waiting_isolates_stop_at_top_level_breakpoints() {
    let service = test_service(RuntimeConfig {
        inspector: InspectorMode::Wait,
        max_isolates: 1,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy(
            "waiting".into(),
            r#"const loaded = "top-level";
export default { fetch() { return new Response(loaded); } };
"#
            .into(),
        )
        .await
        .expect("worker deploys");
    let request = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .invoke("waiting".into(), test_invocation_with_path("/", "waiting"))
                .await
        }
    });
    let target = single_target(&service).await;
    sleep(Duration::from_millis(200)).await;
    assert!(!request.is_finished(), "the isolate waits for a debugger");

    let mut session = target.connect();
    send(&session, 1, "Debugger.enable", json!({}));
    send(
        &session,
        2,
        "Debugger.setBreakpointByUrl",
        json!({ "url": crate::assets::WORKER_SPECIFIER, "lineNumber": 0 }),
    );
    next(&mut session, |message| message["id"] == 2).await;
    send(&session, 3, "Runtime.runIfWaitingForDebugger", json!({}));
    let paused = next(&mut session, |message| {
        message["method"] == "Debugger.paused"
    })
    .await;
    assert_eq!(
        paused["params"]["hitBreakpoints"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        paused["params"]["callFrames"][0]["location"]["lineNumber"],
        0
    );
    sleep(Duration::from_millis(100)).await;
    assert!(!request.is_finished(), "the isolate is paused");
    send(&session, 4, "Debugger.resume", json!({}));
    let output = timeout(Duration::from_secs(10), request)
        .await
        .expect("request finishes once resumed")
        .expect("request task")
        .expect("request succeeds");
    assert_eq!(output.body, b"top-level");
}
