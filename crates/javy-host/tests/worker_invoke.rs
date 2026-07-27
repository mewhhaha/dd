use common::{WorkerInvocation, WorkerOutput};
use javy_host::{InvokeOptions, JavyWorker, WorkerOptions};
use std::collections::HashMap;
use std::time::Duration;

fn fixture_bytes(name: &str) -> Vec<u8> {
    let path = format!("{}/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|error| panic!("missing fixture {path}: {error}"))
}

fn get(url: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: url.to_string(),
        headers: vec![("user-agent".to_string(), "javy-test".to_string())],
        body: Vec::new(),
        request_id: "javy-test-request".to_string(),
    }
}

fn invoke(worker: &JavyWorker, url: &str) -> WorkerOutput {
    worker
        .invoke(get(url), InvokeOptions::default())
        .expect("worker invocation")
}

#[test]
fn react_renders_html_inside_the_javy_wasm_worker() {
    let worker = JavyWorker::from_bytes(&fixture_bytes("react_worker.wasm")).expect("worker");
    let output = invoke(&worker, "http://worker.local/?name=Ada");

    assert_eq!(output.status, 200);
    assert!(
        output
            .headers
            .iter()
            .any(|(name, value)| name == "content-type" && value.starts_with("text/html")),
        "headers were {:?}",
        output.headers
    );
    let html = String::from_utf8(output.body).expect("HTML body");
    assert!(html.contains("Hello, <!-- -->Ada"), "{html}");
    assert!(html.contains("React inside QuickJS inside Wasm"), "{html}");
}

#[test]
fn react_readable_stream_is_buffered_into_the_worker_response() {
    let worker = JavyWorker::from_bytes(&fixture_bytes("react_worker.wasm")).expect("worker");
    let output = invoke(&worker, "http://worker.local/stream?name=Lin");

    assert_eq!(output.status, 200);
    let html = String::from_utf8(output.body).expect("HTML body");
    assert!(html.contains("Hello, <!-- -->Lin"), "{html}");
    assert!(html.contains("React inside QuickJS inside Wasm"), "{html}");
}

#[test]
fn request_headers_query_parameters_and_env_reach_the_worker() {
    let worker = JavyWorker::new(
        &fixture_bytes("react_worker.wasm"),
        WorkerOptions {
            env: HashMap::from([("GREETING".to_string(), "welcome".to_string())]),
            ..WorkerOptions::default()
        },
    )
    .expect("worker");
    let output = invoke(&worker, "http://worker.local/json?name=Grace");
    let body: serde_json::Value = serde_json::from_slice(&output.body).expect("JSON body");

    assert_eq!(body["greeting"], "welcome");
    assert_eq!(body["method"], "GET");
    assert_eq!(body["name"], "Grace");
    assert_eq!(body["userAgent"], "javy-test");
}

#[test]
fn worker_crypto_uses_host_supplied_secure_randomness() {
    let worker = JavyWorker::from_bytes(&fixture_bytes("react_worker.wasm")).expect("worker");
    let first: serde_json::Value =
        serde_json::from_slice(&invoke(&worker, "http://worker.local/crypto").body)
            .expect("first JSON body");
    let second: serde_json::Value =
        serde_json::from_slice(&invoke(&worker, "http://worker.local/crypto").body)
            .expect("second JSON body");

    let bytes = first["bytes"].as_array().expect("random byte array");
    assert_eq!(bytes.len(), 16);
    assert!(
        bytes
            .iter()
            .all(|byte| byte.as_u64().is_some_and(|byte| byte <= 255))
    );
    let uuid = first["uuid"].as_str().expect("random UUID");
    assert_eq!(uuid.len(), 36);
    assert_eq!(&uuid[14..15], "4");
    assert!(matches!(&uuid[19..20], "8" | "9" | "a" | "b"));
    assert_ne!(first, second);
}

#[test]
fn kv_and_transactional_memory_persist_between_fresh_wasm_instances() {
    let worker = JavyWorker::new(
        &fixture_bytes("react_worker.wasm"),
        WorkerOptions {
            kv_bindings: vec!["TEST_KV".to_string()],
            memory_bindings: vec!["TEST_MEMORY".to_string()],
            ..WorkerOptions::default()
        },
    )
    .expect("worker");

    let first: serde_json::Value =
        serde_json::from_slice(&invoke(&worker, "http://worker.local/bindings?name=first").body)
            .expect("first JSON body");
    let second: serde_json::Value =
        serde_json::from_slice(&invoke(&worker, "http://worker.local/bindings?name=second").body)
            .expect("second JSON body");

    assert_eq!(first, serde_json::json!({ "count": 1, "previous": null }));
    assert_eq!(
        second,
        serde_json::json!({ "count": 2, "previous": "first" })
    );
}

#[test]
fn runaway_worker_stops_at_the_execution_deadline() {
    let worker = JavyWorker::from_bytes(&fixture_bytes("infinite_worker.wasm")).expect("worker");
    let error = worker
        .invoke(
            get("http://worker.local/"),
            InvokeOptions {
                timeout: Duration::from_millis(20),
            },
        )
        .expect_err("infinite worker must be interrupted");

    let message = error.to_string();
    assert!(message.contains("wasm trap: interrupt"), "{message}");
    assert!(message.contains("javy-test-request"), "{message}");
}

#[test]
fn module_without_start_export_is_rejected_at_load_time() {
    let Err(error) = JavyWorker::from_bytes(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00])
    else {
        panic!("empty module cannot serve requests");
    };
    assert!(error.to_string().contains("_start"), "{error}");
}
