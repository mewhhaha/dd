//! End-to-end tests against real Perry-compiled fixtures.
//!
//! Regenerate fixtures with `scripts/build-perry-wasm-fixtures.sh` (requires
//! the `perry` compiler from npm: `npm install @perryts/perry`).

use common::{WorkerInvocation, WorkerOutput};
use wasm_host::{InvokeOptions, WorkerModule};

fn invoke(fixture: &str, invocation: WorkerInvocation) -> common::Result<WorkerOutput> {
    let path = format!("{}/fixtures/{fixture}", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {path}: {e}"));
    let module = WorkerModule::from_bytes(&bytes)?;
    module.invoke(invocation, InvokeOptions::default())
}

fn get(url: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: url.to_string(),
        headers: vec![("User-Agent".to_string(), "dd-test/1".to_string())],
        body: Vec::new(),
        request_id: "test".to_string(),
    }
}

#[test]
fn hello_worker_builds_json_response_from_url_and_headers() {
    let output = invoke("hello_worker.wasm", get("http://w.local/perry")).expect("invoke");
    assert_eq!(output.status, 200);
    assert!(
        output
            .headers
            .iter()
            .any(|(k, v)| k == "content-type" && v == "application/json"),
        "headers were {:?}",
        output.headers
    );
    let body: serde_json::Value =
        serde_json::from_slice(&output.body).expect("body should be JSON");
    assert_eq!(body["greeting"], "hello perry");
    assert_eq!(body["method"], "GET");
    assert_eq!(body["agent"], "dd-test/1");
    assert_eq!(body["echo"], "");
}

#[test]
fn hello_worker_defaults_root_path_to_world() {
    let output = invoke("hello_worker.wasm", get("http://w.local/")).expect("invoke");
    let body: serde_json::Value = serde_json::from_slice(&output.body).expect("JSON body");
    assert_eq!(body["greeting"], "hello world");
}

#[test]
fn hello_worker_echoes_request_body() {
    let mut invocation = get("http://w.local/perry");
    invocation.method = "POST".to_string();
    invocation.body = b"payload".to_vec();
    let output = invoke("hello_worker.wasm", invocation).expect("invoke");
    let body: serde_json::Value = serde_json::from_slice(&output.body).expect("JSON body");
    assert_eq!(body["method"], "POST");
    assert_eq!(body["echo"], "payload");
}

#[test]
fn features_worker_runs_classes_sort_and_json_parse() {
    let mut invocation = get("http://w.local/");
    invocation.method = "POST".to_string();
    invocation.body = br#"{"tenant":"acme"}"#.to_vec();
    let output = invoke("features_worker.wasm", invocation).expect("invoke");
    assert_eq!(output.status, 200);
    assert!(
        output
            .headers
            .iter()
            .any(|(k, v)| k == "x-greet" && v == "hey, dd"),
        "headers were {:?}",
        output.headers
    );
    let body: serde_json::Value = serde_json::from_slice(&output.body).expect("JSON body");
    assert_eq!(body["sorted"], serde_json::json!([1, 2, 3]));
    assert_eq!(body["parsed"]["tenant"], "acme");
    assert_eq!(body["upper"], "ABC");
}

#[test]
fn module_without_worker_exports_is_reported_clearly() {
    // Minimal valid wasm (magic + version only): loads, but cannot serve.
    let module = WorkerModule::from_bytes(&[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00])
        .expect("an empty module is still valid wasm");
    let error = module
        .invoke(get("http://w.local/"), InvokeOptions::default())
        .expect_err("empty module cannot serve requests");
    let message = error.to_string();
    assert!(
        message.contains("__indirect_function_table") || message.contains("_start"),
        "unexpected error: {message}"
    );
}
