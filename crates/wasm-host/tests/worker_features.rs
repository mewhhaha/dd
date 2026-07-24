//! End-to-end tests for the dd feature surface (KV, memory namespaces,
//! cache, service bindings, outbound fetch) against real Perry-compiled
//! fixtures. Regenerate fixtures with `scripts/build-perry-wasm-fixtures.sh`.

use common::{WorkerInvocation, WorkerOutput};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::{Arc, RwLock};
use wasm_host::{InvokeOptions, ServiceRegistry, WorkerModule, WorkerOptions, WorkerStores};

fn fixture_bytes(fixture: &str) -> Vec<u8> {
    let path = format!("{}/fixtures/{fixture}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {path}: {e}"))
}

fn temp_store() -> (tempfile::TempDir, Arc<WorkerStores>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let stores = futures_block_on(WorkerStores::open(dir.path())).expect("stores open");
    (dir, stores)
}

/// The engine blocks on its own io runtime internally; tests only need a
/// one-off block_on for store setup.
fn futures_block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn get(url: &str) -> WorkerInvocation {
    WorkerInvocation {
        method: "GET".to_string(),
        url: url.to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: "test".to_string(),
    }
}

fn invoke(module: &WorkerModule, invocation: WorkerInvocation) -> WorkerOutput {
    module
        .invoke(invocation, InvokeOptions::default())
        .expect("invoke")
}

#[test]
fn memory_atomic_counts_per_key_and_kv_persists_across_requests() {
    let (_dir, stores) = temp_store();
    let module = WorkerModule::new(
        &fixture_bytes("stateful_worker.wasm"),
        WorkerOptions {
            name: Some("stateful".to_string()),
            stores: Some(stores),
            services: None,
        },
    )
    .expect("module");

    let first: serde_json::Value =
        serde_json::from_slice(&invoke(&module, get("http://w.local/alice")).body).expect("json");
    assert_eq!(first["user"], "alice");
    assert_eq!(first["count"], 1);
    assert_eq!(first["previous"], serde_json::Value::Null);

    let second: serde_json::Value =
        serde_json::from_slice(&invoke(&module, get("http://w.local/alice")).body).expect("json");
    assert_eq!(second["count"], 2, "same key increments");
    assert_eq!(second["previous"], "alice", "kv survives across requests");

    let other: serde_json::Value =
        serde_json::from_slice(&invoke(&module, get("http://w.local/bob")).body).expect("json");
    assert_eq!(other["count"], 1, "different key gets its own counter");
}

#[test]
fn cache_misses_then_hits_for_the_same_url() {
    let (_dir, stores) = temp_store();
    let module = WorkerModule::new(
        &fixture_bytes("edge_worker.wasm"),
        WorkerOptions {
            name: Some("edge".to_string()),
            stores: Some(stores),
            services: None,
        },
    )
    .expect("module");

    let first = invoke(&module, get("http://w.local/page"));
    assert!(
        first
            .headers
            .iter()
            .any(|(k, v)| k == "x-cache" && v == "miss"),
        "headers were {:?}",
        first.headers
    );
    assert_eq!(first.body, b"computed:/page");

    let second = invoke(&module, get("http://w.local/page"));
    assert!(
        second
            .headers
            .iter()
            .any(|(k, v)| k == "x-cache" && v == "hit"),
        "headers were {:?}",
        second.headers
    );
    assert_eq!(second.body, b"computed:/page");
}

#[test]
fn service_binding_reaches_co_deployed_worker() {
    let (_dir, stores) = temp_store();
    let services: ServiceRegistry = Arc::new(RwLock::new(HashMap::new()));
    let auth = Arc::new(
        WorkerModule::new(
            &fixture_bytes("auth_worker.wasm"),
            WorkerOptions {
                name: Some("auth".to_string()),
                stores: None,
                services: None,
            },
        )
        .expect("auth module"),
    );
    services
        .write()
        .expect("registry")
        .insert("auth".to_string(), auth);
    let edge = WorkerModule::new(
        &fixture_bytes("edge_worker.wasm"),
        WorkerOptions {
            name: Some("edge".to_string()),
            stores: Some(stores),
            services: Some(services),
        },
    )
    .expect("edge module");

    let output = invoke(&edge, get("http://w.local/auth"));
    assert_eq!(output.status, 200);
    assert_eq!(output.body, b"session:ok");
}

#[test]
fn outbound_fetch_proxies_a_local_http_server() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut buffer = [0u8; 1024];
            let _ = stream.read(&mut buffer);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\nconnection: close\r\n\r\nupstream",
            );
        }
    });

    let module = WorkerModule::from_bytes(&fixture_bytes("edge_worker.wasm")).expect("module");
    let mut invocation = get("http://w.local/proxy");
    invocation.method = "POST".to_string();
    invocation.body = format!("http://127.0.0.1:{port}/data").into_bytes();
    let output = invoke(&module, invocation);
    assert_eq!(output.status, 200);
    assert_eq!(output.body, b"upstream");
    assert!(
        output
            .headers
            .iter()
            .any(|(k, v)| k == "x-proxied" && v == "yes"),
        "headers were {:?}",
        output.headers
    );
}

#[test]
fn storage_functions_error_clearly_without_a_store() {
    let module = WorkerModule::from_bytes(&fixture_bytes("stateful_worker.wasm")).expect("module");
    let error = module
        .invoke(get("http://w.local/alice"), InvokeOptions::default())
        .expect_err("no store attached");
    assert!(
        error.to_string().contains("--store-dir"),
        "unexpected error: {error}"
    );
}
