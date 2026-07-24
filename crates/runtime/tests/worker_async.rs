//! Promise-returning fetch handlers: `dd_sleep(...).then(...)` and native
//! `fetch(url).then(r => r.text())` chains, resolved by the engine's event
//! loop against a real Perry-compiled fixture.

use common::{WorkerInvocation, WorkerOutput};
use runtime::{InvokeOptions, WorkerModule};
use std::io::{Read, Write};

fn async_module() -> WorkerModule {
    let path = format!("{}/fixtures/async_worker.wasm", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {path}: {e}"));
    WorkerModule::from_bytes(&bytes).expect("module")
}

fn invoke(module: &WorkerModule, method: &str, url: &str, body: &[u8]) -> WorkerOutput {
    module
        .invoke(
            WorkerInvocation {
                method: method.to_string(),
                url: url.to_string(),
                headers: Vec::new(),
                body: body.to_vec(),
                request_id: "test".to_string(),
            },
            InvokeOptions::default(),
        )
        .expect("invoke")
}

#[test]
fn handler_returning_a_sleep_promise_resolves_through_the_event_loop() {
    let module = async_module();
    let started = std::time::Instant::now();
    let output = invoke(&module, "GET", "http://w.local/", b"");
    assert_eq!(output.status, 201);
    assert!(
        output
            .headers
            .iter()
            .any(|(k, v)| k == "x-async" && v == "yes"),
        "headers were {:?}",
        output.headers
    );
    assert_eq!(output.body, b"slept:GET");
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(20),
        "response arrived before the sleep elapsed"
    );
}

#[test]
fn native_fetch_then_chain_proxies_a_local_upstream() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut buffer = [0u8; 1024];
            let _ = stream.read(&mut buffer);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\nconnection: close\r\n\r\nhello",
            );
        }
    });

    let module = async_module();
    let output = invoke(
        &module,
        "POST",
        "http://w.local/proxy",
        format!("http://127.0.0.1:{port}/data").as_bytes(),
    );
    assert_eq!(output.status, 200);
    assert!(
        output
            .headers
            .iter()
            .any(|(k, v)| k == "x-via" && v == "native-fetch"),
        "headers were {:?}",
        output.headers
    );
    assert_eq!(output.body, b"upstream said: hello");
}
