//! Black-box test of the dd_server binary: deploy over the private API, then
//! exercise host routing and a live websocket round trip.

use futures_util::{SinkExt, StreamExt};
use std::process::{Child, Command};
use std::time::Duration;

struct ServerGuard {
    child: Child,
    _store: tempfile::TempDir,
    public_port: u16,
    private_port: u16,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn start_server() -> ServerGuard {
    let store = tempfile::tempdir().expect("tempdir");
    let public_port = free_port();
    let private_port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_dd_server"))
        .args([
            "--public-addr",
            &format!("127.0.0.1:{public_port}"),
            "--private-addr",
            &format!("127.0.0.1:{private_port}"),
            "--store-dir",
        ])
        .arg(store.path())
        .spawn()
        .expect("dd_server starts");
    let guard = ServerGuard {
        child,
        _store: store,
        public_port,
        private_port,
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::net::TcpStream::connect(("127.0.0.1", guard.private_port)).is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "dd_server did not start listening"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    guard
}

fn deploy(guard: &ServerGuard, name: &str, fixture: &str, public: bool) {
    let path = format!("{}/fixtures/{fixture}", env!("CARGO_MANIFEST_DIR"));
    let wasm = std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {path}: {e}"));
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(&wasm);
    let payload = serde_json::json!({
        "name": name,
        "wasm_base64": encoded,
        "config": { "public": public },
    });
    let response = ureq_post(
        &format!("http://127.0.0.1:{}/v1/deploy", guard.private_port),
        &payload.to_string(),
    );
    assert!(
        response.contains("\"ok\":true"),
        "deploy of {name} failed: {response}"
    );
}

/// Minimal HTTP client over std TcpStream: enough for the private API
/// without pulling an async client into the test.
fn ureq_post(url: &str, body: &str) -> String {
    let without_scheme = url.trim_start_matches("http://");
    let (authority, path) = without_scheme.split_once('/').expect("path");
    let mut stream = std::net::TcpStream::connect(authority).expect("connect");
    use std::io::{Read, Write};
    write!(
        stream,
        "POST /{path} HTTP/1.1\r\nhost: {authority}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("request write");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response read");
    response
}

#[test]
fn deployed_chat_worker_serves_http_and_websockets_by_host() {
    let guard = start_server();
    deploy(&guard, "chat", "chat_worker.wasm", true);
    deploy(&guard, "hello", "hello_worker.wasm", true);

    // Host-label routing on plain HTTP.
    let response = ureq_post(
        &format!(
            "http://127.0.0.1:{}/v1/invoke/hello/e2e",
            guard.private_port
        ),
        "",
    );
    assert!(
        response.contains("hello e2e"),
        "invoke through private API failed: {response}"
    );

    // Live websocket round trip with Host-based routing.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let request =
            tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(
                format!("ws://127.0.0.1:{}/", guard.public_port),
            )
            .map(|mut request| {
                request
                    .headers_mut()
                    .insert("host", "chat.example.com".parse().expect("host header"));
                request
            })
            .expect("client request");
        let (mut ws, _) = tokio::time::timeout(
            Duration::from_secs(5),
            tokio_tungstenite::connect_async(request),
        )
        .await
        .expect("connect timeout")
        .expect("websocket connects");

        let welcome = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("welcome timeout")
            .expect("frame")
            .expect("welcome frame");
        assert!(
            welcome
                .to_text()
                .expect("text frame")
                .starts_with("welcome"),
            "unexpected first frame: {welcome:?}"
        );

        ws.send(tokio_tungstenite::tungstenite::Message::Text("ping".into()))
            .await
            .expect("send");
        let broadcast = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("broadcast timeout")
            .expect("frame")
            .expect("broadcast frame");
        assert!(
            broadcast.to_text().expect("text frame").ends_with(": ping"),
            "unexpected broadcast: {broadcast:?}"
        );
        ws.close(None).await.expect("close");
    });
}
