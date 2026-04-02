use common::{DeployBinding, DeployConfig, DeployRequest};
use quiche::h3::{Header, NameValue};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

const MAX_QUIC_DATAGRAM_SIZE: usize = 1350;

#[test]
fn blackbox_public_h3_websocket_echoes_text_and_binary() {
    std::thread::Builder::new()
        .name("dd-h3-websocket-echo-blackbox".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            runtime.block_on(async {
                let _guard = test_lock().lock().await;
                let harness = ServerHarness::start().await;
                harness
                    .deploy_public_worker(
                        "echo",
                        r#"
                export function openSocket(state, payload) {
                  const { response } = state.accept(payload.request);
                  return response;
                }

                export async function echoFrame(stub, event) {
                  if (event.data === "bye") {
                    await stub.apply([{ type: "socket.close", handle: event.handle, code: 4001, reason: "done" }]);
                    return;
                  }
                  if (ArrayBuffer.isView(event.data) || event.data instanceof ArrayBuffer) {
                    await stub.apply([{ type: "socket.send", handle: event.handle, payload: event.data, kind: "binary" }]);
                    return;
                  }
                  await stub.apply([{ type: "socket.send", handle: event.handle, payload: `echo:${event.data}`, kind: "text" }]);
                }

                export default {
                  async fetch(request, env) {
                    return await env.CHAT.get(env.CHAT.idFromName("global")).atomic(openSocket, { request });
                  },

                  async wake(event, env) {
                    if (event.type === "socketmessage") {
                      await echoFrame(event.stub, event);
                    }
                  }
                };
            "#,
                        DeployConfig {
                            public: true,
                            bindings: vec![DeployBinding::Actor {
                                binding: "CHAT".to_string(),
                            }],
                            ..Default::default()
                        },
                    )
                    .await;

                let mut client = RawQuicheH3Client::connect(harness.public_addr).await;
                let stream_id = client
                    .send_request(
                        vec![
                            Header::new(b":method", b"CONNECT"),
                            Header::new(b":scheme", b"https"),
                            Header::new(
                                b":authority",
                                format!("echo.localhost:{}", harness.public_addr.port())
                                    .as_bytes(),
                            ),
                            Header::new(b":path", b"/ws"),
                            Header::new(b":protocol", b"websocket"),
                            Header::new(b"sec-websocket-version", b"13"),
                            Header::new(b"sec-websocket-key", b"dGhlIHNhbXBsZSBub25jZQ=="),
                        ],
                        false,
                    )
                    .await;

                let response = client.recv_response_headers(stream_id).await;
                assert_eq!(header_value(&response, ":status"), Some("200"));
                assert_eq!(header_value(&response, "connection"), None);
                assert_eq!(header_value(&response, "upgrade"), None);
                assert_eq!(header_value(&response, "sec-websocket-accept"), None);

                client
                    .send_websocket_frame(stream_id, WebSocketFrame::Text("hello".to_string()))
                    .await;
                assert_eq!(
                    client.recv_websocket_frame(stream_id).await,
                    WebSocketFrame::Text("echo:hello".to_string())
                );

                client
                    .send_websocket_frame(stream_id, WebSocketFrame::Binary(vec![1, 2, 3, 4]))
                    .await;
                assert_eq!(
                    client.recv_websocket_frame(stream_id).await,
                    WebSocketFrame::Binary(vec![1, 2, 3, 4])
                );

                client
                    .send_websocket_frame(stream_id, WebSocketFrame::Ping(vec![9, 8, 7]))
                    .await;
                assert_eq!(
                    client.recv_websocket_frame(stream_id).await,
                    WebSocketFrame::Pong(vec![9, 8, 7])
                );

                client
                    .send_websocket_frame(stream_id, WebSocketFrame::Text("bye".to_string()))
                    .await;
                assert_eq!(
                    client.recv_websocket_frame(stream_id).await,
                    WebSocketFrame::Close {
                        code: 4001,
                        reason: "done".to_string(),
                    }
                );
            });
        })
        .expect("spawn websocket echo blackbox thread")
        .join()
        .expect("websocket echo blackbox thread panicked");
}

#[test]
fn blackbox_public_h3_websocket_storage_on_message_does_not_close() {
    std::thread::Builder::new()
        .name("dd-h3-websocket-storage-blackbox".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            runtime.block_on(async {
                let _guard = test_lock().lock().await;
                let harness = ServerHarness::start().await;
                harness
                    .deploy_public_worker(
                        "chat",
                        r#"
                export function openSocket(state, payload) {
                  const { response } = state.accept(payload.request);
                  return response;
                }

                export function onMessage(state, event) {
                  const text = typeof event.data === "string"
                    ? event.data
                    : new TextDecoder().decode(event.data);
                  const stored = state.get("chat");
                  const next = {
                    count: Number(stored?.count ?? 0) + 1,
                    last: text,
                  };
                  state.set("chat", next);
                  return {
                    value: next,
                    effects: [{
                      type: "socket.send",
                      handle: event.handle,
                      payload: JSON.stringify({
                        seen: next.last,
                        count: next.count
                      }),
                      kind: "text",
                    }],
                  };
                }

                export default {
                  async fetch(request, env) {
                    return await env.CHAT.get(env.CHAT.idFromName("global")).atomic(openSocket, { request });
                  },

                  async wake(event, env) {
                    if (event.type === "socketmessage") {
                      const tx = await event.stub.atomic(onMessage, event);
                      await event.stub.apply(tx.effects ?? []);
                    }
                  }
                };
            "#,
                        DeployConfig {
                            public: true,
                            bindings: vec![DeployBinding::Actor {
                                binding: "CHAT".to_string(),
                            }],
                            ..Default::default()
                        },
                    )
                    .await;

                let mut client = RawQuicheH3Client::connect(harness.public_addr).await;
                let stream_id = client
                    .send_request(
                        vec![
                            Header::new(b":method", b"CONNECT"),
                            Header::new(b":scheme", b"https"),
                            Header::new(
                                b":authority",
                                format!("chat.localhost:{}", harness.public_addr.port())
                                    .as_bytes(),
                            ),
                            Header::new(b":path", b"/ws"),
                            Header::new(b":protocol", b"websocket"),
                            Header::new(b"sec-websocket-version", b"13"),
                            Header::new(b"sec-websocket-key", b"dGhlIHNhbXBsZSBub25jZQ=="),
                        ],
                        false,
                    )
                    .await;

                let response = client.recv_response_headers(stream_id).await;
                assert_eq!(header_value(&response, ":status"), Some("200"));

                client
                    .send_websocket_frame(stream_id, WebSocketFrame::Text("ready".to_string()))
                    .await;
                assert_eq!(
                    client.recv_websocket_frame(stream_id).await,
                    WebSocketFrame::Text(r#"{"seen":"ready","count":1}"#.to_string())
                );

                client
                    .send_websocket_frame(stream_id, WebSocketFrame::Text("again".to_string()))
                    .await;
                assert_eq!(
                    client.recv_websocket_frame(stream_id).await,
                    WebSocketFrame::Text(r#"{"seen":"again","count":2}"#.to_string())
                );
            });
        })
        .expect("spawn websocket storage blackbox thread")
        .join()
        .expect("websocket storage blackbox thread panicked");
}

#[test]
fn blackbox_public_h3_rejects_unknown_extended_connect_protocol() {
    std::thread::Builder::new()
        .name("dd-h3-invalid-connect-blackbox".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            runtime.block_on(async {
                let _guard = test_lock().lock().await;
                let harness = ServerHarness::start().await;

                let mut client = RawQuicheH3Client::connect(harness.public_addr).await;
                let stream_id = client
                    .send_request(
                        vec![
                            Header::new(b":method", b"CONNECT"),
                            Header::new(b":scheme", b"https"),
                            Header::new(
                                b":authority",
                                format!("echo.localhost:{}", harness.public_addr.port()).as_bytes(),
                            ),
                            Header::new(b":path", b"/session"),
                            Header::new(b":protocol", b"not-webtransport"),
                        ],
                        true,
                    )
                    .await;

                let response = client.recv_response_headers(stream_id).await;
                assert_eq!(header_value(&response, ":status"), Some("501"));

                let body = client.recv_body_chunk(stream_id).await;
                assert!(
                    String::from_utf8_lossy(&body)
                        .contains("unsupported extended CONNECT protocol"),
                    "unexpected error body: {}",
                    String::from_utf8_lossy(&body)
                );
            });
        })
        .expect("spawn invalid connect blackbox thread")
        .join()
        .expect("invalid connect blackbox thread panicked");
}

#[test]
fn blackbox_public_h3_transport_echoes_stream() {
    std::thread::Builder::new()
        .name("dd-h3-transport-blackbox".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            runtime.block_on(async {
                let _guard = test_lock().lock().await;
                tokio::time::timeout(Duration::from_secs(15), async {
                    let harness = ServerHarness::start().await;
                    harness
                        .deploy_public_worker(
                            "transport",
                            r#"
                export default {
                  async fetch(request, env) {
                    return await env.MEDIA.get(env.MEDIA.idFromName("global")).atomic((state) => {
                      const { response } = state.accept(request);
                      return response;
                    });
                  },

                  async wake(event, env) {
                    if (event.type !== "transportstream" || !event.stub || !event.handle) {
                      return;
                    }
                    const session = await event.stub.transports.session(event.handle);
                    const writer = session.stream.writable.getWriter();
                    await writer.write(event.data);
                  }
                };
            "#,
                            DeployConfig {
                                public: true,
                                bindings: vec![DeployBinding::Actor {
                                    binding: "MEDIA".to_string(),
                                }],
                                ..Default::default()
                            },
                        )
                        .await;

                    let mut client = RawQuicheH3Client::connect(harness.public_addr).await;
                    let stream_id = client
                        .send_request(
                            vec![
                                Header::new(b":method", b"CONNECT"),
                                Header::new(b":scheme", b"https"),
                                Header::new(
                                    b":authority",
                                    format!("transport.localhost:{}", harness.public_addr.port())
                                        .as_bytes(),
                                ),
                                Header::new(b":path", b"/session"),
                                Header::new(b":protocol", b"webtransport"),
                            ],
                            false,
                        )
                        .await;

                    let response = client.recv_response_headers(stream_id).await;
                    assert_eq!(header_value(&response, ":status"), Some("200"));
                    assert_eq!(header_value(&response, "connection"), None);
                    assert_eq!(header_value(&response, "upgrade"), None);

                    client
                        .send_body_chunk(stream_id, b"stream-hello", false)
                        .await;
                    assert_eq!(client.recv_body_chunk(stream_id).await, b"stream-hello");
                })
                .await
                .expect("transport blackbox test timed out");
            });
        })
        .expect("spawn transport blackbox thread")
        .join()
        .expect("transport blackbox thread panicked");
}

#[test]
fn blackbox_public_h3_blocks_control_plane_routes() {
    std::thread::Builder::new()
        .name("dd-public-h3-blocks-control-plane".to_string())
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime");
            runtime.block_on(async {
                let _guard = test_lock().lock().await;
                let harness = ServerHarness::start().await;
                let response = send_h3_request(
                    harness.public_addr,
                    "localhost",
                    "POST",
                    "/v1/deploy",
                    &[("content-type".to_string(), "application/json".to_string())],
                    b"{}",
                )
                .await;
                assert_eq!(response.0, 404);
            });
        })
        .expect("spawn public h3 block control-plane thread")
        .join()
        .expect("public h3 block control-plane thread panicked");
}

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct ServerHarness {
    _root_dir: PathBuf,
    public_addr: std::net::SocketAddr,
    private_addr: std::net::SocketAddr,
    child: tokio::sync::Mutex<Child>,
}

impl ServerHarness {
    async fn start() -> Self {
        let root_dir = std::env::temp_dir().join(format!("dd-h3-blackbox-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root_dir)
            .await
            .expect("create blackbox test root");
        let cert_dir = root_dir.join("certs");
        tokio::fs::create_dir_all(&cert_dir)
            .await
            .expect("create cert dir");

        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate test cert");
        let cert_path = cert_dir.join("public-cert.pem");
        let key_path = cert_dir.join("public-key.pem");
        tokio::fs::write(&cert_path, certified.cert.pem())
            .await
            .expect("write cert");
        tokio::fs::write(&key_path, certified.signing_key.serialize_pem())
            .await
            .expect("write key");

        let public_addr = reserve_socket_addr();
        let private_addr = reserve_socket_addr();

        let mut child = Command::new(env!("CARGO_BIN_EXE_dd_server"))
            .current_dir(&root_dir)
            .env("BIND_PUBLIC_ADDR", public_addr.to_string())
            .env("BIND_PRIVATE_ADDR", private_addr.to_string())
            .env("PUBLIC_BASE_DOMAIN", "localhost")
            .env("PUBLIC_TLS_CERT_PATH", &cert_path)
            .env("PUBLIC_TLS_KEY_PATH", &key_path)
            .env("RUST_LOG", "warn")
            .spawn()
            .expect("spawn dd_server");

        wait_for_private_listener(private_addr, &mut child).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        Self {
            _root_dir: root_dir,
            public_addr,
            private_addr,
            child: tokio::sync::Mutex::new(child),
        }
    }

    async fn deploy_public_worker(&self, name: &str, source: &str, config: DeployConfig) {
        let response = post_http_json(
            self.private_addr,
            "/v1/deploy",
            &DeployRequest {
                name: name.to_string(),
                source: source.to_string(),
                config,
            },
        )
        .await;
        assert_eq!(response.0, 200, "deploy failed: {}", response.1);
    }
}

impl Drop for ServerHarness {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.try_lock() {
            let _ = child.start_kill();
        }
    }
}

async fn wait_for_private_listener(private_addr: std::net::SocketAddr, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            panic!("dd_server exited before becoming ready: {status}");
        }

        match tokio::net::TcpStream::connect(private_addr).await {
            Ok(mut stream) => {
                let request = b"POST /v1/deploy HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
                if stream.write_all(request).await.is_ok() {
                    let mut response = Vec::new();
                    let _ = stream.read_to_end(&mut response).await;
                    return;
                }
            }
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await
            }
            Err(error) => panic!("private listener did not become ready: {error}"),
        }
    }
}

async fn post_http_json<T: serde::Serialize>(
    address: std::net::SocketAddr,
    path: &str,
    payload: &T,
) -> (u16, String) {
    let body = serde_json::to_vec(payload).expect("serialize request");
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect http endpoint");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request head");
    stream.write_all(&body).await.expect("write request body");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    parse_http_response(&response)
}

fn parse_http_response(bytes: &[u8]) -> (u16, String) {
    let text = String::from_utf8_lossy(bytes);
    let mut lines = text.lines();
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let body = text
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_string();
    (status, body)
}

async fn send_h3_request(
    address: std::net::SocketAddr,
    authority_host: &str,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> (u16, String) {
    let mut client = RawQuicheH3Client::connect(address).await;
    let mut request_headers = vec![
        Header::new(b":method", method.as_bytes()),
        Header::new(b":scheme", b"https"),
        Header::new(
            b":authority",
            format!("{authority_host}:{}", address.port()).as_bytes(),
        ),
        Header::new(b":path", path.as_bytes()),
    ];
    for (name, value) in headers {
        request_headers.push(Header::new(name.as_bytes(), value.as_bytes()));
    }
    let stream_id = client.send_request(request_headers, body.is_empty()).await;
    if !body.is_empty() {
        client.send_body_chunk(stream_id, body, true).await;
    }
    let headers = client.recv_response_headers(stream_id).await;
    let status = header_value(&headers, ":status")
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or_default();
    let body = client.recv_full_body(stream_id).await;
    (status, String::from_utf8_lossy(&body).to_string())
}

struct RawQuicheH3Client {
    socket: tokio::net::UdpSocket,
    local_addr: std::net::SocketAddr,
    conn: quiche::Connection,
    h3: quiche::h3::Connection,
    read_buffer: [u8; 65535],
    write_buffer: [u8; 65535],
    websocket_buffer: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WebSocketFrame {
    Text(String),
    Binary(Vec<u8>),
    Close { code: u16, reason: String },
    Ping(Vec<u8>),
    Pong(Vec<u8>),
}

impl RawQuicheH3Client {
    async fn connect(public_addr: std::net::SocketAddr) -> Self {
        Self::try_connect(public_addr)
            .await
            .expect("connect raw quiche client")
    }

    async fn try_connect(public_addr: std::net::SocketAddr) -> Result<Self, String> {
        let bind_addr = match public_addr {
            std::net::SocketAddr::V4(_) => "0.0.0.0:0",
            std::net::SocketAddr::V6(_) => "[::]:0",
        };
        let socket = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .map_err(|error| format!("bind raw quiche client socket: {error}"))?;
        let local_addr = socket
            .local_addr()
            .map_err(|error| format!("raw quiche client local addr: {error}"))?;

        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)
            .map_err(|error| format!("quiche config: {error}"))?;
        config.verify_peer(false);
        config
            .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
            .map_err(|error| format!("set h3 alpn: {error}"))?;
        config.set_max_idle_timeout(5_000);
        config.set_max_recv_udp_payload_size(MAX_QUIC_DATAGRAM_SIZE);
        config.set_max_send_udp_payload_size(MAX_QUIC_DATAGRAM_SIZE);
        config.set_initial_max_data(10_000_000);
        config.set_initial_max_stream_data_bidi_local(1_000_000);
        config.set_initial_max_stream_data_bidi_remote(1_000_000);
        config.set_initial_max_stream_data_uni(1_000_000);
        config.set_initial_max_streams_bidi(100);
        config.set_initial_max_streams_uni(100);
        config.set_disable_active_migration(true);
        config.enable_dgram(true, 100, 100);

        let scid_bytes = *Uuid::new_v4().as_bytes();
        let scid = quiche::ConnectionId::from_ref(&scid_bytes);
        let mut conn = quiche::connect(
            Some("localhost"),
            &scid,
            local_addr,
            public_addr,
            &mut config,
        )
        .map_err(|error| format!("connect raw quiche client: {error}"))?;
        let mut read_buffer = [0; 65535];
        let mut write_buffer = [0; 65535];

        flush_quiche_connection(&socket, &mut conn, &mut write_buffer).await;
        while !conn.is_established() {
            pump_quiche_connection(
                &socket,
                local_addr,
                &mut conn,
                &mut read_buffer,
                &mut write_buffer,
                Duration::from_millis(50),
            )
            .await;
        }

        let h3 = quiche::h3::Connection::with_transport(
            &mut conn,
            &quiche::h3::Config::new().map_err(|error| format!("h3 config: {error}"))?,
        )
        .map_err(|error| format!("create raw h3 connection: {error}"))?;

        Ok(Self {
            socket,
            local_addr,
            conn,
            h3,
            read_buffer,
            write_buffer,
            websocket_buffer: Vec::new(),
        })
    }

    async fn send_request(&mut self, headers: Vec<quiche::h3::Header>, fin: bool) -> u64 {
        let stream_id = self
            .h3
            .send_request(&mut self.conn, &headers, fin)
            .expect("send raw h3 request");
        self.flush().await;
        stream_id
    }

    async fn recv_response_headers(&mut self, expected_stream_id: u64) -> Vec<quiche::h3::Header> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for h3 response headers"
            );
            match self.poll_event(Duration::from_secs(2)).await {
                (stream_id, quiche::h3::Event::Headers { list, .. })
                    if stream_id == expected_stream_id =>
                {
                    return list;
                }
                (stream_id, quiche::h3::Event::Data) if stream_id == expected_stream_id => {
                    self.read_body_into_websocket_buffer(stream_id);
                }
                (stream_id, quiche::h3::Event::Finished) if stream_id == expected_stream_id => {
                    panic!("response stream finished before headers");
                }
                (_, quiche::h3::Event::Headers { .. })
                | (_, quiche::h3::Event::GoAway)
                | (_, quiche::h3::Event::PriorityUpdate)
                | (_, quiche::h3::Event::Data)
                | (_, quiche::h3::Event::Finished) => {}
                (_, quiche::h3::Event::Reset(error)) => {
                    panic!("raw h3 response stream reset: {error}");
                }
            }
        }
    }

    async fn send_websocket_frame(&mut self, stream_id: u64, frame: WebSocketFrame) {
        let payload = encode_client_websocket_frame(frame);
        self.h3
            .send_body(&mut self.conn, stream_id, &payload, false)
            .expect("send websocket frame body");
        self.flush().await;
    }

    async fn send_body_chunk(&mut self, stream_id: u64, chunk: &[u8], fin: bool) {
        self.h3
            .send_body(&mut self.conn, stream_id, chunk, fin)
            .expect("send h3 request body");
        self.flush().await;
    }

    async fn recv_body_chunk(&mut self, expected_stream_id: u64) -> Vec<u8> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for h3 response body"
            );
            match self.poll_event(Duration::from_secs(2)).await {
                (stream_id, quiche::h3::Event::Data) if stream_id == expected_stream_id => {
                    let chunk = self.read_body_chunk(stream_id);
                    if !chunk.is_empty() {
                        return chunk;
                    }
                }
                (stream_id, quiche::h3::Event::Finished) if stream_id == expected_stream_id => {
                    return Vec::new();
                }
                (_, quiche::h3::Event::Headers { .. })
                | (_, quiche::h3::Event::GoAway)
                | (_, quiche::h3::Event::PriorityUpdate)
                | (_, quiche::h3::Event::Data)
                | (_, quiche::h3::Event::Finished) => {}
                (_, quiche::h3::Event::Reset(error)) => {
                    panic!("raw h3 response stream reset: {error}");
                }
            }
        }
    }

    async fn recv_full_body(&mut self, expected_stream_id: u64) -> Vec<u8> {
        let mut body = Vec::new();
        loop {
            let chunk = self.recv_body_chunk(expected_stream_id).await;
            if chunk.is_empty() {
                return body;
            }
            body.extend_from_slice(&chunk);
        }
    }

    #[allow(dead_code)]
    async fn send_transport_datagram(&mut self, stream_id: u64, payload: &[u8]) {
        let flow_id = stream_id / 4;
        let mut datagram = encode_small_varint(flow_id);
        datagram.extend_from_slice(payload);
        self.conn
            .dgram_send_vec(datagram)
            .expect("send h3 datagram");
        self.flush().await;
    }

    #[allow(dead_code)]
    async fn recv_transport_datagram(&mut self, expected_stream_id: u64) -> Vec<u8> {
        let expected_flow_id = expected_stream_id / 4;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for h3 transport datagram"
            );
            self.pump_only(Duration::from_secs(2)).await;
            match self.conn.dgram_recv_vec() {
                Ok(bytes) => {
                    let (flow_id, payload) = decode_small_varint(&bytes);
                    if flow_id == expected_flow_id {
                        return payload.to_vec();
                    }
                }
                Err(quiche::Error::Done) => {}
                Err(error) => panic!("raw h3 datagram recv failed: {error}"),
            }
        }
    }

    async fn recv_websocket_frame(&mut self, expected_stream_id: u64) -> WebSocketFrame {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for h3 websocket frame"
            );
            if let Some(frame) = try_decode_server_websocket_frame(&mut self.websocket_buffer) {
                return frame;
            }
            match self.poll_event(Duration::from_secs(2)).await {
                (stream_id, quiche::h3::Event::Data) if stream_id == expected_stream_id => {
                    self.read_body_into_websocket_buffer(stream_id);
                }
                (stream_id, quiche::h3::Event::Finished) if stream_id == expected_stream_id => {
                    return WebSocketFrame::Close {
                        code: 1006,
                        reason: String::new(),
                    };
                }
                (_, quiche::h3::Event::Headers { .. })
                | (_, quiche::h3::Event::GoAway)
                | (_, quiche::h3::Event::PriorityUpdate)
                | (_, quiche::h3::Event::Data)
                | (_, quiche::h3::Event::Finished) => {}
                (_, quiche::h3::Event::Reset(error)) => {
                    panic!("raw h3 websocket stream reset: {error}");
                }
            }
        }
    }

    async fn poll_event(&mut self, timeout_after: Duration) -> (u64, quiche::h3::Event) {
        loop {
            match self.h3.poll(&mut self.conn) {
                Ok(event) => return event,
                Err(quiche::h3::Error::Done) => {}
                Err(error) => panic!("raw h3 poll failed: {error}"),
            }
            pump_quiche_connection(
                &self.socket,
                self.local_addr,
                &mut self.conn,
                &mut self.read_buffer,
                &mut self.write_buffer,
                timeout_after,
            )
            .await;
        }
    }

    #[allow(dead_code)]
    async fn pump_only(&mut self, timeout_after: Duration) {
        pump_quiche_connection(
            &self.socket,
            self.local_addr,
            &mut self.conn,
            &mut self.read_buffer,
            &mut self.write_buffer,
            timeout_after,
        )
        .await;
    }

    async fn flush(&mut self) {
        flush_quiche_connection(&self.socket, &mut self.conn, &mut self.write_buffer).await;
    }

    fn read_body_into_websocket_buffer(&mut self, stream_id: u64) {
        let mut chunk = [0_u8; 8192];
        loop {
            match self.h3.recv_body(&mut self.conn, stream_id, &mut chunk) {
                Ok(read) => self.websocket_buffer.extend_from_slice(&chunk[..read]),
                Err(quiche::h3::Error::Done) => break,
                Err(error) => panic!("failed to read websocket body chunk: {error}"),
            }
        }
    }

    fn read_body_chunk(&mut self, stream_id: u64) -> Vec<u8> {
        let mut chunk = [0_u8; 8192];
        match self.h3.recv_body(&mut self.conn, stream_id, &mut chunk) {
            Ok(read) => chunk[..read].to_vec(),
            Err(quiche::h3::Error::Done) => Vec::new(),
            Err(error) => panic!("failed to read body chunk: {error}"),
        }
    }
}

async fn flush_quiche_connection(
    socket: &tokio::net::UdpSocket,
    conn: &mut quiche::Connection,
    write_buffer: &mut [u8; 65535],
) {
    loop {
        match conn.send(write_buffer) {
            Ok((written, send_info)) => {
                socket
                    .send_to(&write_buffer[..written], send_info.to)
                    .await
                    .expect("send raw quiche packet");
            }
            Err(quiche::Error::Done) => return,
            Err(error) => panic!("raw quiche send failed: {error}"),
        }
    }
}

async fn pump_quiche_connection(
    socket: &tokio::net::UdpSocket,
    local_addr: std::net::SocketAddr,
    conn: &mut quiche::Connection,
    read_buffer: &mut [u8; 65535],
    write_buffer: &mut [u8; 65535],
    timeout_after: Duration,
) {
    flush_quiche_connection(socket, conn, write_buffer).await;
    match tokio::time::timeout(timeout_after, socket.recv_from(read_buffer)).await {
        Ok(Ok((len, from))) => {
            let recv_info = quiche::RecvInfo {
                from,
                to: local_addr,
            };
            match conn.recv(&mut read_buffer[..len], recv_info) {
                Ok(_) | Err(quiche::Error::Done) => {}
                Err(error) => panic!("raw quiche recv failed: {error}"),
            }
        }
        Ok(Err(error)) => panic!("raw quiche socket recv failed: {error}"),
        Err(_) => conn.on_timeout(),
    }
    flush_quiche_connection(socket, conn, write_buffer).await;
}

fn encode_client_websocket_frame(frame: WebSocketFrame) -> Vec<u8> {
    let (opcode, payload) = match frame {
        WebSocketFrame::Text(text) => (0x1, text.into_bytes()),
        WebSocketFrame::Binary(bytes) => (0x2, bytes),
        WebSocketFrame::Close { code, reason } => {
            let mut payload = Vec::with_capacity(2 + reason.len());
            payload.extend_from_slice(&code.to_be_bytes());
            payload.extend_from_slice(reason.as_bytes());
            (0x8, payload)
        }
        WebSocketFrame::Ping(bytes) => (0x9, bytes),
        WebSocketFrame::Pong(bytes) => (0xA, bytes),
    };

    let mut frame_bytes = Vec::with_capacity(2 + 8 + payload.len());
    frame_bytes.push(0x80 | opcode);
    if payload.len() < 126 {
        frame_bytes.push(0x80 | (payload.len() as u8));
    } else if payload.len() <= u16::MAX as usize {
        frame_bytes.push(0x80 | 126);
        frame_bytes.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame_bytes.push(0x80 | 127);
        frame_bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }

    let mask = [0x12, 0x34, 0x56, 0x78];
    frame_bytes.extend_from_slice(&mask);
    for (index, byte) in payload.iter().copied().enumerate() {
        frame_bytes.push(byte ^ mask[index % 4]);
    }
    frame_bytes
}

#[allow(dead_code)]
fn encode_small_varint(value: u64) -> Vec<u8> {
    assert!(value < 64, "test helper only supports small varints");
    vec![value as u8]
}

#[allow(dead_code)]
fn decode_small_varint(bytes: &[u8]) -> (u64, &[u8]) {
    let Some((&first, rest)) = bytes.split_first() else {
        panic!("missing varint prefix");
    };
    let prefix = first >> 6;
    assert_eq!(prefix, 0, "test helper only supports small varints");
    ((first & 0x3f) as u64, rest)
}

fn try_decode_server_websocket_frame(buffer: &mut Vec<u8>) -> Option<WebSocketFrame> {
    if buffer.len() < 2 {
        return None;
    }
    let first = buffer[0];
    let second = buffer[1];
    let opcode = first & 0x0f;
    let masked = (second & 0x80) != 0;
    if masked {
        return None;
    }

    let mut offset = 2usize;
    let payload_len = match second & 0x7f {
        len @ 0..=125 => len as usize,
        126 => {
            if buffer.len() < offset + 2 {
                return None;
            }
            let len = u16::from_be_bytes([buffer[offset], buffer[offset + 1]]) as usize;
            offset += 2;
            len
        }
        127 => {
            if buffer.len() < offset + 8 {
                return None;
            }
            let len = u64::from_be_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ]) as usize;
            offset += 8;
            len
        }
        _ => unreachable!("masked bit is stripped before payload length decoding"),
    };

    if buffer.len() < offset + payload_len {
        return None;
    }

    let payload = buffer[offset..offset + payload_len].to_vec();
    buffer.drain(..offset + payload_len);

    Some(match opcode {
        0x1 => WebSocketFrame::Text(String::from_utf8(payload).expect("utf8 websocket text")),
        0x2 => WebSocketFrame::Binary(payload),
        0x8 => {
            let (code, reason) = if payload.len() >= 2 {
                let code = u16::from_be_bytes([payload[0], payload[1]]);
                let reason = String::from_utf8_lossy(&payload[2..]).into_owned();
                (code, reason)
            } else {
                (1005, String::new())
            };
            WebSocketFrame::Close { code, reason }
        }
        0x9 => WebSocketFrame::Ping(payload),
        0xA => WebSocketFrame::Pong(payload),
        other => panic!("unsupported websocket opcode from server: {other}"),
    })
}

fn reserve_socket_addr() -> std::net::SocketAddr {
    let socket = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).expect("bind");
    let addr = socket.local_addr().expect("addr");
    drop(socket);
    addr
}

fn header_value<'a>(headers: &'a [Header], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|header| header.name() == name.as_bytes())
        .and_then(|header| std::str::from_utf8(header.value()).ok())
}
