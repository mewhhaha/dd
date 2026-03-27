#![cfg(test)]

use crate::{serve, AppState};
use common::DeployConfig;
use h3::client;
use quinn::crypto::rustls::QuicClientConfig;
use runtime::{RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use rustls::pki_types::CertificateDer;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;
use uuid::Uuid;

const MAX_QUIC_DATAGRAM_SIZE: usize = 1350;

pub(crate) async fn deploy_public_worker(state: &AppState, name: &str, source: &str) {
    deploy_public_worker_with_config(state, name, source, DeployConfig::default()).await;
}

pub(crate) async fn deploy_public_worker_with_config(
    state: &AppState,
    name: &str,
    source: &str,
    mut config: DeployConfig,
) {
    config.public = true;
    state
        .runtime
        .deploy_with_config(name.to_string(), source.to_string(), config)
        .await
        .expect("deploy public worker");
}

pub(crate) async fn start_h3_harness(
    state: AppState,
    cert: CertificateDer<'static>,
    public_addr: std::net::SocketAddr,
    private_addr: std::net::SocketAddr,
) -> (
    tokio::task::JoinHandle<common::Result<()>>,
    h3::client::Connection<h3_quinn::Connection, bytes::Bytes>,
    h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
) {
    let server = tokio::spawn({
        let state = state.clone();
        async move { serve(public_addr, private_addr, state).await }
    });
    tokio::time::sleep(Duration::from_millis(250)).await;

    let client_connection = connect_h3_client(public_addr, cert).await;
    let (driver, send_request) = client::new(h3_quinn::Connection::new(client_connection))
        .await
        .expect("create h3 client");
    (server, driver, send_request)
}

pub(crate) async fn create_h3_test_state() -> (
    AppState,
    CertificateDer<'static>,
    std::net::SocketAddr,
    std::net::SocketAddr,
    PathBuf,
) {
    let store_dir = PathBuf::from(format!("./target/test-store-h3-{}", Uuid::new_v4()));
    let cert_dir = store_dir.join("certs");
    tokio::fs::create_dir_all(&cert_dir)
        .await
        .expect("create cert dir");
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generate test cert");
    let cert = CertificateDer::from(certified.cert.der().to_vec());
    let cert_pem = certified.cert.pem();
    let key_pem = certified.signing_key.serialize_pem();
    let cert_path = cert_dir.join("public-cert.pem");
    let key_path = cert_dir.join("public-key.pem");
    tokio::fs::write(&cert_path, cert_pem)
        .await
        .expect("write cert");
    tokio::fs::write(&key_path, key_pem)
        .await
        .expect("write key");

    let public_addr = reserve_socket_addr();
    let private_addr = reserve_socket_addr();
    let storage = RuntimeStorageConfig {
        store_dir: store_dir.clone(),
        database_url: format!("file:{}/dd-test.db", store_dir.display()),
        worker_store_enabled: false,
        ..RuntimeStorageConfig::default()
    };
    let runtime = RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: Default::default(),
        storage,
    })
    .await
    .expect("runtime");
    let state = AppState::new(
        runtime,
        1024 * 1024,
        "localhost".to_string(),
        Some(cert_path),
        Some(key_path),
    );
    (state, cert, public_addr, private_addr, store_dir)
}

pub(crate) async fn connect_h3_client(
    public_addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
) -> quinn::Connection {
    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.add(cert).expect("add cert");
    let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("tls13")
    .with_root_certificates(root_cert_store)
    .with_no_client_auth();
    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).expect("quic")));

    let mut endpoint =
        quinn::Endpoint::client("[::]:0".parse().expect("client bind")).expect("client endpoint");
    endpoint.set_default_client_config(client_config);
    endpoint
        .connect(public_addr, "localhost")
        .expect("connect")
        .await
        .expect("quic connection")
}

pub(crate) struct RawQuicheH3Client {
    socket: tokio::net::UdpSocket,
    local_addr: std::net::SocketAddr,
    conn: quiche::Connection,
    h3: quiche::h3::Connection,
    read_buffer: [u8; 65535],
    write_buffer: [u8; 65535],
    websocket_buffer: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WebSocketFrame {
    Text(String),
    Binary(Vec<u8>),
    Close { code: u16, reason: String },
    Ping(Vec<u8>),
    Pong(Vec<u8>),
}

impl RawQuicheH3Client {
    pub(crate) async fn connect(public_addr: std::net::SocketAddr) -> Self {
        let bind_addr = match public_addr {
            std::net::SocketAddr::V4(_) => "0.0.0.0:0",
            std::net::SocketAddr::V6(_) => "[::]:0",
        };
        let socket = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .expect("bind raw quiche client socket");
        let local_addr = socket.local_addr().expect("raw quiche client local addr");

        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).expect("quiche config");
        config.verify_peer(false);
        config
            .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
            .expect("set h3 alpn");
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

        let scid_bytes = *Uuid::new_v4().as_bytes();
        let scid = quiche::ConnectionId::from_ref(&scid_bytes);
        let mut conn = quiche::connect(
            Some("localhost"),
            &scid,
            local_addr,
            public_addr,
            &mut config,
        )
        .expect("connect raw quiche client");
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
            &quiche::h3::Config::new().expect("h3 config"),
        )
        .expect("create raw h3 connection");

        Self {
            socket,
            local_addr,
            conn,
            h3,
            read_buffer,
            write_buffer,
            websocket_buffer: Vec::new(),
        }
    }

    pub(crate) async fn send_request(
        &mut self,
        headers: Vec<quiche::h3::Header>,
        fin: bool,
    ) -> u64 {
        let stream_id = self
            .h3
            .send_request(&mut self.conn, &headers, fin)
            .expect("send raw h3 request");
        self.flush().await;
        stream_id
    }

    pub(crate) async fn recv_response_headers(
        &mut self,
        expected_stream_id: u64,
    ) -> Vec<quiche::h3::Header> {
        loop {
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

    pub(crate) async fn recv_body(&mut self, expected_stream_id: u64) -> Vec<u8> {
        let mut body = Vec::new();
        loop {
            match self.poll_event(Duration::from_secs(2)).await {
                (stream_id, quiche::h3::Event::Data) if stream_id == expected_stream_id => {
                    let mut chunk = [0_u8; 8192];
                    loop {
                        match self.h3.recv_body(&mut self.conn, stream_id, &mut chunk) {
                            Ok(read) => body.extend_from_slice(&chunk[..read]),
                            Err(quiche::h3::Error::Done) => break,
                            Err(error) => panic!("failed to read raw h3 body: {error}"),
                        }
                    }
                }
                (stream_id, quiche::h3::Event::Finished) if stream_id == expected_stream_id => {
                    return body;
                }
                (_, quiche::h3::Event::Headers { .. })
                | (_, quiche::h3::Event::GoAway)
                | (_, quiche::h3::Event::PriorityUpdate)
                | (_, quiche::h3::Event::Data)
                | (_, quiche::h3::Event::Finished) => {}
                (_, quiche::h3::Event::Reset(error)) => {
                    panic!("raw h3 body stream reset: {error}");
                }
            }
        }
    }

    pub(crate) async fn send_websocket_frame(&mut self, stream_id: u64, frame: WebSocketFrame) {
        let payload = encode_client_websocket_frame(frame);
        self.h3
            .send_body(&mut self.conn, stream_id, &payload, false)
            .expect("send websocket frame body");
        self.flush().await;
    }

    pub(crate) async fn recv_websocket_frame(&mut self, expected_stream_id: u64) -> WebSocketFrame {
        loop {
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

pub(crate) fn reserve_socket_addr() -> std::net::SocketAddr {
    let socket = std::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, 0)).expect("bind");
    let addr = socket.local_addr().expect("addr");
    drop(socket);
    addr
}

pub(crate) fn h3_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
