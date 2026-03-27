use crate::handlers::{
    annotate_response_with_trace_id, build_worker_url, ensure_public_worker, full_body,
    handle_public_h3_request, handle_websocket_session, open_transport_session_from_parts,
    open_websocket_session_from_parts, parse_public_worker_name_from_request,
    sanitize_websocket_handshake_headers, ResponseBody,
};
use crate::state::AppState;
use common::{PlatformError, Result};
use futures_util::SinkExt;
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, Request, Response, StatusCode, Version};
use http_body_util::BodyExt;
use runtime::InvokeRequestBodyReceiver;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_quiche::buf_factory::BufFactory;
use tokio_quiche::http3::driver::{
    InboundFrame, InboundFrameStream, IncomingH3Headers, OutboundFrame, OutboundFrameSender,
    ServerH3Event,
};
use tokio_quiche::http3::settings::Http3Settings;
use tokio_quiche::metrics::DefaultMetrics;
use tokio_quiche::quiche;
use tokio_quiche::quiche::h3::NameValue;
use tokio_quiche::settings::{
    CertificateKind, ConnectionParams, Hooks, QuicSettings, TlsCertificatePaths,
};
use tokio_quiche::{listen, ServerH3Driver};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::WebSocketStream;
use tracing::warn;

const REQUEST_BODY_STREAM_CAPACITY: usize = 8;

pub async fn serve_public_h3(public_addr: std::net::SocketAddr, state: AppState) -> Result<()> {
    let cert_path = state
        .public_tls_cert_path
        .as_ref()
        .ok_or_else(|| PlatformError::internal("PUBLIC_TLS_CERT_PATH is required for public h3"))?
        .to_string_lossy()
        .into_owned();
    let key_path = state
        .public_tls_key_path
        .as_ref()
        .ok_or_else(|| PlatformError::internal("PUBLIC_TLS_KEY_PATH is required for public h3"))?
        .to_string_lossy()
        .into_owned();

    let socket = tokio::net::UdpSocket::bind(public_addr)
        .await
        .map_err(|error| {
            PlatformError::internal(format!("failed to bind public quic listener: {error}"))
        })?;

    let mut quic_settings = QuicSettings::default();
    quic_settings.alpn = quiche::h3::APPLICATION_PROTOCOL
        .iter()
        .map(|value| value.to_vec())
        .collect();
    let params = ConnectionParams::new_server(
        quic_settings,
        TlsCertificatePaths {
            cert: &cert_path,
            private_key: &key_path,
            kind: CertificateKind::X509,
        },
        Hooks::default(),
    );
    let mut listeners = listen([socket], params, DefaultMetrics).map_err(|error| {
        PlatformError::internal(format!("failed to start public quic listener: {error}"))
    })?;
    let mut accept_stream = listeners
        .pop()
        .ok_or_else(|| PlatformError::internal("public quic listener was not created"))?;

    loop {
        let Some(incoming) = tokio_stream::StreamExt::next(&mut accept_stream).await else {
            return Err(PlatformError::internal("public quic accept stream closed"));
        };
        match incoming {
            Ok(conn) => {
                let (driver, mut controller) = ServerH3Driver::new(Http3Settings {
                    enable_extended_connect: true,
                    ..Default::default()
                });
                conn.start(driver);
                let state = state.clone();
                tokio::spawn(async move {
                    let mut pending_transport_headers =
                        HashMap::<u64, (ParsedQuicheRequest, IncomingH3Headers)>::new();
                    let mut pending_transport_flows =
                        HashMap::<u64, (OutboundFrameSender, InboundFrameStream)>::new();
                    while let Some(event) = controller.event_receiver_mut().recv().await {
                        match event {
                            ServerH3Event::Headers {
                                incoming_headers, ..
                            } => match parse_quiche_request(&incoming_headers.headers) {
                                Ok(parsed)
                                    if parsed.method == Method::CONNECT
                                        && parsed.connect_protocol.as_deref()
                                            == Some("webtransport") =>
                                {
                                    let flow_id = incoming_headers.stream_id / 4;
                                    if let Some((flow_send, flow_recv)) =
                                        pending_transport_flows.remove(&flow_id)
                                    {
                                        let state = state.clone();
                                        tokio::spawn(async move {
                                            if let Err(error) = handle_public_h3_transport(
                                                state,
                                                parsed,
                                                incoming_headers,
                                                flow_id,
                                                flow_send,
                                                flow_recv,
                                            )
                                            .await
                                            {
                                                warn!(
                                                    error = %error,
                                                    "public quic h3 transport request failed"
                                                );
                                            }
                                        });
                                    } else {
                                        pending_transport_headers
                                            .insert(flow_id, (parsed, incoming_headers));
                                    }
                                }
                                Ok(_) | Err(_) => {
                                    let state = state.clone();
                                    tokio::spawn(async move {
                                        if let Err(error) =
                                            handle_public_h3_headers(state, incoming_headers).await
                                        {
                                            warn!(
                                                error = %error,
                                                "public quic h3 request failed"
                                            );
                                        }
                                    });
                                }
                            },
                            ServerH3Event::Core(
                                tokio_quiche::http3::driver::H3Event::NewFlow {
                                    flow_id,
                                    send,
                                    recv,
                                },
                            ) => {
                                if let Some((parsed, incoming_headers)) =
                                    pending_transport_headers.remove(&flow_id)
                                {
                                    let state = state.clone();
                                    tokio::spawn(async move {
                                        if let Err(error) = handle_public_h3_transport(
                                            state,
                                            parsed,
                                            incoming_headers,
                                            flow_id,
                                            send,
                                            recv,
                                        )
                                        .await
                                        {
                                            warn!(
                                                error = %error,
                                                "public quic h3 transport request failed"
                                            );
                                        }
                                    });
                                } else {
                                    pending_transport_flows.insert(flow_id, (send, recv));
                                }
                            }
                            ServerH3Event::Core(_) => {}
                        }
                    }
                });
            }
            Err(error) => warn!(error = %error, "public quic accept failed"),
        }
    }
}

async fn handle_public_h3_headers(
    state: AppState,
    incoming_headers: IncomingH3Headers,
) -> Result<()> {
    let parsed = parse_quiche_request(&incoming_headers.headers)?;
    if parsed.method == Method::CONNECT {
        return match parsed.connect_protocol.as_deref() {
            Some("websocket") => handle_public_h3_websocket(state, parsed, incoming_headers).await,
            Some(_) => {
                let response = simple_response(
                    StatusCode::NOT_IMPLEMENTED,
                    "unsupported extended CONNECT protocol",
                )?;
                send_quiche_response(incoming_headers.send, response).await
            }
            None => {
                let response = simple_response(
                    StatusCode::BAD_REQUEST,
                    "CONNECT is unsupported over http/3",
                )?;
                send_quiche_response(incoming_headers.send, response).await
            }
        };
    }

    let request_body_stream = if request_method_allows_body(&parsed.method) {
        Some(stream_quiche_request_body(
            incoming_headers.recv,
            state.invoke_max_body_bytes,
        ))
    } else {
        None
    };
    let response = handle_public_h3_request(state, parsed.request, request_body_stream).await;
    send_quiche_response(incoming_headers.send, response).await
}

async fn handle_public_h3_websocket(
    state: AppState,
    parsed: ParsedQuicheRequest,
    incoming_headers: IncomingH3Headers,
) -> Result<()> {
    validate_h3_websocket_request(&parsed.request)?;

    let (parts, _body) = parsed.request.into_parts();
    let worker_name = parse_public_worker_name_from_request(
        &parts.headers,
        &parts.uri,
        &state.public_base_domain,
    )?;
    ensure_public_worker(&state, &worker_name).await?;
    let url = build_worker_url(parts.uri.path(), parts.uri.path_and_query());
    let synthetic_parts = build_synthetic_websocket_parts(parts)?;

    let runtime::WebSocketOpen {
        session_id,
        worker_name: runtime_worker_name,
        output,
    } = open_websocket_session_from_parts(&state, &synthetic_parts, worker_name, url).await?;

    if output.status != 101 {
        let response = simple_response(
            StatusCode::BAD_REQUEST,
            "websocket upgrade rejected by worker",
        )?;
        return send_quiche_response(incoming_headers.send, response).await;
    }

    {
        let mut sessions = state.websocket_sessions.lock().await;
        sessions.insert(
            session_id.clone(),
            crate::state::WebSocketSession {
                id: session_id.clone(),
                worker_name: runtime_worker_name.clone(),
                started_at: std::time::Instant::now(),
            },
        );
    }

    let response_headers = build_h3_websocket_response_headers(output.headers);
    let send = clone_outbound_sender(&incoming_headers.send)?;
    send.send(OutboundFrame::Headers(response_headers, None))
        .await
        .map_err(|_| PlatformError::internal("failed to send websocket response headers"))?;

    let websocket = websocket_stream_over_h3(send, incoming_headers.recv, Role::Server).await;
    handle_websocket_session(
        state.clone(),
        websocket,
        session_id,
        runtime_worker_name,
        state.runtime.clone(),
        output.body,
    )
    .await;

    Ok(())
}

async fn handle_public_h3_transport(
    state: AppState,
    parsed: ParsedQuicheRequest,
    incoming_headers: IncomingH3Headers,
    flow_id: u64,
    mut flow_send: OutboundFrameSender,
    mut flow_recv: InboundFrameStream,
) -> Result<()> {
    validate_h3_transport_request(&parsed.request)?;

    let (parts, _body) = parsed.request.into_parts();
    let worker_name = parse_public_worker_name_from_request(
        &parts.headers,
        &parts.uri,
        &state.public_base_domain,
    )?;
    ensure_public_worker(&state, &worker_name).await?;
    let url = build_worker_url(parts.uri.path(), parts.uri.path_and_query());
    let synthetic_parts = build_synthetic_transport_parts(parts)?;

    let (outbound_stream_tx, mut outbound_stream_rx) = mpsc::unbounded_channel();
    let (outbound_datagram_tx, mut outbound_datagram_rx) = mpsc::unbounded_channel();
    let runtime::TransportOpen {
        session_id,
        worker_name: runtime_worker_name,
        output,
    } = open_transport_session_from_parts(
        &state,
        &synthetic_parts,
        worker_name,
        url,
        outbound_stream_tx,
        outbound_datagram_tx,
    )
    .await?;

    if output.status != 200 {
        let response =
            simple_response(StatusCode::BAD_REQUEST, "transport open rejected by worker")?;
        return send_quiche_response(incoming_headers.send, response).await;
    }

    let body_sender = clone_outbound_sender(&incoming_headers.send)?;
    let stream_id = incoming_headers.stream_id;
    body_sender
        .send(OutboundFrame::Headers(
            build_h3_transport_response_headers(output.headers),
            None,
        ))
        .await
        .map_err(|_| PlatformError::internal("failed to send transport response headers"))?;

    let closed = Arc::new(AtomicBool::new(false));

    let runtime_for_stream = state.runtime.clone();
    let worker_for_stream = runtime_worker_name.clone();
    let session_for_stream = session_id.clone();
    let closed_for_stream = closed.clone();
    let mut body_recv = incoming_headers.recv;
    let inbound_stream_task = tokio::spawn(async move {
        while let Some(frame) = body_recv.recv().await {
            match frame {
                InboundFrame::Body(body, fin) => {
                    if !body.as_ref().is_empty()
                        && runtime_for_stream
                            .transport_push_stream(
                                worker_for_stream.clone(),
                                session_for_stream.clone(),
                                body.as_ref().to_vec(),
                                false,
                            )
                            .await
                            .is_err()
                    {
                        break;
                    }
                    if fin {
                        break;
                    }
                }
                InboundFrame::Datagram(_) => {}
            }
        }
        let _ = close_transport_session_once(
            &runtime_for_stream,
            &worker_for_stream,
            &session_for_stream,
            0,
            "client closed",
            &closed_for_stream,
        )
        .await;
    });

    let runtime_for_datagrams = state.runtime.clone();
    let worker_for_datagrams = runtime_worker_name.clone();
    let session_for_datagrams = session_id.clone();
    let closed_for_datagrams = closed.clone();
    let inbound_datagram_task = tokio::spawn(async move {
        while let Some(frame) = flow_recv.recv().await {
            match frame {
                InboundFrame::Datagram(datagram) => {
                    if runtime_for_datagrams
                        .transport_push_datagram(
                            worker_for_datagrams.clone(),
                            session_for_datagrams.clone(),
                            datagram.as_ref().to_vec(),
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                InboundFrame::Body(_, fin) if fin => break,
                InboundFrame::Body(_, _) => {}
            }
        }
        let _ = close_transport_session_once(
            &runtime_for_datagrams,
            &worker_for_datagrams,
            &session_for_datagrams,
            0,
            "client closed",
            &closed_for_datagrams,
        )
        .await;
    });

    let mut outbound_datagram_sender = flow_send.clone();
    let outbound_datagram_task = tokio::spawn(async move {
        while let Some(datagram) = outbound_datagram_rx.recv().await {
            if outbound_datagram_sender
                .send(OutboundFrame::Datagram(
                    BufFactory::dgram_from_slice(&datagram),
                    flow_id,
                ))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    while let Some(chunk) = outbound_stream_rx.recv().await {
        if body_sender
            .send(OutboundFrame::body(
                BufFactory::buf_from_slice(&chunk),
                false,
            ))
            .await
            .is_err()
        {
            break;
        }
    }

    let _ = body_sender
        .send(OutboundFrame::body(BufFactory::buf_from_slice(&[]), true))
        .await;
    let _ = flow_send
        .send(OutboundFrame::FlowShutdown { flow_id, stream_id })
        .await;

    let _ = close_transport_session_once(
        &state.runtime,
        &runtime_worker_name,
        &session_id,
        0,
        "transport finished",
        &closed,
    )
    .await;
    inbound_stream_task.abort();
    inbound_datagram_task.abort();
    outbound_datagram_task.abort();
    Ok(())
}

pub(crate) async fn websocket_stream_over_h3(
    send: mpsc::Sender<OutboundFrame>,
    mut recv: InboundFrameStream,
    role: Role,
) -> WebSocketStream<tokio::io::DuplexStream> {
    let (socket_io, transport_io) = tokio::io::duplex(64 * 1024);
    let (mut transport_reader, mut transport_writer) = tokio::io::split(transport_io);
    let outbound = send.clone();

    tokio::spawn(async move {
        while let Some(frame) = recv.recv().await {
            match frame {
                InboundFrame::Body(body, fin) => {
                    if !body.as_ref().is_empty() {
                        if transport_writer.write_all(body.as_ref()).await.is_err() {
                            return;
                        }
                    }
                    if fin {
                        let _ = transport_writer.shutdown().await;
                        return;
                    }
                }
                InboundFrame::Datagram(_) => {}
            }
        }
        let _ = transport_writer.shutdown().await;
    });

    tokio::spawn(async move {
        let mut buffer = [0_u8; 8192];
        loop {
            match transport_reader.read(&mut buffer).await {
                Ok(0) => {
                    let _ = outbound
                        .send(OutboundFrame::body(BufFactory::buf_from_slice(&[]), true))
                        .await;
                    return;
                }
                Ok(read) => {
                    if outbound
                        .send(OutboundFrame::body(
                            BufFactory::buf_from_slice(&buffer[..read]),
                            false,
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) => {
                    let _ = outbound
                        .send(OutboundFrame::body(BufFactory::buf_from_slice(&[]), true))
                        .await;
                    return;
                }
            }
        }
    });

    WebSocketStream::from_raw_socket(socket_io, role, None).await
}

async fn send_quiche_response(
    send: tokio_quiche::http3::driver::OutboundFrameSender,
    response: Response<ResponseBody>,
) -> Result<()> {
    let sender = clone_outbound_sender(&send)?;
    let (parts, mut body) = response.into_parts();
    sender
        .send(OutboundFrame::Headers(
            response_to_quiche_headers(parts.status, &parts.headers),
            None,
        ))
        .await
        .map_err(|_| PlatformError::internal("failed to send h3 response headers"))?;

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            PlatformError::internal(format!("failed to stream response body: {error}"))
        })?;
        if let Some(data) = frame.data_ref() {
            sender
                .send(OutboundFrame::body(
                    BufFactory::buf_from_slice(data.as_ref()),
                    false,
                ))
                .await
                .map_err(|_| PlatformError::internal("failed to send h3 response body"))?;
        }
    }

    sender
        .send(OutboundFrame::body(BufFactory::buf_from_slice(&[]), true))
        .await
        .map_err(|_| PlatformError::internal("failed to finish h3 response body"))?;
    Ok(())
}

fn stream_quiche_request_body(
    mut recv: InboundFrameStream,
    max_body_bytes: usize,
) -> InvokeRequestBodyReceiver {
    let (tx, rx) = mpsc::channel(REQUEST_BODY_STREAM_CAPACITY);
    tokio::spawn(async move {
        let mut total = 0usize;
        while let Some(frame) = recv.recv().await {
            match frame {
                InboundFrame::Body(body, fin) => {
                    if !body.as_ref().is_empty() {
                        total = total.saturating_add(body.as_ref().len());
                        if total > max_body_bytes {
                            let _ = tx
                                .send(Err(format!(
                                    "request body too large (max {max_body_bytes} bytes)"
                                )))
                                .await;
                            return;
                        }
                        if tx.send(Ok(body.as_ref().to_vec())).await.is_err() {
                            return;
                        }
                    }
                    if fin {
                        return;
                    }
                }
                InboundFrame::Datagram(_) => {}
            }
        }
    });
    rx
}

fn build_synthetic_websocket_parts(parts: http::request::Parts) -> Result<http::request::Parts> {
    let mut builder = Request::builder()
        .method(Method::GET)
        .uri(parts.uri.clone())
        .version(Version::HTTP_3);
    for (name, value) in &parts.headers {
        builder = builder.header(name, value);
    }
    builder = builder
        .header("connection", "Upgrade")
        .header("upgrade", "websocket");
    let (mut synthetic_parts, _body) = builder
        .body(())
        .map_err(|error| PlatformError::internal(error.to_string()))?
        .into_parts();
    synthetic_parts.extensions = parts.extensions;
    Ok(synthetic_parts)
}

fn build_synthetic_transport_parts(parts: http::request::Parts) -> Result<http::request::Parts> {
    let mut builder = Request::builder()
        .method(Method::CONNECT)
        .uri(parts.uri.clone())
        .version(Version::HTTP_3);
    for (name, value) in &parts.headers {
        builder = builder.header(name, value);
    }
    builder = builder.header("x-dd-transport-protocol", "webtransport");
    let (mut synthetic_parts, _body) = builder
        .body(())
        .map_err(|error| PlatformError::internal(error.to_string()))?
        .into_parts();
    synthetic_parts.extensions = parts.extensions;
    Ok(synthetic_parts)
}

fn validate_h3_websocket_request(request: &Request<()>) -> Result<()> {
    let headers = request.headers();
    let version = headers
        .get("sec-websocket-version")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if version != "13" {
        return Err(PlatformError::bad_request(
            "websocket over http/3 requires sec-websocket-version: 13",
        ));
    }
    let key = headers
        .get("sec-websocket-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim();
    if key.is_empty() {
        return Err(PlatformError::bad_request(
            "websocket over http/3 requires sec-websocket-key",
        ));
    }
    Ok(())
}

fn validate_h3_transport_request(request: &Request<()>) -> Result<()> {
    if request.method() != Method::CONNECT {
        return Err(PlatformError::bad_request(
            "transport over http/3 requires CONNECT",
        ));
    }
    Ok(())
}

fn clone_outbound_sender(
    send: &tokio_quiche::http3::driver::OutboundFrameSender,
) -> Result<mpsc::Sender<OutboundFrame>> {
    send.get_ref()
        .cloned()
        .ok_or_else(|| PlatformError::internal("missing h3 outbound sender"))
}

fn request_method_allows_body(method: &Method) -> bool {
    *method != Method::GET && *method != Method::HEAD
}

fn response_to_quiche_headers(status: StatusCode, headers: &HeaderMap) -> Vec<quiche::h3::Header> {
    let mut output = Vec::with_capacity(headers.len() + 1);
    output.push(quiche::h3::Header::new(
        b":status",
        status.as_str().as_bytes(),
    ));
    for (name, value) in headers {
        output.push(quiche::h3::Header::new(
            name.as_str().as_bytes(),
            value.as_bytes(),
        ));
    }
    output
}

fn build_h3_websocket_response_headers(headers: Vec<(String, String)>) -> Vec<quiche::h3::Header> {
    let filtered = sanitize_websocket_handshake_headers(headers)
        .into_iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("connection"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("upgrade"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("sec-websocket-accept"))
        .collect::<Vec<_>>();

    let mut output = Vec::with_capacity(filtered.len() + 1);
    output.push(quiche::h3::Header::new(b":status", b"200"));
    for (name, value) in filtered {
        output.push(quiche::h3::Header::new(name.as_bytes(), value.as_bytes()));
    }
    output
}

fn build_h3_transport_response_headers(headers: Vec<(String, String)>) -> Vec<quiche::h3::Header> {
    let filtered = headers
        .into_iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("connection"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("upgrade"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-dd-transport-accept"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-dd-transport-session"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-dd-transport-handle"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-dd-transport-actor-binding"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-dd-transport-actor-key"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-dd-transport-close-code"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("x-dd-transport-close-reason"))
        .collect::<Vec<_>>();

    let mut output = Vec::with_capacity(filtered.len() + 1);
    output.push(quiche::h3::Header::new(b":status", b"200"));
    for (name, value) in filtered {
        output.push(quiche::h3::Header::new(name.as_bytes(), value.as_bytes()));
    }
    output
}

async fn close_transport_session_once(
    runtime: &runtime::RuntimeService,
    worker_name: &str,
    session_id: &str,
    close_code: u16,
    close_reason: &str,
    closed: &Arc<AtomicBool>,
) -> Result<()> {
    if closed.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    runtime
        .transport_close(
            worker_name.to_string(),
            session_id.to_string(),
            close_code,
            close_reason.to_string(),
        )
        .await
}

fn parse_quiche_request(headers: &[quiche::h3::Header]) -> Result<ParsedQuicheRequest> {
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut connect_protocol = None;
    let mut header_map = HeaderMap::new();

    for header in headers {
        let name = std::str::from_utf8(header.name()).map_err(|error| {
            PlatformError::bad_request(format!("invalid h3 header name: {error}"))
        })?;
        let value = std::str::from_utf8(header.value()).map_err(|error| {
            PlatformError::bad_request(format!("invalid h3 header value: {error}"))
        })?;
        match name {
            ":method" => {
                method = Some(Method::from_bytes(value.as_bytes()).map_err(|error| {
                    PlatformError::bad_request(format!("invalid h3 method: {error}"))
                })?)
            }
            ":scheme" => scheme = Some(value.to_string()),
            ":authority" => authority = Some(value.to_string()),
            ":path" => path = Some(value.to_string()),
            ":protocol" => connect_protocol = Some(value.to_string()),
            _ if name.starts_with(':') => {}
            _ => {
                let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                    PlatformError::bad_request(format!("invalid header name {name}: {error}"))
                })?;
                let header_value = HeaderValue::from_str(value).map_err(|error| {
                    PlatformError::bad_request(format!("invalid header value for {name}: {error}"))
                })?;
                header_map.append(header_name, header_value);
            }
        }
    }

    let method = method.ok_or_else(|| PlatformError::bad_request("missing :method"))?;
    let authority = authority.ok_or_else(|| PlatformError::bad_request("missing :authority"))?;
    let scheme = scheme.unwrap_or_else(|| "https".to_string());
    let path = path.unwrap_or_else(|| "/".to_string());
    let uri: http::Uri = format!("{scheme}://{authority}{path}")
        .parse()
        .map_err(|error| PlatformError::bad_request(format!("invalid h3 request uri: {error}")))?;

    let mut request = Request::builder()
        .method(method.clone())
        .uri(uri)
        .version(Version::HTTP_3)
        .body(())
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    *request.headers_mut() = header_map;

    Ok(ParsedQuicheRequest {
        method,
        connect_protocol,
        request,
    })
}

fn simple_response(status: StatusCode, message: &str) -> Result<Response<ResponseBody>> {
    let mut response = Response::builder()
        .status(status)
        .body(full_body(message.as_bytes().to_vec()))
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    annotate_response_with_trace_id(&mut response);
    Ok(response)
}

struct ParsedQuicheRequest {
    method: Method,
    connect_protocol: Option<String>,
    request: Request<()>,
}
