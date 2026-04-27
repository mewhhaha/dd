use super::*;
pub(super) async fn invoke_worker_websocket_private<B>(
    state: AppState,
    request: Request<B>,
    ws_upgrade: Option<PreparedWebSocketUpgrade>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    let (worker_name, url) =
        parse_invoke_request_uri(parts.uri.path(), parts.uri.path_and_query())?;
    invoke_worker_websocket_with_target(state, parts, body, worker_name, url, ws_upgrade).await
}

pub(super) async fn invoke_worker_websocket_public<B>(
    state: AppState,
    request: Request<B>,
    ws_upgrade: Option<PreparedWebSocketUpgrade>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    let worker_name = parse_public_worker_name_from_request(
        &parts.headers,
        &parts.uri,
        &state.public_base_domain,
    )?;
    ensure_public_worker(&state, &worker_name).await?;
    let url = build_public_request_url(&parts.headers, &parts.uri)?;
    invoke_worker_websocket_with_target(state, parts, body, worker_name, url, ws_upgrade).await
}

async fn invoke_worker_websocket_with_target<B>(
    state: AppState,
    parts: http::request::Parts,
    _body: B,
    worker_name: String,
    url: String,
    ws_upgrade: Option<PreparedWebSocketUpgrade>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let Some(ws_upgrade) = ws_upgrade else {
        return Err(PlatformError::bad_request("missing websocket upgrade").into());
    };
    let runtime::WebSocketOpen {
        session_id,
        worker_name: runtime_worker_name,
        output,
    } = open_websocket_session_from_parts(&state, &parts, worker_name, url).await?;

    if output.status != 101 {
        return Err(PlatformError::bad_request("websocket upgrade rejected by worker").into());
    }

    let filtered_headers = sanitize_websocket_handshake_headers(output.headers);
    let runtime = state.runtime.clone();
    let state_for_session = state.clone();
    let handshake_session_id = session_id.clone();
    let runtime_worker_name_for_frames = runtime_worker_name.clone();
    let on_upgrade = ws_upgrade.on_upgrade;

    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                {
                    let mut sessions = state_for_session.websocket_sessions.lock().await;
                    sessions.insert(
                        handshake_session_id.clone(),
                        crate::state::WebSocketSession {
                            id: handshake_session_id.clone(),
                            worker_name: runtime_worker_name_for_frames.clone(),
                            started_at: std::time::Instant::now(),
                        },
                    );
                }

                let socket =
                    WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None)
                        .await;
                handle_websocket_session(
                    state_for_session,
                    socket,
                    handshake_session_id,
                    runtime_worker_name_for_frames,
                    runtime,
                    output.body,
                )
                .await;
            }
            Err(error) => {
                tracing::warn!(
                    session_id = %handshake_session_id,
                    error = %error,
                    "websocket upgrade failed"
                );
                let _ = runtime
                    .websocket_close(
                        runtime_worker_name_for_frames,
                        handshake_session_id,
                        1011,
                        "upgrade failed".to_string(),
                    )
                    .await;
            }
        }
    });

    let mut response = build_websocket_handshake_response(&ws_upgrade.handshake_request)?;
    for (name, value) in &filtered_headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            response.headers_mut().append(name, value);
        }
    }
    if let Ok(session_header) = HeaderValue::from_str(&session_id) {
        response.headers_mut().append(
            HeaderName::from_static("x-dd-websocket-session"),
            session_header,
        );
    }
    response
        .headers_mut()
        .remove(HeaderName::from_static(HEADER_WS_SESSION));
    Ok(response)
}

pub(crate) async fn open_websocket_session_from_parts(
    state: &AppState,
    parts: &http::request::Parts,
    worker_name: String,
    url: String,
) -> Result<runtime::WebSocketOpen, PlatformError> {
    if parts.method.as_str().to_ascii_uppercase() != "GET" {
        return Err(PlatformError::bad_request("websocket upgrade requires GET"));
    }
    if !is_websocket_upgrade(&parts.headers) {
        return Err(PlatformError::bad_request(
            "missing websocket upgrade headers",
        ));
    }

    let mut headers = Vec::with_capacity(parts.headers.len());
    for (name, value) in &parts.headers {
        let value = value.to_str().map_err(|error| {
            PlatformError::bad_request(format!("invalid header value for {name}: {error}"))
        })?;
        if is_internal_proxy_upgrade_header(name.as_str()) {
            continue;
        }
        headers.push((name.as_str().to_string(), value.to_string()));
    }
    inject_current_trace_context(&mut headers);
    let request_id = Uuid::new_v4().to_string();

    let invocation = WorkerInvocation {
        method: parts.method.as_str().to_string(),
        url,
        headers,
        body: Vec::new(),
        request_id,
    };

    state
        .runtime
        .open_websocket(worker_name, invocation, None)
        .await
        .map_err(|error| PlatformError::bad_request(format!("websocket open failed: {error}")))
}

pub(crate) async fn open_transport_session_from_parts(
    state: &AppState,
    parts: &http::request::Parts,
    worker_name: String,
    url: String,
    stream_sender: mpsc::UnboundedSender<Vec<u8>>,
    datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
) -> Result<runtime::TransportOpen, PlatformError> {
    if parts.method != Method::CONNECT {
        return Err(PlatformError::bad_request(
            "transport open requires CONNECT",
        ));
    }

    let mut headers = Vec::with_capacity(parts.headers.len());
    for (name, value) in &parts.headers {
        let value = value.to_str().map_err(|error| {
            PlatformError::bad_request(format!("invalid header value for {name}: {error}"))
        })?;
        headers.push((name.as_str().to_string(), value.to_string()));
    }
    inject_current_trace_context(&mut headers);
    let request_id = Uuid::new_v4().to_string();

    let invocation = WorkerInvocation {
        method: parts.method.as_str().to_string(),
        url,
        headers,
        body: Vec::new(),
        request_id,
    };

    state
        .runtime
        .open_transport(worker_name, invocation, stream_sender, datagram_sender)
        .await
        .map_err(|error| PlatformError::bad_request(format!("transport open failed: {error}")))
}

pub(crate) async fn handle_websocket_session<S>(
    state: AppState,
    socket: WebSocketStream<S>,
    session_id: String,
    worker_name: String,
    runtime: runtime::RuntimeService,
    _initial_response_body: Vec<u8>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut sender, mut receiver) = socket.split();
    let mut wait_for_frame =
        Box::pin(runtime.websocket_wait_frame(worker_name.clone(), session_id.clone()));

    loop {
        tokio::select! {
            message = receiver.next() => {
                let Some(message) = message else {
                    break;
                };
                match message {
                    Ok(message) => match message {
                        Message::Text(text) => {
                            if forward_websocket_frame(
                                &mut sender,
                                &runtime,
                                &worker_name,
                                &session_id,
                                text.to_string().into_bytes(),
                                false,
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                        Message::Binary(payload) => {
                            if forward_websocket_frame(
                                &mut sender,
                                &runtime,
                                &worker_name,
                                &session_id,
                                payload.to_vec(),
                                true,
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                        Message::Close(frame) => {
                            let reason = frame
                                .as_ref()
                                .map(|frame| frame.reason.to_string())
                                .unwrap_or_default();
                            let code = frame
                                .as_ref()
                                .map(|frame| u16::from(frame.code))
                                .unwrap_or(1000);
                            let _ = runtime
                                .websocket_close(worker_name.clone(), session_id.clone(), code, reason)
                                .await;
                            break;
                        }
                        Message::Ping(payload) => {
                            if sender.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Message::Pong(_) => {}
                        Message::Frame(_) => {}
                    },
                    Err(_) => break,
                }
            }
            wait = &mut wait_for_frame => {
                match wait {
                    Ok(()) => {
                        let mut should_stop = false;
                        loop {
                            match runtime
                                .websocket_drain_frame(worker_name.clone(), session_id.clone())
                                .await
                            {
                                Ok(Some(output)) => {
                                    if deliver_websocket_output(
                                        &mut sender,
                                        &runtime,
                                        &worker_name,
                                        &session_id,
                                        output,
                                        false,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        should_stop = true;
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    let _ = sender.send(Message::Close(None)).await;
                                    should_stop = true;
                                    break;
                                }
                            }
                        }
                        if should_stop {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ = sender.send(Message::Close(None)).await;
                        break;
                    }
                }
                wait_for_frame = Box::pin(runtime.websocket_wait_frame(
                    worker_name.clone(),
                    session_id.clone(),
                ));
            }
        }
    }

    let _ = runtime
        .websocket_close(worker_name.clone(), session_id.clone(), 1000, String::new())
        .await;
    let mut sessions = state.websocket_sessions.lock().await;
    sessions.remove(&session_id);
}

async fn forward_websocket_frame<S>(
    sender: &mut SplitSink<WebSocketStream<S>, Message>,
    runtime: &runtime::RuntimeService,
    worker_name: &str,
    session_id: &str,
    payload: Vec<u8>,
    is_binary: bool,
) -> std::result::Result<(), ()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match runtime
        .websocket_send_frame(
            worker_name.to_string(),
            session_id.to_string(),
            payload,
            is_binary,
        )
        .await
    {
        Ok(output) => {
            deliver_websocket_output(sender, runtime, worker_name, session_id, output, is_binary)
                .await
        }
        Err(_) => {
            let _ = sender.send(Message::Close(None)).await;
            Err(())
        }
    }
}

async fn deliver_websocket_output<S>(
    sender: &mut SplitSink<WebSocketStream<S>, Message>,
    runtime: &runtime::RuntimeService,
    worker_name: &str,
    session_id: &str,
    output: WorkerOutput,
    fallback_binary: bool,
) -> std::result::Result<(), ()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if let Some((close_code, close_reason)) = extract_websocket_close_signal(&output.headers) {
        let _ = sender
            .send(Message::Close(Some(CloseFrame {
                code: close_code.into(),
                reason: close_reason.into(),
            })))
            .await;
        let _ = runtime
            .websocket_close(
                worker_name.to_string(),
                session_id.to_string(),
                close_code,
                String::new(),
            )
            .await;
        return Err(());
    }

    if output.body.is_empty() {
        return Ok(());
    }

    let response_binary = fallback_binary
        || output
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case(HEADER_WS_BINARY) && value == "1");
    if response_binary {
        if sender
            .send(Message::Binary(output.body.into()))
            .await
            .is_err()
        {
            return Err(());
        }
        return Ok(());
    }

    if let Ok(body) = String::from_utf8(output.body.clone()) {
        if sender.send(Message::Text(body.into())).await.is_err() {
            return Err(());
        }
    } else if sender
        .send(Message::Binary(output.body.into()))
        .await
        .is_err()
    {
        return Err(());
    }

    Ok(())
}

pub(crate) fn sanitize_websocket_handshake_headers(
    headers: Vec<(String, String)>,
) -> Vec<(String, String)> {
    headers
        .into_iter()
        .filter(|(name, _)| !is_internal_proxy_upgrade_header(name))
        .filter(|(name, _)| {
            !name.eq_ignore_ascii_case(HEADER_WS_SESSION)
                && !name.eq_ignore_ascii_case(HEADER_WS_BINARY)
                && !name.eq_ignore_ascii_case(HEADER_WS_CLOSE_CODE)
                && !name.eq_ignore_ascii_case(HEADER_WS_CLOSE_REASON)
        })
        .collect()
}

pub(super) fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let Some(connection_value) = headers
        .get("connection")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(upgrade_value) = headers.get("upgrade").and_then(|value| value.to_str().ok()) else {
        return false;
    };
    if !connection_value
        .split(',')
        .map(|value| value.trim())
        .any(|value| value.eq_ignore_ascii_case("upgrade"))
    {
        return false;
    }
    upgrade_value.trim().eq_ignore_ascii_case("websocket")
}

pub(super) fn prepare_websocket_upgrade<B>(
    request: &mut Request<B>,
) -> Result<PreparedWebSocketUpgrade, PlatformError> {
    let mut builder = Request::builder()
        .method(request.method())
        .uri(request.uri().clone())
        .version(request.version());
    for (name, value) in request.headers() {
        builder = builder.header(name, value);
    }
    let handshake_request = builder
        .body(())
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    Ok(PreparedWebSocketUpgrade {
        on_upgrade: hyper::upgrade::on(request),
        handshake_request,
    })
}

fn build_websocket_handshake_response(
    request: &Request<()>,
) -> Result<Response<ResponseBody>, PlatformError> {
    let response = create_ws_response(request).map_err(|error| {
        PlatformError::bad_request(format!("invalid websocket upgrade: {error}"))
    })?;
    let (parts, _) = response.into_parts();
    let mut output = Response::builder()
        .status(parts.status)
        .body(empty_body())
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    *output.headers_mut() = parts.headers;
    Ok(output)
}

fn is_internal_proxy_upgrade_header(name: &str) -> bool {
    name.to_ascii_lowercase()
        .starts_with(HEADER_WS_INTERNAL_PREFIX)
}

fn extract_websocket_close_signal(headers: &[(String, String)]) -> Option<(u16, String)> {
    let mut close_code = None;
    let mut close_reason = String::new();
    for (name, value) in headers {
        if name.eq_ignore_ascii_case(HEADER_WS_CLOSE_CODE) {
            close_code = value.parse::<u16>().ok();
        }
        if name.eq_ignore_ascii_case(HEADER_WS_CLOSE_REASON) {
            close_reason = value.clone();
        }
    }
    close_code.map(|code| (code, close_reason))
}

pub struct PreparedWebSocketUpgrade {
    on_upgrade: OnUpgrade,
    handshake_request: Request<()>,
}
