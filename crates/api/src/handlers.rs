use crate::state::AppState;
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::http::header::{HeaderName, HeaderValue, CONTENT_LENGTH, HOST};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use common::{
    DeployBinding, DeployInternalConfig, DeployRequest, DeployResponse, ErrorBody, ErrorKind,
    PlatformError, WorkerInvocation, WorkerOutput,
};
use futures_util::{stream::SplitSink, SinkExt};
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use runtime::{CacheLookup, CacheRequest, CacheResponse};
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub type ApiResult<T> = std::result::Result<T, ApiError>;

pub struct ApiError(pub PlatformError);

const REQUEST_BODY_STREAM_CAPACITY: usize = 8;
const HEADER_CACHE: &str = "x-dd-cache";
const HEADER_CACHE_FALLBACK: &str = "x-dd-cache-fallback";
const HEADER_CACHE_BYPASS_STALE: &str = "x-dd-cache-bypass-stale";
const HEADER_TRACE_ID: &str = "x-dd-trace-id";
const HEADER_WS_INTERNAL_PREFIX: &str = "x-dd-";
const HEADER_WS_SESSION: &str = "x-dd-ws-session";
const HEADER_WS_BINARY: &str = "x-dd-ws-binary";
const HEADER_WS_CLOSE_CODE: &str = "x-dd-ws-close-code";
const HEADER_WS_CLOSE_REASON: &str = "x-dd-ws-close-reason";

pub async fn deploy_worker(
    State(state): State<AppState>,
    Json(payload): Json<DeployRequest>,
) -> ApiResult<Json<DeployResponse>> {
    let name = payload.name.trim();
    let span = tracing::info_span!("http.deploy", worker.name = %name);
    let _guard = span.enter();
    if name.is_empty() {
        return Err(PlatformError::bad_request("Worker name must not be empty").into());
    }

    validate_deploy_bindings(&payload.config.bindings)?;
    validate_internal_config(&payload.config.internal)?;
    let deployment_id = state
        .runtime
        .deploy_with_config(name.to_string(), payload.source, payload.config)
        .await?;
    tracing::info!(deployment_id = %deployment_id, "worker deployed");

    Ok(Json(DeployResponse {
        ok: true,
        worker: name.to_string(),
        deployment_id,
    }))
}

pub async fn invoke_worker_private(
    State(state): State<AppState>,
    ws_upgrade: Option<WebSocketUpgrade>,
    request: Request,
) -> ApiResult<Response<Body>> {
    if ws_upgrade.is_some() && is_websocket_upgrade(request.headers()) {
        return invoke_worker_websocket_private(state, request, ws_upgrade).await;
    }
    let (parts, body) = request.into_parts();
    let (worker_name, url) =
        parse_invoke_request_uri(parts.uri.path(), parts.uri.path_and_query())?;
    invoke_worker_with_target(state, parts, body, worker_name, url).await
}

pub async fn invoke_worker_public(
    State(state): State<AppState>,
    ws_upgrade: Option<WebSocketUpgrade>,
    request: Request,
) -> ApiResult<Response<Body>> {
    if ws_upgrade.is_some() && is_websocket_upgrade(request.headers()) {
        return invoke_worker_websocket_public(state, request, ws_upgrade).await;
    }
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    if path.starts_with("/v1/deploy") || path.starts_with("/v1/invoke") {
        return Err(PlatformError::not_found("not found").into());
    }
    let worker_name =
        parse_public_worker_name_from_host(&parts.headers, &state.public_base_domain)?;
    ensure_public_worker(&state, &worker_name).await?;
    let url = build_worker_url(parts.uri.path(), parts.uri.path_and_query());
    invoke_worker_with_target(state, parts, body, worker_name, url).await
}

async fn invoke_worker_with_target(
    state: AppState,
    parts: axum::http::request::Parts,
    body: Body,
    worker_name: String,
    url: String,
) -> ApiResult<Response<Body>> {
    let method = parts.method.as_str().to_string();
    let invoke_span = tracing::info_span!(
        "http.invoke",
        worker.name = %worker_name,
        http.method = %method,
        http.route = %parts.uri.path()
    );
    set_span_parent_from_http_headers(&invoke_span, &parts.headers);
    let _invoke_guard = invoke_span.enter();

    let max_body_bytes = state.invoke_max_body_bytes;
    if request_content_length(&parts.headers).is_some_and(|value| value > max_body_bytes as u64) {
        return Err(PlatformError::bad_request(format!(
            "request body too large (max {max_body_bytes} bytes)"
        ))
        .into());
    }
    let request_body_stream = build_request_body_stream(body, max_body_bytes);

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
        method,
        url,
        headers,
        body: Vec::new(),
        request_id: request_id.clone(),
    };
    tracing::info!(request_id = %request_id, "invoke request accepted");

    if !is_cacheable_request(&invocation) {
        let output = state
            .runtime
            .invoke_stream_with_request_body(worker_name, invocation, Some(request_body_stream))
            .await?;
        let mut response = build_worker_stream_response(output)?;
        annotate_response_with_trace_id(&mut response);
        return Ok(response);
    }

    let cache_request = build_edge_cache_request(&worker_name, &invocation);
    match state.runtime.cache_match(cache_request.clone()).await? {
        CacheLookup::Fresh(response) => {
            tracing::info!(request_id = %request_id, cache_status = "HIT", "edge cache hit");
            return build_cached_response(response, "HIT");
        }
        CacheLookup::StaleWhileRevalidate(response) => {
            tracing::info!(
                request_id = %request_id,
                cache_status = "STALE",
                "edge cache stale hit, scheduling revalidation"
            );
            maybe_spawn_edge_revalidation(
                state.clone(),
                worker_name.clone(),
                invocation.clone(),
                cache_request.clone(),
            )
            .await;
            return build_cached_response(response, "STALE");
        }
        CacheLookup::StaleIfError(response) => {
            let origin = state
                .runtime
                .invoke_with_request_body(worker_name, invocation, Some(request_body_stream))
                .await;
            return match origin {
                Ok(output) => {
                    if output.status >= 500 {
                        tracing::warn!(
                            request_id = %request_id,
                            status = output.status,
                            cache_status = "STALE-IF-ERROR",
                            "origin returned 5xx, serving stale"
                        );
                        let mut fallback = build_cached_response(response, "STALE-IF-ERROR")?;
                        fallback.headers_mut().append(
                            HeaderName::from_static(HEADER_CACHE_FALLBACK),
                            HeaderValue::from_static("origin-status"),
                        );
                        return Ok(fallback);
                    }
                    store_worker_output_in_cache(&state, &cache_request, &output).await;
                    tracing::info!(request_id = %request_id, cache_status = "MISS", "cache refreshed from origin");
                    build_worker_buffered_response(output, "MISS")
                }
                Err(_error) => {
                    tracing::warn!(
                        request_id = %request_id,
                        cache_status = "STALE-IF-ERROR",
                        "origin failed, serving stale"
                    );
                    let mut response = build_cached_response(response, "STALE-IF-ERROR")?;
                    response.headers_mut().append(
                        HeaderName::from_static(HEADER_CACHE_FALLBACK),
                        HeaderValue::from_static("origin-error"),
                    );
                    Ok(response)
                }
            };
        }
        CacheLookup::Miss => {}
    }

    let output = state
        .runtime
        .invoke_with_request_body(worker_name, invocation, Some(request_body_stream))
        .await?;
    store_worker_output_in_cache(&state, &cache_request, &output).await;
    tracing::info!(request_id = %request_id, cache_status = "MISS", "origin miss stored");
    build_worker_buffered_response(output, "MISS")
}

async fn invoke_worker_websocket_private(
    state: AppState,
    request: Request,
    ws_upgrade: Option<WebSocketUpgrade>,
) -> ApiResult<Response<Body>> {
    let (parts, body) = request.into_parts();
    let (worker_name, url) =
        parse_invoke_request_uri(parts.uri.path(), parts.uri.path_and_query())?;
    invoke_worker_websocket_with_target(state, parts, body, worker_name, url, ws_upgrade).await
}

async fn invoke_worker_websocket_public(
    state: AppState,
    request: Request,
    ws_upgrade: Option<WebSocketUpgrade>,
) -> ApiResult<Response<Body>> {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    if path.starts_with("/v1/deploy") || path.starts_with("/v1/invoke") {
        return Err(PlatformError::not_found("not found").into());
    }
    let worker_name =
        parse_public_worker_name_from_host(&parts.headers, &state.public_base_domain)?;
    ensure_public_worker(&state, &worker_name).await?;
    let url = build_worker_url(parts.uri.path(), parts.uri.path_and_query());
    invoke_worker_websocket_with_target(state, parts, body, worker_name, url, ws_upgrade).await
}

async fn invoke_worker_websocket_with_target(
    state: AppState,
    parts: axum::http::request::Parts,
    _body: Body,
    worker_name: String,
    url: String,
    ws_upgrade: Option<WebSocketUpgrade>,
) -> ApiResult<Response<Body>> {
    let Some(ws_upgrade) = ws_upgrade else {
        return Err(PlatformError::bad_request("missing websocket upgrade"));
    };

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

    let runtime::WebSocketOpen {
        session_id,
        worker_name: runtime_worker_name,
        output,
    } = state
        .runtime
        .open_websocket(worker_name, invocation, None)
        .await
        .map_err(|error| PlatformError::bad_request(format!("websocket open failed: {error}")))?;

    if output.status != 101 {
        return Err(PlatformError::bad_request(
            "websocket upgrade rejected by worker",
        ));
    }

    let filtered_headers = sanitize_websocket_handshake_headers(output.headers);
    let runtime = state.runtime.clone();
    let state_for_session = state.clone();
    let handshake_session_id = session_id.clone();
    let runtime_worker_name_for_frames = runtime_worker_name.clone();

    let response = ws_upgrade.on_upgrade(move |socket| {
        let request_worker_name = runtime_worker_name_for_frames.clone();
        async move {
            {
                let mut sessions = state_for_session.websocket_sessions.lock().await;
                sessions.insert(
                    handshake_session_id.clone(),
                    crate::state::WebSocketSession {
                        id: handshake_session_id.clone(),
                        worker_name: request_worker_name.clone(),
                        started_at: std::time::Instant::now(),
                    },
                );
            }
            handle_websocket_session(
                state_for_session,
                socket,
                handshake_session_id,
                request_worker_name,
                runtime,
                output.body,
            )
            .await;
        }
    });

    let mut response = response.into_response();
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

async fn handle_websocket_session(
    state: AppState,
    socket: WebSocket,
    session_id: String,
    worker_name: String,
    runtime: runtime::RuntimeService,
    _initial_response_body: Vec<u8>,
) {
    let (mut sender, mut receiver) = socket.split();

    while let Some(message) = receiver.next().await {
        match message {
            Ok(message) => match message {
                Message::Text(text) => {
                    if forward_websocket_frame(
                        &mut sender,
                        &runtime,
                        &worker_name,
                        &session_id,
                        text.into_bytes(),
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
                    let _ = runtime
                        .websocket_close(worker_name.clone(), session_id.clone(), 1000, reason)
                        .await;
                    break;
                }
                Message::Ping(payload) => {
                    if sender.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Message::Pong(_) => {}
            },
            Err(_) => break,
        }
    }

    let _ = runtime
        .websocket_close(worker_name.clone(), session_id.clone(), 1000, String::new())
        .await;
    let mut sessions = state.websocket_sessions.lock().await;
    sessions.remove(&session_id);
}

async fn forward_websocket_frame(
    sender: &mut SplitSink<WebSocket, Message>,
    runtime: &runtime::RuntimeService,
    worker_name: &str,
    session_id: &str,
    payload: Vec<u8>,
    is_binary: bool,
) -> std::result::Result<(), ()> {
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
            if let Some((close_code, close_reason)) =
                extract_websocket_close_signal(&output.headers)
            {
                let _ = sender
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: close_code,
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
            if !output.body.is_empty() {
                let response_binary = is_binary
                    || output.headers.iter().any(|(name, value)| {
                        name.eq_ignore_ascii_case(HEADER_WS_BINARY) && value == "1"
                    });
                if response_binary {
                    if sender
                        .send(Message::Binary(output.body.into()))
                        .await
                        .is_err()
                    {
                        return Err(());
                    }
                } else if let Ok(body) = String::from_utf8(output.body.clone()) {
                    if sender.send(Message::Text(body)).await.is_err() {
                        return Err(());
                    }
                } else if sender
                    .send(Message::Binary(output.body.into()))
                    .await
                    .is_err()
                {
                    return Err(());
                }
            }
            Ok(())
        }
        Err(_) => {
            let _ = sender.send(Message::Close(None)).await;
            Err(())
        }
    }
}

fn sanitize_websocket_handshake_headers(headers: Vec<(String, String)>) -> Vec<(String, String)> {
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

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
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

fn parse_public_worker_name_from_host(
    headers: &HeaderMap,
    public_base_domain: &str,
) -> Result<String, PlatformError> {
    let host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| PlatformError::not_found("not found"))?;
    parse_worker_from_host(host, public_base_domain)
}

async fn ensure_public_worker(state: &AppState, worker_name: &str) -> Result<(), PlatformError> {
    let Some(stats) = state.runtime.stats(worker_name.to_string()).await else {
        return Err(PlatformError::not_found("not found"));
    };
    if !stats.public {
        return Err(PlatformError::not_found("not found"));
    }
    Ok(())
}

fn parse_invoke_request_uri(
    path: &str,
    path_and_query: Option<&axum::http::uri::PathAndQuery>,
) -> Result<(String, String), PlatformError> {
    let remainder = path
        .strip_prefix("/v1/invoke")
        .ok_or_else(|| PlatformError::bad_request("invalid invoke route"))?;
    let remainder = remainder.strip_prefix('/').unwrap_or(remainder);
    if remainder.is_empty() {
        return Err(PlatformError::bad_request("worker name must not be empty"));
    }

    let mut segments = remainder.splitn(2, '/');
    let worker_name = segments.next().unwrap_or_default().trim();
    if worker_name.is_empty() {
        return Err(PlatformError::bad_request("worker name must not be empty"));
    }

    let path_suffix = segments.next().unwrap_or_default();
    let url_path = if path_suffix.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path_suffix)
    };
    let query_suffix = match path_and_query {
        Some(path_and_query) => path_and_query
            .as_str()
            .split_once('?')
            .map(|(_, query)| format!("?{query}"))
            .unwrap_or_default(),
        None => String::new(),
    };
    let url = format!("http://worker{}{}", url_path, query_suffix);

    Ok((worker_name.to_string(), url))
}

fn build_worker_url(path: &str, path_and_query: Option<&axum::http::uri::PathAndQuery>) -> String {
    let normalized_path = if path.is_empty() { "/" } else { path };
    let query_suffix = match path_and_query {
        Some(path_and_query) => path_and_query
            .as_str()
            .split_once('?')
            .map(|(_, query)| format!("?{query}"))
            .unwrap_or_default(),
        None => String::new(),
    };
    format!("http://worker{}{}", normalized_path, query_suffix)
}

fn parse_worker_from_host(host: &str, public_base_domain: &str) -> Result<String, PlatformError> {
    let Some(host) = normalize_host(host) else {
        return Err(PlatformError::not_found("not found"));
    };
    let Some(base_domain) = normalize_host(public_base_domain) else {
        return Err(PlatformError::internal("invalid PUBLIC_BASE_DOMAIN"));
    };
    if host == base_domain {
        return Err(PlatformError::not_found("not found"));
    }

    let suffix = format!(".{base_domain}");
    if !host.ends_with(&suffix) {
        return Err(PlatformError::not_found("not found"));
    }

    let prefix = &host[..host.len() - suffix.len()];
    let worker_name = prefix.split('.').next().unwrap_or_default().trim();
    if worker_name.is_empty() {
        return Err(PlatformError::not_found("not found"));
    }

    Ok(worker_name.to_string())
}

fn normalize_host(host: &str) -> Option<String> {
    let trimmed = host.trim().trim_end_matches('.');
    if trimmed.is_empty() {
        return None;
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with('[') {
        return Some(lower);
    }

    if let Some((name, port)) = lower.rsplit_once(':') {
        if port.chars().all(|value| value.is_ascii_digit()) {
            if name.is_empty() {
                return None;
            }
            return Some(name.to_string());
        }
    }

    Some(lower)
}

fn build_request_body_stream(body: Body, max_bytes: usize) -> runtime::InvokeRequestBodyReceiver {
    let (tx, rx) = mpsc::channel(REQUEST_BODY_STREAM_CAPACITY);
    tokio::spawn(async move {
        let mut stream = body.into_data_stream();
        let mut total = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = tx
                        .send(Err(format!("failed to read request body: {error}")))
                        .await;
                    return;
                }
            };
            total = total.saturating_add(chunk.len());
            if total > max_bytes {
                let _ = tx
                    .send(Err(format!(
                        "request body too large (max {max_bytes} bytes)"
                    )))
                    .await;
                return;
            }
            if tx.send(Ok(chunk.to_vec())).await.is_err() {
                return;
            }
        }
    });
    rx
}

fn request_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn validate_deploy_bindings(bindings: &[DeployBinding]) -> Result<(), PlatformError> {
    let mut seen = HashSet::new();
    for binding in bindings {
        match binding {
            DeployBinding::Kv { binding } | DeployBinding::Actor { binding, .. }
                if binding.trim().is_empty() =>
            {
                return Err(PlatformError::bad_request("binding name must not be empty"));
            }
            DeployBinding::Actor { class, .. } if class.trim().is_empty() => {
                return Err(PlatformError::bad_request("actor class must not be empty"));
            }
            DeployBinding::Kv { binding } | DeployBinding::Actor { binding, .. } => {
                let normalized = binding.trim().to_string();
                if !seen.insert(normalized.clone()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {normalized}"
                    )));
                }
            }
        }
    }

    Ok(())
}

fn validate_internal_config(internal: &DeployInternalConfig) -> Result<(), PlatformError> {
    let Some(trace) = internal.trace.as_ref() else {
        return Ok(());
    };

    let worker = trace.worker.trim();
    if worker.is_empty() {
        return Err(PlatformError::bad_request("trace worker must not be empty"));
    }

    let path = trace.path.trim();
    if path.is_empty() {
        return Err(PlatformError::bad_request("trace path must not be empty"));
    }
    if !path.starts_with('/') {
        return Err(PlatformError::bad_request("trace path must start with '/'"));
    }

    Ok(())
}

fn build_worker_stream_response(
    worker_response: runtime::WorkerStreamOutput,
) -> ApiResult<Response<Body>> {
    let mut response = Response::builder()
        .status(worker_response.status)
        .body(Body::from_stream(
            tokio_stream::wrappers::UnboundedReceiverStream::new(worker_response.body).map(
                |chunk| {
                    chunk
                        .map(axum::body::Bytes::from)
                        .map_err(|error| std::io::Error::other(error.to_string()))
                },
            ),
        ))
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    for (name, value) in worker_response.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().append(name, value);
        }
    }
    annotate_response_with_trace_id(&mut response);

    Ok(response)
}

fn is_cacheable_request(invocation: &WorkerInvocation) -> bool {
    if !invocation.method.eq_ignore_ascii_case("GET") {
        return false;
    }
    !invocation.headers.iter().any(|(name, value)| {
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<u64>()
                .map(|size| size > 0)
                .unwrap_or(true);
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return !value.trim().is_empty();
        }
        false
    })
}

fn build_edge_cache_request(worker_name: &str, invocation: &WorkerInvocation) -> CacheRequest {
    CacheRequest {
        cache_name: format!("edge:{worker_name}"),
        method: invocation.method.clone(),
        url: invocation.url.clone(),
        headers: invocation.headers.clone(),
        bypass_stale: false,
    }
}

fn build_cached_response(
    cache_response: CacheResponse,
    cache_status: &str,
) -> ApiResult<Response<Body>> {
    build_buffered_response(
        cache_response.status,
        cache_response.headers,
        cache_response.body,
        cache_status,
    )
}

fn build_worker_buffered_response(
    output: WorkerOutput,
    cache_status: &str,
) -> ApiResult<Response<Body>> {
    build_buffered_response(output.status, output.headers, output.body, cache_status)
}

fn build_buffered_response(
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    cache_status: &str,
) -> ApiResult<Response<Body>> {
    let mut response = Response::builder()
        .status(status)
        .body(Body::from(body))
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().append(name, value);
        }
    }

    response.headers_mut().insert(
        HeaderName::from_static(HEADER_CACHE),
        HeaderValue::from_str(cache_status)
            .map_err(|error| PlatformError::internal(error.to_string()))?,
    );
    annotate_response_with_trace_id(&mut response);
    Ok(response)
}

async fn store_worker_output_in_cache(
    state: &AppState,
    request: &CacheRequest,
    output: &WorkerOutput,
) {
    let _ = state
        .runtime
        .cache_put(
            request.clone(),
            CacheResponse {
                status: output.status,
                headers: output.headers.clone(),
                body: output.body.clone(),
            },
        )
        .await;
}

async fn maybe_spawn_edge_revalidation(
    state: AppState,
    worker_name: String,
    mut invocation: WorkerInvocation,
    cache_request: CacheRequest,
) {
    let key = edge_revalidation_key(&worker_name, &cache_request);
    {
        let mut inflight = state.edge_revalidations.lock().await;
        if !inflight.insert(key.clone()) {
            return;
        }
    }

    invocation.request_id = format!("edge-revalidate-{}", Uuid::new_v4());
    if !invocation
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case(HEADER_CACHE_BYPASS_STALE))
    {
        invocation
            .headers
            .push((HEADER_CACHE_BYPASS_STALE.to_string(), "1".to_string()));
    }

    tokio::spawn(async move {
        let origin = state.runtime.invoke(worker_name, invocation).await;
        if let Ok(output) = origin {
            let _ = state
                .runtime
                .cache_put(
                    cache_request,
                    CacheResponse {
                        status: output.status,
                        headers: output.headers,
                        body: output.body,
                    },
                )
                .await;
        }
        let mut inflight = state.edge_revalidations.lock().await;
        inflight.remove(&key);
    });
}

fn edge_revalidation_key(worker_name: &str, cache_request: &CacheRequest) -> String {
    let mut headers: Vec<(String, String)> = cache_request
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
        .collect();
    headers.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let header_key = headers
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    format!(
        "{worker_name}:{}:{}:{}:{header_key}",
        cache_request.cache_name,
        cache_request.method.to_ascii_uppercase(),
        cache_request.url
    )
}

fn set_span_parent_from_http_headers(span: &Span, headers: &HeaderMap) {
    global::get_text_map_propagator(|propagator| {
        let parent = propagator.extract(&HttpHeaderExtractor(headers));
        if parent.span().span_context().is_valid() {
            span.set_parent(parent);
        }
    });
}

fn inject_current_trace_context(headers: &mut Vec<(String, String)>) {
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        let mut injector = InvocationHeaderInjector(headers);
        propagator.inject_context(&context, &mut injector);
    });
}

fn annotate_response_with_trace_id(response: &mut Response<Body>) {
    let context = Span::current().context();
    let span = context.span();
    let span_context = span.span_context();
    if !span_context.is_valid() {
        return;
    }
    if let Ok(value) = HeaderValue::from_str(&span_context.trace_id().to_string()) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(HEADER_TRACE_ID), value);
    }
}

struct HttpHeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HttpHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

struct InvocationHeaderInjector<'a>(&'a mut Vec<(String, String)>);

impl Injector for InvocationHeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Some(existing) = self
            .0
            .iter_mut()
            .find(|(name, _)| name.eq_ignore_ascii_case(key))
        {
            existing.1 = value;
            return;
        }
        self.0.push((key.to_string(), value));
    }
}

impl From<PlatformError> for ApiError {
    fn from(value: PlatformError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        let status = match self.0.kind() {
            ErrorKind::BadRequest | ErrorKind::Runtime => StatusCode::BAD_REQUEST,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let mut response = (status, Json(ErrorBody::from_error(&self.0))).into_response();
        annotate_response_with_trace_id(&mut response);
        response
    }
}

#[cfg(test)]
mod tests {
    use super::{
        deploy_worker, invoke_worker_private, invoke_worker_public, parse_invoke_request_uri,
        parse_worker_from_host, validate_deploy_bindings, validate_internal_config,
    };
    use crate::app::{private_router, public_router};
    use crate::state::AppState;
    use axum::body::{to_bytes, Body};
    use axum::extract::State;
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::Json;
    use common::{
        DeployBinding, DeployConfig, DeployInternalConfig, DeployRequest, DeployTraceDestination,
        ErrorKind,
    };
    use runtime::{RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
    use std::path::PathBuf;
    use tower::ServiceExt;
    use uuid::Uuid;

    async fn create_test_state(public_base_domain: &str) -> AppState {
        let store_dir = PathBuf::from(format!("./target/test-store-api-{}", Uuid::new_v4()));
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
        AppState::new(runtime, 1024 * 1024, public_base_domain.to_string())
    }

    #[test]
    fn parse_invoke_path_and_query() {
        let uri: axum::http::Uri = "/v1/invoke/hello/api/v1?x=1&y=2".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/api/v1?x=1&y=2");
    }

    #[test]
    fn parse_invoke_root_path() {
        let uri: axum::http::Uri = "/v1/invoke/hello".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/");
    }

    #[test]
    fn duplicate_bindings_are_rejected() {
        let bindings = vec![
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
        ];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn kv_and_actor_binding_name_collision_is_rejected() {
        let bindings = vec![
            DeployBinding::Kv {
                binding: "SHARED".to_string(),
            },
            DeployBinding::Actor {
                binding: "SHARED".to_string(),
                class: "SharedActor".to_string(),
            },
        ];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn empty_actor_class_is_rejected() {
        let bindings = vec![DeployBinding::Actor {
            binding: "MY_ACTOR".to_string(),
            class: String::new(),
        }];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn accepts_no_trace_config() {
        let config = DeployInternalConfig::default();
        assert!(validate_internal_config(&config).is_ok());
    }

    #[test]
    fn rejects_empty_trace_worker() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: " ".to_string(),
                path: "/ingest".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_err());
    }

    #[test]
    fn rejects_empty_trace_path() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: "worker".to_string(),
                path: " ".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_err());
    }

    #[test]
    fn rejects_trace_path_without_leading_slash() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: "worker".to_string(),
                path: "ingest".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_err());
    }

    #[test]
    fn accepts_valid_trace_config() {
        let config = DeployInternalConfig {
            trace: Some(DeployTraceDestination {
                worker: "worker".to_string(),
                path: "/ingest".to_string(),
            }),
        };
        assert!(validate_internal_config(&config).is_ok());
    }

    #[test]
    fn worker_name_is_extracted_from_subdomain_host() {
        let worker = parse_worker_from_host("echo.example.com:443", "example.com").expect("ok");
        assert_eq!(worker, "echo");
    }

    #[test]
    fn worker_name_is_extracted_with_case_insensitive_host() {
        let worker = parse_worker_from_host("Echo.Example.Com", "EXAMPLE.COM").expect("ok");
        assert_eq!(worker, "echo");
    }

    #[test]
    fn apex_host_is_rejected() {
        let error = parse_worker_from_host("example.com", "example.com").expect_err("error");
        assert_eq!(error.kind(), common::ErrorKind::NotFound);
    }

    #[test]
    fn foreign_host_is_rejected() {
        let error = parse_worker_from_host("echo.other.com", "example.com").expect_err("error");
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    #[ignore = "starts full runtime service; run manually in isolation"]
    async fn public_listener_blocks_deploy_path() {
        let state = create_test_state("example.com").await;
        let request = Request::builder()
            .method("POST")
            .uri("/v1/deploy")
            .header("host", "echo.example.com")
            .body(Body::empty())
            .expect("request");
        let error = invoke_worker_public(State(state), None, request)
            .await
            .expect_err("public deploy path should be blocked");
        assert_eq!(error.0.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    #[ignore = "starts full runtime service; run manually in isolation"]
    async fn private_deploy_and_invoke_succeeds() {
        let state = create_test_state("example.com").await;
        let deploy = DeployRequest {
            name: "echo".to_string(),
            source: "export default { async fetch() { return new Response('ok'); } }".to_string(),
            config: DeployConfig {
                public: false,
                bindings: vec![],
                ..Default::default()
            },
        };
        let response = match deploy_worker(State(state.clone()), Json(deploy)).await {
            Ok(value) => value,
            Err(_) => panic!("deploy should succeed"),
        };
        assert!(response.0.ok);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/invoke/echo")
            .body(Body::empty())
            .expect("request");
        let response = match invoke_worker_private(State(state), None, request).await {
            Ok(value) => value,
            Err(_) => panic!("invoke should succeed"),
        };
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(body.as_ref(), b"ok");
    }

    #[tokio::test]
    #[ignore = "starts full runtime service; run manually in isolation"]
    async fn public_host_invoke_routes_by_subdomain() {
        let state = create_test_state("example.com").await;
        state
            .runtime
            .deploy_with_config(
                "echo".to_string(),
                "export default { async fetch() { return new Response('host-ok'); } }".to_string(),
                DeployConfig {
                    public: true,
                    bindings: vec![],
                    ..Default::default()
                },
            )
            .await
            .expect("deploy");

        let request = Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "echo.example.com")
            .body(Body::empty())
            .expect("request");
        let response = match invoke_worker_public(State(state), None, request).await {
            Ok(value) => value,
            Err(_) => panic!("invoke should succeed"),
        };
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(body.as_ref(), b"host-ok");
    }

    #[tokio::test]
    #[ignore = "starts full runtime service; run manually in isolation"]
    async fn public_host_invoke_rejects_private_worker() {
        let state = create_test_state("example.com").await;
        state
            .runtime
            .deploy_with_config(
                "private-worker".to_string(),
                "export default { async fetch() { return new Response('private-ok'); } }"
                    .to_string(),
                DeployConfig {
                    public: false,
                    bindings: vec![],
                    ..Default::default()
                },
            )
            .await
            .expect("deploy");

        let request = Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "private-worker.example.com")
            .body(Body::empty())
            .expect("request");
        let error = invoke_worker_public(State(state), None, request)
            .await
            .expect_err("private worker should not be public");
        assert_eq!(error.0.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn websocket_upgrade_checks_required_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", "Upgrade".parse().expect("header"));
        headers.insert("upgrade", "websocket".parse().expect("header"));
        assert!(super::is_websocket_upgrade(&headers));

        let mut non_ws = HeaderMap::new();
        non_ws.insert("connection", "keep-alive".parse().expect("header"));
        non_ws.insert("upgrade", "websocket".parse().expect("header"));
        assert!(!super::is_websocket_upgrade(&non_ws));
    }

    #[tokio::test]
    #[ignore = "starts full runtime service and http stack; run manually in isolation"]
    async fn private_websocket_route_rejects_non_actor_upgrade() {
        let state = create_test_state("example.com").await;
        state
            .runtime
            .deploy_with_config(
                "echo".to_string(),
                "export default { async fetch() { return new Response('ok'); } }".to_string(),
                DeployConfig {
                    public: false,
                    bindings: vec![],
                    ..Default::default()
                },
            )
            .await
            .expect("deploy");

        let app = private_router(state.clone());
        let request = Request::builder()
            .method("GET")
            .uri("/v1/invoke/echo")
            .header("upgrade", "websocket")
            .header("connection", "Upgrade")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13")
            .body(Body::empty())
            .expect("request");

        let response = app.oneshot(request).await.expect("request").status();
        assert_eq!(response, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    #[ignore = "starts full runtime service and http stack; run manually in isolation"]
    async fn public_websocket_route_rejects_non_actor_upgrade() {
        let state = create_test_state("example.com").await;
        state
            .runtime
            .deploy_with_config(
                "echo".to_string(),
                "export default { async fetch() { return new Response('ok'); } }".to_string(),
                DeployConfig {
                    public: true,
                    bindings: vec![],
                    ..Default::default()
                },
            )
            .await
            .expect("deploy");

        let app = public_router(state.clone());
        let request = Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "echo.example.com")
            .header("upgrade", "websocket")
            .header("connection", "Upgrade")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13")
            .body(Body::empty())
            .expect("request");

        let response = app.oneshot(request).await.expect("request").status();
        assert_eq!(response, StatusCode::BAD_REQUEST);
    }
}
