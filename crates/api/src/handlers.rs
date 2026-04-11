use crate::state::AppState;
use bytes::Bytes;
use common::{
    DeployBinding, DeployInternalConfig, DeployRequest, DeployResponse, DynamicDeployRequest,
    DynamicDeployResponse, ErrorBody, ErrorKind, PlatformError, WorkerInvocation, WorkerOutput,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use http::header::{
    HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, HOST, WWW_AUTHENTICATE,
};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body::Body as HttpBody;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::Frame;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use runtime::{CacheLookup, CacheRequest, CacheResponse};
use std::collections::HashSet;
use std::convert::Infallible;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::tungstenite::handshake::server::create_response as create_ws_response;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, Role};
use tokio_tungstenite::WebSocketStream;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub type ApiResult<T> = std::result::Result<T, ApiError>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ResponseBody = BoxBody<Bytes, BoxError>;

#[derive(Debug)]
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

pub async fn handle_private_request<B>(
    state: AppState,
    request: Request<B>,
) -> Response<ResponseBody>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    respond(route_private_request(state, request).await)
}

pub async fn handle_public_request<B>(
    state: AppState,
    request: Request<B>,
) -> Response<ResponseBody>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    respond(route_public_request(state, request).await)
}

pub async fn handle_public_h3_request(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> Response<ResponseBody> {
    respond(route_public_h3_request(state, request, request_body_stream).await)
}

async fn route_private_request<B>(
    state: AppState,
    mut request: Request<B>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let path = request.uri().path().to_string();
    if private_route_requires_auth(&path)
        && !private_request_is_authorized(&state, request.headers())
    {
        return Ok(private_auth_response());
    }
    if request.method() == Method::POST && path == "/v1/deploy" {
        let payload: DeployRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let response = deploy_worker(state, payload).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if request.method() == Method::POST && path == "/v1/dynamic/deploy" {
        let payload: DynamicDeployRequest =
            read_json_body(request.into_body(), state.invoke_max_body_bytes).await?;
        let response = deploy_dynamic_worker(state, payload).await?;
        return Ok(json_response(StatusCode::OK, &response)?);
    }
    if path == "/v1/invoke" || path.starts_with("/v1/invoke/") {
        let ws_upgrade = if is_websocket_upgrade(request.headers()) {
            Some(prepare_websocket_upgrade(&mut request)?)
        } else {
            None
        };
        return invoke_worker_private(state, request, ws_upgrade).await;
    }
    Err(PlatformError::not_found("not found").into())
}

async fn route_public_request<B>(
    state: AppState,
    mut request: Request<B>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let path = request.uri().path().to_string();
    if path.starts_with("/v1/deploy")
        || path.starts_with("/v1/dynamic")
        || path.starts_with("/v1/invoke")
    {
        return Err(PlatformError::not_found("not found").into());
    }
    let ws_upgrade = if is_websocket_upgrade(request.headers()) {
        Some(prepare_websocket_upgrade(&mut request)?)
    } else {
        None
    };
    invoke_worker_public(state, request, ws_upgrade).await
}

async fn route_public_h3_request(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> ApiResult<Response<ResponseBody>> {
    let path = request.uri().path().to_string();
    if path.starts_with("/v1/deploy")
        || path.starts_with("/v1/dynamic")
        || path.starts_with("/v1/invoke")
    {
        return Err(PlatformError::not_found("not found").into());
    }
    if is_websocket_upgrade(request.headers()) {
        return Err(
            PlatformError::bad_request("websocket upgrade is unsupported over http/3").into(),
        );
    }
    if request.method() == Method::CONNECT {
        return Err(PlatformError::bad_request("CONNECT is unsupported over http/3").into());
    }
    invoke_worker_public_h3(state, request, request_body_stream).await
}

pub async fn deploy_worker(state: AppState, payload: DeployRequest) -> ApiResult<DeployResponse> {
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
        .deploy_with_bundle_config(
            name.to_string(),
            payload.source,
            payload.config,
            payload.assets,
            payload.asset_headers,
        )
        .await?;
    tracing::info!(deployment_id = %deployment_id, "worker deployed");

    Ok(DeployResponse {
        ok: true,
        worker: name.to_string(),
        deployment_id,
    })
}

pub async fn deploy_dynamic_worker(
    state: AppState,
    payload: DynamicDeployRequest,
) -> ApiResult<DynamicDeployResponse> {
    if payload.source.trim().is_empty() {
        return Err(PlatformError::bad_request("Worker source must not be empty").into());
    }
    let deployed = state
        .runtime
        .deploy_dynamic(payload.source, payload.env, payload.egress_allow_hosts)
        .await?;

    Ok(DynamicDeployResponse {
        ok: true,
        worker: deployed.worker,
        deployment_id: deployed.deployment_id,
        env_placeholders: deployed.env_placeholders,
    })
}

pub async fn invoke_worker_private<B>(
    state: AppState,
    request: Request<B>,
    ws_upgrade: Option<PreparedWebSocketUpgrade>,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    if ws_upgrade.is_some() {
        return invoke_worker_websocket_private(state, request, ws_upgrade).await;
    }
    let (parts, body) = request.into_parts();
    let (worker_name, url) =
        parse_invoke_request_uri(parts.uri.path(), parts.uri.path_and_query())?;
    invoke_worker_with_target(state, parts, body, worker_name, url).await
}

pub async fn invoke_worker_public<B>(
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
    let request = Request::from_parts(parts, body);
    if ws_upgrade.is_some() {
        return invoke_worker_websocket_public(state, request, ws_upgrade).await;
    }
    let (parts, body) = request.into_parts();
    invoke_worker_with_target(state, parts, body, worker_name, url).await
}

pub async fn invoke_worker_public_h3(
    state: AppState,
    request: Request<()>,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
) -> ApiResult<Response<ResponseBody>> {
    let (parts, _body) = request.into_parts();
    let worker_name = parse_public_worker_name_from_request(
        &parts.headers,
        &parts.uri,
        &state.public_base_domain,
    )?;
    ensure_public_worker(&state, &worker_name).await?;
    let url = build_public_request_url(&parts.headers, &parts.uri)?;
    invoke_worker_from_body_stream(state, parts, request_body_stream, worker_name, url).await
}

async fn invoke_worker_with_target<B>(
    state: AppState,
    parts: http::request::Parts,
    body: B,
    worker_name: String,
    url: String,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
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
    let request_body_stream = if request_method_allows_body(&parts.method) {
        Some(build_request_body_stream(body, max_body_bytes))
    } else {
        None
    };
    invoke_worker_from_body_stream(state, parts, request_body_stream, worker_name, url).await
}

async fn invoke_worker_from_body_stream(
    state: AppState,
    parts: http::request::Parts,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
    worker_name: String,
    url: String,
) -> ApiResult<Response<ResponseBody>> {
    let method = parts.method.as_str().to_string();
    let invoke_span = tracing::info_span!(
        "http.invoke",
        worker.name = %worker_name,
        http.method = %method,
        http.route = %parts.uri.path()
    );
    set_span_parent_from_http_headers(&invoke_span, &parts.headers);
    let _invoke_guard = invoke_span.enter();

    let mut headers = Vec::with_capacity(parts.headers.len());
    for (name, value) in &parts.headers {
        let value = value.to_str().map_err(|error| {
            PlatformError::bad_request(format!("invalid header value for {name}: {error}"))
        })?;
        headers.push((name.as_str().to_string(), value.to_string()));
    }
    if let Some(asset_response) = try_serve_static_asset(
        &state,
        &worker_name,
        &method,
        request_host_for_matching(&parts.headers, &parts.uri),
        worker_asset_path(&url)?,
        headers.clone(),
    )
    .await?
    {
        return Ok(asset_response);
    }
    inject_current_trace_context(&mut headers);
    let request_id = Uuid::new_v4().to_string();

    let invocation = WorkerInvocation {
        method: method.clone(),
        url,
        headers,
        body: Vec::new(),
        request_id: request_id.clone(),
    };
    tracing::info!(request_id = %request_id, "invoke request accepted");

    if !is_cacheable_request(&invocation) {
        let output = state
            .runtime
            .invoke_stream_with_request_body(worker_name, invocation, request_body_stream)
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
                .invoke_with_request_body(worker_name, invocation, request_body_stream)
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
                    tracing::info!(
                        request_id = %request_id,
                        cache_status = "MISS",
                        "cache refreshed from origin"
                    );
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
        .invoke_with_request_body(worker_name, invocation, request_body_stream)
        .await?;
    store_worker_output_in_cache(&state, &cache_request, &output).await;
    tracing::info!(request_id = %request_id, cache_status = "MISS", "origin miss stored");
    build_worker_buffered_response(output, "MISS")
}

async fn invoke_worker_websocket_private<B>(
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

async fn invoke_worker_websocket_public<B>(
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

fn prepare_websocket_upgrade<B>(
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

pub(crate) fn parse_public_worker_name_from_request(
    headers: &HeaderMap,
    uri: &http::Uri,
    public_base_domain: &str,
) -> Result<String, PlatformError> {
    let host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            uri.authority()
                .map(|authority| authority.as_str().to_string())
        })
        .ok_or_else(|| PlatformError::not_found("not found"))?;
    parse_worker_from_host(host, public_base_domain)
}

pub(crate) async fn ensure_public_worker(
    state: &AppState,
    worker_name: &str,
) -> Result<(), PlatformError> {
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
    path_and_query: Option<&http::uri::PathAndQuery>,
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

pub(crate) fn build_public_request_url(
    headers: &HeaderMap,
    uri: &http::Uri,
) -> Result<String, PlatformError> {
    let normalized_path = if uri.path().is_empty() {
        "/"
    } else {
        uri.path()
    };
    let query_suffix = uri
        .path_and_query()
        .and_then(|path_and_query| path_and_query.as_str().split_once('?'))
        .map(|(_, query)| format!("?{query}"))
        .unwrap_or_default();
    let host = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| uri.authority().map(|authority| authority.as_str()))
        .ok_or_else(|| PlatformError::bad_request("public request is missing a host"))?;
    let scheme = uri
        .scheme_str()
        .map(normalize_public_request_scheme)
        .unwrap_or("https");
    Ok(format!("{scheme}://{host}{normalized_path}{query_suffix}"))
}

fn normalize_public_request_scheme(value: &str) -> &'static str {
    match value
        .trim()
        .trim_end_matches(':')
        .to_ascii_lowercase()
        .as_str()
    {
        "http" | "ws" => "http",
        "https" | "wss" => "https",
        _ => "https",
    }
}

fn worker_asset_path(url: &str) -> Result<String, PlatformError> {
    let uri: http::Uri = url
        .parse()
        .map_err(|error| PlatformError::internal(format!("invalid worker url {url:?}: {error}")))?;
    Ok(uri.path().to_string())
}

fn request_host_for_matching(headers: &HeaderMap, uri: &http::Uri) -> Option<String> {
    headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            uri.authority()
                .map(|authority| authority.as_str().to_string())
        })
}

fn private_route_requires_auth(path: &str) -> bool {
    path == "/v1/deploy"
        || path == "/v1/dynamic/deploy"
        || path == "/v1/invoke"
        || path.starts_with("/v1/invoke/")
}

fn private_request_is_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let Some(expected_token) = state.private_bearer_token.as_deref() else {
        return true;
    };
    let Some(value) = headers.get(AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    token.trim() == expected_token
}

fn private_auth_response() -> Response<ResponseBody> {
    let mut response = Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("content-type", "application/json")
        .header(WWW_AUTHENTICATE, "Bearer")
        .body(full_body(
            serde_json::to_vec(&ErrorBody {
                ok: false,
                error: "private control plane requires bearer auth".to_string(),
            })
            .unwrap_or_else(|_| {
                b"{\"ok\":false,\"error\":\"private control plane requires bearer auth\"}".to_vec()
            }),
        ))
        .unwrap_or_else(|_| {
            Response::new(full_body(
                b"{\"ok\":false,\"error\":\"private control plane requires bearer auth\"}".to_vec(),
            ))
        });
    annotate_response_with_trace_id(&mut response);
    response
}

async fn try_serve_static_asset(
    state: &AppState,
    worker_name: &str,
    method: &str,
    host: Option<String>,
    path: String,
    headers: Vec<(String, String)>,
) -> Result<Option<Response<ResponseBody>>, PlatformError> {
    let Some(asset) = state
        .runtime
        .resolve_asset(
            worker_name.to_string(),
            method.to_string(),
            host,
            path,
            headers,
        )
        .await?
    else {
        return Ok(None);
    };

    Ok(Some(
        build_direct_buffered_response(asset.status, asset.headers, asset.body)
            .map_err(|error| error.0)?,
    ))
}

fn parse_worker_from_host(
    host: impl AsRef<str>,
    public_base_domain: &str,
) -> Result<String, PlatformError> {
    let Some(host) = normalize_host(host.as_ref()) else {
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

fn build_request_body_stream<B>(body: B, max_bytes: usize) -> runtime::InvokeRequestBodyReceiver
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
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

fn request_method_allows_body(method: &Method) -> bool {
    *method != Method::GET && *method != Method::HEAD
}

fn validate_deploy_bindings(bindings: &[DeployBinding]) -> Result<(), PlatformError> {
    let mut seen = HashSet::new();
    for binding in bindings {
        match binding {
            DeployBinding::Kv { binding }
            | DeployBinding::Actor { binding }
            | DeployBinding::Dynamic { binding }
                if binding.trim().is_empty() =>
            {
                return Err(PlatformError::bad_request("binding name must not be empty"));
            }
            DeployBinding::Kv { binding }
            | DeployBinding::Actor { binding }
            | DeployBinding::Dynamic { binding } => {
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
) -> ApiResult<Response<ResponseBody>> {
    let stream = UnboundedReceiverStream::new(worker_response.body).map(|chunk| {
        chunk
            .map(Bytes::from)
            .map(Frame::data)
            .map_err(|error| -> BoxError { std::io::Error::other(error.to_string()).into() })
    });
    let mut response = Response::builder()
        .status(worker_response.status)
        .body(BodyExt::boxed(StreamBody::new(stream)))
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
) -> ApiResult<Response<ResponseBody>> {
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
) -> ApiResult<Response<ResponseBody>> {
    build_buffered_response(output.status, output.headers, output.body, cache_status)
}

fn build_direct_buffered_response(
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> ApiResult<Response<ResponseBody>> {
    let mut response = Response::builder()
        .status(status)
        .body(full_body(body))
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    for (name, value) in headers {
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

fn build_buffered_response(
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    cache_status: &str,
) -> ApiResult<Response<ResponseBody>> {
    let mut response = Response::builder()
        .status(status)
        .body(full_body(body))
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

pub(crate) fn inject_current_trace_context(headers: &mut Vec<(String, String)>) {
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        let mut injector = InvocationHeaderInjector(headers);
        propagator.inject_context(&context, &mut injector);
    });
}

pub(crate) fn annotate_response_with_trace_id(response: &mut Response<ResponseBody>) {
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

impl ApiError {
    fn into_http_response(self) -> Response<ResponseBody> {
        let status = match self.0.kind() {
            ErrorKind::BadRequest | ErrorKind::Runtime => StatusCode::BAD_REQUEST,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::to_vec(&ErrorBody::from_error(&self.0))
            .unwrap_or_else(|_| b"{\"error\":\"internal error\"}".to_vec());
        let mut response = Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(full_body(body))
            .unwrap_or_else(|_| {
                Response::new(full_body(b"{\"error\":\"internal error\"}".to_vec()))
            });
        annotate_response_with_trace_id(&mut response);
        response
    }
}

fn respond(result: ApiResult<Response<ResponseBody>>) -> Response<ResponseBody> {
    match result {
        Ok(response) => response,
        Err(error) => error.into_http_response(),
    }
}

pub(crate) fn empty_body() -> ResponseBody {
    Empty::<Bytes>::new()
        .map_err(infallible_to_box_error)
        .boxed()
}

pub(crate) fn full_body(body: Vec<u8>) -> ResponseBody {
    Full::new(Bytes::from(body))
        .map_err(infallible_to_box_error)
        .boxed()
}

fn infallible_to_box_error(value: Infallible) -> BoxError {
    match value {}
}

fn json_response<T: serde::Serialize>(
    status: StatusCode,
    value: &T,
) -> Result<Response<ResponseBody>, PlatformError> {
    let body =
        serde_json::to_vec(value).map_err(|error| PlatformError::internal(error.to_string()))?;
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full_body(body))
        .map_err(|error| PlatformError::internal(error.to_string()))
}

async fn read_json_body<T, B>(body: B, max_bytes: usize) -> Result<T, PlatformError>
where
    T: serde::de::DeserializeOwned,
    B: HttpBody<Data = Bytes> + Send,
    B::Error: std::fmt::Display,
{
    let collected = body.collect().await.map_err(|error| {
        PlatformError::bad_request(format!("failed to read request body: {error}"))
    })?;
    let bytes = collected.to_bytes();
    if bytes.len() > max_bytes {
        return Err(PlatformError::bad_request(format!(
            "request body too large (max {max_bytes} bytes)"
        )));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| PlatformError::bad_request(format!("invalid json: {error}")))
}

pub struct PreparedWebSocketUpgrade {
    on_upgrade: OnUpgrade,
    handshake_request: Request<()>,
}

#[cfg(test)]
mod tests {
    use super::{
        build_public_request_url, deploy_worker, handle_private_request, handle_public_request,
        invoke_worker_private, invoke_worker_public, parse_invoke_request_uri,
        parse_worker_from_host, validate_deploy_bindings, validate_internal_config,
    };
    use crate::state::AppState;
    use bytes::Bytes;
    use common::{
        DeployAsset, DeployBinding, DeployConfig, DeployInternalConfig, DeployRequest,
        DeployTraceDestination, ErrorKind,
    };
    use http::{HeaderMap, Request, StatusCode};
    use http_body_util::{BodyExt, Empty};
    use runtime::{RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
    use std::path::PathBuf;
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
        AppState::new(
            runtime,
            1024 * 1024,
            public_base_domain.to_string(),
            Some("test-private-token".to_string()),
            None,
            None,
        )
    }

    fn test_assets() -> Vec<DeployAsset> {
        vec![DeployAsset {
            path: "/a.js".to_string(),
            content_base64: "YXNzZXQtYm9keQ==".to_string(),
        }]
    }

    #[test]
    fn parse_invoke_path_and_query() {
        let uri: http::Uri = "/v1/invoke/hello/api/v1?x=1&y=2".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/api/v1?x=1&y=2");
    }

    #[test]
    fn parse_invoke_root_path() {
        let uri: http::Uri = "/v1/invoke/hello".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/");
    }

    #[test]
    fn build_public_request_url_ignores_spoofed_forwarded_host_and_proto() {
        let uri: http::Uri = "/rooms/test?x=1".parse().expect("uri");
        let request = Request::builder()
            .uri(uri)
            .header("host", "chat.example.com")
            .header("x-forwarded-host", "chat.wdyt.chat")
            .header("x-forwarded-proto", "https")
            .body(())
            .expect("request");
        let url = build_public_request_url(request.headers(), request.uri()).expect("url");
        assert_eq!(url, "https://chat.example.com/rooms/test?x=1");
    }

    #[test]
    fn build_public_request_url_ignores_spoofed_forwarded_proto() {
        let uri: http::Uri = "/ws".parse().expect("uri");
        let request = Request::builder()
            .uri(uri)
            .header("host", "chat.example.com")
            .header("x-forwarded-proto", "wss")
            .body(())
            .expect("request");
        let url = build_public_request_url(request.headers(), request.uri()).expect("url");
        assert_eq!(url, "https://chat.example.com/ws");
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
            },
        ];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn empty_actor_binding_name_is_rejected() {
        let bindings = vec![DeployBinding::Actor {
            binding: String::new(),
        }];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }

    #[test]
    fn dynamic_binding_name_collision_is_rejected() {
        let bindings = vec![
            DeployBinding::Kv {
                binding: "SHARED".to_string(),
            },
            DeployBinding::Dynamic {
                binding: "SHARED".to_string(),
            },
        ];
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
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = invoke_worker_public(state, request, None)
            .await
            .expect_err("blocked");
        assert_eq!(response.0.kind(), ErrorKind::NotFound);
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
            assets: Vec::new(),
            asset_headers: None,
        };
        let response = deploy_worker(state.clone(), deploy).await.expect("deploy");
        assert!(response.ok);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/invoke/echo")
            .header("authorization", "Bearer test-private-token")
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = invoke_worker_private(state, request, None)
            .await
            .expect("invoke");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
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
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = invoke_worker_public(state, request, None)
            .await
            .expect("invoke");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body.as_ref(), b"host-ok");
    }

    #[tokio::test]
    #[ignore = "starts full runtime service; run manually in isolation"]
    async fn public_host_invoke_ignores_spoofed_forwarded_request_url() {
        let state = create_test_state("example.com").await;
        state
            .runtime
            .deploy_with_config(
                "echo".to_string(),
                "export default { async fetch(request) { return new Response(request.url); } }"
                    .to_string(),
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
            .uri("/rooms/test?x=1")
            .header("host", "echo.example.com")
            .header("x-forwarded-host", "echo.wdyt.chat")
            .header("x-forwarded-proto", "https")
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = invoke_worker_public(state, request, None)
            .await
            .expect("invoke");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body.as_ref(), b"https://echo.example.com/rooms/test?x=1");
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
            .body(Empty::<Bytes>::new())
            .expect("request");
        let error = invoke_worker_public(state, request, None)
            .await
            .expect_err("private worker should not be public");
        assert_eq!(error.0.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn private_invoke_serves_asset_before_worker_code() {
        let state = create_test_state("example.com").await;
        state
            .runtime
            .deploy_with_bundle_config(
                "assets".to_string(),
                "export default { async fetch() { return new Response('worker-fallback'); } }"
                    .to_string(),
                DeployConfig::default(),
                test_assets(),
                Some("/a.js\n  Cache-Control: public, max-age=60\n".to_string()),
            )
            .await
            .expect("deploy");

        let request = Request::builder()
            .method("GET")
            .uri("/v1/invoke/assets/a.js")
            .header("authorization", "Bearer test-private-token")
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = invoke_worker_private(state.clone(), request, None)
            .await
            .expect("invoke");
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(body.as_ref(), b"asset-body");
        assert_eq!(
            headers
                .get("cache-control")
                .and_then(|value| value.to_str().ok()),
            Some("public, max-age=60")
        );

        let fallback_request = Request::builder()
            .method("GET")
            .uri("/v1/invoke/assets/missing")
            .header("authorization", "Bearer test-private-token")
            .body(Empty::<Bytes>::new())
            .expect("request");
        let fallback = invoke_worker_private(state, fallback_request, None)
            .await
            .expect("invoke fallback");
        let fallback_body = fallback
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(fallback_body.as_ref(), b"worker-fallback");
    }

    #[tokio::test]
    async fn public_host_invoke_serves_assets_for_public_workers() {
        let state = create_test_state("example.com").await;
        state
            .runtime
            .deploy_with_bundle_config(
                "assets".to_string(),
                "export default { async fetch() { return new Response('worker-fallback'); } }"
                    .to_string(),
                DeployConfig {
                    public: true,
                    ..DeployConfig::default()
                },
                test_assets(),
                None,
            )
            .await
            .expect("deploy");

        let request = Request::builder()
            .method("HEAD")
            .uri("/a.js")
            .header("host", "assets.example.com")
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = invoke_worker_public(state, request, None)
            .await
            .expect("invoke");
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert!(body.is_empty());
        assert_eq!(
            headers
                .get("content-length")
                .and_then(|value| value.to_str().ok()),
            Some("10")
        );
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

        let request = Request::builder()
            .method("GET")
            .uri("/v1/invoke/echo")
            .header("authorization", "Bearer test-private-token")
            .header("upgrade", "websocket")
            .header("connection", "Upgrade")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13")
            .body(Empty::<Bytes>::new())
            .expect("request");

        let response = handle_private_request(state, request).await.status();
        assert_eq!(response, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn private_routes_reject_missing_bearer_token() {
        let state = create_test_state("example.com").await;
        let request = Request::builder()
            .method("POST")
            .uri("/v1/deploy")
            .body(Empty::<Bytes>::new())
            .expect("request");
        let response = handle_private_request(state, request).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get("www-authenticate")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer")
        );
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

        let request = Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "echo.example.com")
            .header("upgrade", "websocket")
            .header("connection", "Upgrade")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("sec-websocket-version", "13")
            .body(Empty::<Bytes>::new())
            .expect("request");

        let response = handle_public_request(state, request).await.status();
        assert_eq!(response, StatusCode::BAD_REQUEST);
    }
}
