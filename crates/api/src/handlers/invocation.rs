use super::*;
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

pub(super) fn parse_invoke_request_uri(
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

pub(super) fn worker_asset_path(url: &str) -> Result<String, PlatformError> {
    let uri: http::Uri = url
        .parse()
        .map_err(|error| PlatformError::internal(format!("invalid worker url {url:?}: {error}")))?;
    Ok(uri.path().to_string())
}

pub(super) fn request_host_for_matching(headers: &HeaderMap, uri: &http::Uri) -> Option<String> {
    headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            uri.authority()
                .map(|authority| authority.as_str().to_string())
        })
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

pub(super) fn parse_worker_from_host(
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
