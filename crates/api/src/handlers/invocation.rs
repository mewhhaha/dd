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
    invoke_worker_with_target(state, parts, body, worker_name, url, false).await
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
    let url = build_public_request_url(&parts.headers, &parts.uri)?;
    let request = Request::from_parts(parts, body);
    if ws_upgrade.is_some() {
        ensure_public_worker(&state, &worker_name)?;
        return invoke_worker_websocket_public(state, request, ws_upgrade).await;
    }
    let (parts, body) = request.into_parts();
    invoke_worker_with_target(state, parts, body, worker_name, url, true).await
}

#[cfg(feature = "http3")]
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
    let url = build_public_request_url(&parts.headers, &parts.uri)?;
    invoke_worker_from_body_stream(state, parts, request_body_stream, worker_name, url, true).await
}

async fn invoke_worker_with_target<B>(
    state: AppState,
    parts: http::request::Parts,
    body: B,
    worker_name: String,
    url: String,
    require_public_worker: bool,
) -> ApiResult<Response<ResponseBody>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let max_body_bytes = state.invoke_max_body_bytes;
    validate_request_body_headers(&parts.method, &parts.headers, max_body_bytes)?;
    let request_body_stream = if request_method_forbids_body(&parts.method) {
        drain_forbidden_request_body(&parts.method, body, max_body_bytes).await?;
        None
    } else {
        Some(build_request_body_stream(body, max_body_bytes))
    };
    invoke_worker_from_body_stream(
        state,
        parts,
        request_body_stream,
        worker_name,
        url,
        require_public_worker,
    )
    .await
}

async fn invoke_worker_from_body_stream(
    state: AppState,
    parts: http::request::Parts,
    request_body_stream: Option<runtime::InvokeRequestBodyReceiver>,
    worker_name: String,
    url: String,
    require_public_worker: bool,
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
    let static_asset = try_serve_static_asset(
        &state,
        &worker_name,
        &method,
        request_host_for_matching(&parts.headers, &parts.uri),
        &worker_asset_path(&url)?,
        &headers,
        require_public_worker,
    )?;
    if let Some(asset_response) = static_asset.response {
        return Ok(asset_response);
    }
    if require_public_worker && !static_asset.public_worker {
        return Err(PlatformError::not_found("not found").into());
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

    if state.runtime.worker_cache_enabled(&worker_name)
        && is_front_cacheable_request(&invocation, request_body_stream.is_some())
    {
        let cache_request = front_cache_request(&worker_name, &invocation);
        match state.runtime.cache_match(cache_request.clone()).await? {
            CacheLookup::Fresh(response) => {
                return build_front_cached_response(response, &method, "HIT");
            }
            CacheLookup::StaleWhileRevalidate(response) => {
                spawn_front_cache_revalidation(
                    state.clone(),
                    worker_name.clone(),
                    invocation.clone(),
                    cache_request,
                )
                .await;
                return build_front_cached_response(response, &method, "STALE");
            }
            CacheLookup::StaleIfError(stale) => {
                return match state
                    .runtime
                    .invoke_with_request_body(worker_name, invocation, request_body_stream)
                    .await
                {
                    Ok(output) if output.status < 500 => {
                        maybe_store_front_cache(&state, &cache_request, &output).await;
                        build_front_origin_response(output, &method, "MISS")
                    }
                    Ok(_) | Err(_) => build_front_cached_response(stale, &method, "STALE"),
                };
            }
            CacheLookup::Miss => {
                let output = state
                    .runtime
                    .invoke_with_request_body(worker_name, invocation, request_body_stream)
                    .await?;
                maybe_store_front_cache(&state, &cache_request, &output).await;
                return build_front_origin_response(output, &method, "MISS");
            }
        }
    }

    let output = state
        .runtime
        .invoke_stream_with_request_body(worker_name, invocation, request_body_stream)
        .await?;
    build_worker_stream_response(output)
}

fn is_front_cacheable_request(invocation: &WorkerInvocation, has_body_stream: bool) -> bool {
    !has_body_stream
        && matches!(invocation.method.as_str(), "GET" | "HEAD")
        && !invocation.headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("authorization") || name.eq_ignore_ascii_case("cookie")
        })
}

fn front_cache_request(worker_name: &str, invocation: &WorkerInvocation) -> CacheRequest {
    CacheRequest {
        cache_name: format!("front:{worker_name}"),
        method: invocation.method.clone(),
        url: invocation.url.clone(),
        headers: invocation.headers.clone(),
        bypass_stale: false,
    }
}

fn front_response_is_cacheable(output: &WorkerOutput) -> bool {
    if output.status != 200 {
        return false;
    }
    let cache_control = output
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("cache-control"))
        .map(|(_, value)| value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(",");
    let explicitly_public = cache_control
        .split(',')
        .any(|directive| directive.trim() == "public");
    let positive_ttl = cache_control.split(',').any(|directive| {
        let directive = directive.trim();
        ["s-maxage=", "max-age="].into_iter().any(|prefix| {
            directive
                .strip_prefix(prefix)
                .and_then(|value| value.trim().parse::<u64>().ok())
                .is_some_and(|value| value > 0)
        })
    });
    explicitly_public
        && positive_ttl
        && !cache_control.contains("no-store")
        && !cache_control.contains("private")
        && !output.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("set-cookie")
                || (name.eq_ignore_ascii_case("vary") && value.trim() == "*")
        })
}

async fn maybe_store_front_cache(state: &AppState, request: &CacheRequest, output: &WorkerOutput) {
    if !front_response_is_cacheable(output) {
        return;
    }
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

async fn spawn_front_cache_revalidation(
    state: AppState,
    worker_name: String,
    invocation: WorkerInvocation,
    mut cache_request: CacheRequest,
) {
    let key = format!("{worker_name}:{}", cache_request.url);
    if !state
        .front_cache_revalidations
        .lock()
        .await
        .insert(key.clone())
    {
        return;
    }
    cache_request.bypass_stale = true;
    tokio::spawn(async move {
        if let Ok(output) = state.runtime.invoke(worker_name, invocation).await {
            maybe_store_front_cache(&state, &cache_request, &output).await;
        }
        state.front_cache_revalidations.lock().await.remove(&key);
    });
}

fn build_front_cached_response(
    response: CacheResponse,
    method: &str,
    status: &'static str,
) -> ApiResult<Response<ResponseBody>> {
    build_front_response(
        response.status,
        response.headers,
        response.body,
        method,
        status,
    )
}

fn build_front_origin_response(
    output: WorkerOutput,
    method: &str,
    status: &'static str,
) -> ApiResult<Response<ResponseBody>> {
    build_front_response(output.status, output.headers, output.body, method, status)
}

fn build_front_response(
    status_code: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    method: &str,
    cache_status: &'static str,
) -> ApiResult<Response<ResponseBody>> {
    let content_length = body.len();
    let response_body = if method == "HEAD" { Vec::new() } else { body };
    let mut response = Response::builder()
        .status(status_code)
        .body(full_body(response_body))
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    append_safe_worker_headers(response.headers_mut(), headers);
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| PlatformError::internal(error.to_string()))?,
    );
    response
        .headers_mut()
        .insert("x-dd-cache", HeaderValue::from_static(cache_status));
    annotate_response_with_trace_id(&mut response);
    Ok(response)
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

pub(crate) fn ensure_public_worker(
    state: &AppState,
    worker_name: &str,
) -> Result<(), PlatformError> {
    if !state.runtime.worker_is_public(worker_name) {
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

    Ok((validate_worker_name(worker_name)?, url))
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

struct StaticAssetLookup {
    response: Option<Response<ResponseBody>>,
    public_worker: bool,
}

fn try_serve_static_asset(
    state: &AppState,
    worker_name: &str,
    method: &str,
    host: Option<String>,
    path: &str,
    headers: &[(String, String)],
    public_only: bool,
) -> Result<StaticAssetLookup, PlatformError> {
    let (asset, public_worker) = if public_only {
        let resolution = state.runtime.resolve_public_route_asset(
            worker_name,
            method,
            host.as_deref(),
            path,
            headers,
        )?;
        (resolution.asset, resolution.public_worker)
    } else {
        (
            state
                .runtime
                .resolve_asset(worker_name, method, host.as_deref(), path, headers)?,
            false,
        )
    };
    let Some(asset) = asset else {
        return Ok(StaticAssetLookup {
            response: None,
            public_worker,
        });
    };

    Ok(StaticAssetLookup {
        response: Some(
            build_direct_buffered_response(asset.status, asset.headers, asset.body)
                .map_err(|error| error.0)?,
        ),
        public_worker,
    })
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
    if prefix.contains('.') {
        return Err(PlatformError::not_found("not found"));
    }
    let worker_name = prefix.trim();
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

    if let Some((name, port)) = lower.rsplit_once(':')
        && port.chars().all(|value| value.is_ascii_digit())
    {
        if name.is_empty() {
            return None;
        }
        return Some(name.to_string());
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
            if tx.send(Ok(chunk)).await.is_err() {
                return;
            }
        }
    });
    rx
}

async fn drain_forbidden_request_body<B>(
    method: &Method,
    body: B,
    max_bytes: usize,
) -> ApiResult<()>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync + 'static,
{
    let mut stream = body.into_data_stream();
    let mut total = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            PlatformError::bad_request(format!("failed to read request body: {error}"))
        })?;
        if chunk.is_empty() {
            continue;
        }
        total = total.saturating_add(chunk.len());
        if total > max_bytes {
            return Err(PlatformError::bad_request(format!(
                "request body too large (max {max_bytes} bytes)"
            ))
            .into());
        }
        return Err(request_body_not_supported(method).into());
    }
    Ok(())
}

fn build_worker_stream_response(
    worker_response: runtime::WorkerStreamOutput,
) -> ApiResult<Response<ResponseBody>> {
    let stream = futures_util::stream::unfold(worker_response.body, |mut body| async move {
        body.recv().await.map(|chunk| (chunk, body))
    })
    .map(|chunk| {
        chunk
            .map(Frame::data)
            .map_err(|error| -> BoxError { std::io::Error::other(error.to_string()).into() })
    });
    let mut response = Response::builder()
        .status(worker_response.status)
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    append_safe_worker_headers(response.headers_mut(), worker_response.headers);
    annotate_response_with_trace_id(&mut response);

    Ok(response)
}

fn build_direct_buffered_response(
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
) -> ApiResult<Response<ResponseBody>> {
    let declared_content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok());
    let content_length = if body.is_empty() {
        declared_content_length.unwrap_or(0)
    } else {
        body.len()
    };
    let mut response = Response::builder()
        .status(status)
        .body(full_body(body))
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    append_safe_worker_headers(response.headers_mut(), headers);
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| PlatformError::internal(error.to_string()))?,
    );
    annotate_response_with_trace_id(&mut response);
    Ok(response)
}
