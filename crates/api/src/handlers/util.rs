use super::*;
pub(super) fn private_route_requires_auth(path: &str) -> bool {
    path == "/v1/deploy"
        || path == "/v1/dynamic/deploy"
        || path == "/v1/invoke"
        || path.starts_with("/v1/invoke/")
}

pub(super) fn private_request_is_authorized(state: &AppState, headers: &HeaderMap) -> bool {
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

pub(super) fn private_auth_response() -> Response<ResponseBody> {
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
pub(super) fn validate_deploy_bindings(bindings: &[DeployBinding]) -> Result<(), PlatformError> {
    let mut seen = HashSet::new();
    for binding in bindings {
        match binding {
            DeployBinding::Kv { binding }
            | DeployBinding::Memory { binding }
            | DeployBinding::Dynamic { binding }
                if binding.trim().is_empty() =>
            {
                return Err(PlatformError::bad_request("binding name must not be empty"));
            }
            DeployBinding::Kv { binding }
            | DeployBinding::Memory { binding }
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

pub(super) fn validate_internal_config(
    internal: &DeployInternalConfig,
) -> Result<(), PlatformError> {
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
pub(super) fn set_span_parent_from_http_headers(span: &Span, headers: &HeaderMap) {
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

pub(super) fn respond(result: ApiResult<Response<ResponseBody>>) -> Response<ResponseBody> {
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

pub(super) fn json_response<T: serde::Serialize>(
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

pub(super) async fn read_json_body<T, B>(body: B, max_bytes: usize) -> Result<T, PlatformError>
where
    T: serde::de::DeserializeOwned,
    B: HttpBody<Data = Bytes> + Send,
    B::Error: std::fmt::Display,
{
    let mut stream = std::pin::pin!(body.into_data_stream());
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            PlatformError::bad_request(format!("failed to read request body: {error}"))
        })?;
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(PlatformError::bad_request(format!(
                "request body too large (max {max_bytes} bytes)"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| PlatformError::bad_request(format!("invalid json: {error}")))
}
