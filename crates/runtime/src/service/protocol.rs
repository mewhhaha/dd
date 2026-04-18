use super::*;

pub(super) fn decode_completion_payload(
    payload: String,
) -> Result<(String, String, usize, Result<WorkerOutput>)> {
    let completion: CompletionPayload = crate::json::from_string(payload)
        .map_err(|error| PlatformError::runtime(format!("invalid completion payload: {error}")))?;
    if completion.ok {
        let output = completion
            .result
            .ok_or_else(|| PlatformError::runtime("completion is missing result"))?;
        Ok((
            completion.request_id,
            completion.completion_token,
            completion.wait_until_count,
            Ok(output),
        ))
    } else {
        let message = completion
            .error
            .unwrap_or_else(|| "worker execution failed".to_string());
        Ok((
            completion.request_id,
            completion.completion_token,
            completion.wait_until_count,
            Err(PlatformError::runtime(message)),
        ))
    }
}

pub(super) fn decode_wait_until_payload(payload: String) -> Result<(String, String)> {
    let done: WaitUntilPayload = crate::json::from_string(payload)
        .map_err(|error| PlatformError::runtime(format!("invalid waitUntil payload: {error}")))?;
    Ok((done.request_id, done.completion_token))
}

pub(super) fn decode_response_start_payload(
    payload: String,
) -> Result<(String, String, u16, Vec<(String, String)>)> {
    let start: ResponseStartPayload = crate::json::from_string(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid response start payload: {error}"))
    })?;
    Ok((
        start.request_id,
        start.completion_token,
        start.status,
        start.headers,
    ))
}

pub(super) fn decode_response_chunk_payload(payload: String) -> Result<(String, String, Vec<u8>)> {
    let chunk: ResponseChunkPayload = crate::json::from_string(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid response chunk payload: {error}"))
    })?;
    Ok((chunk.request_id, chunk.completion_token, chunk.chunk))
}

pub(super) fn decode_cache_revalidate_payload(payload: String) -> Result<CacheRevalidatePayload> {
    let request: CacheRevalidatePayload = crate::json::from_string(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid cache revalidate payload: {error}"))
    })?;
    if request.cache_name.trim().is_empty() {
        return Err(PlatformError::runtime(
            "cache revalidate payload is missing cache_name",
        ));
    }
    if request.method.trim().is_empty() {
        return Err(PlatformError::runtime(
            "cache revalidate payload is missing method",
        ));
    }
    if request.url.trim().is_empty() {
        return Err(PlatformError::runtime(
            "cache revalidate payload is missing url",
        ));
    }
    Ok(request)
}

pub(super) fn cache_revalidation_key(
    worker_name: &str,
    generation: u64,
    payload: &CacheRevalidatePayload,
) -> String {
    let mut headers: Vec<(String, String)> = payload
        .headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
        .collect();
    headers.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let header_key = crate::json::to_string(&headers).unwrap_or_default();
    format!(
        "{worker_name}:{generation}:{}:{}:{}:{header_key}",
        payload.cache_name.trim(),
        payload.method.trim().to_ascii_uppercase(),
        payload.url.trim()
    )
}

pub(super) fn append_internal_trace_headers(
    headers: &mut Vec<(String, String)>,
    worker: &str,
    generation: u64,
) {
    append_or_update_header(headers, INTERNAL_HEADER, "1");
    append_or_update_header(headers, INTERNAL_REASON_HEADER, "trace");
    append_or_update_header(headers, TRACE_SOURCE_WORKER_HEADER, worker);
    append_or_update_header(
        headers,
        TRACE_SOURCE_GENERATION_HEADER,
        generation.to_string().as_str(),
    );
}

pub(super) fn append_or_update_header(headers: &mut Vec<(String, String)>, key: &str, value: &str) {
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case(key));
    headers.push((key.to_string(), value.to_string()));
}

pub(super) fn memory_owner_key(binding: &str, key: &str) -> String {
    format!("{binding}\u{001f}{key}")
}

pub(super) fn memory_handle_key(binding: &str, key: &str, handle: &str) -> String {
    format!("{binding}\u{001f}{key}\u{001f}{handle}")
}

pub(super) fn internal_header_value(headers: &[(String, String)], key: &str) -> Option<String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.clone())
}

pub(super) fn parse_websocket_open_metadata(
    output: &WorkerOutput,
    expected_session_id: &str,
) -> Result<(String, String, String)> {
    if output.status != 101 {
        return Err(PlatformError::bad_request(
            "websocket upgrade rejected by worker",
        ));
    }
    let accepted = internal_header_value(&output.headers, INTERNAL_WS_ACCEPT_HEADER)
        .map(|value| value == "1")
        .unwrap_or(false);
    if !accepted {
        return Err(PlatformError::bad_request(
            "worker did not accept websocket request",
        ));
    }
    let handle = internal_header_value(&output.headers, INTERNAL_WS_HANDLE_HEADER)
        .unwrap_or_else(|| expected_session_id.to_string());
    let binding = internal_header_value(&output.headers, INTERNAL_WS_BINDING_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing websocket memory binding metadata"))?;
    let key = internal_header_value(&output.headers, INTERNAL_WS_KEY_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing websocket memory key metadata"))?;
    if let Some(session_id) = internal_header_value(&output.headers, INTERNAL_WS_SESSION_HEADER) {
        if session_id != expected_session_id {
            return Err(PlatformError::bad_request(
                "websocket session metadata mismatch",
            ));
        }
    }
    Ok((handle, binding, key))
}

pub(super) fn strip_websocket_open_internal_headers(
    headers: &[(String, String)],
) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_ACCEPT_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_SESSION_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_HANDLE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_BINDING_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_KEY_HEADER))
        .cloned()
        .collect()
}

pub(super) fn strip_websocket_frame_internal_headers(
    headers: &[(String, String)],
) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_ACCEPT_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_SESSION_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_HANDLE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_BINDING_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_KEY_HEADER))
        .cloned()
        .collect()
}

pub(super) fn parse_transport_open_metadata(
    output: &WorkerOutput,
    expected_session_id: &str,
) -> Result<(String, String, String)> {
    if output.status != 200 {
        return Err(PlatformError::bad_request(
            "transport connect rejected by worker",
        ));
    }
    let accepted = internal_header_value(&output.headers, INTERNAL_TRANSPORT_ACCEPT_HEADER)
        .map(|value| value == "1")
        .unwrap_or(false);
    if !accepted {
        return Err(PlatformError::bad_request(
            "worker did not accept transport request",
        ));
    }
    let handle = internal_header_value(&output.headers, INTERNAL_TRANSPORT_HANDLE_HEADER)
        .unwrap_or_else(|| expected_session_id.to_string());
    let binding = internal_header_value(&output.headers, INTERNAL_TRANSPORT_BINDING_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing transport memory binding metadata"))?;
    let key = internal_header_value(&output.headers, INTERNAL_TRANSPORT_KEY_HEADER)
        .ok_or_else(|| PlatformError::bad_request("missing transport memory key metadata"))?;
    if let Some(session_id) =
        internal_header_value(&output.headers, INTERNAL_TRANSPORT_SESSION_HEADER)
    {
        if session_id != expected_session_id {
            return Err(PlatformError::bad_request(
                "transport session metadata mismatch",
            ));
        }
    }
    Ok((handle, binding, key))
}

pub(super) fn strip_transport_open_internal_headers(
    headers: &[(String, String)],
) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_ACCEPT_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_SESSION_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_HANDLE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_BINDING_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_KEY_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_CLOSE_CODE_HEADER))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_TRANSPORT_CLOSE_REASON_HEADER))
        .cloned()
        .collect()
}

pub(super) fn normalize_trace_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

pub(super) fn traceparent_from_headers(headers: &[(String, String)]) -> Option<String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("traceparent"))
        .map(|(_, value)| value.clone())
}

pub(super) fn set_span_parent_from_traceparent(span: &tracing::Span, traceparent: Option<&str>) {
    let Some(traceparent) = traceparent.filter(|value| !value.trim().is_empty()) else {
        return;
    };
    global::get_text_map_propagator(|propagator| {
        let extractor = TraceparentExtractor(traceparent);
        let parent = propagator.extract(&extractor);
        if parent.span().span_context().is_valid() {
            span.set_parent(parent);
        }
    });
}

pub(super) struct TraceparentExtractor<'a>(&'a str);

impl Extractor for TraceparentExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            Some(self.0)
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}

pub(super) fn next_runtime_token(prefix: &str) -> String {
    format!(
        "{prefix}-{:x}",
        NEXT_RUNTIME_TOKEN.fetch_add(1, Ordering::Relaxed)
    )
}
