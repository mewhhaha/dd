use super::*;

#[derive(Debug, Serialize)]
pub(crate) struct KvListItem {
    key: String,
    value_handle: u32,
    encoding: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvGetResult {
    ok: bool,
    found: bool,
    wrong_encoding: bool,
    value: String,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvGetManyItem {
    found: bool,
    wrong_encoding: bool,
    value: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvGetManyResult {
    ok: bool,
    values: Vec<KvGetManyItem>,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvProfileResult {
    ok: bool,
    snapshot: Option<KvProfileSnapshot>,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvGetValueResult {
    ok: bool,
    found: bool,
    value_handle: u32,
    encoding: String,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvOpResult {
    ok: bool,
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvListResult {
    ok: bool,
    entries: Vec<KvListItem>,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CacheMatchResult {
    ok: bool,
    found: bool,
    stale: bool,
    should_revalidate: bool,
    status: u16,
    headers_handle: u32,
    body_handle: u32,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CacheDeleteResult {
    ok: bool,
    deleted: bool,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct HttpPrepareResult {
    ok: bool,
    method: String,
    url: String,
    headers_handle: u32,
    body_handle: u32,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct HttpUrlCheckResult {
    ok: bool,
    url: String,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ResponseChunkEmitResult {
    ok: bool,
    error: String,
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_kv_get(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
) -> KvGetResult {
    let started = Instant::now();
    let store = state.borrow().borrow::<KvStore>().clone();
    let result = match store.get_utf8(&worker_name, &binding, &key).await {
        Ok(Ok(decoded)) => KvGetResult {
            ok: true,
            found: true,
            wrong_encoding: false,
            value: decoded,
            error: String::new(),
        },
        Ok(Err(KvUtf8Lookup::Missing)) => KvGetResult {
            ok: true,
            found: false,
            wrong_encoding: false,
            value: String::new(),
            error: String::new(),
        },
        Ok(Err(KvUtf8Lookup::WrongEncoding)) => KvGetResult {
            ok: false,
            found: true,
            wrong_encoding: true,
            value: String::new(),
            error: String::new(),
        },
        Err(error) => KvGetResult {
            ok: false,
            found: false,
            wrong_encoding: false,
            value: String::new(),
            error: error.to_string(),
        },
    };
    store.record_profile(
        KvProfileMetricKind::OpGet,
        started.elapsed().as_micros() as u64,
        1,
    );
    result
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_kv_get_many_utf8(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[serde] keys: Vec<String>,
) -> KvGetManyResult {
    let started = Instant::now();
    let store = state.borrow().borrow::<KvStore>().clone();
    let item_count = keys.len() as u64;
    let result = match store.get_utf8_many(&worker_name, &binding, &keys).await {
        Ok(values) => KvGetManyResult {
            ok: true,
            values: values
                .into_iter()
                .map(|value| match value {
                    Ok(decoded) => KvGetManyItem {
                        found: true,
                        wrong_encoding: false,
                        value: decoded,
                    },
                    Err(KvUtf8Lookup::Missing) => KvGetManyItem {
                        found: false,
                        wrong_encoding: false,
                        value: String::new(),
                    },
                    Err(KvUtf8Lookup::WrongEncoding) => KvGetManyItem {
                        found: true,
                        wrong_encoding: true,
                        value: String::new(),
                    },
                })
                .collect(),
            error: String::new(),
        },
        Err(error) => KvGetManyResult {
            ok: false,
            values: Vec::new(),
            error: error.to_string(),
        },
    };
    store.record_profile(
        KvProfileMetricKind::OpGetManyUtf8,
        started.elapsed().as_micros() as u64,
        item_count,
    );
    result
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_kv_get_value(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
) -> KvGetValueResult {
    let started = Instant::now();
    let store = state.borrow().borrow::<KvStore>().clone();
    let result = match store.get(&worker_name, &binding, &key).await {
        Ok(Some(value)) => {
            let value_handle = state
                .borrow_mut()
                .borrow_mut::<HttpPreparedBodies>()
                .insert(Bytes::from(value.value));
            KvGetValueResult {
                ok: true,
                found: true,
                value_handle,
                encoding: value.encoding,
                error: String::new(),
            }
        }
        Ok(None) => KvGetValueResult {
            ok: true,
            found: false,
            value_handle: 0,
            encoding: "utf8".to_string(),
            error: String::new(),
        },
        Err(error) => KvGetValueResult {
            ok: false,
            found: false,
            value_handle: 0,
            encoding: "utf8".to_string(),
            error: error.to_string(),
        },
    };
    store.record_profile(
        KvProfileMetricKind::OpGetValue,
        started.elapsed().as_micros() as u64,
        1,
    );
    result
}

#[deno_core::op2(fast)]
pub(crate) fn op_kv_profile_record_js(
    state: &mut OpState,
    #[string] metric: String,
    duration_us: u32,
    items: u32,
) {
    let kind = match metric.as_str() {
        "js_request_total" => KvProfileMetricKind::JsRequestTotal,
        "js_batch_flush" => KvProfileMetricKind::JsBatchFlush,
        "kv_cache_hit" => KvProfileMetricKind::JsCacheHit,
        "kv_cache_miss" => KvProfileMetricKind::JsCacheMiss,
        "kv_cache_stale" => KvProfileMetricKind::JsCacheStale,
        "kv_cache_fill" => KvProfileMetricKind::JsCacheFill,
        "kv_cache_invalidate" => KvProfileMetricKind::JsCacheInvalidate,
        _ => return,
    };
    let store = state.borrow::<KvStore>().clone();
    store.record_profile(kind, u64::from(duration_us), u64::from(items.max(1)));
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_kv_profile_take(state: &mut OpState) -> KvProfileResult {
    let store = state.borrow::<KvStore>().clone();
    KvProfileResult {
        ok: true,
        snapshot: Some(store.take_profile_snapshot_and_reset()),
        error: String::new(),
    }
}

#[deno_core::op2(fast)]
pub(crate) fn op_kv_profile_reset(state: &mut OpState) {
    let store = state.borrow::<KvStore>().clone();
    store.reset_profile();
}

#[deno_core::op2(fast)]
pub(crate) fn op_kv_take_failed_write_version(state: &mut OpState, #[bigint] version: i64) -> bool {
    let store = state.borrow::<KvStore>().clone();
    store.take_failed_write_version(version)
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_kv_put(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] value: String,
) -> KvOpResult {
    let store = state.borrow().borrow::<KvStore>().clone();
    match store.put(&worker_name, &binding, &key, &value).await {
        Ok(version) => KvOpResult {
            ok: true,
            error: String::new(),
            version: Some(version),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_kv_put_value_bytes(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] encoding: String,
    #[buffer] value: JsBuffer,
) -> KvOpResult {
    let value = Bytes::copy_from_slice(value.as_ref());
    let store = state.borrow().borrow::<KvStore>().clone();
    match store
        .put_value(&worker_name, &binding, &key, value.as_ref(), &encoding)
        .await
    {
        Ok(version) => KvOpResult {
            ok: true,
            error: String::new(),
            version: Some(version),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_kv_delete(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
) -> KvOpResult {
    let store = state.borrow().borrow::<KvStore>().clone();
    match store.delete(&worker_name, &binding, &key).await {
        Ok(version) => KvOpResult {
            ok: true,
            error: String::new(),
            version: Some(version),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_kv_enqueue_put(
    state: &mut OpState,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] value: String,
) -> KvOpResult {
    let store = state.borrow::<KvStore>().clone();
    match store.enqueue_batch_versions(
        &worker_name,
        &binding,
        &[KvBatchMutation {
            key,
            value: value.into_bytes(),
            encoding: "utf8".to_string(),
            deleted: false,
        }],
    ) {
        Ok(versions) => KvOpResult {
            ok: true,
            error: String::new(),
            version: versions.first().copied(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_kv_enqueue_put_value_bytes(
    state: &mut OpState,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
    #[string] encoding: String,
    #[buffer] value: JsBuffer,
) -> KvOpResult {
    let value = Bytes::copy_from_slice(value.as_ref());
    let store = state.borrow::<KvStore>().clone();
    match store.enqueue_batch_versions(
        &worker_name,
        &binding,
        &[KvBatchMutation {
            key,
            value: value.to_vec(),
            encoding,
            deleted: false,
        }],
    ) {
        Ok(versions) => KvOpResult {
            ok: true,
            error: String::new(),
            version: versions.first().copied(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_kv_enqueue_delete(
    state: &mut OpState,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] key: String,
) -> KvOpResult {
    let store = state.borrow::<KvStore>().clone();
    match store.enqueue_batch_versions(
        &worker_name,
        &binding,
        &[KvBatchMutation {
            key,
            value: Vec::new(),
            encoding: "utf8".to_string(),
            deleted: true,
        }],
    ) {
        Ok(versions) => KvOpResult {
            ok: true,
            error: String::new(),
            version: versions.first().copied(),
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_kv_list(
    state: Rc<RefCell<OpState>>,
    #[string] worker_name: String,
    #[string] binding: String,
    #[string] prefix: String,
    limit: u32,
) -> KvListResult {
    let store = state.borrow().borrow::<KvStore>().clone();
    let clamped_limit = limit.clamp(1, 1000) as usize;
    match store
        .list(&worker_name, &binding, &prefix, clamped_limit)
        .await
    {
        Ok(values) => {
            let entries = {
                let mut op_state = state.borrow_mut();
                let bodies = op_state.borrow_mut::<HttpPreparedBodies>();
                values
                    .into_iter()
                    .map(|entry| to_list_item(entry, bodies))
                    .collect()
            };
            KvListResult {
                ok: true,
                entries,
                error: String::new(),
            }
        }
        Err(error) => KvListResult {
            ok: false,
            entries: Vec::new(),
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_cache_match(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] cache_name: String,
    #[string] method: String,
    #[string] url: String,
    headers_handle: u32,
    bypass_stale: bool,
) -> CacheMatchResult {
    let headers = state
        .borrow_mut()
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default();
    let request = CacheRequest {
        cache_name,
        method,
        url,
        headers,
        bypass_stale,
    };
    if let Err(error) = ensure_cache_allowed(&state, request_context_handle) {
        return CacheMatchResult {
            ok: false,
            found: false,
            stale: false,
            should_revalidate: false,
            status: 0,
            headers_handle: 0,
            body_handle: 0,
            error,
        };
    }
    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.get(&request).await {
        Ok(CacheLookup::Fresh(response)) => cache_match_hit_result(&state, response, false, false),
        Ok(CacheLookup::StaleWhileRevalidate(response)) => {
            cache_match_hit_result(&state, response, true, true)
        }
        Ok(CacheLookup::StaleIfError(response)) => {
            cache_match_hit_result(&state, response, true, false)
        }
        Ok(CacheLookup::Miss) => CacheMatchResult {
            ok: true,
            found: false,
            stale: false,
            should_revalidate: false,
            status: 0,
            headers_handle: 0,
            body_handle: 0,
            error: String::new(),
        },
        Err(error) => CacheMatchResult {
            ok: false,
            found: false,
            stale: false,
            should_revalidate: false,
            status: 0,
            headers_handle: 0,
            body_handle: 0,
            error: error.to_string(),
        },
    }
}

fn cache_match_hit_result(
    state: &Rc<RefCell<OpState>>,
    response: CacheResponse,
    stale: bool,
    should_revalidate: bool,
) -> CacheMatchResult {
    let (headers_handle, body_handle) = {
        let mut op_state = state.borrow_mut();
        let headers_handle = op_state
            .borrow_mut::<HttpPreparedHeaders>()
            .insert(response.headers);
        let body_handle = op_state
            .borrow_mut::<HttpPreparedBodies>()
            .insert(response.body);
        (headers_handle, body_handle)
    };
    CacheMatchResult {
        ok: true,
        found: true,
        stale,
        should_revalidate,
        status: response.status,
        headers_handle,
        body_handle,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_cache_put(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] cache_name: String,
    #[string] method: String,
    #[string] url: String,
    request_headers_handle: u32,
    response_status: u16,
    response_headers_handle: u32,
    response_body_handle: u32,
) -> KvOpResult {
    if let Err(error) = ensure_cache_allowed(&state, request_context_handle) {
        return KvOpResult {
            ok: false,
            error,
            version: None,
        };
    }
    let (request_headers, response_headers, body) = {
        let mut op_state = state.borrow_mut();
        let request_headers = op_state
            .borrow_mut::<HttpPreparedHeaders>()
            .take(request_headers_handle)
            .unwrap_or_default();
        let response_headers = op_state
            .borrow_mut::<HttpPreparedHeaders>()
            .take(response_headers_handle)
            .unwrap_or_default();
        let body = op_state
            .borrow_mut::<HttpPreparedBodies>()
            .take(response_body_handle)
            .unwrap_or_default();
        (request_headers, response_headers, body)
    };
    let request = CacheRequest {
        cache_name,
        method,
        url,
        headers: request_headers,
        bypass_stale: false,
    };
    let response = CacheResponse {
        status: response_status,
        headers: response_headers,
        body: body.to_vec(),
    };
    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.put(&request, response).await {
        Ok(_) => KvOpResult {
            ok: true,
            error: String::new(),
            version: None,
        },
        Err(error) => KvOpResult {
            ok: false,
            error: error.to_string(),
            version: None,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_cache_delete(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] cache_name: String,
    #[string] method: String,
    #[string] url: String,
    headers_handle: u32,
) -> CacheDeleteResult {
    let headers = state
        .borrow_mut()
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default();
    let request = CacheRequest {
        cache_name,
        method,
        url,
        headers,
        bypass_stale: false,
    };
    if let Err(error) = ensure_cache_allowed(&state, request_context_handle) {
        return CacheDeleteResult {
            ok: false,
            deleted: false,
            error,
        };
    }

    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.delete(&request).await {
        Ok(deleted) => CacheDeleteResult {
            ok: true,
            deleted,
            error: String::new(),
        },
        Err(error) => CacheDeleteResult {
            ok: false,
            deleted: false,
            error: error.to_string(),
        },
    }
}

fn ensure_cache_allowed(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
) -> std::result::Result<(), String> {
    if request_context_handle == 0 {
        return Ok(());
    }
    let allowed = {
        let state_ref = state.borrow();
        state_ref
            .borrow::<RequestSecretContexts>()
            .get(request_context_handle)
            .map(|context| context.execution.allow_cache)
            .unwrap_or(true)
    };
    if allowed {
        return Ok(());
    }
    Err("dynamic child policy blocks cache access".to_string())
}

type PreparedHttpFetchRequest = (
    reqwest::Method,
    reqwest::Url,
    Vec<(String, String)>,
    Vec<u8>,
    Arc<AtomicBool>,
    Arc<Notify>,
);

pub(crate) fn prepare_http_fetch_request(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
    method: &str,
    url: &str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> std::result::Result<PreparedHttpFetchRequest, String> {
    let (execution, canceled, canceled_notify) = http_fetch_context(state, request_context_handle)?;
    if canceled.load(Ordering::SeqCst) {
        canceled_notify.notify_waiters();
        return Err("host fetch request canceled".to_string());
    }

    let method_raw = replace_placeholders_text(method, execution.replacements.as_ref());
    let method = reqwest::Method::from_bytes(method_raw.trim().to_ascii_uppercase().as_bytes())
        .map_err(|error| format!("invalid host fetch method: {error}"))?;

    let url = replace_placeholders_text(url, execution.replacements.as_ref());
    let parsed_url =
        reqwest::Url::parse(&url).map_err(|error| format!("invalid host fetch URL: {error}"))?;
    if !is_egress_url_allowed(&parsed_url, execution.egress_allow_hosts.as_ref()) {
        if let Some(quota_state) = &execution.dynamic_quota_state {
            quota_state
                .egress_deny_count
                .fetch_add(1, Ordering::Relaxed);
        }
        state
            .borrow()
            .borrow::<DynamicProfile>()
            .record_egress_deny();
        return Err(format!(
            "egress origin is not allowed: {}",
            parsed_url.origin().ascii_serialization()
        ));
    }
    if let Some(quota_state) = &execution.dynamic_quota_state {
        let next = quota_state
            .outbound_requests
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if execution
            .max_outbound_requests
            .map(|limit| next > limit)
            .unwrap_or(false)
        {
            quota_state.quota_kill_count.fetch_add(1, Ordering::Relaxed);
            state
                .borrow()
                .borrow::<DynamicProfile>()
                .record_quota_kill();
            let command_sender = state
                .borrow()
                .borrow::<crate::service::RuntimeFastCommandSender>()
                .clone();
            let _ =
                command_sender
                    .0
                    .try_send(crate::service::RuntimeCommand::RetireDynamicWorker {
                        worker_name: execution.worker_name.as_ref().to_string(),
                        reason: "dynamic child exceeded max_outbound_requests".to_string(),
                    });
            return Err("dynamic child exceeded max_outbound_requests".to_string());
        }
    }

    let headers = headers
        .into_iter()
        .filter_map(|(name, value)| {
            let normalized_name = replace_placeholders_text(&name, execution.replacements.as_ref());
            let normalized_value =
                replace_placeholders_text(&value, execution.replacements.as_ref());
            let trimmed = normalized_name.trim().to_string();
            if trimmed.eq_ignore_ascii_case("host")
                || trimmed.eq_ignore_ascii_case("content-length")
            {
                return None;
            }
            Some((trimmed, normalized_value))
        })
        .collect::<Vec<_>>();
    let body = replace_placeholders_in_body(body, execution.replacements.as_ref());

    Ok((method, parsed_url, headers, body, canceled, canceled_notify))
}

pub(crate) fn check_http_fetch_url(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
    url: &str,
) -> std::result::Result<String, String> {
    let (execution, canceled, canceled_notify) = http_fetch_context(state, request_context_handle)?;
    if canceled.load(Ordering::SeqCst) {
        canceled_notify.notify_waiters();
        return Err("host fetch request canceled".to_string());
    }
    let url = replace_placeholders_text(url, execution.replacements.as_ref());
    let parsed_url =
        reqwest::Url::parse(&url).map_err(|error| format!("invalid host fetch URL: {error}"))?;
    if !is_egress_url_allowed(&parsed_url, execution.egress_allow_hosts.as_ref()) {
        if let Some(quota_state) = &execution.dynamic_quota_state {
            quota_state
                .egress_deny_count
                .fetch_add(1, Ordering::Relaxed);
        }
        state
            .borrow()
            .borrow::<DynamicProfile>()
            .record_egress_deny();
        return Err(format!(
            "egress origin is not allowed: {}",
            parsed_url.origin().ascii_serialization()
        ));
    }
    Ok(parsed_url.to_string())
}

pub(crate) fn http_fetch_context(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
) -> std::result::Result<(RequestExecutionContext, Arc<AtomicBool>, Arc<Notify>), String> {
    if request_context_handle == 0 {
        return Err("host fetch request context handle must not be empty".to_string());
    }
    let context = {
        let state_ref = state.borrow();
        state_ref
            .borrow::<RequestSecretContexts>()
            .get(request_context_handle)
            .map(|context| {
                (
                    context.execution.clone(),
                    context.canceled.clone(),
                    context.canceled_notify.clone(),
                )
            })
    };
    context.ok_or_else(|| "host fetch context is unavailable (request likely canceled)".to_string())
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_http_prepare(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] method: String,
    #[string] url: String,
    headers_handle: u32,
    body_handle: u32,
) -> HttpPrepareResult {
    let (headers, body) = {
        let mut op_state = state.borrow_mut();
        let headers = op_state
            .borrow_mut::<HttpPreparedHeaders>()
            .take(headers_handle)
            .unwrap_or_default();
        let body = op_state
            .borrow_mut::<HttpPreparedBodies>()
            .take(body_handle)
            .unwrap_or_default();
        (headers, body)
    };
    match prepare_http_fetch_request(
        &state,
        request_context_handle,
        &method,
        &url,
        headers,
        body.to_vec(),
    ) {
        Ok((method, url, headers, body, _, _)) => {
            let (headers_handle, body_handle) = {
                let mut op_state = state.borrow_mut();
                let headers_handle = op_state.borrow_mut::<HttpPreparedHeaders>().insert(headers);
                let body_handle = op_state.borrow_mut::<HttpPreparedBodies>().insert(body);
                (headers_handle, body_handle)
            };
            HttpPrepareResult {
                ok: true,
                method: method.as_str().to_string(),
                url: url.to_string(),
                headers_handle,
                body_handle,
                error: String::new(),
            }
        }
        Err(error) => HttpPrepareResult {
            ok: false,
            method: String::new(),
            url: String::new(),
            headers_handle: 0,
            body_handle: 0,
            error,
        },
    }
}

#[deno_core::op2]
#[buffer]
pub(crate) fn op_http_take_prepared_body(state: &mut OpState, body_handle: u32) -> Vec<u8> {
    state
        .borrow_mut::<HttpPreparedBodies>()
        .take(body_handle)
        .map(|body| body.to_vec())
        .unwrap_or_default()
}

#[deno_core::op2]
pub(crate) fn op_http_store_prepared_body(state: &mut OpState, #[buffer] body: JsBuffer) -> u32 {
    state
        .borrow_mut::<HttpPreparedBodies>()
        .insert(Bytes::copy_from_slice(body.as_ref()))
}

#[deno_core::op2]
pub(crate) fn op_http_store_prepared_headers(
    state: &mut OpState,
    #[serde] headers: Vec<(String, String)>,
) -> u32 {
    state.borrow_mut::<HttpPreparedHeaders>().insert(headers)
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_http_take_prepared_headers(
    state: &mut OpState,
    headers_handle: u32,
) -> Vec<(String, String)> {
    state
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default()
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_http_check_url(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    #[string] url: String,
) -> HttpUrlCheckResult {
    match check_http_fetch_url(&state, request_context_handle, &url) {
        Ok(url) => HttpUrlCheckResult {
            ok: true,
            url,
            error: String::new(),
        },
        Err(error) => HttpUrlCheckResult {
            ok: false,
            url: String::new(),
            error,
        },
    }
}

#[deno_core::op2(fast)]
pub(crate) fn op_emit_completion_ok(
    state: &mut OpState,
    completion_handle: u32,
    status: u16,
    headers_handle: u32,
    body_handle: u32,
) {
    let Some(context) = active_request_context_for_completion(state, completion_handle) else {
        return;
    };
    let headers = state
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default();
    let body = state
        .borrow_mut::<HttpPreparedBodies>()
        .take(body_handle)
        .unwrap_or_default();
    emit_completion_result(
        state,
        context.request_id,
        context.completion_token,
        context.request_context_handle,
        context.wait_until_count,
        Ok(WorkerOutput {
            status,
            headers,
            body: body.to_vec(),
        }),
    );
}

#[deno_core::op2(fast)]
pub(crate) fn op_emit_completion_error(
    state: &mut OpState,
    completion_handle: u32,
    #[string] error: String,
) {
    let Some(context) = active_request_context_for_completion(state, completion_handle) else {
        return;
    };
    let message = if error.is_empty() {
        "worker execution failed".to_string()
    } else {
        error
    };
    emit_completion_result(
        state,
        context.request_id,
        context.completion_token,
        context.request_context_handle,
        context.wait_until_count,
        Err(PlatformError::runtime(message)),
    );
}

fn emit_completion_result(
    state: &mut OpState,
    request_id: String,
    completion_token: String,
    request_context_handle: u32,
    wait_until_count: usize,
    result: Result<WorkerOutput>,
) {
    if wait_until_count == 0 && request_context_handle > 0 {
        clear_memory_command_handles(state, request_context_handle);
        clear_memory_byte_handles(state, request_context_handle);
        clear_memory_batch_handles(state, request_context_handle);
    }
    let _ = emit_isolate_event(
        state,
        IsolateEventPayload::Completion {
            request_id,
            completion_token,
            wait_until_count,
            result,
        },
    );
}

#[deno_core::op2(fast)]
pub(crate) fn op_emit_wait_until_done(state: &mut OpState, completion_handle: u32) -> bool {
    let Some(context) = state
        .borrow_mut::<ActiveRequestContextHandles>()
        .mark_wait_until_done(completion_handle)
    else {
        return false;
    };
    let request_context_handle = context.request_context_handle;
    if request_context_handle > 0 {
        clear_memory_command_handles(state, request_context_handle);
        clear_memory_byte_handles(state, request_context_handle);
        clear_memory_batch_handles(state, request_context_handle);
    }
    let _ = emit_isolate_event(
        state,
        IsolateEventPayload::WaitUntilDone {
            request_id: context.request_id,
            completion_token: context.completion_token,
        },
    );
    true
}

#[deno_core::op2(fast)]
pub(crate) fn op_emit_response_start(
    state: &mut OpState,
    completion_handle: u32,
    status: u16,
    headers_handle: u32,
) {
    let Some(context) = active_request_context_for_completion(state, completion_handle) else {
        return;
    };
    let headers = state
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default();
    let _ = emit_isolate_event(
        state,
        IsolateEventPayload::ResponseStart {
            request_id: context.request_id,
            completion_token: context.completion_token,
            status,
            headers,
        },
    );
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_emit_response_chunk(
    state: Rc<RefCell<OpState>>,
    completion_handle: u32,
    #[buffer] chunk: JsBuffer,
) -> ResponseChunkEmitResult {
    let context = {
        let op_state = state.borrow();
        active_request_context_for_completion(&op_state, completion_handle)
    };
    let Some(context) = context else {
        return ResponseChunkEmitResult {
            ok: false,
            error: "request context handle is unavailable".to_string(),
        };
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    if emit_isolate_event_from_rc(
        &state,
        IsolateEventPayload::ResponseChunk {
            request_id: context.request_id,
            completion_token: context.completion_token,
            chunk: Bytes::copy_from_slice(chunk.as_ref()),
            reply: reply_tx,
        },
    )
    .is_err()
    {
        return ResponseChunkEmitResult {
            ok: false,
            error: "runtime response stream is unavailable".to_string(),
        };
    }

    match reply_rx.await {
        Ok(Ok(())) => ResponseChunkEmitResult {
            ok: true,
            error: String::new(),
        },
        Ok(Err(error)) => ResponseChunkEmitResult {
            ok: false,
            error: error.to_string(),
        },
        Err(_) => ResponseChunkEmitResult {
            ok: false,
            error: "response stream acknowledgment channel closed".to_string(),
        },
    }
}

#[deno_core::op2(fast)]
pub(crate) fn op_emit_cache_revalidate(
    state: &mut OpState,
    #[string] cache_name: String,
    #[string] method: String,
    #[string] url: String,
    headers_handle: u32,
) {
    let headers = state
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default();
    let _ = emit_isolate_event(
        state,
        IsolateEventPayload::CacheRevalidate(CacheRevalidatePayload {
            cache_name,
            method,
            url,
            headers,
        }),
    );
}

pub(crate) fn replace_placeholders_text(
    value: &str,
    replacements: &HashMap<String, String>,
) -> String {
    if replacements.is_empty() {
        return value.to_string();
    }
    let mut output = value.to_string();
    for (placeholder, secret) in replacements {
        if placeholder.is_empty() {
            continue;
        }
        output = output.replace(placeholder, secret);
    }
    output
}

pub(crate) fn replace_placeholders_in_body(
    body: Vec<u8>,
    replacements: &HashMap<String, String>,
) -> Vec<u8> {
    if replacements.is_empty() || body.is_empty() {
        return body;
    }
    match String::from_utf8(body) {
        Ok(value) => replace_placeholders_text(&value, replacements).into_bytes(),
        Err(error) => error.into_bytes(),
    }
}

pub(crate) fn is_egress_url_allowed(url: &reqwest::Url, allow_hosts: &[EgressAllowHost]) -> bool {
    if allow_hosts.is_empty() {
        return false;
    }
    let host = url
        .host_str()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if host.is_empty() {
        return false;
    }
    let Some(request_port) = url.port_or_known_default() else {
        return false;
    };
    let Some(default_port) = default_port_for_scheme(url.scheme()) else {
        return false;
    };
    allow_hosts
        .iter()
        .any(|allowed| allowed.matches(&host, request_port, default_port))
}

pub(crate) fn default_port_for_scheme(scheme: &str) -> Option<u16> {
    match scheme {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    }
}

pub(crate) fn to_list_item(entry: KvEntry, bodies: &mut HttpPreparedBodies) -> KvListItem {
    KvListItem {
        key: entry.key,
        value_handle: bodies.insert(Bytes::from(entry.value)),
        encoding: entry.encoding,
    }
}
