use super::*;

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
pub(crate) struct CachePutResult {
    ok: bool,
    error: String,
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_cache_match(
    state: Rc<RefCell<OpState>>,
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
        cache_name: scoped_cache_name(&state, &cache_name),
        method,
        url,
        headers,
        bypass_stale,
    };
    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.get(&request).await {
        Ok(CacheLookup::Fresh(response)) => cache_match_hit_result(&state, response, false, false),
        Ok(CacheLookup::StaleWhileRevalidate(response)) => {
            cache_match_hit_result(&state, response, true, true)
        }
        Ok(CacheLookup::StaleIfError(_)) => CacheMatchResult {
            ok: true,
            found: false,
            stale: false,
            should_revalidate: false,
            status: 0,
            headers_handle: 0,
            body_handle: 0,
            error: String::new(),
        },
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
    #[string] cache_name: String,
    #[string] method: String,
    #[string] url: String,
    request_headers_handle: u32,
    response_status: u16,
    response_headers_handle: u32,
    response_body_handle: u32,
) -> CachePutResult {
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
        cache_name: scoped_cache_name(&state, &cache_name),
        method,
        url,
        headers: request_headers,
        bypass_stale: false,
    };
    let response = CacheResponse {
        status: response_status,
        headers: response_headers,
        body,
    };
    let store = state.borrow().borrow::<CacheStore>().clone();
    match store.put(&request, response).await {
        Ok(_) => CachePutResult {
            ok: true,
            error: String::new(),
        },
        Err(error) => CachePutResult {
            ok: false,
            error: error.to_string(),
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) async fn op_cache_delete(
    state: Rc<RefCell<OpState>>,
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
        cache_name: scoped_cache_name(&state, &cache_name),
        method,
        url,
        headers,
        bypass_stale: false,
    };

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

fn scoped_cache_name(state: &Rc<RefCell<OpState>>, cache_name: &str) -> String {
    let state = state.borrow();
    let worker = &state.borrow::<WorkerCacheNamespace>().0;
    format!("worker:{worker}:{}", cache_name.trim())
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
