use super::*;

/// A cached response, its headers and body waiting behind handles.
#[derive(Debug, Serialize)]
pub(crate) struct CacheMatch {
    should_revalidate: bool,
    status: u16,
    headers_handle: u32,
    body_handle: u32,
}

/// The cached response for a request, or `null` on a miss.
pub(crate) async fn op_cache_match(
    state: Rc<RefCell<OpState>>,
    cache_name: String,
    method: String,
    url: String,
    headers_handle: u32,
    bypass_stale: bool,
) -> std::result::Result<Option<CacheMatch>, OpError> {
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
    Ok(match store.get(&request).await.map_err(js_error)? {
        CacheLookup::Fresh(response) => Some(cache_match_hit(&state, response, false)),
        CacheLookup::StaleWhileRevalidate(response) => {
            Some(cache_match_hit(&state, response, true))
        }
        CacheLookup::StaleIfError(_) | CacheLookup::Miss => None,
    })
}

fn cache_match_hit(
    state: &Rc<RefCell<OpState>>,
    response: CacheResponse,
    should_revalidate: bool,
) -> CacheMatch {
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
    CacheMatch {
        should_revalidate,
        status: response.status,
        headers_handle,
        body_handle,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn op_cache_put(
    state: Rc<RefCell<OpState>>,
    cache_name: String,
    method: String,
    url: String,
    request_headers_handle: u32,
    response_status: u16,
    response_headers_handle: u32,
    response_body_handle: u32,
) -> std::result::Result<(), OpError> {
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
    store.put(&request, response).await.map_err(js_error)?;
    Ok(())
}

pub(crate) async fn op_cache_delete(
    state: Rc<RefCell<OpState>>,
    cache_name: String,
    method: String,
    url: String,
    headers_handle: u32,
) -> std::result::Result<bool, OpError> {
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
    store.delete(&request).await.map_err(js_error)
}

fn scoped_cache_name(state: &Rc<RefCell<OpState>>, cache_name: &str) -> String {
    let state = state.borrow();
    let worker = &state.borrow::<WorkerCacheNamespace>().0;
    format!("worker:{worker}:{}", cache_name.trim())
}

pub(crate) fn op_cache_revalidate(
    state: &mut OpState,
    cache_name: String,
    method: String,
    url: String,
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
