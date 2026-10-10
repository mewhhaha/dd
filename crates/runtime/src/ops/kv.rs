use super::*;

#[derive(Debug, Serialize)]
pub(crate) struct KvListItem {
    key: String,
    value_handle: u32,
    encoding: String,
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

pub(crate) async fn op_kv_get_value(
    state: Rc<RefCell<OpState>>,
    binding: String,
    key: String,
) -> KvGetValueResult {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
    let started = Instant::now();
    let store = state.borrow().borrow::<KvStore>().clone();
    let result = match store.get(&worker_name, &binding, &key).await {
        Ok(Some(value)) => {
            let value_handle = state
                .borrow_mut()
                .borrow_mut::<HttpPreparedBodies>()
                .insert(value.value);
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

pub(crate) fn op_kv_profile_record_js(
    state: &mut OpState,
    metric: String,
    duration_us: u32,
    items: u32,
) {
    let kind = match metric.as_str() {
        "js_request_total" => KvProfileMetricKind::JsRequestTotal,
        _ => return,
    };
    let store = state.borrow::<KvStore>().clone();
    store.record_profile(kind, u64::from(duration_us), u64::from(items.max(1)));
}

pub(crate) fn op_kv_profile_take(state: &mut OpState) -> KvProfileResult {
    let store = state.borrow::<KvStore>().clone();
    KvProfileResult {
        ok: true,
        snapshot: Some(store.take_profile_snapshot_and_reset()),
        error: String::new(),
    }
}

pub(crate) fn op_kv_profile_reset(state: &mut OpState) {
    let store = state.borrow::<KvStore>().clone();
    store.reset_profile();
}

pub(crate) async fn op_kv_put(
    state: Rc<RefCell<OpState>>,
    binding: String,
    key: String,
    value: String,
) -> KvOpResult {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
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

pub(crate) async fn op_kv_put_value_bytes(
    state: Rc<RefCell<OpState>>,
    binding: String,
    key: String,
    encoding: String,
    value: JsBuffer,
) -> KvOpResult {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
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

pub(crate) async fn op_kv_delete(
    state: Rc<RefCell<OpState>>,
    binding: String,
    key: String,
) -> KvOpResult {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
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

pub(crate) async fn op_kv_list(
    state: Rc<RefCell<OpState>>,
    binding: String,
    prefix: String,
    limit: u32,
) -> KvListResult {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
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

fn to_list_item(entry: KvEntry, bodies: &mut HttpPreparedBodies) -> KvListItem {
    KvListItem {
        key: entry.key,
        value_handle: bodies.insert(entry.value),
        encoding: entry.encoding,
    }
}
