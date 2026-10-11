use super::*;

/// A stored value, its bytes waiting behind a handle.
#[derive(Debug, Serialize)]
pub(crate) struct KvValue {
    value_handle: u32,
    encoding: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct KvListItem {
    key: String,
    value_handle: u32,
    encoding: String,
}

/// The value stored under `key`, or `null` when there is none.
pub(crate) async fn op_kv_get(
    state: Rc<RefCell<OpState>>,
    binding: String,
    key: String,
) -> std::result::Result<Option<KvValue>, OpError> {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
    let started = Instant::now();
    let store = state.borrow().borrow::<KvStore>().clone();
    let result = match store.get(&worker_name, &binding, &key).await {
        Ok(value) => Ok(value.map(|value| KvValue {
            value_handle: state
                .borrow_mut()
                .borrow_mut::<HttpPreparedBodies>()
                .insert(value.value),
            encoding: value.encoding,
        })),
        Err(error) => Err(OpError::new(format!("kv get {key}: {error}"))),
    };
    store.record_profile(
        KvProfileMetricKind::OpGetValue,
        started.elapsed().as_micros() as u64,
        1,
    );
    result
}

pub(crate) fn op_kv_profile_record(
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

pub(crate) fn op_kv_profile_take(state: &mut OpState) -> KvProfileSnapshot {
    let store = state.borrow::<KvStore>().clone();
    store.take_profile_snapshot_and_reset()
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
) -> std::result::Result<(), OpError> {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
    let store = state.borrow().borrow::<KvStore>().clone();
    store
        .put(&worker_name, &binding, &key, &value)
        .await
        .map_err(|error| OpError::new(format!("kv put {key}: {error}")))?;
    Ok(())
}

pub(crate) async fn op_kv_put_bytes(
    state: Rc<RefCell<OpState>>,
    binding: String,
    key: String,
    encoding: String,
    value: JsBuffer,
) -> std::result::Result<(), OpError> {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
    let value = Bytes::copy_from_slice(value.as_ref());
    let store = state.borrow().borrow::<KvStore>().clone();
    store
        .put_value(&worker_name, &binding, &key, value.as_ref(), &encoding)
        .await
        .map_err(|error| OpError::new(format!("kv put {key}: {error}")))?;
    Ok(())
}

pub(crate) async fn op_kv_delete(
    state: Rc<RefCell<OpState>>,
    binding: String,
    key: String,
) -> std::result::Result<(), OpError> {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
    let store = state.borrow().borrow::<KvStore>().clone();
    store
        .delete(&worker_name, &binding, &key)
        .await
        .map_err(|error| OpError::new(format!("kv delete {key}: {error}")))?;
    Ok(())
}

pub(crate) async fn op_kv_list(
    state: Rc<RefCell<OpState>>,
    binding: String,
    prefix: String,
    limit: u32,
) -> std::result::Result<Vec<KvListItem>, OpError> {
    let worker_name = state.borrow().borrow::<WorkerCacheNamespace>().0.clone();
    let store = state.borrow().borrow::<KvStore>().clone();
    let clamped_limit = limit.clamp(1, 1000) as usize;
    let values = store
        .list(&worker_name, &binding, &prefix, clamped_limit)
        .await
        .map_err(|error| OpError::new(format!("kv list {prefix}: {error}")))?;
    let mut op_state = state.borrow_mut();
    let bodies = op_state.borrow_mut::<HttpPreparedBodies>();
    Ok(values
        .into_iter()
        .map(|entry| to_list_item(entry, bodies))
        .collect())
}

fn to_list_item(entry: KvEntry, bodies: &mut HttpPreparedBodies) -> KvListItem {
    KvListItem {
        key: entry.key,
        value_handle: bodies.insert(entry.value),
        encoding: entry.encoding,
    }
}
