use super::*;

pub struct DynamicWorkerCreateEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub id: String,
    pub source: String,
    pub env: HashMap<String, String>,
    pub timeout: u64,
    pub policy: DynamicWorkerPolicy,
    pub host_rpc_bindings: Vec<DynamicHostRpcBindingSpec>,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

#[derive(Debug, Clone)]
pub struct DynamicWorkerCreateReply {
    pub handle: String,
    pub worker_name: String,
    pub timeout: u64,
}

pub struct DynamicWorkerLookupEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub id: String,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct DynamicWorkerListEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct DynamicWorkerDeleteEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub id: String,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct DynamicWorkerInvokeEvent {
    pub owner_worker: String,
    pub owner_generation: u64,
    pub owner_isolate_id: u64,
    pub binding: String,
    pub handle: String,
    pub request: WorkerInvocation,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicHostRpcBindingSpec {
    pub binding: String,
    pub target_id: String,
    #[serde(default)]
    pub methods: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicWorkerPolicy {
    #[serde(default)]
    pub egress_allow_hosts: Vec<String>,
    #[serde(default)]
    pub allow_host_rpc: bool,
    #[serde(default)]
    pub allow_websocket: bool,
    #[serde(default)]
    pub allow_transport: bool,
    #[serde(default)]
    pub allow_state_bindings: bool,
    #[serde(default = "default_dynamic_max_request_bytes")]
    pub max_request_bytes: u64,
    #[serde(default = "default_dynamic_max_response_bytes")]
    pub max_response_bytes: u64,
    #[serde(default = "default_dynamic_max_outbound_requests")]
    pub max_outbound_requests: u64,
    #[serde(default = "default_dynamic_max_concurrency")]
    pub max_concurrency: u64,
}

impl Default for DynamicWorkerPolicy {
    fn default() -> Self {
        Self {
            egress_allow_hosts: Vec::new(),
            allow_host_rpc: false,
            allow_websocket: false,
            allow_transport: false,
            allow_state_bindings: false,
            max_request_bytes: default_dynamic_max_request_bytes(),
            max_response_bytes: default_dynamic_max_response_bytes(),
            max_outbound_requests: default_dynamic_max_outbound_requests(),
            max_concurrency: default_dynamic_max_concurrency(),
        }
    }
}

const fn default_dynamic_max_request_bytes() -> u64 {
    1_048_576
}

const fn default_dynamic_max_response_bytes() -> u64 {
    2_097_152
}

const fn default_dynamic_max_outbound_requests() -> u64 {
    16
}

const fn default_dynamic_max_concurrency() -> u64 {
    32
}

pub struct DynamicHostRpcInvokeEvent {
    pub caller_worker: String,
    pub caller_generation: u64,
    pub _caller_isolate_id: u64,
    pub binding: String,
    pub method_name: String,
    pub args: Vec<u8>,
    pub reply_id: String,
    pub pending_replies: DynamicPendingReplies,
}

pub struct TestAsyncReplyEvent {
    pub reply_id: String,
    pub replies: TestAsyncReplies,
    pub delay_ms: u64,
    pub ok: bool,
    pub value: String,
    pub error: String,
}

#[derive(Clone)]
pub struct TestAsyncReplyOwner {
    pub worker_name: String,
    pub generation: u64,
    pub isolate_id: u64,
}

pub struct TestNestedTargetedInvokeEvent {
    pub worker_name: String,
    pub generation: u64,
    pub caller_isolate_id: u64,
    pub target_mode: String,
    pub target_id: String,
    pub method_name: String,
    pub args: Vec<u8>,
    pub reply_id: String,
    pub replies: TestAsyncReplies,
}

#[derive(Clone, Default)]
pub struct DynamicPendingReplies {
    pub(crate) entries: Arc<StdMutex<HashMap<String, DynamicPendingReplyEntry>>>,
    pub(crate) next_id: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
pub struct TestAsyncReplies {
    pub(crate) entries: Arc<StdMutex<HashMap<String, TestAsyncReplyEntry>>>,
    pub(crate) next_id: Arc<AtomicU64>,
}

pub(crate) struct TestAsyncReplyEntry {
    pub(crate) owner: TestAsyncReplyOwner,
}

#[derive(Clone)]
pub struct DynamicPendingReplyOwner {
    pub worker_name: String,
    pub generation: u64,
    pub isolate_id: u64,
}

pub(crate) struct DynamicPendingReplyEntry {
    pub(crate) owner: DynamicPendingReplyOwner,
}

pub enum DynamicPendingReplyPayload {
    Create(Result<DynamicWorkerCreateReply>),
    Lookup(Result<Option<DynamicWorkerCreateReply>>),
    List(Result<Vec<String>>),
    Delete(Result<bool>),
    Invoke(Result<WorkerOutput>),
    Fetch {
        result: Result<WorkerOutput>,
        stale_handle: bool,
        boundary: Option<TimeBoundary>,
    },
    HostRpc(Result<Vec<u8>>),
}

pub struct DynamicPendingReplyDelivery {
    pub owner: DynamicPendingReplyOwner,
    pub payload: DynamicPushedReplyPayload,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum DynamicPushedReplyPayload {
    Dynamic(DynamicPendingReplyResult),
    Fetch(DynamicFetchReplyResult),
    TestAsync(TestAsyncReplyResult),
}

#[derive(Serialize)]
pub struct DynamicPendingReplyResult {
    pub reply_id: String,
    pub(crate) ready: bool,
    pub(crate) kind: String,
    pub(crate) ok: bool,
    pub(crate) found: bool,
    pub(crate) deleted: bool,
    pub(crate) handle: String,
    pub(crate) worker: String,
    pub(crate) timeout: u64,
    pub(crate) ids: Vec<String>,
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) error: String,
}

#[derive(Serialize)]
pub struct DynamicFetchReplyResult {
    pub reply_id: String,
    pub ok: bool,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub error: String,
    pub stale_handle: bool,
    pub boundary_changed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_now_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_perf_ms: Option<f64>,
}

#[derive(Serialize)]
pub(crate) struct DynamicPendingReplyStartResult {
    pub(crate) ok: bool,
    pub(crate) reply_id: String,
    pub(crate) error: String,
}

#[derive(Serialize)]
pub struct TestAsyncReplyResult {
    pub reply_id: String,
    pub ok: bool,
    pub value: String,
    pub error: String,
}

#[derive(Clone)]
pub struct DynamicControlInbox {
    pub(crate) inner: Arc<StdMutex<DrainInboxState<DynamicControlItem>>>,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "payload")]
pub enum DynamicControlItem {
    #[serde(rename = "reply")]
    Reply(DynamicPushedReplyPayload),
}

pub(crate) struct DrainInboxState<T> {
    pub(crate) items: VecDeque<T>,
    pub(crate) drain_scheduled: bool,
}

impl<T> Default for DrainInboxState<T> {
    fn default() -> Self {
        Self {
            items: VecDeque::new(),
            drain_scheduled: false,
        }
    }
}

impl Default for DynamicControlInbox {
    fn default() -> Self {
        Self {
            inner: Arc::new(StdMutex::new(DrainInboxState::default())),
        }
    }
}

impl Default for DynamicPendingReplyResult {
    fn default() -> Self {
        Self {
            reply_id: String::new(),
            ready: false,
            kind: String::new(),
            ok: false,
            found: false,
            deleted: false,
            handle: String::new(),
            worker: String::new(),
            timeout: 0,
            ids: Vec::new(),
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            value: Vec::new(),
            error: String::new(),
        }
    }
}

impl DynamicPendingReplies {
    pub fn allocate(&self, owner: DynamicPendingReplyOwner) -> String {
        let reply_id = format!("dynr-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic pending reply lock poisoned");
        entries.insert(reply_id.clone(), DynamicPendingReplyEntry { owner });
        reply_id
    }

    pub fn cancel(&self, reply_id: &str) {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic pending reply lock poisoned");
        entries.remove(reply_id);
    }

    pub fn finish(
        &self,
        reply_id: String,
        payload: DynamicPendingReplyPayload,
    ) -> Option<DynamicPendingReplyDelivery> {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic pending reply lock poisoned");
        let entry = entries.remove(&reply_id)?;
        Some(DynamicPendingReplyDelivery {
            owner: entry.owner,
            payload: payload.into_delivery(reply_id),
        })
    }
}

const DYNAMIC_INBOX_BATCH_SIZE: usize = 64;

impl DynamicControlInbox {
    pub fn push_reply(&self, payload: DynamicPushedReplyPayload) -> bool {
        let mut inner = self
            .inner
            .lock()
            .expect("dynamic control inbox lock poisoned");
        inner.items.push_back(DynamicControlItem::Reply(payload));
        if inner.drain_scheduled {
            return false;
        }
        inner.drain_scheduled = true;
        true
    }

    pub fn take_batch(&self) -> Vec<DynamicControlItem> {
        take_drain_batch(&self.inner, "dynamic control inbox")
    }
}

fn take_drain_batch<T>(inner: &Arc<StdMutex<DrainInboxState<T>>>, label: &str) -> Vec<T> {
    let mut inner = inner.lock().expect(label);
    if inner.items.is_empty() {
        inner.drain_scheduled = false;
        return Vec::new();
    }
    let count = inner.items.len().min(DYNAMIC_INBOX_BATCH_SIZE);
    inner.items.drain(..count).collect()
}

impl DynamicPendingReplyPayload {
    fn into_delivery(self, reply_id: String) -> DynamicPushedReplyPayload {
        match self {
            Self::Create(result) => match result {
                Ok(created) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "create".to_string(),
                    ok: true,
                    handle: created.handle,
                    worker: created.worker_name,
                    timeout: created.timeout,
                    ..DynamicPendingReplyResult::default()
                }),
                Err(error) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "create".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                }),
            },
            Self::Lookup(result) => match result {
                Ok(Some(found)) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "lookup".to_string(),
                    ok: true,
                    found: true,
                    handle: found.handle,
                    worker: found.worker_name,
                    timeout: found.timeout,
                    ..DynamicPendingReplyResult::default()
                }),
                Ok(None) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "lookup".to_string(),
                    ok: true,
                    found: false,
                    ..DynamicPendingReplyResult::default()
                }),
                Err(error) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "lookup".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                }),
            },
            Self::List(result) => match result {
                Ok(ids) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "list".to_string(),
                    ok: true,
                    ids,
                    ..DynamicPendingReplyResult::default()
                }),
                Err(error) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "list".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                }),
            },
            Self::Delete(result) => match result {
                Ok(deleted) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "delete".to_string(),
                    ok: true,
                    deleted,
                    ..DynamicPendingReplyResult::default()
                }),
                Err(error) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "delete".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                }),
            },
            Self::Invoke(result) => match result {
                Ok(output) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "invoke".to_string(),
                    ok: true,
                    status: output.status,
                    headers: output.headers,
                    body: output.body,
                    ..DynamicPendingReplyResult::default()
                }),
                Err(error) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "invoke".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                }),
            },
            Self::Fetch {
                result,
                stale_handle,
                boundary,
            } => match result {
                Ok(output) => DynamicPushedReplyPayload::Fetch(DynamicFetchReplyResult {
                    reply_id,
                    ok: true,
                    status: output.status,
                    headers: output.headers,
                    body: output.body,
                    error: String::new(),
                    stale_handle,
                    boundary_changed: boundary.is_some(),
                    boundary_now_ms: boundary.as_ref().map(|value| value.now_ms),
                    boundary_perf_ms: boundary.as_ref().map(|value| value.perf_ms),
                }),
                Err(error) => DynamicPushedReplyPayload::Fetch(DynamicFetchReplyResult {
                    reply_id,
                    ok: false,
                    status: 0,
                    headers: Vec::new(),
                    body: Vec::new(),
                    error: error.to_string(),
                    stale_handle,
                    boundary_changed: boundary.is_some(),
                    boundary_now_ms: boundary.as_ref().map(|value| value.now_ms),
                    boundary_perf_ms: boundary.as_ref().map(|value| value.perf_ms),
                }),
            },
            Self::HostRpc(result) => match result {
                Ok(value) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "host-rpc".to_string(),
                    ok: true,
                    value,
                    ..DynamicPendingReplyResult::default()
                }),
                Err(error) => DynamicPushedReplyPayload::Dynamic(DynamicPendingReplyResult {
                    reply_id,
                    ready: true,
                    kind: "host-rpc".to_string(),
                    ok: false,
                    error: error.to_string(),
                    ..DynamicPendingReplyResult::default()
                }),
            },
        }
    }
}

impl TestAsyncReplies {
    pub fn allocate(&self, owner: TestAsyncReplyOwner) -> String {
        let reply_id = format!("tasr-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut entries = self.entries.lock().expect("test async reply lock poisoned");
        entries.insert(reply_id.clone(), TestAsyncReplyEntry { owner });
        reply_id
    }

    pub fn cancel(&self, reply_id: &str) {
        let mut entries = self.entries.lock().expect("test async reply lock poisoned");
        entries.remove(reply_id);
    }

    pub fn finish(
        &self,
        reply_id: String,
        result: Result<String>,
    ) -> Option<TestAsyncReplyDelivery> {
        let mut entries = self.entries.lock().expect("test async reply lock poisoned");
        let entry = entries.remove(&reply_id)?;
        let (ok, value, error) = match result {
            Ok(value) => (true, value, String::new()),
            Err(error) => (false, String::new(), error.to_string()),
        };
        Some(TestAsyncReplyDelivery {
            owner: entry.owner,
            payload: TestAsyncReplyResult {
                reply_id,
                ok,
                value,
                error,
            },
        })
    }
}

pub struct TestAsyncReplyDelivery {
    pub owner: TestAsyncReplyOwner,
    pub payload: TestAsyncReplyResult,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DynamicWorkerCreatePayload {
    pub(crate) request_id: String,
    pub(crate) binding: String,
    pub(crate) id: String,
    pub(crate) source: String,
    #[serde(default)]
    pub(crate) env: HashMap<String, String>,
    #[serde(default = "default_dynamic_worker_timeout")]
    pub(crate) timeout: u64,
    #[serde(default)]
    pub(crate) policy: DynamicWorkerPolicy,
    #[serde(default)]
    pub(crate) host_rpc_bindings: Vec<DynamicHostRpcBindingSpec>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DynamicWorkerLookupPayload {
    pub(crate) request_id: String,
    pub(crate) binding: String,
    pub(crate) id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DynamicWorkerListPayload {
    pub(crate) request_id: String,
    pub(crate) binding: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DynamicWorkerDeletePayload {
    pub(crate) request_id: String,
    pub(crate) binding: String,
    pub(crate) id: String,
}

fn default_dynamic_worker_timeout() -> u64 {
    5_000
}

#[derive(Debug, Deserialize)]
pub(crate) struct DynamicWorkerInvokePayload {
    pub(crate) request_id: String,
    pub(crate) subrequest_id: String,
    pub(crate) binding: String,
    pub(crate) handle: String,
    pub(crate) method: String,
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) headers: Vec<(String, String)>,
    #[serde(default)]
    pub(crate) body: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DynamicHostRpcInvokePayload {
    pub(crate) request_id: String,
    pub(crate) binding: String,
    pub(crate) method_name: String,
    #[serde(default)]
    pub(crate) args: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct DynamicProfile {
    pub(crate) warm_isolate_hit: Arc<AtomicU64>,
    pub(crate) fallback_dispatch: Arc<AtomicU64>,
    pub(crate) async_reply_completion: Arc<AtomicU64>,
    pub(crate) provider_task_callback: Arc<AtomicU64>,
    pub(crate) egress_deny_count: Arc<AtomicU64>,
    pub(crate) rpc_deny_count: Arc<AtomicU64>,
    pub(crate) quota_kill_count: Arc<AtomicU64>,
    pub(crate) upgrade_deny_count: Arc<AtomicU64>,
    pub(crate) direct_fetch_fast_path_hit: Arc<AtomicU64>,
    pub(crate) direct_fetch_fast_path_fallback: Arc<AtomicU64>,
    pub(crate) direct_fetch_dispatch_us: Arc<AtomicU64>,
    pub(crate) direct_fetch_dispatch_count: Arc<AtomicU64>,
    pub(crate) direct_fetch_child_execute_us: Arc<AtomicU64>,
    pub(crate) direct_fetch_child_execute_count: Arc<AtomicU64>,
    pub(crate) control_drain_batch: Arc<AtomicU64>,
    pub(crate) control_drain_item: Arc<AtomicU64>,
    pub(crate) reply_drain_batch: Arc<AtomicU64>,
    pub(crate) reply_drain_item: Arc<AtomicU64>,
    pub(crate) provider_task_drain_batch: Arc<AtomicU64>,
    pub(crate) provider_task_drain_item: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DynamicProfileSnapshot {
    pub warm_isolate_hit: u64,
    pub fallback_dispatch: u64,
    pub async_reply_completion: u64,
    pub provider_task_callback: u64,
    pub egress_deny_count: u64,
    pub rpc_deny_count: u64,
    pub quota_kill_count: u64,
    pub upgrade_deny_count: u64,
    pub direct_fetch_fast_path_hit: u64,
    pub direct_fetch_fast_path_fallback: u64,
    pub direct_fetch_dispatch_us: u64,
    pub direct_fetch_dispatch_count: u64,
    pub direct_fetch_child_execute_us: u64,
    pub direct_fetch_child_execute_count: u64,
    pub control_drain_batch: u64,
    pub control_drain_item: u64,
    pub reply_drain_batch: u64,
    pub reply_drain_item: u64,
    pub provider_task_drain_batch: u64,
    pub provider_task_drain_item: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct DynamicProfileResult {
    pub(crate) ok: bool,
    pub(crate) snapshot: Option<DynamicProfileSnapshot>,
    pub(crate) error: String,
}

impl DynamicProfile {
    pub fn record_warm_isolate_hit(&self) {
        self.warm_isolate_hit.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fallback_dispatch(&self) {
        self.fallback_dispatch.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_async_reply_completion(&self) {
        self.async_reply_completion.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_provider_task_callback(&self) {
        self.provider_task_callback.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_egress_deny(&self) {
        self.egress_deny_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rpc_deny(&self) {
        self.rpc_deny_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_quota_kill(&self) {
        self.quota_kill_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_upgrade_deny(&self) {
        self.upgrade_deny_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_direct_fetch_fast_path_hit(&self) {
        self.direct_fetch_fast_path_hit
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_direct_fetch_fast_path_fallback(&self) {
        self.direct_fetch_fast_path_fallback
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_direct_fetch_dispatch(&self, duration: Duration) {
        self.direct_fetch_dispatch_count
            .fetch_add(1, Ordering::Relaxed);
        self.direct_fetch_dispatch_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn record_direct_fetch_child_execute(&self, duration: Duration) {
        self.direct_fetch_child_execute_count
            .fetch_add(1, Ordering::Relaxed);
        self.direct_fetch_child_execute_us.fetch_add(
            duration.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn record_control_drain(&self, items: usize) {
        self.control_drain_batch.fetch_add(1, Ordering::Relaxed);
        self.control_drain_item
            .fetch_add(items as u64, Ordering::Relaxed);
    }

    pub fn record_reply_drain(&self, items: usize) {
        self.reply_drain_batch.fetch_add(1, Ordering::Relaxed);
        self.reply_drain_item
            .fetch_add(items as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> DynamicProfileSnapshot {
        DynamicProfileSnapshot {
            warm_isolate_hit: self.warm_isolate_hit.load(Ordering::Relaxed),
            fallback_dispatch: self.fallback_dispatch.load(Ordering::Relaxed),
            async_reply_completion: self.async_reply_completion.load(Ordering::Relaxed),
            provider_task_callback: self.provider_task_callback.load(Ordering::Relaxed),
            egress_deny_count: self.egress_deny_count.load(Ordering::Relaxed),
            rpc_deny_count: self.rpc_deny_count.load(Ordering::Relaxed),
            quota_kill_count: self.quota_kill_count.load(Ordering::Relaxed),
            upgrade_deny_count: self.upgrade_deny_count.load(Ordering::Relaxed),
            direct_fetch_fast_path_hit: self.direct_fetch_fast_path_hit.load(Ordering::Relaxed),
            direct_fetch_fast_path_fallback: self
                .direct_fetch_fast_path_fallback
                .load(Ordering::Relaxed),
            direct_fetch_dispatch_us: self.direct_fetch_dispatch_us.load(Ordering::Relaxed),
            direct_fetch_dispatch_count: self.direct_fetch_dispatch_count.load(Ordering::Relaxed),
            direct_fetch_child_execute_us: self
                .direct_fetch_child_execute_us
                .load(Ordering::Relaxed),
            direct_fetch_child_execute_count: self
                .direct_fetch_child_execute_count
                .load(Ordering::Relaxed),
            control_drain_batch: self.control_drain_batch.load(Ordering::Relaxed),
            control_drain_item: self.control_drain_item.load(Ordering::Relaxed),
            reply_drain_batch: self.reply_drain_batch.load(Ordering::Relaxed),
            reply_drain_item: self.reply_drain_item.load(Ordering::Relaxed),
            provider_task_drain_batch: self.provider_task_drain_batch.load(Ordering::Relaxed),
            provider_task_drain_item: self.provider_task_drain_item.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.warm_isolate_hit.store(0, Ordering::Relaxed);
        self.fallback_dispatch.store(0, Ordering::Relaxed);
        self.async_reply_completion.store(0, Ordering::Relaxed);
        self.provider_task_callback.store(0, Ordering::Relaxed);
        self.egress_deny_count.store(0, Ordering::Relaxed);
        self.rpc_deny_count.store(0, Ordering::Relaxed);
        self.quota_kill_count.store(0, Ordering::Relaxed);
        self.upgrade_deny_count.store(0, Ordering::Relaxed);
        self.direct_fetch_fast_path_hit.store(0, Ordering::Relaxed);
        self.direct_fetch_fast_path_fallback
            .store(0, Ordering::Relaxed);
        self.direct_fetch_dispatch_us.store(0, Ordering::Relaxed);
        self.direct_fetch_dispatch_count.store(0, Ordering::Relaxed);
        self.direct_fetch_child_execute_us
            .store(0, Ordering::Relaxed);
        self.direct_fetch_child_execute_count
            .store(0, Ordering::Relaxed);
        self.control_drain_batch.store(0, Ordering::Relaxed);
        self.control_drain_item.store(0, Ordering::Relaxed);
        self.reply_drain_batch.store(0, Ordering::Relaxed);
        self.reply_drain_item.store(0, Ordering::Relaxed);
        self.provider_task_drain_batch.store(0, Ordering::Relaxed);
        self.provider_task_drain_item.store(0, Ordering::Relaxed);
    }
}
