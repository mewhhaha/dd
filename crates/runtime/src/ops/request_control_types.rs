use super::*;

#[derive(Clone, Debug)]
pub enum WorkerSource {
    Inline(Arc<str>),
    Module {
        graph_id: String,
        entrypoint: String,
    },
}

impl WorkerSource {
    pub(crate) fn inline(source: String) -> Self {
        Self::Inline(Arc::<str>::from(source))
    }
}

pub struct TestAsyncReplyEvent {
    pub reply_id: String,
    pub replies: TestAsyncReplies,
    pub delay_ms: u64,
    pub ok: bool,
    pub value: String,
    pub error: String,
}

#[derive(Clone, Default)]
pub struct PendingReplies {
    pub(crate) entries: Arc<StdMutex<HashSet<String>>>,
    pub(crate) next_id: Arc<AtomicU64>,
}

#[derive(Clone, Default)]
pub struct TestAsyncReplies {
    pub(crate) entries: Arc<StdMutex<HashMap<String, TestAsyncReplyEntry>>>,
    pub(crate) next_id: Arc<AtomicU64>,
}

pub(crate) struct TestAsyncReplyEntry {
    pub(crate) owner: PendingReplyOwner,
}

#[derive(Clone)]
pub struct PendingReplyOwner {
    pub worker_name: String,
    pub generation: u64,
    pub isolate_id: u64,
}

pub enum PendingReplyPayload {
    Fetch {
        result: Result<WorkerOutput>,
        boundary: Option<TimeBoundary>,
    },
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum PushedReplyPayload {
    Fetch(FetchReplyResult),
    TestAsync(TestAsyncReplyResult),
}

#[derive(Serialize)]
pub struct FetchReplyResult {
    pub reply_id: String,
    pub ok: bool,
    pub status: u16,
    pub headers_handle: u32,
    pub body_handle: u32,
    #[serde(skip_serializing)]
    pub pending_output: Option<WorkerOutput>,
    pub error: String,
    pub boundary_changed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_now_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_perf_ms: Option<f64>,
}

impl FetchReplyResult {
    fn from_result(
        reply_id: String,
        result: Result<WorkerOutput>,
        boundary: Option<TimeBoundary>,
    ) -> Self {
        let boundary_changed = boundary.is_some();
        let boundary_now_ms = boundary.as_ref().map(|value| value.now_ms);
        let boundary_perf_ms = boundary.as_ref().map(|value| value.perf_ms);
        match result {
            Ok(output) => Self {
                reply_id,
                ok: true,
                status: output.status,
                headers_handle: 0,
                body_handle: 0,
                pending_output: Some(output),
                error: String::new(),
                boundary_changed,
                boundary_now_ms,
                boundary_perf_ms,
            },
            Err(error) => Self {
                reply_id,
                ok: false,
                status: 0,
                headers_handle: 0,
                body_handle: 0,
                pending_output: None,
                error: error.to_string(),
                boundary_changed,
                boundary_now_ms,
                boundary_perf_ms,
            },
        }
    }
}

#[derive(Serialize)]
pub(crate) struct PendingReplyStartResult {
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
pub struct RequestControlInbox {
    pub(crate) ready: Arc<Notify>,
    pub(crate) inner: Arc<StdMutex<DrainInboxState<RequestControlItem>>>,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "payload")]
pub enum RequestControlItem {
    #[serde(rename = "reply")]
    Reply(PushedReplyPayload),
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

impl Default for RequestControlInbox {
    fn default() -> Self {
        Self {
            ready: Arc::new(Notify::new()),
            inner: Arc::new(StdMutex::new(DrainInboxState::default())),
        }
    }
}

impl PendingReplies {
    pub fn allocate(&self) -> String {
        let reply_id = format!("reply-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut entries = self.entries.lock().expect("pending reply lock poisoned");
        entries.insert(reply_id.clone());
        reply_id
    }

    pub fn cancel(&self, reply_id: &str) {
        let mut entries = self.entries.lock().expect("pending reply lock poisoned");
        entries.remove(reply_id);
    }

    pub(crate) fn finish_into(
        &self,
        reply_id: String,
        payload: PendingReplyPayload,
        inbox: &RequestControlInbox,
    ) {
        let mut entries = self.entries.lock().expect("pending reply lock poisoned");
        if entries.remove(&reply_id) {
            inbox.push_reply(payload.into_delivery(reply_id));
        }
    }
}

const REQUEST_INBOX_BATCH_SIZE: usize = 64;

impl RequestControlInbox {
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .expect("request control inbox lock poisoned")
            .items
            .is_empty()
    }

    pub fn push_reply(&self, payload: PushedReplyPayload) -> bool {
        let mut inner = self
            .inner
            .lock()
            .expect("request control inbox lock poisoned");
        inner.items.push_back(RequestControlItem::Reply(payload));
        self.ready.notify_one();
        if inner.drain_scheduled {
            return false;
        }
        inner.drain_scheduled = true;
        true
    }

    pub fn take_batch(&self) -> Vec<RequestControlItem> {
        take_drain_batch(&self.inner, "request control inbox")
    }
}

fn take_drain_batch<T>(inner: &Arc<StdMutex<DrainInboxState<T>>>, label: &str) -> Vec<T> {
    let mut inner = inner.lock().expect(label);
    if inner.items.is_empty() {
        inner.drain_scheduled = false;
        return Vec::new();
    }
    let count = inner.items.len().min(REQUEST_INBOX_BATCH_SIZE);
    inner.items.drain(..count).collect()
}

impl PendingReplyPayload {
    fn into_delivery(self, reply_id: String) -> PushedReplyPayload {
        match self {
            Self::Fetch { result, boundary } => {
                PushedReplyPayload::Fetch(FetchReplyResult::from_result(reply_id, result, boundary))
            }
        }
    }
}

impl TestAsyncReplies {
    pub fn allocate(&self, owner: PendingReplyOwner) -> String {
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
    pub owner: PendingReplyOwner,
    pub payload: TestAsyncReplyResult,
}
