use super::*;

pub type RequestBodyChunk = std::result::Result<Vec<u8>, String>;
pub type RequestBodyReceiver = mpsc::Receiver<RequestBodyChunk>;


#[derive(Default)]
pub struct MemoryRequestScopes {
    pub(crate) scopes: HashMap<String, MemoryRequestScope>,
}

#[derive(Clone)]
pub(crate) struct MemoryRequestScope {
    pub(crate) namespace: String,
    pub(crate) memory_key: String,
}

#[derive(Default)]
pub struct RequestSecretContexts {
    pub(crate) contexts: HashMap<String, RequestSecretContext>,
}

pub(crate) struct RequestSecretContext {
    pub(crate) worker_name: String,
    pub(crate) generation: u64,
    pub(crate) isolate_id: u64,
    pub(crate) dynamic_bindings: HashSet<String>,
    pub(crate) dynamic_rpc_bindings: HashSet<String>,
    pub(crate) replacements: HashMap<String, String>,
    pub(crate) egress_allow_hosts: Vec<String>,
    pub(crate) canceled: Arc<AtomicBool>,
    pub(crate) canceled_notify: Arc<Notify>,
}

#[derive(Default)]
pub struct RequestBodyStreams {
    pub(crate) streams: HashMap<String, Arc<RequestBodyStream>>,
}

pub(crate) struct RequestBodyStream {
    pub(crate) receiver: Mutex<RequestBodyReceiver>,
    pub(crate) canceled: AtomicBool,
    pub(crate) canceled_notify: Notify,
}

impl RequestBodyStream {
    pub(crate) fn new(receiver: RequestBodyReceiver) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            canceled: AtomicBool::new(false),
            canceled_notify: Notify::new(),
        }
    }

    pub(crate) fn cancel(&self) {
        self.canceled.store(true, Ordering::SeqCst);
        self.canceled_notify.notify_waiters();
    }

    pub(crate) fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct RequestBodyReadResult {
    pub(crate) ok: bool,
    pub(crate) done: bool,
    pub(crate) chunk: Vec<u8>,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CompletionMeta {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) wait_until_count: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WaitUntilRequestId {
    pub(crate) request_id: String,
}
