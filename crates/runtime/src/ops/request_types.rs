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

#[derive(Clone)]
pub(crate) struct RequestExecutionContext {
    pub(crate) worker_name: Arc<str>,
    pub(crate) generation: u64,
    pub(crate) dynamic_bindings: Arc<HashSet<String>>,
    pub(crate) dynamic_rpc_bindings: Arc<HashSet<String>>,
    pub(crate) replacements: Arc<HashMap<String, String>>,
    pub(crate) egress_allow_hosts: Arc<Vec<EgressAllowHost>>,
    pub(crate) allow_cache: bool,
    pub(crate) max_outbound_requests: Option<u64>,
    pub(crate) dynamic_quota_state: Option<Arc<crate::service::DynamicQuotaState>>,
}

impl Default for RequestExecutionContext {
    fn default() -> Self {
        Self {
            worker_name: Arc::<str>::from(""),
            generation: 0,
            dynamic_bindings: Arc::new(HashSet::new()),
            dynamic_rpc_bindings: Arc::new(HashSet::new()),
            replacements: Arc::new(HashMap::new()),
            egress_allow_hosts: Arc::new(Vec::new()),
            allow_cache: true,
            max_outbound_requests: None,
            dynamic_quota_state: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EgressAllowHost {
    pub(crate) host: String,
    pub(crate) wildcard: bool,
    pub(crate) port: Option<u16>,
}

impl EgressAllowHost {
    pub(crate) fn matches(&self, host: &str, request_port: u16, default_port: u16) -> bool {
        let port_matches = match self.port {
            Some(port) => port == request_port,
            None => request_port == default_port,
        };
        if !port_matches {
            return false;
        }
        if !self.wildcard {
            return host == self.host;
        }
        host == self.host
            || host
                .strip_suffix(self.host.as_str())
                .map(|prefix| prefix.ends_with('.'))
                .unwrap_or(false)
    }
}

pub(crate) fn parse_egress_allow_host(allowed: &str) -> Option<EgressAllowHost> {
    let allowed = allowed.trim().to_ascii_lowercase();
    if allowed.is_empty() {
        return None;
    }
    let (host, port) = match allowed.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|char| char.is_ascii_digit()) => {
            let parsed = port.parse::<u16>().ok().filter(|port| *port > 0)?;
            (host, Some(parsed))
        }
        _ => (allowed.as_str(), None),
    };
    let (host, wildcard) = match host.strip_prefix("*.") {
        Some(suffix) if !suffix.is_empty() => (suffix, true),
        Some(_) => return None,
        None => (host, false),
    };
    if host.is_empty() {
        return None;
    }
    Some(EgressAllowHost {
        host: host.to_string(),
        wildcard,
        port,
    })
}

impl RequestExecutionContext {
    pub(crate) fn new(
        worker_name: String,
        generation: u64,
        dynamic_bindings: Vec<String>,
        dynamic_rpc_bindings: Vec<String>,
        replacements: Vec<(String, String)>,
        egress_allow_hosts: Vec<String>,
        allow_cache: bool,
        max_outbound_requests: Option<u64>,
        dynamic_quota_state: Option<Arc<crate::service::DynamicQuotaState>>,
    ) -> Self {
        let dynamic_bindings = dynamic_bindings
            .into_iter()
            .map(|binding| binding.trim().to_string())
            .filter(|binding| !binding.is_empty())
            .collect();
        let dynamic_rpc_bindings = dynamic_rpc_bindings
            .into_iter()
            .map(|binding| binding.trim().to_string())
            .filter(|binding| !binding.is_empty())
            .collect();
        let replacements = replacements
            .into_iter()
            .filter_map(|(placeholder, value)| {
                let key = placeholder.trim().to_string();
                if key.is_empty() {
                    return None;
                }
                Some((key, value))
            })
            .collect();
        let egress_allow_hosts = egress_allow_hosts
            .into_iter()
            .filter_map(|host| parse_egress_allow_host(&host))
            .collect();

        Self {
            worker_name: Arc::<str>::from(worker_name),
            generation,
            dynamic_bindings: Arc::new(dynamic_bindings),
            dynamic_rpc_bindings: Arc::new(dynamic_rpc_bindings),
            replacements: Arc::new(replacements),
            egress_allow_hosts: Arc::new(egress_allow_hosts),
            allow_cache,
            max_outbound_requests,
            dynamic_quota_state,
        }
    }
}

pub(crate) struct RequestSecretContext {
    pub(crate) isolate_id: u64,
    pub(crate) execution: RequestExecutionContext,
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
