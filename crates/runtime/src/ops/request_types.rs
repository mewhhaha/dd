use super::*;

pub type RequestBodyChunk = std::result::Result<Bytes, String>;
pub type RequestBodyReceiver = mpsc::Receiver<RequestBodyChunk>;

#[derive(Default)]
pub struct HttpPreparedBodies {
    next: u32,
    bodies: HashMap<u32, Bytes>,
}

impl HttpPreparedBodies {
    pub(crate) fn insert(&mut self, body: impl Into<Bytes>) -> u32 {
        let body = body.into();
        if body.is_empty() {
            return 0;
        }
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.bodies.contains_key(&self.next) {
                self.bodies.insert(self.next, body);
                return self.next;
            }
        }
    }

    pub(crate) fn take(&mut self, handle: u32) -> Option<Bytes> {
        if handle == 0 {
            return Some(Bytes::new());
        }
        self.bodies.remove(&handle)
    }
}

#[derive(Default)]
pub struct HttpPreparedHeaders {
    next: u32,
    headers: HashMap<u32, Vec<(String, String)>>,
}

impl HttpPreparedHeaders {
    pub(crate) fn insert(&mut self, headers: Vec<(String, String)>) -> u32 {
        if headers.is_empty() {
            return 0;
        }
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.headers.contains_key(&self.next) {
                self.headers.insert(self.next, headers);
                return self.next;
            }
        }
    }

    pub(crate) fn take(&mut self, handle: u32) -> Option<Vec<(String, String)>> {
        if handle == 0 {
            return Some(Vec::new());
        }
        self.headers.remove(&handle)
    }
}

#[derive(Default)]
pub struct RequestInvocationHandles {
    pub(crate) next: u32,
    pub(crate) payloads: HashMap<u32, WorkerRequestPayload>,
}

impl RequestInvocationHandles {
    pub(crate) fn insert(&mut self, payload: WorkerRequestPayload) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.payloads.contains_key(&self.next) {
                self.payloads.insert(self.next, payload);
                return self.next;
            }
        }
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<WorkerRequestPayload> {
        self.payloads.remove(&handle)
    }

    pub(crate) fn take_descriptor(
        &mut self,
        handle: u32,
    ) -> Option<WorkerRequestDescriptorPayload> {
        self.payloads
            .remove(&handle)
            .map(|payload| WorkerRequestDescriptorPayload::from_payload(&payload))
    }
}

#[derive(Default)]
pub struct WorkerDeploymentHandles {
    pub(crate) next: u32,
    pub(crate) payloads: HashMap<u32, WorkerDeploymentPayload>,
}

impl WorkerDeploymentHandles {
    pub(crate) fn insert(&mut self, payload: WorkerDeploymentPayload) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.payloads.contains_key(&self.next) {
                self.payloads.insert(self.next, payload);
                return self.next;
            }
        }
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<WorkerDeploymentPayload> {
        self.payloads.remove(&handle)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkerDeploymentPayload {
    pub(crate) worker_name: String,
    pub(crate) kv_bindings: Vec<String>,
    pub(crate) kv_read_cache_config: WorkerKvReadCacheConfigPayload,
    pub(crate) memory_bindings: Vec<String>,
    pub(crate) dynamic_bindings: Vec<String>,
    pub(crate) dynamic_rpc_bindings: Vec<String>,
    pub(crate) service_bindings: Vec<WorkerServiceBindingPayload>,
    pub(crate) dynamic_env: Vec<(String, String)>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkerServiceBindingPayload {
    pub(crate) binding: String,
    pub(crate) service: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkerKvReadCacheConfigPayload {
    pub(crate) max_entries: usize,
    pub(crate) max_bytes: usize,
    pub(crate) hit_ttl_ms: u64,
    pub(crate) miss_ttl_ms: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct WorkerRequestPayload {
    pub(crate) request_id: String,
    pub(crate) request_context_handle: u32,
    pub(crate) completion_handle: u32,
    pub(crate) memory_request_scope_handle: u32,
    pub(crate) memory_call: Option<MemoryExecutionCall>,
    pub(crate) host_rpc_call: Option<HostRpcExecutionCall>,
    pub(crate) request_body_stream_handle: u32,
    pub(crate) request_headers_handle: u32,
    pub(crate) request_body_handle: u32,
    pub(crate) stream_response: bool,
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) input_request_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct WorkerRequestDescriptorPayload {
    pub(crate) request_id: String,
    pub(crate) request_context_handle: u32,
    pub(crate) completion_handle: u32,
    pub(crate) memory_request_scope_handle: u32,
    pub(crate) memory_call: Option<MemoryExecutionCall>,
    pub(crate) host_rpc_call: Option<HostRpcExecutionCall>,
    pub(crate) request_body_stream_handle: u32,
    pub(crate) request_headers_handle: u32,
    pub(crate) request_body_handle: u32,
    pub(crate) stream_response: bool,
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) input_request_id: String,
}

impl WorkerRequestDescriptorPayload {
    fn from_payload(payload: &WorkerRequestPayload) -> Self {
        Self {
            request_id: payload.request_id.clone(),
            request_context_handle: payload.request_context_handle,
            completion_handle: payload.completion_handle,
            memory_request_scope_handle: payload.memory_request_scope_handle,
            memory_call: payload.memory_call.clone(),
            host_rpc_call: payload.host_rpc_call.clone(),
            request_body_stream_handle: payload.request_body_stream_handle,
            request_headers_handle: payload.request_headers_handle,
            request_body_handle: payload.request_body_handle,
            stream_response: payload.stream_response,
            method: payload.method.clone(),
            url: payload.url.clone(),
            input_request_id: payload.input_request_id.clone(),
        }
    }
}

#[derive(Default)]
pub struct MemoryRequestScopes {
    next: u32,
    pub(crate) scopes: HashMap<u32, MemoryRequestScope>,
}

impl MemoryRequestScopes {
    pub(crate) fn insert(&mut self, scope: MemoryRequestScope) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.scopes.contains_key(&self.next) {
                self.scopes.insert(self.next, scope);
                return self.next;
            }
        }
    }

    pub(crate) fn get(&self, handle: u32) -> Option<&MemoryRequestScope> {
        if handle == 0 {
            return None;
        }
        self.scopes.get(&handle)
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<MemoryRequestScope> {
        if handle == 0 {
            return None;
        }
        self.scopes.remove(&handle)
    }
}

#[derive(Clone)]
pub(crate) struct MemoryRequestScope {
    pub(crate) namespace: String,
    pub(crate) memory_key: String,
    pub(crate) owner_epoch: i64,
}

#[derive(Default)]
pub struct ActiveRequestContextHandles {
    next_completion: u32,
    request_ids: HashMap<String, u32>,
    contexts: HashMap<u32, ActiveRequestContext>,
    completion_handles: HashMap<u32, u32>,
}

impl ActiveRequestContextHandles {
    pub(crate) fn insert(&mut self, mut context: ActiveRequestContext) -> u32 {
        if context.request_id.trim().is_empty()
            || context.completion_token.trim().is_empty()
            || context.request_context_handle == 0
        {
            return 0;
        }
        let completion_handle = loop {
            self.next_completion = self.next_completion.wrapping_add(1);
            if self.next_completion == 0 {
                continue;
            }
            if !self.completion_handles.contains_key(&self.next_completion) {
                break self.next_completion;
            }
        };
        context.completion_handle = completion_handle;
        if let Some(previous) = self
            .request_ids
            .insert(context.request_id.clone(), context.request_context_handle)
        {
            if let Some(previous_context) = self.contexts.remove(&previous) {
                self.completion_handles
                    .remove(&previous_context.completion_handle);
            }
        }
        if let Some(previous) = self
            .contexts
            .insert(context.request_context_handle, context.clone())
        {
            self.request_ids.remove(&previous.request_id);
            self.completion_handles.remove(&previous.completion_handle);
        }
        self.completion_handles
            .insert(completion_handle, context.request_context_handle);
        completion_handle
    }

    pub(crate) fn get_request(&self, request_id: &str) -> Option<u32> {
        self.request_ids
            .get(request_id)
            .copied()
            .filter(|handle| *handle > 0)
    }

    pub(crate) fn get_completion(&self, completion_handle: u32) -> Option<&ActiveRequestContext> {
        let request_context_handle = self.completion_handles.get(&completion_handle)?;
        self.contexts.get(request_context_handle)
    }

    pub(crate) fn increment_wait_until(&mut self, completion_handle: u32) -> Option<usize> {
        let request_context_handle = self.completion_handles.get(&completion_handle)?;
        let context = self.contexts.get_mut(request_context_handle)?;
        context.wait_until_count = context.wait_until_count.saturating_add(1);
        Some(context.wait_until_count)
    }

    pub(crate) fn mark_wait_until_done(
        &mut self,
        completion_handle: u32,
    ) -> Option<ActiveRequestContext> {
        let request_context_handle = self.completion_handles.get(&completion_handle)?;
        let context = self.contexts.get_mut(request_context_handle)?;
        if context.wait_until_done_sent {
            return None;
        }
        context.wait_until_done_sent = true;
        Some(context.clone())
    }

    pub(crate) fn get_handle(&self, request_context_handle: u32) -> Option<&ActiveRequestContext> {
        if request_context_handle == 0 {
            return None;
        }
        self.contexts.get(&request_context_handle)
    }

    pub(crate) fn remove_handle(&mut self, handle: u32) -> Option<ActiveRequestContext> {
        if handle == 0 {
            return None;
        }
        let context = self.contexts.remove(&handle)?;
        self.request_ids.remove(&context.request_id);
        self.completion_handles.remove(&context.completion_handle);
        Some(context)
    }
}

#[derive(Clone)]
pub(crate) struct ActiveRequestContext {
    pub(crate) request_context_handle: u32,
    pub(crate) completion_handle: u32,
    pub(crate) request_id: String,
    pub(crate) completion_token: String,
    pub(crate) wait_until_count: usize,
    pub(crate) wait_until_done_sent: bool,
}

#[derive(Default)]
pub struct RequestSecretContexts {
    next: u32,
    pub(crate) contexts: HashMap<u32, RequestSecretContext>,
}

impl RequestSecretContexts {
    pub(crate) fn insert(&mut self, context: RequestSecretContext) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.contexts.contains_key(&self.next) {
                self.contexts.insert(self.next, context);
                return self.next;
            }
        }
    }

    pub(crate) fn get(&self, handle: u32) -> Option<&RequestSecretContext> {
        if handle == 0 {
            return None;
        }
        self.contexts.get(&handle)
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<RequestSecretContext> {
        if handle == 0 {
            return None;
        }
        self.contexts.remove(&handle)
    }
}

#[derive(Clone)]
pub(crate) struct RequestExecutionContext {
    pub(crate) worker_name: Arc<str>,
    pub(crate) generation: u64,
    pub(crate) dynamic_bindings: Arc<HashSet<String>>,
    pub(crate) dynamic_rpc_bindings: Arc<HashSet<String>>,
    pub(crate) service_bindings: Arc<HashMap<String, String>>,
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
            service_bindings: Arc::new(HashMap::new()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_invocation_descriptor_keeps_request_state_behind_handles() {
        let mut handles = RequestInvocationHandles::default();
        let handle = handles.insert(WorkerRequestPayload {
            request_id: "runtime-1".to_string(),
            request_context_handle: 0,
            completion_handle: 0,
            memory_request_scope_handle: 0,
            memory_call: None,
            host_rpc_call: None,
            request_body_stream_handle: 0,
            request_headers_handle: 7,
            request_body_handle: 9,
            stream_response: false,
            method: "POST".to_string(),
            url: "http://worker/path".to_string(),
            input_request_id: "user-1".to_string(),
        });

        let descriptor = handles
            .take_descriptor(handle)
            .expect("descriptor should exist");
        assert_eq!(descriptor.request_id, "runtime-1");
        assert_eq!(descriptor.method, "POST");
        assert_eq!(descriptor.request_headers_handle, 7);
        assert_eq!(descriptor.request_body_handle, 9);
        assert_eq!(descriptor.input_request_id, "user-1");
        assert!(descriptor.memory_call.is_none());
        assert!(descriptor.host_rpc_call.is_none());
        let descriptor_value =
            serde_json::to_value(&descriptor).expect("descriptor should serialize");
        assert!(
            descriptor_value.get("body").is_none(),
            "request body must stay behind request-handle ops"
        );
        assert!(
            descriptor_value.get("headers").is_none(),
            "request headers must stay behind request-handle ops"
        );
        assert!(
            descriptor_value.get("request").is_none(),
            "descriptor should not reconstruct the request object before body take"
        );
        assert!(handles.remove(handle).is_none());
        assert!(handles.take_descriptor(handle).is_none());
    }

    #[test]
    fn active_request_context_rejects_removed_or_stale_handles() {
        let mut handles = ActiveRequestContextHandles::default();
        let completion = handles.insert(ActiveRequestContext {
            request_context_handle: 42,
            completion_handle: 0,
            request_id: "runtime-1".to_string(),
            completion_token: "token-1".to_string(),
            wait_until_count: 0,
            wait_until_done_sent: false,
        });
        assert_ne!(completion, 0);
        assert_eq!(handles.get_request("runtime-1"), Some(42));
        assert_eq!(
            handles
                .get_completion(completion)
                .map(|context| context.request_context_handle),
            Some(42)
        );

        let removed = handles
            .remove_handle(42)
            .expect("active request context should remove");
        assert_eq!(removed.request_id, "runtime-1");
        assert_eq!(handles.get_request("runtime-1"), None);
        assert!(handles.get_handle(42).is_none());
        assert!(handles.get_completion(completion).is_none());
        assert!(handles.mark_wait_until_done(completion).is_none());
    }
}

impl RequestExecutionContext {
    pub(crate) fn new(init: RequestExecutionContextInit) -> Self {
        let RequestExecutionContextInit {
            worker_name,
            generation,
            dynamic_bindings,
            dynamic_rpc_bindings,
            service_bindings,
            replacements,
            egress_allow_hosts,
            allow_cache,
            max_outbound_requests,
            dynamic_quota_state,
        } = init;
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
        let service_bindings = service_bindings
            .into_iter()
            .filter_map(|binding| {
                let env_name = binding.binding.trim().to_string();
                let service = binding.service.trim().to_string();
                if env_name.is_empty() || service.is_empty() {
                    return None;
                }
                Some((env_name, service))
            })
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
            service_bindings: Arc::new(service_bindings),
            replacements: Arc::new(replacements),
            egress_allow_hosts: Arc::new(egress_allow_hosts),
            allow_cache,
            max_outbound_requests,
            dynamic_quota_state,
        }
    }
}

pub(crate) struct RequestExecutionContextInit {
    pub(crate) worker_name: String,
    pub(crate) generation: u64,
    pub(crate) dynamic_bindings: Vec<String>,
    pub(crate) dynamic_rpc_bindings: Vec<String>,
    pub(crate) service_bindings: Vec<WorkerServiceBindingPayload>,
    pub(crate) replacements: Vec<(String, String)>,
    pub(crate) egress_allow_hosts: Vec<String>,
    pub(crate) allow_cache: bool,
    pub(crate) max_outbound_requests: Option<u64>,
    pub(crate) dynamic_quota_state: Option<Arc<crate::service::DynamicQuotaState>>,
}

pub(crate) struct RequestSecretContext {
    pub(crate) isolate_id: u64,
    pub(crate) execution: RequestExecutionContext,
    pub(crate) canceled: Arc<AtomicBool>,
    pub(crate) canceled_notify: Arc<Notify>,
}

#[derive(Default)]
pub struct RequestBodyStreams {
    next: u32,
    pub(crate) streams: HashMap<u32, Arc<RequestBodyStream>>,
}

impl RequestBodyStreams {
    pub(crate) fn insert(&mut self, stream: RequestBodyStream) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.streams.contains_key(&self.next) {
                self.streams.insert(self.next, Arc::new(stream));
                return self.next;
            }
        }
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<Arc<RequestBodyStream>> {
        if handle == 0 {
            return None;
        }
        self.streams.remove(&handle)
    }

    pub(crate) fn get(&self, handle: u32) -> Option<Arc<RequestBodyStream>> {
        if handle == 0 {
            return None;
        }
        self.streams.get(&handle).cloned()
    }
}

pub(crate) struct RequestBodyStream {
    pub(crate) receiver: Mutex<RequestBodyReceiver>,
    pub(crate) canceled: AtomicBool,
    pub(crate) canceled_notify: Notify,
    pub(crate) bytes_read: AtomicUsize,
    pub(crate) max_bytes: usize,
}

impl RequestBodyStream {
    pub(crate) fn new(receiver: RequestBodyReceiver, max_bytes: usize) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            canceled: AtomicBool::new(false),
            canceled_notify: Notify::new(),
            bytes_read: AtomicUsize::new(0),
            max_bytes,
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
    pub(crate) body_handle: u32,
    pub(crate) error: String,
}
