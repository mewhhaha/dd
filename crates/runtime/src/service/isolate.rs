use super::*;
pub(super) enum IsolateCommand {
    Execute {
        runtime_request_id: String,
        completion_token: String,
        worker_name_json: Arc<str>,
        kv_bindings_json: Arc<str>,
        kv_read_cache_config_json: Arc<str>,
        memory_bindings_json: Arc<str>,
        dynamic_bindings_json: Arc<str>,
        dynamic_rpc_bindings_json: Arc<str>,
        dynamic_env_json: Arc<str>,
        dynamic_bindings: Vec<String>,
        dynamic_rpc_bindings: Vec<String>,
        secret_replacements: Vec<(String, String)>,
        egress_allow_hosts: Vec<String>,
        allow_cache: bool,
        max_outbound_requests: Option<u64>,
        dynamic_quota_state: Option<Arc<DynamicQuotaState>>,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        stream_response: bool,
        memory_call: Option<MemoryExecutionCall>,
        host_rpc_call: Option<HostRpcExecutionCall>,
        memory_route: Option<MemoryRoute>,
    },
    Abort {
        runtime_request_id: String,
    },
    DrainDynamicControl,
    PollEventLoop,
    Shutdown,
}

pub(super) struct IsolateEventLoopWaker {
    pub(super) sender: std_mpsc::Sender<IsolateCommand>,
}

impl Wake for IsolateEventLoopWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.sender.send(IsolateCommand::PollEventLoop);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.sender.send(IsolateCommand::PollEventLoop);
    }
}

#[derive(Clone)]
pub(super) enum MemoryExecutionCall {
    Method {
        binding: String,
        key: String,
        name: String,
        args: Vec<u8>,
    },
    Message {
        binding: String,
        key: String,
        handle: String,
        is_text: bool,
        data: Vec<u8>,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    Close {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    TransportDatagram {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    TransportStream {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
    TransportClose {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
        socket_handles: Vec<String>,
        transport_handles: Vec<String>,
    },
}

#[derive(Clone)]
pub(super) struct HostRpcExecutionCall {
    pub(super) target_id: String,
    pub(super) method: String,
    pub(super) args: Vec<u8>,
}

#[derive(Clone)]
pub(super) struct InvokeCancelGuard {
    pub(super) cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
    pub(super) worker_name: String,
    pub(super) runtime_request_id: String,
    pub(super) armed: bool,
}

impl InvokeCancelGuard {
    pub(super) fn new(
        cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
        worker_name: String,
        runtime_request_id: String,
    ) -> Self {
        Self {
            cancel_sender,
            worker_name,
            runtime_request_id,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InvokeCancelGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let _ = self.cancel_sender.send(RuntimeCommand::Cancel {
            worker_name: self.worker_name.clone(),
            runtime_request_id: self.runtime_request_id.clone(),
        });
    }
}
