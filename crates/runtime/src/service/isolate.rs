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
        request_context: RequestExecutionContext,
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

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
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
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        socket_handles: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transport_handles: Vec<String>,
    },
    Close {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        socket_handles: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transport_handles: Vec<String>,
    },
    #[serde(rename = "transport_datagram")]
    TransportDatagram {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        socket_handles: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transport_handles: Vec<String>,
    },
    #[serde(rename = "transport_stream")]
    TransportStream {
        binding: String,
        key: String,
        handle: String,
        data: Vec<u8>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        socket_handles: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transport_handles: Vec<String>,
    },
    #[serde(rename = "transport_close")]
    TransportClose {
        binding: String,
        key: String,
        handle: String,
        code: u16,
        reason: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        socket_handles: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        transport_handles: Vec<String>,
    },
}

#[derive(serde::Serialize)]
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
