//! Runtime commands and events.

use super::*;

#[derive(Clone)]
pub(crate) struct RuntimeFastCommandSender(pub RuntimeCommandSender);

pub(crate) enum RuntimeCommand {
    Admitted {
        command: Box<RuntimeCommand>,
        admission: QueueAdmission,
    },
    InvokeInternal(EnqueueInvokeRequest),
    CheckDeployment {
        worker_name: String,
        temporary: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    Deploy {
        prepared: PreparedWorkerDeployment,
        deployment_id: String,
        expires_at_ms: Option<i64>,
        reply: oneshot::Sender<Result<String>>,
    },
    Undeploy {
        worker_name: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Invoke {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    InvokeStream {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    ServiceBindingFetchStart {
        reply_inbox: crate::ops::RequestControlInbox,
        owner_worker: String,
        owner_generation: u64,
        binding: String,
        target_worker: String,
        request: WorkerInvocation,
        reply_id: String,
        pending_replies: crate::ops::PendingReplies,
        cancellation: crate::ops::PendingReplyCancellation,
    },
    Cancel {
        worker_name: String,
        runtime_request_id: String,
    },
    Stats {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerStats>>,
    },
    DebugDump {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerDebugDump>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<()>>,
    },
    #[cfg(test)]
    ForceFailIsolate {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        reply: oneshot::Sender<bool>,
    },
    OpenWebsocket {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        session_id: String,
        reply: oneshot::Sender<Result<WebSocketOpen>>,
    },
    SendWebsocketFrame {
        worker_name: String,
        session_id: String,
        frame: Vec<u8>,
        is_binary: bool,
        reply: oneshot::Sender<Result<WebSocketFrameOutput>>,
    },
    WaitWebsocketFrame {
        worker_name: String,
        session_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    DrainWebsocketFrame {
        worker_name: String,
        session_id: String,
        reply: oneshot::Sender<Result<Option<WebSocketFrameOutput>>>,
    },
    CloseWebsocket {
        worker_name: String,
        session_id: String,
        close_code: u16,
        close_reason: String,
        reply: oneshot::Sender<Result<()>>,
    },
}
pub(in crate::service) enum RuntimeEvent {
    RequestFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        completion_token: String,
        finished_at: Instant,
        wait_until_count: usize,
        result: Result<WorkerOutput>,
    },
    WaitUntilFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        completion_token: String,
    },
    ResponseStart {
        worker_name: String,
        request_id: String,
        completion_token: String,
        status: u16,
        headers: Vec<(String, String)>,
    },
    ResponseChunk {
        worker_name: String,
        request_id: String,
        completion_token: String,
        chunk: Bytes,
        reply: oneshot::Sender<Result<()>>,
    },
    CacheRevalidate {
        worker_name: String,
        generation: u64,
        payload: CacheRevalidatePayload,
    },
    MemorySocketClose(crate::ops::MemorySocketCloseEvent),
    #[cfg(test)]
    TestAsyncReply(crate::ops::TestAsyncReplyEvent),
    #[cfg(test)]
    TestAsyncReplyComplete {
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        result: Result<String>,
    },
    IsolateReady {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
    },
    IsolateFailed {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    },
    IsolateExited {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
    },
    MemoryOutboxDelivery {
        shard_index: usize,
        claims: Vec<MemoryOutboxClaim>,
        saturated: bool,
        reply: oneshot::Sender<Vec<MemoryOutboxDeliveryOutcome>>,
    },
    MemoryOutboxAckFailed {
        shard_index: usize,
    },
    MemoryOutboxWorkerStats {
        pending_shards: usize,
        in_flight_shards: usize,
        parallelism_limit: usize,
        parallelism_peak: usize,
        duplicate_schedule_coalesced_count: u64,
        task_failure_count: u64,
        shard_requeue_count: u64,
    },
}
