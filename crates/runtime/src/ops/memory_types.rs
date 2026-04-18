use super::*;

pub struct MemoryInvokeEvent {
    pub request_frame: Vec<u8>,
    pub caller_worker_name: String,
    pub caller_generation: u64,
    pub caller_isolate_id: u64,
    pub prefer_caller_isolate: bool,
    pub reply: oneshot::Sender<Result<Vec<u8>>>,
}

pub struct MemorySocketSendEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub is_text: bool,
    pub message: Vec<u8>,
}

pub struct MemorySocketCloseEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub code: u16,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemorySocketCloseReplayEvent {
    pub code: u16,
    pub reason: String,
}

pub struct MemorySocketConsumeCloseEvent {
    pub reply: oneshot::Sender<Result<Vec<MemorySocketCloseReplayEvent>>>,
    pub binding: String,
    pub key: String,
    pub handle: String,
}

pub struct MemoryTransportSendStreamEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub chunk: Vec<u8>,
}

pub struct MemoryTransportSendDatagramEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub datagram: Vec<u8>,
}

pub struct MemoryTransportRecvStreamEvent {
    pub reply: oneshot::Sender<Result<TransportRecvEvent>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
}

pub struct MemoryTransportRecvDatagramEvent {
    pub reply: oneshot::Sender<Result<TransportRecvEvent>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
}

pub struct MemoryTransportCloseEvent {
    pub reply: oneshot::Sender<Result<()>>,
    pub handle: String,
    pub binding: String,
    pub key: String,
    pub code: u16,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryTransportCloseReplayEvent {
    pub code: u16,
    pub reason: String,
}

pub struct MemoryTransportConsumeCloseEvent {
    pub reply: oneshot::Sender<Result<Vec<MemoryTransportCloseReplayEvent>>>,
    pub binding: String,
    pub key: String,
    pub handle: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportRecvEvent {
    pub done: bool,
    pub chunk: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeBoundary {
    pub now_ms: u64,
    pub perf_ms: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryInvokeMethodPayload {
    pub(crate) caller_request_id: String,
    pub(crate) worker_name: String,
    pub(crate) binding: String,
    pub(crate) key: String,
    pub(crate) method_name: String,
    #[serde(default)]
    pub(crate) prefer_caller_isolate: bool,
    #[serde(default)]
    pub(crate) args: Vec<u8>,
    pub(crate) request_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryInvokeMethodResult {
    pub(crate) ok: bool,
    pub(crate) value: Vec<u8>,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemorySocketSendPayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) message_kind: String,
    pub(crate) message: Vec<u8>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketSendResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemorySocketClosePayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) code: u16,
    pub(crate) reason: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemorySocketListPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketListResult {
    pub(crate) ok: bool,
    pub(crate) handles: Vec<String>,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemorySocketConsumeClosePayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketReplayClose {
    pub(crate) code: u16,
    pub(crate) reason: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketConsumeCloseResult {
    pub(crate) ok: bool,
    pub(crate) events: Vec<MemorySocketReplayClose>,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketCloseResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryTransportSendStreamPayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) chunk: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryTransportSendDatagramPayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) datagram: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryTransportRecvPayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportSendResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportRecvResult {
    pub(crate) ok: bool,
    pub(crate) done: bool,
    pub(crate) chunk: Vec<u8>,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryTransportClosePayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) code: u16,
    pub(crate) reason: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryTransportListPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportListResult {
    pub(crate) ok: bool,
    pub(crate) handles: Vec<String>,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryTransportConsumeClosePayload {
    pub(crate) request_id: String,
    pub(crate) handle: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportReplayClose {
    pub(crate) code: u16,
    pub(crate) reason: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportConsumeCloseResult {
    pub(crate) ok: bool,
    pub(crate) events: Vec<MemoryTransportReplayClose>,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportCloseResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateSnapshotPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    #[serde(default)]
    pub(crate) keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateGetPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) item_key: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateGetEntry {
    pub(crate) key: String,
    pub(crate) value: Vec<u8>,
    pub(crate) encoding: String,
    pub(crate) version: i64,
    pub(crate) deleted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateGetResult {
    pub(crate) ok: bool,
    pub(crate) record: Option<MemoryStateGetEntry>,
    pub(crate) max_version: i64,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateSnapshotEntry {
    pub(crate) key: String,
    pub(crate) value: Vec<u8>,
    pub(crate) encoding: String,
    pub(crate) version: i64,
    pub(crate) deleted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateSnapshotResult {
    pub(crate) ok: bool,
    pub(crate) entries: Vec<MemoryStateSnapshotEntry>,
    pub(crate) max_version: i64,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryProfileResult {
    pub(crate) ok: bool,
    pub(crate) snapshot: Option<crate::memory::MemoryProfileSnapshot>,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateVersionIfNewerPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) known_version: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateVersionIfNewerResult {
    pub(crate) ok: bool,
    pub(crate) stale: bool,
    pub(crate) max_version: i64,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateBatchMutationPayload {
    pub(crate) key: String,
    pub(crate) value: Vec<u8>,
    pub(crate) encoding: String,
    pub(crate) version: i64,
    pub(crate) deleted: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateReadDependencyPayload {
    pub(crate) key: String,
    pub(crate) version: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateApplyBatchPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) expected_base_version: i64,
    #[serde(default)]
    pub(crate) transactional: bool,
    #[serde(default)]
    pub(crate) reads: Vec<MemoryStateReadDependencyPayload>,
    #[serde(default)]
    pub(crate) list_gate_version: i64,
    pub(crate) mutations: Vec<MemoryStateBatchMutationPayload>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateApplyBlindBatchPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) mutations: Vec<MemoryStateBatchMutationPayload>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateApplyBatchResult {
    pub(crate) ok: bool,
    pub(crate) conflict: bool,
    pub(crate) max_version: i64,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateEnqueueBatchMutationPayload {
    pub(crate) key: String,
    pub(crate) value: Vec<u8>,
    pub(crate) encoding: String,
    pub(crate) deleted: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateEnqueueBatchPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    pub(crate) mutations: Vec<MemoryStateEnqueueBatchMutationPayload>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateEnqueueBatchResult {
    pub(crate) ok: bool,
    pub(crate) submission_id: u64,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateAwaitSubmissionPayload {
    pub(crate) submission_id: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateAwaitSubmissionResult {
    pub(crate) ok: bool,
    pub(crate) pending: bool,
    pub(crate) max_version: i64,
    pub(crate) error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryStateValidateReadsPayload {
    pub(crate) request_id: String,
    #[serde(default)]
    pub(crate) binding: String,
    #[serde(default)]
    pub(crate) key: String,
    #[serde(default)]
    pub(crate) reads: Vec<MemoryStateReadDependencyPayload>,
    #[serde(default)]
    pub(crate) list_gate_version: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryScopePayload {
    pub(crate) request_id: String,
    pub(crate) binding: String,
    pub(crate) key: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryScopeClearPayload {
    pub(crate) request_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryScopeResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}
