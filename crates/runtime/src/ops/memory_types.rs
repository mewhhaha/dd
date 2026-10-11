use super::*;

pub(crate) type MemoryCommandHandles = OwnedHandleTable<MemoryCommandHandle>;

pub(crate) struct MemoryCommandHandle {
    pub(crate) request_context_handle: u32,
    pub(crate) namespace: String,
    pub(crate) memory_key: String,
    pub(crate) idempotency_key: String,
}

impl OwnedHandle for MemoryCommandHandle {
    const NAME: &'static str = "memory command handles";

    fn owner(&self) -> u32 {
        self.request_context_handle
    }
}

/// Values handed to JavaScript as bytes, each request within a byte budget.
pub(crate) type MemoryByteHandles = OwnedHandleTable<MemoryByteHandle>;

pub(crate) struct MemoryByteHandle {
    pub(crate) request_context_handle: u32,
    pub(crate) value: Bytes,
}

impl OwnedHandle for MemoryByteHandle {
    const NAME: &'static str = "memory byte handles";
    const UNIT: &'static str = "bytes";

    fn owner(&self) -> u32 {
        self.request_context_handle
    }

    fn cost(&self) -> usize {
        self.value.len()
    }
}

impl MemoryByteHandles {
    /// Takes the bytes behind `handle`, which must be the request's own.
    pub(crate) fn take(&mut self, request_context_handle: u32, handle: u32) -> Result<Bytes> {
        let entry = self
            .get(handle)
            .ok_or_else(|| PlatformError::bad_request("memory byte handle is invalid"))?;
        if entry.request_context_handle != request_context_handle {
            return Err(PlatformError::bad_request(
                "memory byte handle owner mismatch",
            ));
        }
        let entry = self
            .remove(handle)
            .expect("memory byte handle must exist after owner check");
        Ok(entry.value)
    }
}

pub(crate) type MemoryReadHandles = OwnedHandleTable<MemoryReadHandle>;

pub(crate) struct MemoryReadHandle {
    pub(crate) request_context_handle: u32,
    pub(crate) snapshot: Arc<crate::memory::MemorySnapshot>,
}

impl OwnedHandle for MemoryReadHandle {
    const NAME: &'static str = "memory read handles";

    fn owner(&self) -> u32 {
        self.request_context_handle
    }
}

pub(crate) type MemoryBatchHandles = OwnedHandleTable<MemoryBatchHandle>;

impl OwnedHandle for MemoryBatchHandle {
    const NAME: &'static str = "memory batch handles";

    fn owner(&self) -> u32 {
        self.request_context_handle
    }
}

pub(crate) struct MemoryBatchHandle {
    // An in-flight commit retains entity ownership after request cancellation.
    pub(crate) _lease: Option<Arc<crate::memory::MemoryLease>>,
    pub(crate) request_context_handle: u32,
    pub(crate) namespace: String,
    pub(crate) memory_key: String,
    pub(crate) owner_epoch: i64,
    pub(crate) command_handle: u32,
    pub(crate) snapshot: Arc<crate::memory::MemorySnapshot>,
    pub(crate) staged_bytes: usize,
    pub(crate) accepted: bool,
    pub(crate) command_result: Option<MemoryBatchCommandResult>,
    pub(crate) mutations: Vec<MemoryBatchMutation>,
    pub(crate) effects: Vec<MemoryOutboxEffectWrite>,
}

pub(crate) struct MemoryBatchCommandResult {
    pub(crate) value: Vec<u8>,
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
pub struct TimeBoundary {
    pub now_ms: u64,
    pub perf_ms: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryLeaseResult {
    pub(crate) handle: u32,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketListResult {
    pub(crate) ok: bool,
    pub(crate) handles: Vec<String>,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketCloseResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryReadValue {
    pub(crate) value: dd_v8::ToJsBuffer,
    pub(crate) encoding: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryProfileResult {
    pub(crate) ok: bool,
    pub(crate) snapshot: Option<crate::memory::MemoryProfileSnapshot>,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateApplyBatchResult {
    pub(crate) ok: bool,
    pub(crate) applied: bool,
    pub(crate) read_only: bool,
    pub(crate) mutation_count: usize,
    pub(crate) effect_count: usize,
    pub(crate) accepted: bool,
    pub(crate) output_gate_required: bool,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryKeysResult {
    pub(crate) ok: bool,
    pub(crate) keys: Vec<String>,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryGetResult {
    pub(crate) ok: bool,
    pub(crate) record: Option<MemoryReadValue>,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryBeginResult {
    pub(crate) ok: bool,
    pub(crate) storage_failure: bool,
    pub(crate) handle: u32,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryBatchMutationResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryCommandBeginResult {
    pub(crate) ok: bool,
    pub(crate) handle: u32,
    pub(crate) hit: bool,
    pub(crate) value_handle: u32,
    pub(crate) revision: i64,
    pub(crate) error: String,
}
