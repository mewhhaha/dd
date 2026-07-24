use super::*;

#[derive(Default)]
pub struct MemoryCommandHandles {
    next: u32,
    handles: HashMap<u32, MemoryCommandHandle>,
    owner_counts: HashMap<u32, usize>,
}

impl MemoryCommandHandles {
    pub(crate) fn insert(
        &mut self,
        handle: MemoryCommandHandle,
        max_owner_handles: usize,
    ) -> Result<u32> {
        let request_context_handle = handle.request_context_handle;
        let next_owner_count = self
            .owner_counts
            .get(&request_context_handle)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        if next_owner_count > max_owner_handles {
            return Err(PlatformError::bad_request(format!(
                "memory command handles exceeded {max_owner_handles} active handles"
            )));
        }
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.handles.contains_key(&self.next) {
                self.handles.insert(self.next, handle);
                self.owner_counts
                    .insert(request_context_handle, next_owner_count);
                return Ok(self.next);
            }
        }
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<MemoryCommandHandle> {
        let removed = self.handles.remove(&handle)?;
        self.decrement_owner_count(removed.request_context_handle);
        Some(removed)
    }

    pub(crate) fn get(&self, handle: u32) -> Option<&MemoryCommandHandle> {
        self.handles.get(&handle)
    }

    pub(crate) fn clear_owner(&mut self, request_context_handle: u32) {
        self.handles
            .retain(|_, handle| handle.request_context_handle != request_context_handle);
        self.owner_counts.remove(&request_context_handle);
    }

    #[cfg(test)]
    pub(crate) fn owner_count(&self, request_context_handle: u32) -> usize {
        self.owner_counts
            .get(&request_context_handle)
            .copied()
            .unwrap_or(0)
    }

    fn decrement_owner_count(&mut self, request_context_handle: u32) {
        let Some(current) = self.owner_counts.get_mut(&request_context_handle) else {
            return;
        };
        *current = current.saturating_sub(1);
        if *current == 0 {
            self.owner_counts.remove(&request_context_handle);
        }
    }
}

pub(crate) struct MemoryCommandHandle {
    pub(crate) request_context_handle: u32,
    pub(crate) namespace: String,
    pub(crate) memory_key: String,
    pub(crate) idempotency_key: String,
}

#[derive(Default)]
pub struct MemoryByteHandles {
    next: u32,
    handles: HashMap<u32, MemoryByteHandle>,
    owner_bytes: HashMap<u32, usize>,
}

impl MemoryByteHandles {
    pub(crate) fn insert(
        &mut self,
        request_context_handle: u32,
        value: Bytes,
        max_owner_bytes: usize,
    ) -> Result<u32> {
        let next_owner_bytes = self
            .owner_bytes
            .get(&request_context_handle)
            .copied()
            .unwrap_or(0)
            .saturating_add(value.len());
        if next_owner_bytes > max_owner_bytes {
            return Err(PlatformError::bad_request(format!(
                "memory byte handles exceeded {max_owner_bytes} bytes"
            )));
        }
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.handles.contains_key(&self.next) {
                self.handles.insert(
                    self.next,
                    MemoryByteHandle {
                        request_context_handle,
                        value,
                    },
                );
                self.owner_bytes
                    .insert(request_context_handle, next_owner_bytes);
                return Ok(self.next);
            }
        }
    }

    pub(crate) fn take(&mut self, request_context_handle: u32, handle: u32) -> Result<Bytes> {
        let entry = self
            .handles
            .get(&handle)
            .ok_or_else(|| PlatformError::bad_request("memory byte handle is invalid"))?;
        if entry.request_context_handle != request_context_handle {
            return Err(PlatformError::bad_request(
                "memory byte handle owner mismatch",
            ));
        }
        let entry = self
            .handles
            .remove(&handle)
            .expect("memory byte handle must exist after owner check");
        self.decrement_owner_bytes(request_context_handle, entry.value.len());
        Ok(entry.value)
    }

    pub(crate) fn clear_owner(&mut self, request_context_handle: u32) {
        self.handles
            .retain(|_, handle| handle.request_context_handle != request_context_handle);
        self.owner_bytes.remove(&request_context_handle);
    }

    #[cfg(test)]
    pub(crate) fn owner_bytes(&self, request_context_handle: u32) -> usize {
        self.owner_bytes
            .get(&request_context_handle)
            .copied()
            .unwrap_or(0)
    }

    fn decrement_owner_bytes(&mut self, request_context_handle: u32, bytes: usize) {
        let Some(current) = self.owner_bytes.get_mut(&request_context_handle) else {
            return;
        };
        *current = current.saturating_sub(bytes);
        if *current == 0 {
            self.owner_bytes.remove(&request_context_handle);
        }
    }
}

pub(crate) struct MemoryByteHandle {
    pub(crate) request_context_handle: u32,
    pub(crate) value: Bytes,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryBytesPutResult {
    pub(crate) ok: bool,
    pub(crate) handle: u32,
    pub(crate) error: String,
}

#[derive(Default)]
pub struct MemoryBatchHandles {
    next: u32,
    handles: HashMap<u32, MemoryBatchHandle>,
    owner_counts: HashMap<u32, usize>,
}

impl MemoryBatchHandles {
    pub(crate) fn insert(
        &mut self,
        handle: MemoryBatchHandle,
        max_owner_handles: usize,
    ) -> Result<u32> {
        let request_context_handle = handle.request_context_handle;
        let next_owner_count = self
            .owner_counts
            .get(&request_context_handle)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        if next_owner_count > max_owner_handles {
            return Err(PlatformError::bad_request(format!(
                "memory batch handles exceeded {max_owner_handles} active handles"
            )));
        }
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next == 0 {
                continue;
            }
            if !self.handles.contains_key(&self.next) {
                self.handles.insert(self.next, handle);
                self.owner_counts
                    .insert(request_context_handle, next_owner_count);
                return Ok(self.next);
            }
        }
    }

    pub(crate) fn get(&self, handle: u32) -> Option<&MemoryBatchHandle> {
        self.handles.get(&handle)
    }

    pub(crate) fn get_mut(&mut self, handle: u32) -> Option<&mut MemoryBatchHandle> {
        self.handles.get_mut(&handle)
    }

    pub(crate) fn remove(&mut self, handle: u32) -> Option<MemoryBatchHandle> {
        let removed = self.handles.remove(&handle)?;
        self.decrement_owner_count(removed.request_context_handle);
        Some(removed)
    }

    pub(crate) fn clear_owner(&mut self, request_context_handle: u32) {
        self.handles
            .retain(|_, handle| handle.request_context_handle != request_context_handle);
        self.owner_counts.remove(&request_context_handle);
    }

    #[cfg(test)]
    pub(crate) fn owner_count(&self, request_context_handle: u32) -> usize {
        self.owner_counts
            .get(&request_context_handle)
            .copied()
            .unwrap_or(0)
    }

    fn decrement_owner_count(&mut self, request_context_handle: u32) {
        let Some(current) = self.owner_counts.get_mut(&request_context_handle) else {
            return;
        };
        *current = current.saturating_sub(1);
        if *current == 0 {
            self.owner_counts.remove(&request_context_handle);
        }
    }
}

pub(crate) struct MemoryBatchHandle {
    pub(crate) request_context_handle: u32,
    pub(crate) namespace: String,
    pub(crate) memory_key: String,
    pub(crate) owner_epoch: i64,
    pub(crate) command_handle: u32,
    pub(crate) staged_bytes: usize,
    pub(crate) accepted: bool,
    pub(crate) command_result: Option<MemoryBatchCommandResult>,
    pub(crate) mutations: Vec<MemoryBatchMutation>,
    pub(crate) effects: Vec<MemoryOutboxEffectWrite>,
}

pub(crate) struct MemoryBatchCommandResult {
    pub(crate) value: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryDirectMutationInput {
    pub(crate) key: String,
    pub(crate) value_handle: u32,
    pub(crate) encoding: String,
    pub(crate) deleted: bool,
}

pub struct MemoryInvokeEvent {
    pub request_frame: Vec<u8>,
    pub created_at: Instant,
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
pub struct TimeBoundary {
    pub now_ms: u64,
    pub perf_ms: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryInvokeMethodResult {
    pub(crate) ok: bool,
    pub(crate) value_handle: u32,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketSendResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemorySocketListResult {
    pub(crate) ok: bool,
    pub(crate) handles: Vec<String>,
    pub(crate) error: String,
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

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportSendResult {
    pub(crate) ok: bool,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryTransportListResult {
    pub(crate) ok: bool,
    pub(crate) handles: Vec<String>,
    pub(crate) error: String,
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

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateGetEntry {
    pub(crate) key: String,
    pub(crate) value_handle: u32,
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
    pub(crate) value_handle: u32,
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

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateVersionIfNewerResult {
    pub(crate) ok: bool,
    pub(crate) stale: bool,
    pub(crate) max_version: i64,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryStateApplyBatchResult {
    pub(crate) ok: bool,
    pub(crate) applied: bool,
    pub(crate) read_only: bool,
    pub(crate) max_version: i64,
    pub(crate) mutation_count: usize,
    pub(crate) effect_count: usize,
    pub(crate) accepted: bool,
    pub(crate) output_gate_required: bool,
    pub(crate) mutations: Vec<MemoryStateSnapshotEntry>,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryBatchListOverlayResult {
    pub(crate) ok: bool,
    pub(crate) entries: Vec<MemoryStateSnapshotEntry>,
    pub(crate) mutation_count: usize,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryBatchGetMutationResult {
    pub(crate) ok: bool,
    pub(crate) record: Option<MemoryStateSnapshotEntry>,
    pub(crate) mutation_count: usize,
    pub(crate) effect_count: usize,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryBatchBeginResult {
    pub(crate) ok: bool,
    pub(crate) handle: u32,
    pub(crate) error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MemoryBatchMutationResult {
    pub(crate) ok: bool,
    pub(crate) record: Option<MemoryStateSnapshotEntry>,
    pub(crate) mutation_count: usize,
    pub(crate) effect_count: usize,
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
