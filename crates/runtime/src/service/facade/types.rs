//! Public runtime statistics, debug views, streams and WebSocket frames.

use super::*;
use serde::Serialize;

#[derive(Clone)]
pub struct FrontCacheDeployment(pub(super) Arc<AssetCatalogEntry>);

impl FrontCacheDeployment {
    pub fn namespace(&self) -> String {
        format!("front:{}:{}", self.0.worker_name, self.0.deployment_id)
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkerStats {
    pub generation: u64,
    pub public: bool,
    pub temporary: bool,
    pub expires_at_ms: Option<i64>,
    pub queued: usize,
    pub busy: usize,
    pub inflight_total: usize,
    pub wait_until_total: usize,
    pub isolates_total: usize,
    pub spawn_count: u64,
    pub reuse_count: u64,
    pub scale_down_count: u64,
    pub targeted_lane_queued: usize,
    pub memory_lane_queued: usize,
    pub general_lane_queued: usize,
    pub memory_active_shards: usize,
    pub memory_max_shard_depth: usize,
    pub memory_median_shard_depth: usize,
    pub memory_owner_queues: usize,
    pub memory_blocked_owner_queues: usize,
    pub active_memory_leases: usize,
    pub oldest_queue_ms: u64,
    pub queued_bytes: usize,
    pub max_queued_requests_per_worker: usize,
    pub max_global_queued_bytes: usize,
    pub memory_affinity_entries: usize,
    pub stale_memory_affinity_entries: usize,
    pub pending_memory_outbox_shards: usize,
    pub memory_affinity_hit_count: u64,
    pub memory_affinity_miss_no_mapping_count: u64,
    pub memory_affinity_miss_stale_count: u64,
    pub memory_affinity_miss_saturated_count: u64,
    pub memory_least_loaded_fallback_count: u64,
    pub memory_atomic_overflow_dispatch_count: u64,
    pub memory_candidate_rejected_owner_lease_count: u64,
    pub memory_candidate_rejected_isolate_state_count: u64,
    pub memory_candidate_heads_inspected_count: u64,
    pub memory_dispatch_no_ready_candidate_count: u64,
    pub runtime_ready_work_budget_exhausted_count: u64,
    pub runtime_max_ready_work_batch_size: usize,
    pub global_isolate_budget: usize,
    pub global_isolates_total: usize,
    pub global_isolates_starting: usize,
    pub global_internal_rescue_isolates: usize,
    pub global_isolate_slots_available: usize,
    pub scale_up_waiting_pools: usize,
    pub scale_up_budget_denied_count: u64,
    pub memory_outbox_claim_batch_count: u64,
    pub memory_outbox_claim_row_count: u64,
    pub memory_outbox_saturated_batch_count: u64,
    pub memory_outbox_delivery_success_count: u64,
    pub memory_outbox_delivery_retry_count: u64,
    pub memory_outbox_terminal_drop_count: u64,
    pub memory_outbox_ack_failure_count: u64,
    pub memory_outbox_channel_full_count: u64,
    pub memory_outbox_reschedule_count: u64,
    pub memory_outbox_worker_pending_shards: usize,
    pub memory_outbox_worker_in_flight_shards: usize,
    pub memory_outbox_worker_parallelism_limit: usize,
    pub memory_outbox_worker_parallelism_peak: usize,
    pub memory_outbox_duplicate_schedule_coalesced_count: u64,
    pub memory_outbox_task_failure_count: u64,
    pub memory_outbox_shard_requeue_count: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeRestoreFailure {
    pub worker: Option<String>,
    pub source: String,
    pub error: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeWorkerStatus {
    pub name: String,
    /// Backlog-based outbox lag proxy. A value of zero means the coordinator
    /// and outbox workers have no scheduled or in-flight shards.
    pub outbox_lag_shards: usize,
    #[serde(flatten)]
    pub stats: WorkerStats,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeAdminSnapshot {
    pub worker_schedulers: usize,
    pub active_deployments: usize,
    pub workers: Vec<RuntimeWorkerStatus>,
    pub restore_failures: Vec<RuntimeRestoreFailure>,
    pub readiness: RuntimeReadiness,
    pub storage_retry_count: u64,
    pub state_storage: ::storage::state::StatePerformanceSnapshot,
    pub memory_snapshot_cache_hits: u64,
    pub memory_snapshot_cache_misses: u64,
    pub memory_snapshot_cache_evictions: u64,
    pub memory_outbox_claimed_bytes: usize,
    pub memory_outbox_max_claimed_bytes: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeReadiness {
    pub ready: bool,
    pub runtime_ready: bool,
    pub migrations_ready: bool,
    pub storage_ready: bool,
    pub worker_restoration_ready: bool,
    pub restore_failure_count: usize,
    pub failed_components: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RuntimeCheckpointResult {
    pub state_shards: usize,
    pub cache: bool,
    pub control: bool,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerDebugDump {
    pub generation: u64,
    pub queued: usize,
    pub isolates: Vec<WorkerDebugIsolate>,
    pub queued_requests: Vec<WorkerDebugRequest>,
    pub memory_scheduler: MemorySchedulerDebug,
    pub memory_outbox: MemoryOutboxDebug,
}

#[derive(Debug, Clone, Default)]
pub struct MemorySchedulerDebug {
    pub queued: usize,
    pub active_leases: usize,
    pub active_shards: usize,
    pub max_shard_depth: usize,
    pub median_shard_depth: usize,
    pub owner_queues: usize,
    pub blocked_owner_queues: usize,
    pub affinity_entries: usize,
    pub stale_affinity_entries: usize,
    pub oldest_queue_ms: u64,
    pub queued_bytes: usize,
    pub max_queued_requests_per_worker: usize,
    pub max_global_queued_bytes: usize,
    pub top_shards: Vec<MemoryShardDebug>,
    pub affinity_hit_count: u64,
    pub affinity_miss_no_mapping_count: u64,
    pub affinity_miss_stale_count: u64,
    pub affinity_miss_saturated_count: u64,
    pub least_loaded_fallback_count: u64,
    pub atomic_overflow_dispatch_count: u64,
    pub candidate_rejected_owner_lease_count: u64,
    pub candidate_rejected_isolate_state_count: u64,
    pub candidate_heads_inspected_count: u64,
    pub dispatch_no_ready_candidate_count: u64,
    pub runtime_ready_work_budget_exhausted_count: u64,
    pub runtime_max_ready_work_batch_size: usize,
    pub global_isolate_budget: usize,
    pub global_isolates_total: usize,
    pub global_isolates_starting: usize,
    pub global_internal_rescue_isolates: usize,
    pub global_isolate_slots_available: usize,
    pub scale_up_waiting_pools: usize,
    pub scale_up_budget_denied_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryShardDebug {
    pub shard_index: usize,
    pub queued: usize,
    pub ready_owners: usize,
    pub blocked_owners: usize,
    pub affinity_isolate_id: Option<u64>,
    pub affinity_stale: bool,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryOutboxDebug {
    pub pending_scheduled_shards: usize,
    pub claim_batch_count: u64,
    pub claim_row_count: u64,
    pub saturated_batch_count: u64,
    pub delivery_success_count: u64,
    pub delivery_retry_count: u64,
    pub terminal_drop_count: u64,
    pub ack_failure_count: u64,
    pub channel_full_count: u64,
    pub reschedule_count: u64,
    pub worker_pending_shards: usize,
    pub worker_in_flight_shards: usize,
    pub worker_parallelism_limit: usize,
    pub worker_parallelism_peak: usize,
    pub duplicate_schedule_coalesced_count: u64,
    pub task_failure_count: u64,
    pub shard_requeue_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerDebugIsolate {
    pub id: u64,
    pub inflight_count: usize,
    pub pending_wait_until: usize,
    pub active_websocket_sessions: usize,
    pub pending_requests: Vec<WorkerDebugRequest>,
}

#[derive(Debug, Clone, Default)]
pub struct WorkerDebugRequest {
    pub runtime_request_id: String,
    pub user_request_id: String,
    pub method: String,
    pub url: String,
    pub memory_key: Option<String>,
    pub target_isolate_id: Option<u64>,
    pub internal_origin: bool,
    pub reply_kind: String,
}

#[derive(Debug)]
pub struct WorkerStreamOutput {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: WorkerStreamBody,
}

pub struct WorkerStreamBody {
    receiver: mpsc::Receiver<Bytes>,
    completion: Option<oneshot::Receiver<Result<()>>>,
    cancel_guard: Option<InvokeCancelGuard>,
}

impl std::fmt::Debug for WorkerStreamBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerStreamBody")
            .field("has_cancel_guard", &self.cancel_guard.is_some())
            .finish_non_exhaustive()
    }
}

impl WorkerStreamBody {
    pub(in crate::service) fn new(
        receiver: mpsc::Receiver<Bytes>,
        completion: oneshot::Receiver<Result<()>>,
    ) -> Self {
        Self {
            receiver,
            completion: Some(completion),
            cancel_guard: None,
        }
    }

    pub(super) fn attach_cancel_guard(&mut self, cancel_guard: InvokeCancelGuard) {
        self.cancel_guard = Some(cancel_guard);
    }

    pub async fn recv(&mut self) -> Option<Result<Bytes>> {
        if let Some(chunk) = self.receiver.recv().await {
            return Some(Ok(chunk));
        }
        let completion = match self.completion.take() {
            Some(completion) => completion.await.unwrap_or_else(|_| {
                Err(PlatformError::internal("stream completion channel closed"))
            }),
            None => Ok(()),
        };
        self.disarm_cancel_guard();
        completion.err().map(Err)
    }

    fn disarm_cancel_guard(&mut self) {
        if let Some(cancel_guard) = self.cancel_guard.as_mut() {
            cancel_guard.disarm();
        }
        self.cancel_guard = None;
    }
}

#[derive(Debug)]
pub struct WebSocketOpen {
    pub session_id: String,
    pub worker_name: String,
    pub output: WorkerOutput,
}

/// Keeps queued and in-flight WebSocket bytes charged until the consumer is done.
#[derive(Debug, Default)]
pub struct WebSocketFrameLease {
    pub(in crate::service) _permits: Option<(
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
    )>,
}

#[derive(Debug)]
pub struct WebSocketFrameOutput {
    pub output: WorkerOutput,
    pub(in crate::service) lease: WebSocketFrameLease,
}

impl From<WorkerOutput> for WebSocketFrameOutput {
    fn from(output: WorkerOutput) -> Self {
        Self {
            output,
            lease: WebSocketFrameLease::default(),
        }
    }
}

impl std::ops::Deref for WebSocketFrameOutput {
    type Target = WorkerOutput;

    fn deref(&self) -> &Self::Target {
        &self.output
    }
}

impl WebSocketFrameOutput {
    /// Retain the returned lease until the transport finishes sending the body.
    pub fn into_parts(self) -> (WorkerOutput, WebSocketFrameLease) {
        (self.output, self.lease)
    }
}

pub struct PublicRouteAssetResolution {
    pub public_worker: bool,
    pub generation: Option<u64>,
    pub asset: Option<AssetResponse>,
}

pub type InvokeRequestBodyReceiver = mpsc::Receiver<std::result::Result<Bytes, String>>;
