use super::*;
use serde::{Deserialize, Serialize};
pub(super) struct WorkerManager {
    pub(super) config: RuntimeConfig,
    pub(super) storage: RuntimeStorageConfig,
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) runtime_fast_sender: mpsc::Sender<RuntimeCommand>,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) memory_outbox_drain_sender: MemoryOutboxDrainSender,
    pub(super) cache_store: CacheStore,
    pub(super) asset_catalog: AssetCatalog,
    pub(super) workers: HashMap<String, WorkerEntry>,
    pub(super) queue_counters: RuntimeQueueCounters,
    pub(super) next_queue_expiry_at: Option<Instant>,
    pub(super) pre_canceled: HashMap<String, HashMap<String, Instant>>,
    pub(super) stream_registrations: HashMap<String, StreamRegistration>,
    pub(super) revalidation_keys: HashSet<String>,
    pub(super) revalidation_requests: HashMap<String, String>,
    pub(super) websocket_sessions: HashMap<String, WorkerWebSocketSession>,
    pub(super) websocket_handle_index: HashMap<String, String>,
    pub(super) websocket_open_handles: HashMap<String, HashSet<String>>,
    pub(super) open_handle_registry: crate::ops::MemoryOpenHandleRegistry,
    pub(super) websocket_pending_closes: HashMap<String, HashMap<String, Vec<SocketCloseEvent>>>,
    pub(super) websocket_outbound_frames: HashMap<String, VecDeque<WebSocketOutboundFrame>>,
    pub(super) websocket_close_signals: HashMap<String, SocketCloseEvent>,
    pub(super) websocket_frame_waiters: HashMap<String, Vec<oneshot::Sender<Result<()>>>>,
    pub(super) websocket_pending_frame_replies: HashMap<String, Vec<WebSocketFrameReply>>,
    pub(super) websocket_open_waiters: HashMap<String, oneshot::Sender<Result<WebSocketOpen>>>,
    pub(super) transport_sessions: HashMap<String, WorkerTransportSession>,
    pub(super) transport_handle_index: HashMap<String, String>,
    pub(super) transport_open_handles: HashMap<String, HashSet<String>>,
    pub(super) transport_pending_closes: HashMap<String, HashMap<String, Vec<TransportCloseEvent>>>,
    pub(super) transport_open_channels: HashMap<String, TransportOpenChannels>,
    pub(super) transport_open_waiters: HashMap<String, oneshot::Sender<Result<TransportOpen>>>,
    pub(super) dynamic_worker_handles: HashMap<String, DynamicWorkerHandle>,
    pub(super) dynamic_worker_ids: HashMap<DynamicWorkerIdKey, HashMap<String, String>>,
    pub(super) host_rpc_providers: HashMap<String, HostRpcProvider>,
    pub(super) dynamic_profile: crate::ops::DynamicProfile,
    pub(super) validated_worker_sources: HashSet<[u8; 32]>,
    pub(super) runtime_batch_depth: usize,
    pub(super) pending_dispatches: HashSet<(String, u64)>,
    pub(super) pending_cleanup_workers: HashSet<String>,
    pub(super) pending_memory_outbox_shards: HashSet<usize>,
    pub(super) scale_up_requests: VecDeque<(String, u64)>,
    pub(super) scale_up_request_members: HashSet<(String, u64)>,
    pub(super) global_isolate_slots_used: usize,
    pub(super) global_isolates_starting: usize,
    pub(super) stats: RuntimeManagerStats,
    pub(super) next_generation: u64,
    pub(super) next_isolate_id: u64,
    pub(super) next_memory_entity_epoch: u64,
}

pub(super) struct WorkerManagerInit {
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) memory_outbox_drain_sender: MemoryOutboxDrainSender,
    pub(super) cache_store: CacheStore,
    pub(super) config: RuntimeConfig,
    pub(super) storage: RuntimeStorageConfig,
    pub(super) runtime_fast_sender: mpsc::Sender<RuntimeCommand>,
    pub(super) asset_catalog: AssetCatalog,
}

pub(crate) struct PreparedWorkerDeployment {
    pub(super) worker_name: String,
    pub(super) source: String,
    pub(super) config: DeployConfig,
    pub(super) assets: Vec<DeployAsset>,
    pub(super) server_modules: Vec<DeployServerModule>,
    pub(super) asset_headers: Option<String>,
    pub(super) compiled_assets: Arc<AssetBundle>,
    pub(super) bindings: DeployBindings,
}

#[derive(Clone, Copy, Default)]
pub(super) struct RuntimeQueueCounters {
    pub(super) requests: usize,
    pub(super) bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RuntimeManagerStats {
    pub(super) ready_work_budget_exhausted_count: u64,
    pub(super) max_ready_work_batch_size: usize,
    pub(super) memory_outbox_claim_batch_count: u64,
    pub(super) memory_outbox_claim_row_count: u64,
    pub(super) memory_outbox_saturated_batch_count: u64,
    pub(super) memory_outbox_delivery_success_count: u64,
    pub(super) memory_outbox_delivery_retry_count: u64,
    pub(super) memory_outbox_terminal_drop_count: u64,
    pub(super) memory_outbox_ack_failure_count: u64,
    pub(super) memory_outbox_channel_full_count: u64,
    pub(super) memory_outbox_reschedule_count: u64,
    pub(super) memory_outbox_worker_pending_shards: usize,
    pub(super) memory_outbox_worker_in_flight_shards: usize,
    pub(super) memory_outbox_worker_parallelism_limit: usize,
    pub(super) memory_outbox_worker_parallelism_peak: usize,
    pub(super) memory_outbox_duplicate_schedule_coalesced_count: u64,
    pub(super) memory_outbox_task_failure_count: u64,
    pub(super) memory_outbox_shard_requeue_count: u64,
    pub(super) scale_up_budget_denied_count: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct DynamicWorkerIdKey {
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
}

#[derive(Clone)]
pub(super) struct DynamicWorkerHandle {
    pub(super) id: String,
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
    pub(super) worker_name: String,
    pub(super) worker_generation: u64,
    pub(super) module_graph_id: Option<String>,
    pub(super) timeout: u64,
    pub(super) policy: ValidatedDynamicWorkerPolicy,
    pub(super) host_rpc_provider_ids: Vec<String>,
    pub(super) preferred_isolate_id: Option<u64>,
    pub(super) quota_state: Arc<DynamicQuotaState>,
}

#[derive(Default)]
pub(crate) struct DynamicQuotaState {
    pub(crate) inflight: AtomicUsize,
    pub(crate) outbound_requests: AtomicU64,
    pub(crate) total_request_bytes: AtomicU64,
    pub(crate) total_response_bytes: AtomicU64,
    pub(crate) egress_deny_count: AtomicU64,
    pub(crate) rpc_deny_count: AtomicU64,
    pub(crate) quota_kill_count: AtomicU64,
    pub(crate) upgrade_deny_count: AtomicU64,
}

pub(super) struct HostRpcProvider {
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) owner_isolate_id: u64,
    pub(super) target_id: String,
    pub(super) methods: HashSet<String>,
}

pub(super) enum TargetedHostRpcReply {
    Dynamic {
        reply_id: String,
        pending_replies: crate::ops::DynamicPendingReplies,
    },
    Test {
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        success_value: String,
    },
}

#[derive(Debug, Clone)]
pub(super) struct DynamicTimeoutDiagnostic {
    pub(super) stage: &'static str,
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
    pub(super) handle: String,
    pub(super) target_worker: String,
    pub(super) target_isolate_id: Option<u64>,
    pub(super) target_generation: Option<u64>,
    pub(super) provider_id: Option<String>,
    pub(super) provider_owner_isolate_id: Option<u64>,
    pub(super) provider_target_id: Option<String>,
    pub(super) timeout_ms: u64,
}

pub(super) struct WorkerWebSocketSession {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) owner_isolate_id: u64,
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) handle: String,
}

#[derive(Clone)]
pub(super) struct SocketCloseEvent {
    pub(super) code: u16,
    pub(super) reason: String,
}

#[derive(Clone)]
pub(super) struct WebSocketOutboundFrame {
    pub(super) is_binary: bool,
    pub(super) payload: Vec<u8>,
}

pub(super) struct WebSocketFrameReply {
    pub(super) output: WorkerOutput,
    pub(super) reply: oneshot::Sender<Result<WorkerOutput>>,
}

pub(super) struct WorkerTransportSession {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) owner_isolate_id: u64,
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) handle: String,
    pub(super) stream_sender: mpsc::Sender<Vec<u8>>,
    pub(super) datagram_sender: mpsc::Sender<Vec<u8>>,
}

pub(super) struct TransportOpenChannels {
    pub(super) stream_sender: mpsc::Sender<Vec<u8>>,
    pub(super) datagram_sender: mpsc::Sender<Vec<u8>>,
}

#[derive(Clone)]
pub(super) struct TransportCloseEvent {
    pub(super) code: u16,
    pub(super) reason: String,
}

pub(super) struct StreamRegistration {
    pub(super) worker_name: String,
    pub(super) completion_token: Option<String>,
    pub(super) ready: Option<oneshot::Sender<Result<WorkerStreamOutput>>>,
    pub(super) body_sender: mpsc::Sender<Result<Bytes>>,
    pub(super) body_receiver: Option<mpsc::Receiver<Result<Bytes>>>,
    pub(super) started: bool,
    pub(super) bytes_sent: usize,
    pub(super) max_bytes: usize,
}

pub(super) struct WorkerEntry {
    pub(super) current_generation: u64,
    pub(super) pools: HashMap<u64, WorkerPool>,
}

#[derive(Clone, Debug)]
pub(super) struct InternalTraceDestination {
    pub(super) worker: String,
    pub(super) path: String,
}

pub(super) struct WorkerPool {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) deployment_id: String,
    pub(super) internal_trace: Option<InternalTraceDestination>,
    pub(super) is_public: bool,
    pub(super) expires_at_ms: Option<i64>,
    pub(super) snapshot: &'static [u8],
    pub(super) snapshot_preloaded: bool,
    pub(super) source: crate::ops::WorkerSource,
    pub(super) memory_bindings: Vec<String>,
    pub(super) dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    pub(super) deployment_config: Arc<crate::ops::WorkerDeploymentPayload>,
    pub(super) request_context: RequestExecutionContext,
    pub(super) dynamic_child_policy: Option<ValidatedDynamicWorkerPolicy>,
    pub(super) dynamic_quota_state: Option<Arc<DynamicQuotaState>>,
    pub(super) strict_request_isolation: bool,
    pub(super) memory_entity_leases: HashMap<String, MemoryEntityLease>,
    pub(super) memory_shard_affinity: HashMap<usize, u64>,
    pub(super) queue: PendingInvokeQueue,
    pub(super) isolates: Vec<IsolateHandle>,
    pub(super) isolate_indices: HashMap<u64, usize>,
    pub(super) stats: PoolStats,
    pub(super) queue_warn_level: usize,
}

impl WorkerPool {
    pub(super) fn push_isolate(&mut self, isolate: IsolateHandle) {
        let isolate_id = isolate.id;
        self.isolate_indices.insert(isolate_id, self.isolates.len());
        self.isolates.push(isolate);
    }

    pub(super) fn isolate_idx(&self, isolate_id: u64) -> Option<usize> {
        self.isolate_indices.get(&isolate_id).copied()
    }

    pub(super) fn swap_remove_isolate(&mut self, isolate_idx: usize) -> Option<IsolateHandle> {
        if isolate_idx >= self.isolates.len() {
            return None;
        }
        let removed = self.isolates.swap_remove(isolate_idx);
        self.isolate_indices.remove(&removed.id);
        if isolate_idx < self.isolates.len() {
            self.isolate_indices
                .insert(self.isolates[isolate_idx].id, isolate_idx);
        }
        Some(removed)
    }

    pub(super) fn clear_isolate_indices(&mut self) {
        self.isolate_indices.clear();
    }

    pub(super) fn acquire_memory_entity_lease(
        &mut self,
        owner_key: String,
        owner_isolate_id: u64,
        epoch: u64,
    ) -> MemoryEntityLease {
        let lease = MemoryEntityLease {
            owner_key: owner_key.clone(),
            epoch: epoch.max(1),
            owner_isolate_id,
        };
        self.memory_entity_leases.insert(owner_key, lease.clone());
        lease
    }

    pub(super) fn release_memory_entity_lease(&mut self, lease: &MemoryEntityLease) -> bool {
        let should_release = self
            .memory_entity_leases
            .get(&lease.owner_key)
            .map(|current| current.matches(lease))
            .unwrap_or(false);
        if should_release {
            self.memory_entity_leases.remove(&lease.owner_key);
            self.queue.mark_memory_owner_ready(&lease.owner_key);
        }
        should_release
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum PendingQueueLane {
    TargetedNested,
    Targeted,
    Memory,
    General,
}

impl PendingQueueLane {
    pub(super) const ALL: [Self; 4] = [
        Self::TargetedNested,
        Self::Targeted,
        Self::Memory,
        Self::General,
    ];
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PendingQueueKey {
    lane: PendingQueueLane,
    sequence: u64,
    memory_shard_index: Option<usize>,
    memory_owner_key: Option<String>,
}

pub(super) struct MemoryOwnerQueue {
    pending: BTreeMap<u64, PendingInvoke>,
}

#[derive(Default)]
pub(super) struct MemoryShardQueue {
    owners: BTreeMap<String, MemoryOwnerQueue>,
    ready: VecDeque<String>,
    ready_membership: HashSet<String>,
    blocked: HashSet<String>,
}

impl MemoryShardQueue {
    fn len(&self) -> usize {
        self.owners
            .values()
            .map(|owner| owner.pending.len())
            .sum::<usize>()
    }

    fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }

    fn owner_count(&self) -> usize {
        self.owners.len()
    }

    fn blocked_owner_count(
        &self,
        memory_entity_leases: &HashMap<String, MemoryEntityLease>,
    ) -> usize {
        self.owners
            .keys()
            .filter(|owner_key| {
                self.blocked.contains(*owner_key) || memory_entity_leases.contains_key(*owner_key)
            })
            .count()
    }

    fn iter(&self) -> impl Iterator<Item = &PendingInvoke> {
        self.owners
            .values()
            .flat_map(|owner| owner.pending.values())
    }

    fn insert(
        &mut self,
        owner_key: String,
        sequence: u64,
        pending: PendingInvoke,
    ) -> Option<PendingInvoke> {
        if !self.owners.contains_key(&owner_key) {
            self.owners.insert(
                owner_key.clone(),
                MemoryOwnerQueue {
                    pending: BTreeMap::new(),
                },
            );
            self.ready_owner(owner_key.clone());
        }
        let owner = self
            .owners
            .get_mut(&owner_key)
            .expect("owner queue should exist after insertion");
        owner.pending.insert(sequence, pending)
    }

    fn remove(&mut self, owner_key: &str, sequence: u64) -> Option<PendingInvoke> {
        let owner = self.owners.get_mut(owner_key)?;
        let pending = owner.pending.remove(&sequence);
        if owner.pending.is_empty() {
            self.owners.remove(owner_key);
            self.ready_membership.remove(owner_key);
            self.blocked.remove(owner_key);
        }
        pending
    }

    fn find_round_robin_owner_head_map<T>(
        &mut self,
        shard_index: usize,
        map: &mut impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<(PendingQueueKey, u64, T)> {
        let mut inspected = 0usize;
        let ready_len = self.ready.len();
        while inspected < ready_len {
            inspected = inspected.saturating_add(1);
            let owner_key = self.ready.front().cloned()?;
            if !self.ready_membership.contains(&owner_key)
                || self.blocked.contains(&owner_key)
                || !self.owners.contains_key(&owner_key)
            {
                self.ready.pop_front();
                self.ready_membership.remove(&owner_key);
                continue;
            }
            let Some(owner) = self.owners.get(&owner_key) else {
                self.ready.pop_front();
                self.ready_membership.remove(&owner_key);
                continue;
            };
            let Some((sequence, pending)) = owner.pending.first_key_value() else {
                self.ready.pop_front();
                self.ready_membership.remove(&owner_key);
                continue;
            };
            let key = PendingQueueKey {
                lane: PendingQueueLane::Memory,
                sequence: *sequence,
                memory_shard_index: Some(shard_index),
                memory_owner_key: Some(owner_key.clone()),
            };
            let Some(value) = map(key.clone(), pending) else {
                continue;
            };
            return Some((key, *sequence, value));
        }
        None
    }

    fn advance_owner_cursor(&mut self, owner_key: &str) {
        if self
            .ready
            .front()
            .is_some_and(|candidate| candidate == owner_key)
        {
            if let Some(owner_key) = self.ready.pop_front() {
                self.ready.push_back(owner_key);
            }
        }
    }

    fn block_owner(&mut self, owner_key: &str) {
        if self.owners.contains_key(owner_key) {
            self.blocked.insert(owner_key.to_string());
            self.ready_membership.remove(owner_key);
        }
    }

    fn ready_owner(&mut self, owner_key: String) -> bool {
        if !self.owners.contains_key(&owner_key) {
            self.blocked.remove(&owner_key);
            self.ready_membership.remove(&owner_key);
            return false;
        }
        self.blocked.remove(&owner_key);
        if self.ready_membership.insert(owner_key.clone()) {
            self.ready.push_back(owner_key);
            return true;
        }
        false
    }

    fn drain_matching(
        &mut self,
        shard_index: usize,
        matches: &mut impl FnMut(&PendingInvoke) -> bool,
        drained: &mut Vec<(PendingQueueKey, PendingInvoke)>,
    ) {
        let mut removals = Vec::new();
        for (owner_key, owner) in &self.owners {
            for (sequence, pending) in &owner.pending {
                if matches(pending) {
                    removals.push((owner_key.clone(), *sequence));
                }
            }
        }
        for (owner_key, sequence) in removals {
            if let Some(pending) = self.remove(&owner_key, sequence) {
                drained.push((
                    PendingQueueKey {
                        lane: PendingQueueLane::Memory,
                        sequence,
                        memory_shard_index: Some(shard_index),
                        memory_owner_key: Some(owner_key),
                    },
                    pending,
                ));
            }
        }
    }
}

pub(super) struct PendingInvokeQueue {
    next_sequence: u64,
    by_runtime_request_id: HashMap<String, PendingQueueKey>,
    by_target_isolate_id: HashMap<u64, HashSet<PendingQueueKey>>,
    by_memory_owner_key: HashMap<String, HashSet<PendingQueueKey>>,
    by_enqueued_at: BTreeMap<Instant, HashSet<PendingQueueKey>>,
    targeted_nested: BTreeMap<u64, PendingInvoke>,
    targeted: BTreeMap<u64, PendingInvoke>,
    memory_shards: BTreeMap<usize, MemoryShardQueue>,
    memory_next_shard_cursor: Option<usize>,
    general: BTreeMap<u64, PendingInvoke>,
}

impl PendingInvokeQueue {
    pub(super) fn new() -> Self {
        Self {
            next_sequence: 0,
            by_runtime_request_id: HashMap::new(),
            by_target_isolate_id: HashMap::new(),
            by_memory_owner_key: HashMap::new(),
            by_enqueued_at: BTreeMap::new(),
            targeted_nested: BTreeMap::new(),
            targeted: BTreeMap::new(),
            memory_shards: BTreeMap::new(),
            memory_next_shard_cursor: None,
            general: BTreeMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn len(&self) -> usize {
        self.targeted_nested
            .len()
            .saturating_add(self.targeted.len())
            .saturating_add(
                self.memory_shards
                    .values()
                    .map(MemoryShardQueue::len)
                    .sum::<usize>(),
            )
            .saturating_add(self.general.len())
    }

    pub(super) fn queued_bytes(&self) -> usize {
        self.iter()
            .map(|pending| pending.queued_bytes)
            .sum::<usize>()
    }

    pub(super) fn lane_depths(&self) -> PendingQueueLaneDepths {
        PendingQueueLaneDepths {
            targeted_nested: self.targeted_nested.len(),
            targeted: self.targeted.len(),
            memory: self
                .memory_shards
                .values()
                .map(MemoryShardQueue::len)
                .sum::<usize>(),
            general: self.general.len(),
        }
    }

    pub(super) fn active_memory_shards(&self) -> usize {
        self.memory_shards.len()
    }

    pub(super) fn memory_shard_depth_summary(&self) -> (usize, usize) {
        let mut depths = self
            .memory_shards
            .values()
            .map(MemoryShardQueue::len)
            .collect::<Vec<_>>();
        if depths.is_empty() {
            return (0, 0);
        }
        depths.sort_unstable();
        let max = depths.last().copied().unwrap_or(0);
        let median = depths[depths.len() / 2];
        (max, median)
    }

    pub(super) fn memory_owner_queues(&self) -> usize {
        self.memory_shards
            .values()
            .map(MemoryShardQueue::owner_count)
            .sum::<usize>()
    }

    pub(super) fn blocked_memory_owner_queues(
        &self,
        memory_entity_leases: &HashMap<String, MemoryEntityLease>,
    ) -> usize {
        self.memory_shards
            .values()
            .map(|shard| shard.blocked_owner_count(memory_entity_leases))
            .sum::<usize>()
    }

    pub(super) fn mark_memory_owner_blocked(&mut self, key: &PendingQueueKey) {
        let (Some(shard_index), Some(owner_key)) =
            (key.memory_shard_index, key.memory_owner_key.as_deref())
        else {
            return;
        };
        if let Some(memory_shard) = self.memory_shards.get_mut(&shard_index) {
            memory_shard.block_owner(owner_key);
        }
    }

    pub(super) fn mark_memory_owner_ready(&mut self, owner_key: &str) -> bool {
        let Some(keys) = self.by_memory_owner_key.get(owner_key) else {
            return false;
        };
        let Some(shard_index) = keys.iter().find_map(|key| key.memory_shard_index) else {
            return false;
        };
        let Some(memory_shard) = self.memory_shards.get_mut(&shard_index) else {
            return false;
        };
        memory_shard.ready_owner(owner_key.to_string())
    }

    pub(super) fn oldest_queue_age_ms(&self, now: Instant) -> u64 {
        self.by_enqueued_at
            .keys()
            .next()
            .map(|enqueued_at| duration_millis_u64(now.saturating_duration_since(*enqueued_at)))
            .unwrap_or(0)
    }

    pub(super) fn memory_shard_debug(
        &self,
        memory_entity_leases: &HashMap<String, MemoryEntityLease>,
        memory_shard_affinity: &HashMap<usize, u64>,
        isolate_indices: &HashMap<u64, usize>,
        limit: usize,
    ) -> Vec<MemoryShardDebug> {
        let mut shards = self
            .memory_shards
            .iter()
            .map(|(shard_index, shard)| {
                let blocked_owners = shard.blocked_owner_count(memory_entity_leases);
                let affinity_isolate_id = memory_shard_affinity.get(shard_index).copied();
                MemoryShardDebug {
                    shard_index: *shard_index,
                    queued: shard.len(),
                    ready_owners: shard.owner_count().saturating_sub(blocked_owners),
                    blocked_owners,
                    affinity_isolate_id,
                    affinity_stale: affinity_isolate_id
                        .map(|isolate_id| !isolate_indices.contains_key(&isolate_id))
                        .unwrap_or(false),
                }
            })
            .collect::<Vec<_>>();
        shards.sort_by(|left, right| {
            right
                .queued
                .cmp(&left.queued)
                .then_with(|| left.shard_index.cmp(&right.shard_index))
        });
        shards.truncate(limit);
        shards
    }

    pub(super) fn push_back(&mut self, pending: PendingInvoke) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let lane = Self::lane_for(&pending);
        let key = Self::key_for(lane, sequence, &pending);
        self.index_pending(key.clone(), &pending);
        let replaced = self.insert_pending(key, pending);
        debug_assert!(replaced.is_none());
    }

    pub(super) fn pop_front(&mut self) -> Option<PendingInvoke> {
        let key = self.oldest_key(PendingQueueLane::ALL)?;
        self.remove(key)
    }

    pub(super) fn remove(&mut self, key: PendingQueueKey) -> Option<PendingInvoke> {
        let pending = self.remove_pending_from_lane(key.clone())?;
        self.unindex_pending(key, &pending);
        Some(pending)
    }

    pub(super) fn remove_by_runtime_request_id(
        &mut self,
        runtime_request_id: &str,
    ) -> Option<PendingInvoke> {
        let key = self
            .by_runtime_request_id
            .get(runtime_request_id)
            .cloned()?;
        let removed = self.remove(key);
        if removed.is_none() {
            self.by_runtime_request_id.remove(runtime_request_id);
        }
        removed
    }

    pub(super) fn drain_target_isolate_id(&mut self, target_isolate_id: u64) -> Vec<PendingInvoke> {
        let Some(keys) = self.by_target_isolate_id.remove(&target_isolate_id) else {
            return Vec::new();
        };
        let mut keys = keys.into_iter().collect::<Vec<_>>();
        keys.sort_by_key(|key| key.sequence);
        keys.into_iter()
            .filter_map(|key| {
                let pending = self.remove_pending_from_lane(key.clone())?;
                self.unindex_pending(key, &pending);
                Some(pending)
            })
            .collect()
    }

    pub(super) fn drain_expired(
        &mut self,
        now: Instant,
        max_queue_wait: Duration,
    ) -> Vec<PendingInvoke> {
        let Some(cutoff) = now.checked_sub(max_queue_wait) else {
            return Vec::new();
        };
        let expired_instants = self
            .by_enqueued_at
            .range(..=cutoff)
            .map(|(enqueued_at, _)| *enqueued_at)
            .collect::<Vec<_>>();
        let mut keys = Vec::new();
        for enqueued_at in expired_instants {
            if let Some(indexed_keys) = self.by_enqueued_at.remove(&enqueued_at) {
                keys.extend(indexed_keys);
            }
        }
        keys.sort_by_key(|key| key.sequence);
        keys.into_iter()
            .filter_map(|key| {
                let pending = self.remove_pending_from_lane(key.clone())?;
                self.unindex_pending(key, &pending);
                Some(pending)
            })
            .collect()
    }

    pub(super) fn next_expiry_at(&self, max_queue_wait: Duration) -> Option<Instant> {
        self.by_enqueued_at
            .keys()
            .next()
            .map(|enqueued_at| *enqueued_at + max_queue_wait)
    }

    pub(super) fn drain_matching(
        &mut self,
        mut matches: impl FnMut(&PendingInvoke) -> bool,
    ) -> Vec<PendingInvoke> {
        let mut drained = Vec::new();
        Self::drain_lane_matching(
            PendingQueueLane::TargetedNested,
            None,
            &mut self.targeted_nested,
            &mut matches,
            &mut drained,
        );
        Self::drain_lane_matching(
            PendingQueueLane::Targeted,
            None,
            &mut self.targeted,
            &mut matches,
            &mut drained,
        );
        let memory_shard_indices = self.memory_shards.keys().copied().collect::<Vec<_>>();
        for shard_index in memory_shard_indices {
            let remove_shard = if let Some(memory_shard) = self.memory_shards.get_mut(&shard_index)
            {
                memory_shard.drain_matching(shard_index, &mut matches, &mut drained);
                memory_shard.is_empty()
            } else {
                false
            };
            if remove_shard {
                self.memory_shards.remove(&shard_index);
            }
        }
        Self::drain_lane_matching(
            PendingQueueLane::General,
            None,
            &mut self.general,
            &mut matches,
            &mut drained,
        );
        drained.sort_by_key(|(key, _)| key.sequence);
        drained
            .into_iter()
            .map(|(key, pending)| {
                self.unindex_pending(key, &pending);
                pending
            })
            .collect::<Vec<_>>()
    }

    pub(super) fn find_oldest_map<T>(
        &self,
        lanes: impl IntoIterator<Item = PendingQueueLane>,
        mut map: impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<T> {
        let mut selected = None;
        for lane in lanes {
            match lane {
                PendingQueueLane::TargetedNested
                | PendingQueueLane::Targeted
                | PendingQueueLane::General => {
                    for (sequence, pending) in self.lane(lane) {
                        let key = PendingQueueKey {
                            lane,
                            sequence: *sequence,
                            memory_shard_index: None,
                            memory_owner_key: None,
                        };
                        let Some(value) = map(key, pending) else {
                            continue;
                        };
                        if selected
                            .as_ref()
                            .is_none_or(|(selected_sequence, _)| sequence < selected_sequence)
                        {
                            selected = Some((*sequence, value));
                        }
                    }
                }
                PendingQueueLane::Memory => {
                    for (shard_index, memory_shard) in &self.memory_shards {
                        for (owner_key, owner_queue) in &memory_shard.owners {
                            let Some((sequence, pending)) = owner_queue.pending.first_key_value()
                            else {
                                continue;
                            };
                            let key = PendingQueueKey {
                                lane,
                                sequence: *sequence,
                                memory_shard_index: Some(*shard_index),
                                memory_owner_key: Some(owner_key.clone()),
                            };
                            let Some(value) = map(key, pending) else {
                                continue;
                            };
                            if selected
                                .as_ref()
                                .is_none_or(|(selected_sequence, _)| sequence < selected_sequence)
                            {
                                selected = Some((*sequence, value));
                            }
                        }
                    }
                }
            }
        }
        selected.map(|(_, value)| value)
    }

    pub(super) fn find_fair_map<T>(
        &mut self,
        lanes: impl IntoIterator<Item = PendingQueueLane>,
        mut map: impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<T> {
        let mut selected = None;
        let mut selected_key = None;
        for lane in lanes {
            let candidate = match lane {
                PendingQueueLane::Memory => self.find_memory_round_robin_head_map(&mut map),
                PendingQueueLane::TargetedNested
                | PendingQueueLane::Targeted
                | PendingQueueLane::General => self.find_lane_oldest_map(lane, &mut map),
            };
            let Some((key, sequence, value)) = candidate else {
                continue;
            };
            if selected
                .as_ref()
                .is_none_or(|(selected_sequence, _)| sequence < *selected_sequence)
            {
                selected = Some((sequence, value));
                selected_key = Some(key);
            }
        }
        if matches!(
            selected_key.as_ref(),
            Some(PendingQueueKey {
                lane: PendingQueueLane::Memory,
                memory_shard_index: Some(_),
                ..
            })
        ) {
            let selected_key = selected_key
                .as_ref()
                .expect("selected memory key must exist");
            self.advance_memory_cursor(selected_key);
        }
        selected.map(|(_, value)| value)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &PendingInvoke> {
        self.targeted_nested
            .values()
            .chain(self.targeted.values())
            .chain(self.memory_shards.values().flat_map(MemoryShardQueue::iter))
            .chain(self.general.values())
    }

    fn oldest_key(
        &self,
        lanes: impl IntoIterator<Item = PendingQueueLane>,
    ) -> Option<PendingQueueKey> {
        self.find_oldest_map(lanes, |key, _| Some(key))
    }

    fn lane_for(pending: &PendingInvoke) -> PendingQueueLane {
        if pending.target_isolate_id.is_some()
            && (pending.host_rpc_call.is_some() || pending.memory_route.is_some())
        {
            PendingQueueLane::TargetedNested
        } else if pending.target_isolate_id.is_some() {
            PendingQueueLane::Targeted
        } else if pending.memory_route.is_some() {
            PendingQueueLane::Memory
        } else {
            PendingQueueLane::General
        }
    }

    fn key_for(lane: PendingQueueLane, sequence: u64, pending: &PendingInvoke) -> PendingQueueKey {
        PendingQueueKey {
            lane,
            sequence,
            memory_shard_index: (lane == PendingQueueLane::Memory)
                .then(|| Self::memory_shard_index_for(pending)),
            memory_owner_key: (lane == PendingQueueLane::Memory)
                .then(|| Self::memory_owner_key_for(pending)),
        }
    }

    fn memory_shard_index_for(pending: &PendingInvoke) -> usize {
        pending
            .memory_route
            .as_ref()
            .and_then(|route| route.shard_index)
            .unwrap_or(0)
    }

    fn memory_owner_key_for(pending: &PendingInvoke) -> String {
        pending
            .memory_route
            .as_ref()
            .map(|route| route.owner_key.clone())
            .unwrap_or_default()
    }

    fn insert_pending(
        &mut self,
        key: PendingQueueKey,
        pending: PendingInvoke,
    ) -> Option<PendingInvoke> {
        match key.lane {
            PendingQueueLane::TargetedNested => self.targeted_nested.insert(key.sequence, pending),
            PendingQueueLane::Targeted => self.targeted.insert(key.sequence, pending),
            PendingQueueLane::Memory => self
                .memory_shards
                .entry(key.memory_shard_index.unwrap_or(0))
                .or_default()
                .insert(
                    key.memory_owner_key.clone().unwrap_or_default(),
                    key.sequence,
                    pending,
                ),
            PendingQueueLane::General => self.general.insert(key.sequence, pending),
        }
    }

    fn remove_pending_from_lane(&mut self, key: PendingQueueKey) -> Option<PendingInvoke> {
        match key.lane {
            PendingQueueLane::TargetedNested => self.targeted_nested.remove(&key.sequence),
            PendingQueueLane::Targeted => self.targeted.remove(&key.sequence),
            PendingQueueLane::Memory => {
                let shard_index = key.memory_shard_index.unwrap_or(0);
                let (pending, remove_shard) = {
                    let memory_shard = self.memory_shards.get_mut(&shard_index)?;
                    let pending = memory_shard.remove(
                        key.memory_owner_key.as_deref().unwrap_or_default(),
                        key.sequence,
                    );
                    (pending, memory_shard.is_empty())
                };
                if remove_shard {
                    self.memory_shards.remove(&shard_index);
                }
                pending
            }
            PendingQueueLane::General => self.general.remove(&key.sequence),
        }
    }

    fn index_pending(&mut self, key: PendingQueueKey, pending: &PendingInvoke) {
        self.by_runtime_request_id
            .insert(pending.runtime_request_id.clone(), key.clone());
        if let Some(target_isolate_id) = pending.target_isolate_id {
            self.by_target_isolate_id
                .entry(target_isolate_id)
                .or_default()
                .insert(key.clone());
        }
        if let Some(memory_route) = pending.memory_route.as_ref() {
            self.by_memory_owner_key
                .entry(memory_route.owner_key.clone())
                .or_default()
                .insert(key.clone());
        }
        self.by_enqueued_at
            .entry(pending.enqueued_at)
            .or_default()
            .insert(key);
    }

    fn unindex_pending(&mut self, key: PendingQueueKey, pending: &PendingInvoke) {
        self.by_runtime_request_id
            .remove(&pending.runtime_request_id);
        if let Some(target_isolate_id) = pending.target_isolate_id {
            let remove_target_entry =
                if let Some(keys) = self.by_target_isolate_id.get_mut(&target_isolate_id) {
                    keys.remove(&key);
                    keys.is_empty()
                } else {
                    false
                };
            if remove_target_entry {
                self.by_target_isolate_id.remove(&target_isolate_id);
            }
        }
        if let Some(memory_route) = pending.memory_route.as_ref() {
            let remove_memory_entry =
                if let Some(keys) = self.by_memory_owner_key.get_mut(&memory_route.owner_key) {
                    keys.remove(&key);
                    keys.is_empty()
                } else {
                    false
                };
            if remove_memory_entry {
                self.by_memory_owner_key.remove(&memory_route.owner_key);
            }
        }
        let remove_enqueued_entry =
            if let Some(keys) = self.by_enqueued_at.get_mut(&pending.enqueued_at) {
                keys.remove(&key);
                keys.is_empty()
            } else {
                false
            };
        if remove_enqueued_entry {
            self.by_enqueued_at.remove(&pending.enqueued_at);
        }
    }

    #[cfg(test)]
    fn indexed_target_count(&self, target_isolate_id: u64) -> usize {
        self.by_target_isolate_id
            .get(&target_isolate_id)
            .map(HashSet::len)
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn indexed_memory_count(&self, owner_key: &str) -> usize {
        self.by_memory_owner_key
            .get(owner_key)
            .map(HashSet::len)
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn indexed_enqueued_count(&self) -> usize {
        self.by_enqueued_at
            .values()
            .map(HashSet::len)
            .sum::<usize>()
    }

    fn lane(&self, lane: PendingQueueLane) -> &BTreeMap<u64, PendingInvoke> {
        match lane {
            PendingQueueLane::TargetedNested => &self.targeted_nested,
            PendingQueueLane::Targeted => &self.targeted,
            PendingQueueLane::Memory => {
                panic!("memory queue is sharded; iterate memory_shards instead")
            }
            PendingQueueLane::General => &self.general,
        }
    }

    fn find_lane_oldest_map<T>(
        &self,
        lane: PendingQueueLane,
        map: &mut impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<(PendingQueueKey, u64, T)> {
        let mut selected = None;
        for (sequence, pending) in self.lane(lane) {
            let key = PendingQueueKey {
                lane,
                sequence: *sequence,
                memory_shard_index: None,
                memory_owner_key: None,
            };
            let Some(value) = map(key.clone(), pending) else {
                continue;
            };
            selected = Some((key, *sequence, value));
            break;
        }
        selected
    }

    fn find_memory_round_robin_head_map<T>(
        &mut self,
        map: &mut impl FnMut(PendingQueueKey, &PendingInvoke) -> Option<T>,
    ) -> Option<(PendingQueueKey, u64, T)> {
        if self.memory_shards.is_empty() {
            return None;
        }

        let cursor = self.memory_next_shard_cursor.unwrap_or(0);
        let shard_indices = self
            .memory_shards
            .range(cursor..)
            .chain(self.memory_shards.range(..cursor))
            .map(|(shard_index, _)| *shard_index)
            .collect::<Vec<_>>();
        for shard_index in shard_indices {
            let Some(memory_shard) = self.memory_shards.get_mut(&shard_index) else {
                continue;
            };
            let Some(candidate) = memory_shard.find_round_robin_owner_head_map(shard_index, map)
            else {
                continue;
            };
            return Some(candidate);
        }
        None
    }

    fn advance_memory_cursor(&mut self, key: &PendingQueueKey) {
        let Some(shard_index) = key.memory_shard_index else {
            return;
        };
        if let (Some(memory_shard), Some(owner_key)) = (
            self.memory_shards.get_mut(&shard_index),
            key.memory_owner_key.as_deref(),
        ) {
            memory_shard.advance_owner_cursor(owner_key);
        }
        self.memory_next_shard_cursor = self
            .memory_shards
            .range((shard_index.saturating_add(1))..)
            .next()
            .map(|(next_shard, _)| *next_shard)
            .or_else(|| self.memory_shards.keys().next().copied());
    }

    fn drain_lane_matching(
        lane_key: PendingQueueLane,
        memory_shard_index: Option<usize>,
        lane: &mut BTreeMap<u64, PendingInvoke>,
        matches: &mut impl FnMut(&PendingInvoke) -> bool,
        drained: &mut Vec<(PendingQueueKey, PendingInvoke)>,
    ) {
        let sequences = lane
            .iter()
            .filter_map(|(sequence, pending)| matches(pending).then_some(*sequence))
            .collect::<Vec<_>>();
        for sequence in sequences {
            if let Some(pending) = lane.remove(&sequence) {
                drained.push((
                    PendingQueueKey {
                        lane: lane_key,
                        sequence,
                        memory_shard_index,
                        memory_owner_key: None,
                    },
                    pending,
                ));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PendingQueueLaneDepths {
    pub(super) targeted_nested: usize,
    pub(super) targeted: usize,
    pub(super) memory: usize,
    pub(super) general: usize,
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone)]
pub(super) struct DynamicRpcBinding {
    pub(super) binding: String,
    pub(super) provider_id: String,
}

#[derive(Default)]
pub(super) struct PoolStats {
    pub(super) spawn_count: u64,
    pub(super) reuse_count: u64,
    pub(super) scale_down_count: u64,
    pub(super) memory_affinity_hit_count: u64,
    pub(super) memory_affinity_miss_no_mapping_count: u64,
    pub(super) memory_affinity_miss_stale_count: u64,
    pub(super) memory_affinity_miss_saturated_count: u64,
    pub(super) memory_least_loaded_fallback_count: u64,
    pub(super) memory_atomic_overflow_dispatch_count: u64,
    pub(super) memory_candidate_rejected_owner_lease_count: u64,
    pub(super) memory_candidate_rejected_isolate_state_count: u64,
    pub(super) memory_candidate_heads_inspected_count: u64,
    pub(super) memory_dispatch_no_ready_candidate_count: u64,
}

impl PoolStats {
    pub(super) fn record_dispatch_attempt(&mut self, stats: DispatchAttemptStats) {
        self.memory_affinity_hit_count = self
            .memory_affinity_hit_count
            .saturating_add(stats.memory_affinity_hit_count);
        self.memory_affinity_miss_no_mapping_count = self
            .memory_affinity_miss_no_mapping_count
            .saturating_add(stats.memory_affinity_miss_no_mapping_count);
        self.memory_affinity_miss_stale_count = self
            .memory_affinity_miss_stale_count
            .saturating_add(stats.memory_affinity_miss_stale_count);
        self.memory_affinity_miss_saturated_count = self
            .memory_affinity_miss_saturated_count
            .saturating_add(stats.memory_affinity_miss_saturated_count);
        self.memory_least_loaded_fallback_count = self
            .memory_least_loaded_fallback_count
            .saturating_add(stats.memory_least_loaded_fallback_count);
        self.memory_atomic_overflow_dispatch_count = self
            .memory_atomic_overflow_dispatch_count
            .saturating_add(stats.memory_atomic_overflow_dispatch_count);
        self.memory_candidate_rejected_owner_lease_count = self
            .memory_candidate_rejected_owner_lease_count
            .saturating_add(stats.memory_candidate_rejected_owner_lease_count);
        self.memory_candidate_rejected_isolate_state_count = self
            .memory_candidate_rejected_isolate_state_count
            .saturating_add(stats.memory_candidate_rejected_isolate_state_count);
        self.memory_candidate_heads_inspected_count = self
            .memory_candidate_heads_inspected_count
            .saturating_add(stats.memory_candidate_heads_inspected_count);
        self.memory_dispatch_no_ready_candidate_count = self
            .memory_dispatch_no_ready_candidate_count
            .saturating_add(stats.memory_dispatch_no_ready_candidate_count);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct DispatchAttemptStats {
    pub(super) memory_affinity_hit_count: u64,
    pub(super) memory_affinity_miss_no_mapping_count: u64,
    pub(super) memory_affinity_miss_stale_count: u64,
    pub(super) memory_affinity_miss_saturated_count: u64,
    pub(super) memory_least_loaded_fallback_count: u64,
    pub(super) memory_atomic_overflow_dispatch_count: u64,
    pub(super) memory_candidate_rejected_owner_lease_count: u64,
    pub(super) memory_candidate_rejected_isolate_state_count: u64,
    pub(super) memory_candidate_heads_inspected_count: u64,
    pub(super) memory_dispatch_no_ready_candidate_count: u64,
}

pub(super) struct PendingInvoke {
    pub(super) runtime_request_id: String,
    pub(super) request: WorkerInvocation,
    pub(super) request_body: Option<InvokeRequestBodyReceiver>,
    pub(super) memory_route: Option<MemoryRoute>,
    pub(super) memory_call: Option<MemoryExecutionCall>,
    pub(super) host_rpc_call: Option<HostRpcExecutionCall>,
    pub(super) target_isolate_id: Option<u64>,
    pub(super) reply_kind: PendingReplyKind,
    pub(super) internal_origin: bool,
    pub(super) reply: oneshot::Sender<Result<WorkerOutput>>,
    pub(super) enqueued_at: Instant,
    pub(super) queued_bytes: usize,
}

pub(super) struct EnqueueInvokeRequest {
    pub(super) worker_name: String,
    pub(super) runtime_request_id: String,
    pub(super) request: WorkerInvocation,
    pub(super) request_body: Option<InvokeRequestBodyReceiver>,
    pub(super) memory_route: Option<MemoryRoute>,
    pub(super) memory_call: Option<MemoryExecutionCall>,
    pub(super) host_rpc_call: Option<HostRpcExecutionCall>,
    pub(super) target_isolate_id: Option<u64>,
    pub(super) target_generation: Option<u64>,
    pub(super) internal_origin: bool,
    pub(super) reply: oneshot::Sender<Result<WorkerOutput>>,
    pub(super) reply_kind: PendingReplyKind,
}

#[derive(Clone, Default)]
pub(super) enum PendingReplyKind {
    #[default]
    Normal,
    Stream,
    DynamicFetch {
        handle: String,
    },
    WebsocketOpen {
        session_id: String,
    },
    WebsocketFrame {
        session_id: String,
    },
    TransportOpen {
        session_id: String,
    },
}

impl PendingReplyKind {
    pub(super) fn label(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Stream => "stream",
            Self::DynamicFetch { .. } => "dynamic-fetch",
            Self::WebsocketOpen { .. } => "websocket-open",
            Self::WebsocketFrame { .. } => "websocket-frame",
            Self::TransportOpen { .. } => "transport-open",
        }
    }
}

pub(super) struct PendingReply {
    pub(super) completion_token: String,
    pub(super) canceled: bool,
    pub(super) memory_key: Option<String>,
    pub(super) active_memory_lease: Option<MemoryEntityLease>,
    pub(super) memory_outbox_shard: Option<usize>,
    pub(super) internal_origin: bool,
    pub(super) reply: oneshot::Sender<Result<WorkerOutput>>,
    pub(super) completion_meta: Option<PendingReplyMeta>,
    pub(super) kind: PendingReplyKind,
    pub(super) dispatched_at: Instant,
}

#[derive(Default)]
pub(super) struct RemovedIsolate {
    pub(super) replies: Vec<(String, oneshot::Sender<Result<WorkerOutput>>)>,
    pub(super) was_starting: bool,
}

#[derive(Clone, Debug)]
pub(super) struct MemoryEntityLease {
    pub(super) owner_key: String,
    pub(super) epoch: u64,
    pub(super) owner_isolate_id: u64,
}

impl MemoryEntityLease {
    fn matches(&self, other: &Self) -> bool {
        self.owner_key == other.owner_key
            && self.epoch == other.epoch
            && self.owner_isolate_id == other.owner_isolate_id
    }
}

pub(super) struct PendingReplyMeta {
    pub(super) method: String,
    pub(super) url: String,
    pub(super) traceparent: Option<String>,
    pub(super) user_request_id: String,
}

pub(super) struct TraceResultMeta {
    pub(super) status: Option<u16>,
    pub(super) error: Option<String>,
}

pub(super) struct DirectDynamicFetchRequest {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) target_isolate_id: u64,
    pub(super) runtime_request_id: String,
    pub(super) request: WorkerInvocation,
    pub(super) reply: oneshot::Sender<Result<WorkerOutput>>,
    pub(super) handle: String,
}

pub(super) enum DirectDynamicFetchDispatch {
    Dispatched,
    Fallback {
        reply: oneshot::Sender<Result<WorkerOutput>>,
        clear_preferred: bool,
    },
}

pub(super) struct FinishRequest {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) request_id: String,
    pub(super) completion_token: String,
    pub(super) finished_at: Instant,
    pub(super) wait_until_count: usize,
    pub(super) result: Result<WorkerOutput>,
}

pub(super) struct TraceForwardRequest {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) request_method: String,
    pub(super) request_url: String,
    pub(super) runtime_request_id: String,
    pub(super) user_request_id: String,
    pub(super) result: TraceResultMeta,
    pub(super) execution_ms: u64,
    pub(super) wait_until_count: usize,
    pub(super) internal_origin: bool,
    pub(super) trace_destination: Option<InternalTraceDestination>,
}

pub(super) struct BuildExecuteCommand {
    pub(super) runtime_request_id: String,
    pub(super) completion_token: String,
    pub(super) request: WorkerInvocation,
    pub(super) request_body: Option<InvokeRequestBodyReceiver>,
    pub(super) stream_response: bool,
    pub(super) memory_call: Option<MemoryExecutionCall>,
    pub(super) host_rpc_call: Option<HostRpcExecutionCall>,
    pub(super) memory_route: Option<MemoryRoute>,
    pub(super) dispatched_at: Instant,
    pub(super) profile_memory_atomic: bool,
}

pub(super) struct TargetedHostRpcInvoke {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) target_id: String,
    pub(super) method_name: String,
    pub(super) args: Vec<u8>,
    pub(super) reply: TargetedHostRpcReply,
}

pub(super) struct DynamicWorkerFetchStart {
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
    pub(super) handle: String,
    pub(super) request: WorkerInvocation,
    pub(super) reply_id: String,
    pub(super) pending_replies: crate::ops::DynamicPendingReplies,
    pub(super) command_tx: mpsc::Sender<RuntimeCommand>,
}

pub(super) struct ServiceBindingFetchStart {
    pub(super) owner_worker: String,
    pub(super) owner_generation: u64,
    pub(super) binding: String,
    pub(super) target_worker: String,
    pub(super) request: WorkerInvocation,
    pub(super) reply_id: String,
    pub(super) pending_replies: crate::ops::DynamicPendingReplies,
}

pub(super) struct RuntimeThreadStart {
    pub(super) receiver: mpsc::Receiver<RuntimeCommand>,
    pub(super) cancel_receiver: mpsc::Receiver<RuntimeCommand>,
    pub(super) runtime_fast_sender: mpsc::Sender<RuntimeCommand>,
    pub(super) asset_catalog: AssetCatalog,
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) cache_store: CacheStore,
    pub(super) config: RuntimeConfig,
    pub(super) storage: RuntimeStorageConfig,
}

pub(super) struct IsolateThreadStart {
    pub(super) snapshot: &'static [u8],
    pub(super) snapshot_preloaded: bool,
    pub(super) source: crate::ops::WorkerSource,
    pub(super) deployment_config: Arc<crate::ops::WorkerDeploymentPayload>,
    pub(super) allow_code_generation: bool,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
    pub(super) cache_store: CacheStore,
    pub(super) open_handle_registry: crate::ops::MemoryOpenHandleRegistry,
    pub(super) dynamic_profile: crate::ops::DynamicProfile,
    pub(super) execution_limits: crate::ops::RuntimeExecutionLimits,
    pub(super) runtime_fast_sender: mpsc::Sender<RuntimeCommand>,
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) event_tx: RuntimeEventSender,
}

pub(super) struct WebSocketSessionRegistration {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) session_id: String,
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) handle: String,
}

pub(super) struct TransportSessionRegistration {
    pub(super) worker_name: String,
    pub(super) generation: u64,
    pub(super) isolate_id: u64,
    pub(super) session_id: String,
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) handle: String,
    pub(super) stream_sender: mpsc::Sender<Vec<u8>>,
    pub(super) datagram_sender: mpsc::Sender<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(super) struct MemoryRoute {
    pub(super) binding: String,
    pub(super) key: String,
    pub(super) owner_epoch: i64,
    pub(super) owner_key: String,
    pub(super) shard_index: Option<usize>,
}

impl MemoryRoute {
    pub(super) fn new(binding: String, key: String) -> Self {
        let owner_key = memory_owner_key(&binding, &key);
        Self {
            binding,
            key,
            owner_epoch: 0,
            owner_key,
            shard_index: None,
        }
    }

    pub(super) fn with_owner_epoch(mut self, owner_epoch: i64) -> Self {
        self.owner_epoch = owner_epoch;
        self
    }
}

pub(super) struct DispatchCandidate {
    pub(super) queue_key: PendingQueueKey,
    pub(super) isolate_idx: usize,
}

pub(super) enum DispatchSelection {
    Dispatch(DispatchCandidate),
    DropStaleTarget { queue_key: PendingQueueKey },
}

pub(super) struct IsolateHandle {
    pub(super) id: u64,
    pub(super) sender: mpsc::Sender<IsolateCommand>,
    pub(super) v8_handle: Arc<StdMutex<Option<deno_core::v8::IsolateHandle>>>,
    pub(super) dynamic_control_inbox: crate::ops::DynamicControlInbox,
    pub(super) startup: IsolateStartup,
    pub(super) inflight_count: usize,
    pub(super) active_websocket_sessions: usize,
    pub(super) active_transport_sessions: usize,
    pub(super) served_requests: u64,
    pub(super) last_used_at: Instant,
    pub(super) pending_replies: HashMap<String, PendingReply>,
    pub(super) pending_wait_until: HashMap<String, String>,
}

#[derive(Clone, Copy)]
pub(super) enum IsolateStartup {
    Starting { started_at: Instant },
    Ready,
    Retiring,
    Failed,
}

impl IsolateStartup {
    pub(super) fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub(super) fn is_starting(&self) -> bool {
        matches!(self, Self::Starting { .. })
    }

    pub(super) fn timed_out(&self, now: Instant, timeout: Duration) -> bool {
        match self {
            Self::Starting { started_at } => now.duration_since(*started_at) >= timeout,
            Self::Ready | Self::Retiring | Self::Failed => false,
        }
    }
}
#[derive(Serialize)]
pub(super) struct TraceEventPayload {
    pub(super) ts_ms: i64,
    pub(super) worker: String,
    pub(super) generation: u64,
    pub(super) request_id: String,
    pub(super) runtime_request_id: String,
    pub(super) method: String,
    pub(super) url: String,
    pub(super) status: Option<u16>,
    pub(super) ok: bool,
    pub(super) error: Option<String>,
    pub(super) execution_ms: u64,
    pub(super) wait_until_count: usize,
}

#[derive(Serialize, Deserialize)]
pub(super) struct StoredWorkerDeployment {
    pub(super) name: String,
    pub(super) source: String,
    pub(super) config: DeployConfig,
    #[serde(default)]
    pub(super) assets: Vec<DeployAsset>,
    #[serde(default)]
    pub(super) server_modules: Vec<DeployServerModule>,
    #[serde(default)]
    pub(super) asset_headers: Option<String>,
    pub(super) deployment_id: String,
    pub(super) updated_at_ms: i64,
    #[serde(default)]
    pub(super) expires_at_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_invoke(
        request_id: &str,
        target_isolate_id: Option<u64>,
        memory_route: Option<MemoryRoute>,
        host_rpc_call: Option<HostRpcExecutionCall>,
    ) -> PendingInvoke {
        let (reply, _) = oneshot::channel();
        PendingInvoke {
            runtime_request_id: request_id.to_string(),
            request: WorkerInvocation {
                method: "GET".to_string(),
                url: "http://worker/".to_string(),
                headers: Vec::new(),
                body: Vec::new(),
                request_id: request_id.to_string(),
            },
            request_body: None,
            memory_route,
            memory_call: None,
            host_rpc_call,
            target_isolate_id,
            reply_kind: PendingReplyKind::Normal,
            internal_origin: false,
            reply,
            enqueued_at: Instant::now(),
            queued_bytes: 1,
        }
    }

    fn memory_route_on_shard(key: &str, shard_index: usize) -> MemoryRoute {
        let mut route = MemoryRoute::new("MEM".to_string(), key.to_string());
        route.shard_index = Some(shard_index);
        route
    }

    #[test]
    fn pending_invoke_queue_lanes_preserve_global_fifo_pop_order() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke("general", None, None, None));
        queue.push_back(pending_invoke(
            "memory",
            None,
            Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
            None,
        ));
        queue.push_back(pending_invoke(
            "targeted-nested",
            Some(7),
            None,
            Some(HostRpcExecutionCall {
                target_id: "provider".to_string(),
                method: "call".to_string(),
                args: Vec::new(),
            }),
        ));

        assert_eq!(queue.general.len(), 1);
        assert_eq!(
            queue
                .memory_shards
                .get(&0)
                .map(MemoryShardQueue::len)
                .unwrap_or_default(),
            1
        );
        assert_eq!(queue.targeted_nested.len(), 1);
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("general".to_string())
        );
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("memory".to_string())
        );
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("targeted-nested".to_string())
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_key_removal_skips_blocked_older_work() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke(
            "blocked-memory",
            None,
            Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
            None,
        ));
        queue.push_back(pending_invoke("ready-general", None, None, None));

        let key = queue
            .find_oldest_map(
                [PendingQueueLane::Memory, PendingQueueLane::General],
                |key, pending| (pending.runtime_request_id == "ready-general").then_some(key),
            )
            .expect("ready general work should be selectable");
        assert_eq!(
            queue.remove(key).map(|pending| pending.runtime_request_id),
            Some("ready-general".to_string())
        );
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("blocked-memory".to_string())
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_stores_memory_work_in_shard_lanes() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke(
            "memory-shard-7",
            None,
            Some(memory_route_on_shard("room-a", 7)),
            None,
        ));
        queue.push_back(pending_invoke(
            "memory-shard-3",
            None,
            Some(memory_route_on_shard("room-b", 3)),
            None,
        ));
        queue.push_back(pending_invoke("general", None, None, None));

        assert_eq!(queue.memory_shards.len(), 2);
        assert_eq!(
            queue
                .memory_shards
                .get(&7)
                .map(MemoryShardQueue::len)
                .unwrap_or_default(),
            1
        );
        assert_eq!(
            queue
                .memory_shards
                .get(&3)
                .map(MemoryShardQueue::len)
                .unwrap_or_default(),
            1
        );

        let key = queue
            .find_oldest_map([PendingQueueLane::Memory], |key, pending| {
                (pending.runtime_request_id == "memory-shard-3").then_some(key)
            })
            .expect("memory shard work should be selectable");
        assert_eq!(key.memory_shard_index, Some(3));
        assert_eq!(
            queue.remove(key).map(|pending| pending.runtime_request_id),
            Some("memory-shard-3".to_string())
        );
        assert!(!queue.memory_shards.contains_key(&3));
        assert!(queue.memory_shards.contains_key(&7));
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("memory-shard-7".to_string())
        );
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("general".to_string())
        );
        assert!(queue.memory_shards.is_empty());
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_memory_lookup_can_bypass_blocked_owner_in_same_shard() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke(
            "owner-a-head",
            None,
            Some(memory_route_on_shard("room-a", 3)),
            None,
        ));
        queue.push_back(pending_invoke(
            "owner-a-tail",
            None,
            Some(memory_route_on_shard("room-a", 3)),
            None,
        ));
        queue.push_back(pending_invoke(
            "owner-b-head",
            None,
            Some(memory_route_on_shard("room-b", 3)),
            None,
        ));
        queue.push_back(pending_invoke(
            "memory-shard-7-head",
            None,
            Some(memory_route_on_shard("room-c", 7)),
            None,
        ));

        let owner_b = queue
            .find_oldest_map([PendingQueueLane::Memory], |key, pending| {
                (pending.runtime_request_id == "owner-b-head").then_some(key)
            })
            .expect("independent owner behind blocked owner should be selectable");
        assert_eq!(owner_b.memory_shard_index, Some(3));
        assert_eq!(
            queue
                .remove(owner_b)
                .map(|pending| pending.runtime_request_id),
            Some("owner-b-head".to_string())
        );

        let owner_a_tail = queue.find_oldest_map([PendingQueueLane::Memory], |key, pending| {
            (pending.runtime_request_id == "owner-a-tail").then_some(key)
        });
        assert!(
            owner_a_tail.is_none(),
            "later same-owner work must not overtake its owner head"
        );

        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("owner-a-head".to_string())
        );
        let exposed_tail = queue
            .find_oldest_map([PendingQueueLane::Memory], |key, pending| {
                (pending.runtime_request_id == "owner-a-tail").then_some(key)
            })
            .expect("tail becomes selectable after the shard head is removed");
        assert_eq!(exposed_tail.memory_shard_index, Some(3));
    }

    #[test]
    fn pending_invoke_queue_fair_memory_lookup_rotates_between_owners_in_one_shard() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke(
            "owner-a-1",
            None,
            Some(memory_route_on_shard("room-a", 3)),
            None,
        ));
        queue.push_back(pending_invoke(
            "owner-a-2",
            None,
            Some(memory_route_on_shard("room-a", 3)),
            None,
        ));
        queue.push_back(pending_invoke(
            "owner-b-1",
            None,
            Some(memory_route_on_shard("room-b", 3)),
            None,
        ));

        let first = queue
            .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
            .expect("first owner should be selectable");
        assert_eq!(
            queue
                .remove(first)
                .map(|pending| pending.runtime_request_id),
            Some("owner-a-1".to_string())
        );

        let second = queue
            .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
            .expect("second owner should rotate ahead of hot owner backlog");
        assert_eq!(
            queue
                .remove(second)
                .map(|pending| pending.runtime_request_id),
            Some("owner-b-1".to_string())
        );

        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("owner-a-2".to_string())
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_blocked_memory_owner_is_skipped_until_woken() {
        let mut queue = PendingInvokeQueue::new();
        let route_a = memory_route_on_shard("room-a", 3);
        let owner_a = route_a.owner_key.clone();
        queue.push_back(pending_invoke("owner-a-1", None, Some(route_a), None));
        queue.push_back(pending_invoke(
            "owner-b-1",
            None,
            Some(memory_route_on_shard("room-b", 3)),
            None,
        ));

        let blocked = queue
            .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
            .expect("owner a should be the first memory head");
        queue.mark_memory_owner_blocked(&blocked);

        let mut inspected = 0usize;
        let ready = queue
            .find_fair_map([PendingQueueLane::Memory], |key, pending| {
                inspected = inspected.saturating_add(1);
                Some((key, pending.runtime_request_id.clone()))
            })
            .expect("owner b should remain dispatchable");
        assert_eq!(ready.1, "owner-b-1");
        assert_eq!(inspected, 1);
        assert_eq!(
            queue
                .remove(ready.0)
                .map(|pending| pending.runtime_request_id),
            Some("owner-b-1".to_string())
        );

        assert!(queue.mark_memory_owner_ready(&owner_a));
        assert!(!queue.mark_memory_owner_ready(&owner_a));
        let woken = queue
            .find_fair_map([PendingQueueLane::Memory], |key, pending| {
                Some((key, pending.runtime_request_id.clone()))
            })
            .expect("woken owner should be dispatchable again");
        assert_eq!(woken.1, "owner-a-1");
    }

    #[test]
    fn pending_invoke_queue_final_memory_owner_removal_clears_ready_and_blocked_state() {
        let mut queue = PendingInvokeQueue::new();
        let route = memory_route_on_shard("room-a", 3);
        let owner_key = route.owner_key.clone();
        queue.push_back(pending_invoke("owner-a-1", None, Some(route), None));

        let key = queue
            .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
            .expect("memory owner should be selectable");
        queue.mark_memory_owner_blocked(&key);
        assert_eq!(queue.blocked_memory_owner_queues(&HashMap::new()), 1);

        assert_eq!(
            queue.remove(key).map(|pending| pending.runtime_request_id),
            Some("owner-a-1".to_string())
        );
        assert_eq!(queue.blocked_memory_owner_queues(&HashMap::new()), 0);
        assert!(!queue.mark_memory_owner_ready(&owner_key));
        assert!(queue.memory_shards.is_empty());
    }

    #[test]
    fn pending_invoke_queue_fair_memory_lookup_rotates_across_shard_heads() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke(
            "hot-1",
            None,
            Some(memory_route_on_shard("hot-a", 1)),
            None,
        ));
        queue.push_back(pending_invoke(
            "hot-2",
            None,
            Some(memory_route_on_shard("hot-b", 1)),
            None,
        ));
        queue.push_back(pending_invoke(
            "cold-1",
            None,
            Some(memory_route_on_shard("cold-a", 2)),
            None,
        ));

        let first_key = queue
            .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
            .expect("first shard head should be selectable");
        assert_eq!(
            queue
                .remove(first_key)
                .map(|pending| pending.runtime_request_id),
            Some("hot-1".to_string())
        );

        let second_key = queue
            .find_fair_map([PendingQueueLane::Memory], |key, _| Some(key))
            .expect("cold shard head should be selected before hot shard backlog");
        assert_eq!(second_key.memory_shard_index, Some(2));
        assert_eq!(
            queue
                .remove(second_key)
                .map(|pending| pending.runtime_request_id),
            Some("cold-1".to_string())
        );

        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("hot-2".to_string())
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_keys_are_stable_after_older_removals() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke("first", None, None, None));
        queue.push_back(pending_invoke("second", None, None, None));
        queue.push_back(pending_invoke("third", None, None, None));

        let third_key = queue
            .find_oldest_map([PendingQueueLane::General], |key, pending| {
                (pending.runtime_request_id == "third").then_some(key)
            })
            .expect("third should be indexed by a stable queue key");

        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("first".to_string())
        );
        assert_eq!(
            queue
                .remove(third_key)
                .map(|pending| pending.runtime_request_id),
            Some("third".to_string())
        );
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("second".to_string())
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_removes_by_runtime_request_id_without_predicate_scan() {
        let mut queue = PendingInvokeQueue::new();
        queue.push_back(pending_invoke("first", None, None, None));
        queue.push_back(pending_invoke(
            "memory",
            None,
            Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
            None,
        ));
        queue.push_back(pending_invoke("last", None, None, None));

        assert_eq!(
            queue
                .remove_by_runtime_request_id("memory")
                .map(|pending| pending.runtime_request_id),
            Some("memory".to_string())
        );
        assert!(queue.remove_by_runtime_request_id("memory").is_none());
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("first".to_string())
        );
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("last".to_string())
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_keeps_target_and_memory_indexes_coherent() {
        let mut queue = PendingInvokeQueue::new();
        let memory_route = MemoryRoute::new("MEM".to_string(), "room".to_string());
        let memory_owner_key = memory_route.owner_key.clone();

        queue.push_back(pending_invoke("general", None, None, None));
        queue.push_back(pending_invoke("targeted", Some(7), None, None));
        queue.push_back(pending_invoke(
            "memory",
            None,
            Some(memory_route.clone()),
            None,
        ));
        queue.push_back(pending_invoke(
            "targeted-memory",
            Some(7),
            Some(memory_route),
            None,
        ));

        assert_eq!(queue.indexed_target_count(7), 2);
        assert_eq!(queue.indexed_memory_count(&memory_owner_key), 2);

        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("general".to_string())
        );
        assert_eq!(queue.indexed_target_count(7), 2);
        assert_eq!(queue.indexed_memory_count(&memory_owner_key), 2);

        assert_eq!(
            queue
                .remove_by_runtime_request_id("targeted")
                .map(|pending| pending.runtime_request_id),
            Some("targeted".to_string())
        );
        assert_eq!(queue.indexed_target_count(7), 1);
        assert_eq!(queue.indexed_memory_count(&memory_owner_key), 2);

        let targeted_memory_key = queue
            .find_oldest_map([PendingQueueLane::TargetedNested], |key, pending| {
                (pending.runtime_request_id == "targeted-memory").then_some(key)
            })
            .expect("targeted memory work should be indexed");
        assert_eq!(
            queue
                .remove(targeted_memory_key)
                .map(|pending| pending.runtime_request_id),
            Some("targeted-memory".to_string())
        );
        assert_eq!(queue.indexed_target_count(7), 0);
        assert_eq!(queue.indexed_memory_count(&memory_owner_key), 1);

        let drained = queue.drain_matching(|pending| pending.memory_route.is_some());
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].runtime_request_id, "memory");
        assert_eq!(queue.indexed_target_count(7), 0);
        assert_eq!(queue.indexed_memory_count(&memory_owner_key), 0);
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_reports_bounded_memory_scheduler_debug_without_owner_keys() {
        let mut queue = PendingInvokeQueue::new();
        let mut hot_route = MemoryRoute::new("MEM".to_string(), "hot".to_string());
        hot_route.shard_index = Some(2);
        let mut cold_route = MemoryRoute::new("MEM".to_string(), "cold".to_string());
        cold_route.shard_index = Some(3);
        let hot_owner_key = hot_route.owner_key.clone();
        queue.push_back(pending_invoke("general", None, None, None));
        queue.push_back(pending_invoke("hot-a", None, Some(hot_route.clone()), None));
        queue.push_back(pending_invoke("hot-b", None, Some(hot_route), None));
        queue.push_back(pending_invoke("cold", None, Some(cold_route), None));

        let lane_depths = queue.lane_depths();
        assert_eq!(lane_depths.general, 1);
        assert_eq!(lane_depths.memory, 3);
        assert_eq!(queue.active_memory_shards(), 2);
        assert_eq!(queue.memory_shard_depth_summary(), (2, 2));
        assert_eq!(queue.memory_owner_queues(), 2);

        let mut leases = HashMap::new();
        leases.insert(
            hot_owner_key.clone(),
            MemoryEntityLease {
                owner_key: hot_owner_key.clone(),
                epoch: 1,
                owner_isolate_id: 10,
            },
        );
        assert_eq!(queue.blocked_memory_owner_queues(&leases), 1);

        let mut affinity = HashMap::new();
        affinity.insert(2, 10);
        affinity.insert(3, 99);
        let isolate_indices = HashMap::from([(10, 0)]);
        let shards = queue.memory_shard_debug(&leases, &affinity, &isolate_indices, 1);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_index, 2);
        assert_eq!(shards[0].queued, 2);
        assert_eq!(shards[0].blocked_owners, 1);
        assert_eq!(shards[0].ready_owners, 0);
        assert_eq!(shards[0].affinity_isolate_id, Some(10));
        assert!(!shards[0].affinity_stale);
        assert!(format!("{:?}", shards).contains("shard_index"));
        assert!(!format!("{:?}", shards).contains(&hot_owner_key));
    }

    #[test]
    fn pending_invoke_queue_drains_targeted_work_by_isolate_index() {
        let mut queue = PendingInvokeQueue::new();
        let memory_route = MemoryRoute::new("MEM".to_string(), "room".to_string());
        let memory_owner_key = memory_route.owner_key.clone();

        queue.push_back(pending_invoke("general", None, None, None));
        queue.push_back(pending_invoke("targeted-a", Some(7), None, None));
        queue.push_back(pending_invoke(
            "targeted-memory",
            Some(7),
            Some(memory_route),
            None,
        ));
        queue.push_back(pending_invoke("targeted-b", Some(8), None, None));

        let drained = queue.drain_target_isolate_id(7);
        let drained_ids = drained
            .iter()
            .map(|pending| pending.runtime_request_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(drained_ids, vec!["targeted-a", "targeted-memory"]);
        assert_eq!(queue.indexed_target_count(7), 0);
        assert_eq!(queue.indexed_target_count(8), 1);
        assert_eq!(queue.indexed_memory_count(&memory_owner_key), 0);
        assert!(queue.remove_by_runtime_request_id("targeted-a").is_none());
        assert!(queue
            .remove_by_runtime_request_id("targeted-memory")
            .is_none());
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("general".to_string())
        );
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("targeted-b".to_string())
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn pending_invoke_queue_drains_expired_work_by_enqueued_index() {
        let mut queue = PendingInvokeQueue::new();
        let now = Instant::now();
        let max_wait = Duration::from_millis(50);
        let mut old_targeted = pending_invoke("old-targeted", Some(7), None, None);
        old_targeted.enqueued_at = now - Duration::from_millis(75);
        let mut fresh_memory = pending_invoke(
            "fresh-memory",
            None,
            Some(MemoryRoute::new("MEM".to_string(), "room".to_string())),
            None,
        );
        fresh_memory.enqueued_at = now - Duration::from_millis(10);
        let memory_owner_key = fresh_memory
            .memory_route
            .as_ref()
            .expect("memory route")
            .owner_key
            .clone();
        let mut old_general = pending_invoke("old-general", None, None, None);
        old_general.enqueued_at = now - Duration::from_millis(55);

        queue.push_back(old_targeted);
        queue.push_back(fresh_memory);
        queue.push_back(old_general);
        assert_eq!(queue.indexed_enqueued_count(), 3);

        let drained = queue.drain_expired(now, max_wait);
        let drained_ids = drained
            .iter()
            .map(|pending| pending.runtime_request_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(drained_ids, vec!["old-targeted", "old-general"]);
        assert_eq!(queue.indexed_target_count(7), 0);
        assert_eq!(queue.indexed_memory_count(&memory_owner_key), 1);
        assert_eq!(queue.indexed_enqueued_count(), 1);
        assert_eq!(
            queue.next_expiry_at(max_wait),
            Some(now + Duration::from_millis(40))
        );
        assert!(queue.remove_by_runtime_request_id("old-targeted").is_none());
        assert!(queue.remove_by_runtime_request_id("old-general").is_none());
        assert_eq!(
            queue.pop_front().map(|pending| pending.runtime_request_id),
            Some("fresh-memory".to_string())
        );
        assert!(queue.is_empty());
        assert_eq!(queue.indexed_enqueued_count(), 0);
    }

    #[test]
    fn memory_entity_lease_matching_is_epoch_and_owner_checked() {
        let first = MemoryEntityLease {
            owner_key: "MEM\u{1f}room".to_string(),
            epoch: 1,
            owner_isolate_id: 7,
        };
        let newer = MemoryEntityLease {
            owner_key: first.owner_key.clone(),
            epoch: 2,
            owner_isolate_id: 7,
        };
        let migrated = MemoryEntityLease {
            owner_key: first.owner_key.clone(),
            epoch: 1,
            owner_isolate_id: 8,
        };
        assert!(first.matches(&first));
        assert!(!newer.matches(&first));
        assert!(!migrated.matches(&first));
    }
}
