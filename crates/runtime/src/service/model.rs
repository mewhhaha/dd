use super::*;
use serde::{Deserialize, Serialize};
pub(super) struct WorkerManager {
    pub(super) config: RuntimeConfig,
    pub(super) storage: RuntimeStorageConfig,
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) runtime_fast_sender: mpsc::Sender<RuntimeCommand>,
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
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
    pub(super) next_generation: u64,
    pub(super) next_isolate_id: u64,
    pub(super) next_memory_entity_epoch: u64,
}

pub(super) struct WorkerManagerInit {
    pub(super) bootstrap_snapshot: &'static [u8],
    pub(super) kv_store: KvStore,
    pub(super) memory_store: MemoryStore,
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
    pub(super) asset_headers: Option<String>,
    pub(super) compiled_assets: Arc<AssetBundle>,
    pub(super) bindings: DeployBindings,
}

#[derive(Clone, Copy, Default)]
pub(super) struct RuntimeQueueCounters {
    pub(super) requests: usize,
    pub(super) bytes: usize,
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

    pub(super) fn memory_entity_is_leased(&self, owner_key: &str) -> bool {
        self.memory_entity_leases.contains_key(owner_key)
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

    pub(super) fn release_memory_entity_lease(&mut self, lease: &MemoryEntityLease) {
        let should_release = self
            .memory_entity_leases
            .get(&lease.owner_key)
            .map(|current| current.matches(lease))
            .unwrap_or(false);
        if should_release {
            self.memory_entity_leases.remove(&lease.owner_key);
        }
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct PendingQueueKey {
    lane: PendingQueueLane,
    sequence: u64,
}

pub(super) struct PendingInvokeQueue {
    next_sequence: u64,
    by_runtime_request_id: HashMap<String, PendingQueueKey>,
    by_target_isolate_id: HashMap<u64, HashSet<PendingQueueKey>>,
    by_memory_owner_key: HashMap<String, HashSet<PendingQueueKey>>,
    by_enqueued_at: BTreeMap<Instant, HashSet<PendingQueueKey>>,
    targeted_nested: BTreeMap<u64, PendingInvoke>,
    targeted: BTreeMap<u64, PendingInvoke>,
    memory: BTreeMap<u64, PendingInvoke>,
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
            memory: BTreeMap::new(),
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
            .saturating_add(self.memory.len())
            .saturating_add(self.general.len())
    }

    pub(super) fn queued_bytes(&self) -> usize {
        self.iter()
            .map(|pending| pending.queued_bytes)
            .sum::<usize>()
    }

    pub(super) fn push_back(&mut self, pending: PendingInvoke) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let lane = Self::lane_for(&pending);
        let key = PendingQueueKey { lane, sequence };
        self.index_pending(key, &pending);
        let replaced = self.lane_mut(lane).insert(sequence, pending);
        debug_assert!(replaced.is_none());
    }

    pub(super) fn pop_front(&mut self) -> Option<PendingInvoke> {
        let key = self.oldest_key(PendingQueueLane::ALL)?;
        self.remove(key)
    }

    pub(super) fn remove(&mut self, key: PendingQueueKey) -> Option<PendingInvoke> {
        let pending = self.lane_mut(key.lane).remove(&key.sequence)?;
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
            .copied()?;
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
                let pending = self.lane_mut(key.lane).remove(&key.sequence)?;
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
                let pending = self.lane_mut(key.lane).remove(&key.sequence)?;
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
            &mut self.targeted_nested,
            &mut matches,
            &mut drained,
        );
        Self::drain_lane_matching(
            PendingQueueLane::Targeted,
            &mut self.targeted,
            &mut matches,
            &mut drained,
        );
        Self::drain_lane_matching(
            PendingQueueLane::Memory,
            &mut self.memory,
            &mut matches,
            &mut drained,
        );
        Self::drain_lane_matching(
            PendingQueueLane::General,
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
            for (sequence, pending) in self.lane(lane) {
                let key = PendingQueueKey {
                    lane,
                    sequence: *sequence,
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
        selected.map(|(_, value)| value)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &PendingInvoke> {
        self.targeted_nested
            .values()
            .chain(self.targeted.values())
            .chain(self.memory.values())
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

    fn index_pending(&mut self, key: PendingQueueKey, pending: &PendingInvoke) {
        self.by_runtime_request_id
            .insert(pending.runtime_request_id.clone(), key);
        if let Some(target_isolate_id) = pending.target_isolate_id {
            self.by_target_isolate_id
                .entry(target_isolate_id)
                .or_default()
                .insert(key);
        }
        if let Some(memory_route) = pending.memory_route.as_ref() {
            self.by_memory_owner_key
                .entry(memory_route.owner_key.clone())
                .or_default()
                .insert(key);
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
            PendingQueueLane::Memory => &self.memory,
            PendingQueueLane::General => &self.general,
        }
    }

    fn lane_mut(&mut self, lane: PendingQueueLane) -> &mut BTreeMap<u64, PendingInvoke> {
        match lane {
            PendingQueueLane::TargetedNested => &mut self.targeted_nested,
            PendingQueueLane::Targeted => &mut self.targeted,
            PendingQueueLane::Memory => &mut self.memory,
            PendingQueueLane::General => &mut self.general,
        }
    }

    fn drain_lane_matching(
        lane_key: PendingQueueLane,
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
                    },
                    pending,
                ));
            }
        }
    }
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
}

impl MemoryRoute {
    pub(super) fn new(binding: String, key: String) -> Self {
        let owner_key = memory_owner_key(&binding, &key);
        Self {
            binding,
            key,
            owner_epoch: 0,
            owner_key,
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
        assert_eq!(queue.memory.len(), 1);
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
