use crate::state::{STATE_SHARDS, StateStore, WriteOptions, storage_error};
use crate::turso_util::{execute_cached, query_cached};
use common::{PlatformError, Result};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use turso::Value;

pub const DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES: usize = 4096;
pub const DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

pub fn worker_namespace(worker: &str, binding: &str) -> String {
    format!("{}:{worker}{binding}", worker.len())
}

#[derive(Clone)]
pub struct MemoryStore {
    state: Arc<StateStore>,
    profile: Arc<MemoryProfile>,
    snapshots: Arc<Mutex<SnapshotCache>>,
}
pub(crate) type MemorySnapshotKey = (String, String);

pub(crate) struct SnapshotCache {
    entries: HashMap<MemorySnapshotKey, CachedSnapshot>,
    order: BTreeMap<u64, MemorySnapshotKey>,
    next_ordinal: u64,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}
struct CachedSnapshot {
    snapshot: Arc<MemorySnapshot>,
    bytes: usize,
    ordinal: u64,
}

impl Default for SnapshotCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: BTreeMap::new(),
            next_ordinal: 0,
            bytes: 0,
            max_entries: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES,
            max_bytes: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_BYTES,
        }
    }
}

impl SnapshotCache {
    pub(crate) fn remove(&mut self, key: &MemorySnapshotKey) -> Option<Arc<MemorySnapshot>> {
        let removed = self.entries.remove(key)?;
        self.bytes -= removed.bytes;
        self.order.remove(&removed.ordinal);
        Some(removed.snapshot)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryProfileMetricKind {
    JsReadOnlyCommit,
    JsHydrateFull,
    JsTxnCommit,
    OpSnapshot,
    OpApplyBatch,
    StoreSnapshotCacheHit,
    StoreSnapshotCacheMiss,
    StoreSnapshotCacheEviction,
    RuntimeSocketQueueWait,
    RuntimeSocketDispatchWait,
    RuntimeSocketExecution,
    RuntimeSocketCompletionWait,
    RuntimeOutboxDrain,
}

#[derive(Default)]
struct MemoryProfileMetric {
    calls: AtomicU64,
    total_us: AtomicU64,
    total_items: AtomicU64,
    max_us: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryProfileMetricSnapshot {
    pub calls: u64,
    pub total_us: u64,
    pub total_items: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryProfileSnapshot {
    pub enabled: bool,
    pub js_read_only_commit: MemoryProfileMetricSnapshot,
    pub js_hydrate_full: MemoryProfileMetricSnapshot,
    pub js_txn_commit: MemoryProfileMetricSnapshot,
    pub op_snapshot: MemoryProfileMetricSnapshot,
    pub op_apply_batch: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_hit: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_miss: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_eviction: MemoryProfileMetricSnapshot,
    pub runtime_socket_queue_wait: MemoryProfileMetricSnapshot,
    pub runtime_socket_dispatch_wait: MemoryProfileMetricSnapshot,
    pub runtime_socket_execution: MemoryProfileMetricSnapshot,
    pub runtime_socket_completion_wait: MemoryProfileMetricSnapshot,
    pub runtime_outbox_drain: MemoryProfileMetricSnapshot,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MemoryCachePerformanceSnapshot {
    pub snapshot_hits: u64,
    pub snapshot_misses: u64,
    pub snapshot_evictions: u64,
}

#[derive(Default)]
pub struct MemoryProfile {
    enabled: AtomicBool,
    snapshot_cache_hits_total: AtomicU64,
    snapshot_cache_misses_total: AtomicU64,
    snapshot_cache_evictions_total: AtomicU64,
    js_read_only_commit: MemoryProfileMetric,
    js_hydrate_full: MemoryProfileMetric,
    js_txn_commit: MemoryProfileMetric,
    op_snapshot: MemoryProfileMetric,
    op_apply_batch: MemoryProfileMetric,
    store_snapshot_cache_hit: MemoryProfileMetric,
    store_snapshot_cache_miss: MemoryProfileMetric,
    store_snapshot_cache_eviction: MemoryProfileMetric,
    runtime_socket_queue_wait: MemoryProfileMetric,
    runtime_socket_dispatch_wait: MemoryProfileMetric,
    runtime_socket_execution: MemoryProfileMetric,
    runtime_socket_completion_wait: MemoryProfileMetric,
    runtime_outbox_drain: MemoryProfileMetric,
}

#[derive(Debug, Clone)]
pub struct MemorySnapshotEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub version: i64,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct MemorySnapshot {
    pub entries: Vec<MemorySnapshotEntry>,
    pub max_version: i64,
}

#[derive(Debug, Clone)]
pub struct MemoryPointRead {
    pub record: Option<MemorySnapshotEntry>,
    pub max_version: i64,
}

#[derive(Debug, Clone)]
pub struct MemoryBatchMutation {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub deleted: bool,
}

#[derive(Default)]
pub struct MemoryCommit<'a> {
    pub mutations: &'a [MemoryBatchMutation],
    pub command_result: Option<&'a MemoryCommandResultWrite>,
    pub outbox_effects: &'a [MemoryOutboxEffectWrite],
    pub owner_epoch: Option<i64>,
    pub lease: Option<Arc<MemoryLease>>,
}

#[derive(Debug, Clone)]
pub struct MemoryCommandResultWrite {
    pub idempotency_key: String,
    pub result: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MemoryOutboxEffectWrite {
    pub kind: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MemoryBatchApplyResult {
    pub max_version: i64,
}

pub struct MemoryCommandResult {
    pub result: Vec<u8>,
    pub revision: i64,
}

#[allow(dead_code)]
pub struct MemoryOutboxRecord {
    pub effect_id: String,
    pub kind: String,
    pub payload: Vec<u8>,
    pub revision: i64,
    pub status: String,
    pub attempt_count: i64,
    pub next_attempt_at_ms: i64,
}

#[allow(dead_code)]
pub struct MemoryOutboxClaim {
    pub namespace: String,
    pub memory_key: String,
    pub record: MemoryOutboxRecord,
}

#[derive(Debug, Clone)]
pub enum MemoryOutboxDeliveryAction {
    Delivered,
    DroppedTerminal,
    Retry { retry_after: Duration },
}

#[derive(Debug, Clone)]
pub struct MemoryOutboxDeliveryOutcome {
    pub namespace: String,
    pub memory_key: String,
    pub effect_id: String,
    pub action: MemoryOutboxDeliveryAction,
}

include!("memory/profiling.rs");
include!("memory/store.rs");

pub use crate::state::MemoryLease;
