use crate::state::{STATE_SHARDS, StateStore, WriteOptions, WriteOutcome, storage_error};
use crate::turso_util::{execute_cached, query_cached};
use bytes::Bytes;
use common::{PlatformError, Result};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
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
    loads: HashMap<MemorySnapshotKey, Weak<tokio::sync::Mutex<()>>>,
    order: BTreeMap<u64, MemorySnapshotKey>,
    next_ordinal: u64,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
    #[cfg(test)]
    before_fill: Option<Arc<SnapshotLoadPause>>,
}

#[cfg(test)]
#[derive(Default)]
struct SnapshotLoadPause {
    loaded: tokio::sync::Notify,
    resume: tokio::sync::Notify,
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
            loads: HashMap::new(),
            order: BTreeMap::new(),
            next_ordinal: 0,
            bytes: 0,
            max_entries: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES,
            max_bytes: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_BYTES,
            #[cfg(test)]
            before_fill: None,
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

    pub(crate) fn publish_committed(
        snapshots: &Mutex<Self>,
        changes: &[(&MemorySnapshotKey, Option<&MemorySnapshotChange>)],
    ) {
        if changes.is_empty() {
            return;
        }
        let mut grouped = HashMap::<_, Vec<_>>::new();
        for (key, change) in changes {
            grouped.entry(*key).or_default().push(*change);
        }
        let pending = {
            let cache = snapshots.lock().expect("memory snapshots lock poisoned");
            grouped
                .into_iter()
                .filter_map(|(key, changes)| {
                    cache
                        .entries
                        .get(key)
                        .map(|cached| (key, Arc::clone(&cached.snapshot), changes))
                })
                .collect::<Vec<_>>()
        };
        // Copy and merge outside the shared lock. The lease stays with the queued
        // commit until publication and acknowledgement, including caller cancellation.
        let updated = pending
            .into_iter()
            .map(|(key, original, changes)| {
                let next = if changes.iter().any(Option::is_none) {
                    None
                } else {
                    let mut next = (*original).clone();
                    let mut complete = true;
                    for change in changes.into_iter().flatten() {
                        // A concurrent SQL fill can already contain the entire committed group.
                        if next.max_version >= change.version {
                            continue;
                        }
                        if next.max_version != change.previous_version {
                            complete = false;
                            break;
                        }
                        for mutation in &change.commit.mutations {
                            let entry = MemorySnapshotEntry {
                                key: mutation.key.clone(),
                                value: mutation.value.clone(),
                                encoding: mutation.encoding.clone(),
                                deleted: mutation.deleted,
                                version: change.version,
                            };
                            match next
                                .entries
                                .binary_search_by(|entry| entry.key.cmp(&mutation.key))
                            {
                                Ok(index) => next.entries[index] = entry,
                                Err(index) => next.entries.insert(index, entry),
                            }
                        }
                        next.max_version = change.version;
                    }
                    complete.then(|| {
                        let bytes = next.cache_bytes(key);
                        (Arc::new(next), bytes)
                    })
                };
                (key, original, next)
            })
            .collect::<Vec<_>>();
        let mut removed = Vec::new();
        {
            let mut cache = snapshots.lock().expect("memory snapshots lock poisoned");
            for (key, original, next) in &updated {
                let Some(cached) = cache.entries.get(*key) else {
                    continue;
                };
                // Eviction, resized budgets, or a newer fill win over this derived update.
                if !Arc::ptr_eq(&cached.snapshot, original) {
                    continue;
                }
                let previous_bytes = cached.bytes;
                if let Some((snapshot, bytes)) = next
                    && *bytes <= cache.max_bytes - (cache.bytes - previous_bytes)
                {
                    cache.bytes = cache.bytes - previous_bytes + bytes;
                    let cached = cache
                        .entries
                        .get_mut(*key)
                        .expect("matched cached snapshot");
                    cached.bytes = *bytes;
                    removed.push(std::mem::replace(
                        &mut cached.snapshot,
                        Arc::clone(snapshot),
                    ));
                } else {
                    removed.push(cache.remove(key).expect("matched cached snapshot"));
                }
            }
        }
        drop(removed);
    }
}
pub(crate) struct MemorySnapshotChange {
    pub previous_version: i64,
    pub version: i64,
    pub commit: Arc<MemoryCommit>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryProfileMetricKind {
    JsReadOnlyCommit,
    JsTxnBegin,
    JsTxnCommit,
    OpSnapshot,
    OpApplyBatch,
    StoreLease,
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
    pub js_txn_begin: MemoryProfileMetricSnapshot,
    pub js_txn_commit: MemoryProfileMetricSnapshot,
    pub op_snapshot: MemoryProfileMetricSnapshot,
    pub op_apply_batch: MemoryProfileMetricSnapshot,
    pub store_lease: MemoryProfileMetricSnapshot,
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
    js_txn_begin: MemoryProfileMetric,
    js_txn_commit: MemoryProfileMetric,
    op_snapshot: MemoryProfileMetric,
    op_apply_batch: MemoryProfileMetric,
    store_lease: MemoryProfileMetric,
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
    pub value: Bytes,
    pub encoding: String,
    pub version: i64,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct MemorySnapshot {
    pub entries: Vec<MemorySnapshotEntry>,
    pub max_version: i64,
}

impl MemorySnapshot {
    fn cache_bytes(&self, key: &MemorySnapshotKey) -> usize {
        key.0.len()
            + key.1.len()
            + 128
            + self
                .entries
                .iter()
                .map(|entry| entry.key.len() + entry.value.len() + entry.encoding.len() + 96)
                .sum::<usize>()
    }
}

#[derive(Debug, Clone)]
pub struct MemoryPointRead {
    pub record: Option<MemorySnapshotEntry>,
    pub max_version: i64,
}

#[derive(Debug, Clone)]
pub struct MemoryBatchMutation {
    pub key: String,
    pub value: Bytes,
    pub encoding: String,
    pub deleted: bool,
}

#[derive(Default)]
pub struct MemoryCommit {
    pub mutations: Vec<MemoryBatchMutation>,
    pub command_result: Option<MemoryCommandResultWrite>,
    pub outbox_effects: Vec<MemoryOutboxEffectWrite>,
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

pub use crate::state::MemoryLease;

include!("memory/profiling.rs");
include!("memory/store.rs");
