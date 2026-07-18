use common::{PlatformError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, OnceCell, OwnedSemaphorePermit, Semaphore};
use turso::{Builder, Connection, Database, Value, transaction::TransactionBehavior};

use crate::turso_util::{
    checkpoint_database, configure_turso_connection, ensure_storage_migration_table,
    execute_cached, health_check_database, is_retryable_turso_error, query_cached,
    record_storage_retry, record_storage_schema_version, storage_schema_version,
};

const MEMORY_SHARD_HASH_DOMAIN: &[u8] = b"dd-memory-shard-v1\0";
const MEMORY_LAYOUT_MANIFEST_FILE: &str = "memory-layout.json";
const MEMORY_LAYOUT_FORMAT_VERSION: u32 = 1;
const LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION: u32 = 0;
const STABLE_SHA256_MEMORY_SHARD_HASH_VERSION: u32 = 1;
const MEMORY_SHARD_HASH_VERSION: u32 = STABLE_SHA256_MEMORY_SHARD_HASH_VERSION;
pub(crate) const MEMORY_ENTITY_CACHE_STRIPES: usize = 64;
pub(crate) const DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES: usize = 4_096;
pub(crate) const DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
const MEMORY_SCHEMA_VERSION: i64 = 1;

struct MemoryDatabaseEntry {
    slot: Arc<MemoryDatabaseSlot>,
    last_used_at: Instant,
    recency_id: u64,
}

struct MemoryDatabaseSlot {
    handle: OnceCell<Arc<MemoryDatabaseHandle>>,
}

impl MemoryDatabaseSlot {
    fn new() -> Self {
        Self {
            handle: OnceCell::new(),
        }
    }
}

struct MemoryDatabaseHandle {
    database: Arc<Database>,
    readers: Arc<MemoryReadPool>,
    writer: Arc<Mutex<Option<PooledMemoryConnection>>>,
}

struct MemoryReadPool {
    idle: StdMutex<Vec<PooledMemoryConnection>>,
    permits: Arc<Semaphore>,
    global_permits: Arc<Semaphore>,
    max_idle: usize,
}

struct PooledMemoryConnection {
    conn: Connection,
    _permit: MemoryConnectionPermit,
    _pool_permit: Option<OwnedSemaphorePermit>,
}

struct MemoryConnectionPermit {
    _permit: OwnedSemaphorePermit,
    live_connections: Arc<AtomicUsize>,
}

impl Drop for MemoryConnectionPermit {
    fn drop(&mut self) {
        self.live_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

struct MemoryReadConnection {
    pool: Arc<MemoryReadPool>,
    pooled: Option<PooledMemoryConnection>,
    healthy: bool,
}

impl std::ops::Deref for MemoryReadConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self
            .pooled
            .as_ref()
            .expect("memory read connection guard must hold a connection")
            .conn
    }
}

impl Drop for MemoryReadConnection {
    fn drop(&mut self) {
        let Some(pooled) = self.pooled.take() else {
            return;
        };
        if !self.healthy {
            return;
        }
        let mut idle = self
            .pool
            .idle
            .lock()
            .expect("memory read pool mutex should not be poisoned");
        if idle.len() < self.pool.max_idle
            && self.pool.permits.available_permits() > 0
            && self.pool.global_permits.available_permits() > 0
        {
            idle.push(pooled);
        }
    }
}

struct MemoryWriterConnection {
    guard: tokio::sync::OwnedMutexGuard<Option<PooledMemoryConnection>>,
    profile: Arc<MemoryProfile>,
    healthy: bool,
}

impl std::ops::Deref for MemoryWriterConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self
            .guard
            .as_ref()
            .expect("memory writer connection guard must hold a connection")
            .conn
    }
}

impl MemoryWriterConnection {
    fn discard(&mut self) {
        self.profile
            .record(MemoryProfileMetricKind::StoreConnectionDiscard, 0, 1);
        self.healthy = false;
    }
}

impl Drop for MemoryWriterConnection {
    fn drop(&mut self) {
        if !self.healthy {
            self.guard.take();
        }
    }
}

impl MemoryDatabaseCache {
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn touch(&mut self, key: &str, now: Instant) -> Option<Arc<MemoryDatabaseSlot>> {
        let entry = self.entries.get_mut(key)?;
        self.recency.remove(&(entry.last_used_at, entry.recency_id));
        entry.last_used_at = now;
        entry.recency_id = self.next_recency_id;
        self.next_recency_id = self.next_recency_id.wrapping_add(1);
        self.recency
            .insert((entry.last_used_at, entry.recency_id), key.to_string());
        Some(Arc::clone(&entry.slot))
    }

    fn insert_slot(&mut self, key: String, slot: Arc<MemoryDatabaseSlot>, now: Instant) {
        let recency_id = self.next_recency_id;
        self.next_recency_id = self.next_recency_id.wrapping_add(1);
        self.recency.insert((now, recency_id), key.clone());
        self.entries.insert(
            key,
            MemoryDatabaseEntry {
                slot,
                last_used_at: now,
                recency_id,
            },
        );
    }

    fn remove(&mut self, key: &str) -> Option<MemoryDatabaseEntry> {
        let entry = self.entries.remove(key)?;
        self.recency.remove(&(entry.last_used_at, entry.recency_id));
        Some(entry)
    }

    fn prune(&mut self, now: Instant, idle_ttl: Duration, max_entries: usize) -> usize {
        let mut evicted = 0usize;
        while let Some(((last_used_at, recency_id), key)) = self
            .recency
            .iter()
            .next()
            .map(|(recency, key)| (*recency, key.clone()))
        {
            if now.duration_since(last_used_at) < idle_ttl {
                break;
            }
            let remove = self
                .entries
                .get(&key)
                .map(|entry| {
                    entry.recency_id == recency_id
                        && entry.last_used_at == last_used_at
                        && entry.slot.handle.get().is_some()
                })
                .unwrap_or(false);
            self.recency.remove(&(last_used_at, recency_id));
            if remove {
                self.entries.remove(&key);
                evicted = evicted.saturating_add(1);
            }
        }
        while self.entries.len() > max_entries {
            let Some(((last_used_at, recency_id), key)) = self
                .recency
                .iter()
                .next()
                .map(|(recency, key)| (*recency, key.clone()))
            else {
                break;
            };
            let remove = self
                .entries
                .get(&key)
                .map(|entry| {
                    entry.recency_id == recency_id
                        && entry.last_used_at == last_used_at
                        && entry.slot.handle.get().is_some()
                })
                .unwrap_or(false);
            self.recency.remove(&(last_used_at, recency_id));
            if remove {
                self.entries.remove(&key);
                evicted = evicted.saturating_add(1);
            }
        }
        evicted
    }
}

impl MemorySharedSnapshotCache {
    fn get_cloned(&mut self, key: &str, now: Instant) -> Option<MemorySharedSnapshotEntry> {
        self.touch(key, now)?;
        let entry = self.entries.get(key)?;
        Some(MemorySharedSnapshotEntry {
            records: entry.records.clone(),
            loaded_keys: entry.loaded_keys.clone(),
            complete: entry.complete,
            max_version: entry.max_version,
            last_used_at: entry.last_used_at,
            recency_id: entry.recency_id,
            approximate_bytes: entry.approximate_bytes,
        })
    }

    fn touch(&mut self, key: &str, now: Instant) -> Option<()> {
        let entry = self.entries.get_mut(key)?;
        self.recency.remove(&(entry.last_used_at, entry.recency_id));
        entry.last_used_at = now;
        entry.recency_id = self.next_recency_id;
        self.next_recency_id = self.next_recency_id.wrapping_add(1);
        self.recency
            .insert((entry.last_used_at, entry.recency_id), key.to_string());
        Some(())
    }

    fn remove(&mut self, key: &str) -> Option<MemorySharedSnapshotEntry> {
        let entry = self.entries.remove(key)?;
        self.approximate_bytes = self
            .approximate_bytes
            .saturating_sub(entry.approximate_bytes);
        self.recency.remove(&(entry.last_used_at, entry.recency_id));
        Some(entry)
    }

    fn insert_or_replace(&mut self, key: String, mut entry: MemorySharedSnapshotEntry) {
        if let Some(old) = self.entries.remove(&key) {
            self.approximate_bytes = self.approximate_bytes.saturating_sub(old.approximate_bytes);
            self.recency.remove(&(old.last_used_at, old.recency_id));
        }
        entry.recency_id = self.next_recency_id;
        self.next_recency_id = self.next_recency_id.wrapping_add(1);
        self.approximate_bytes = self
            .approximate_bytes
            .saturating_add(entry.approximate_bytes);
        self.recency
            .insert((entry.last_used_at, entry.recency_id), key.clone());
        self.entries.insert(key, entry);
    }

    fn prune(
        &mut self,
        now: Instant,
        idle_ttl: Duration,
        max_entries: usize,
        max_bytes: usize,
    ) -> usize {
        let mut evicted = 0usize;
        while let Some(((last_used_at, recency_id), key)) = self
            .recency
            .iter()
            .next()
            .map(|(recency, key)| (*recency, key.clone()))
        {
            if now.duration_since(last_used_at) < idle_ttl {
                break;
            }
            self.recency.remove(&(last_used_at, recency_id));
            let remove = self
                .entries
                .get(&key)
                .map(|entry| entry.recency_id == recency_id && entry.last_used_at == last_used_at)
                .unwrap_or(false);
            if remove {
                self.remove(&key);
                evicted = evicted.saturating_add(1);
            }
        }
        while self.entries.len() > max_entries || self.approximate_bytes > max_bytes {
            let Some(((last_used_at, recency_id), key)) = self
                .recency
                .iter()
                .next()
                .map(|(recency, key)| (*recency, key.clone()))
            else {
                break;
            };
            self.recency.remove(&(last_used_at, recency_id));
            let remove = self
                .entries
                .get(&key)
                .map(|entry| entry.recency_id == recency_id && entry.last_used_at == last_used_at)
                .unwrap_or(false);
            if remove {
                self.remove(&key);
                evicted = evicted.saturating_add(1);
            }
        }
        evicted
    }
}

struct MemorySharedSnapshotEntry {
    records: Arc<HashMap<String, MemorySnapshotEntry>>,
    loaded_keys: Arc<HashSet<String>>,
    complete: bool,
    max_version: i64,
    last_used_at: Instant,
    recency_id: u64,
    approximate_bytes: usize,
}

#[derive(Default)]
struct MemoryDatabaseCache {
    entries: HashMap<String, MemoryDatabaseEntry>,
    recency: BTreeMap<(Instant, u64), String>,
    next_recency_id: u64,
}

#[derive(Default)]
struct MemorySharedSnapshotCache {
    entries: HashMap<String, MemorySharedSnapshotEntry>,
    recency: BTreeMap<(Instant, u64), String>,
    next_recency_id: u64,
    approximate_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MemoryLayoutManifest {
    format_version: u32,
    shard_hash_version: u32,
    namespace_shards: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    namespace_shard_hash_versions: BTreeMap<String, u32>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    namespace_key_shard_overrides: BTreeMap<String, BTreeMap<String, usize>>,
}

impl MemoryLayoutManifest {
    fn current(namespace_shards: usize) -> Self {
        Self {
            format_version: MEMORY_LAYOUT_FORMAT_VERSION,
            shard_hash_version: MEMORY_SHARD_HASH_VERSION,
            namespace_shards,
            namespace_shard_hash_versions: BTreeMap::new(),
            namespace_key_shard_overrides: BTreeMap::new(),
        }
    }
}

struct MemoryShard {
    databases: Mutex<MemoryDatabaseCache>,
    cache_stripes: Box<[MemoryEntityCacheStripe]>,
    version: AtomicU64,
}

struct MemoryEntityCacheStripe {
    state: Mutex<MemoryEntityCacheState>,
}

#[derive(Default)]
struct MemoryEntityCacheState {
    memory_versions: HashMap<String, i64>,
    snapshots: MemorySharedSnapshotCache,
}

impl MemoryShard {
    fn new(version_floor: u64) -> Self {
        Self {
            databases: Mutex::new(MemoryDatabaseCache::default()),
            cache_stripes: (0..MEMORY_ENTITY_CACHE_STRIPES)
                .map(|_| MemoryEntityCacheStripe {
                    state: Mutex::new(MemoryEntityCacheState::default()),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            version: AtomicU64::new(version_floor.max(1)),
        }
    }

    fn observe_version(&self, version: i64) {
        if version < 0 {
            return;
        }
        let floor = version as u64 + 1;
        let mut current = self.version.load(Ordering::Relaxed);
        while current < floor {
            match self.version.compare_exchange_weak(
                current,
                floor,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Clone)]
pub struct MemoryStore {
    root_dir: Arc<PathBuf>,
    shards: Arc<[MemoryShard]>,
    db_cache_max_open: usize,
    db_idle_ttl: Duration,
    db_read_connections_per_database: usize,
    db_connection_permits: Arc<Semaphore>,
    db_live_connections: Arc<AtomicUsize>,
    db_peak_connections: Arc<AtomicUsize>,
    namespace_shards: usize,
    shard_hash_version: u32,
    namespace_shard_hash_versions: Arc<BTreeMap<String, u32>>,
    namespace_key_shard_overrides: Arc<BTreeMap<String, BTreeMap<String, usize>>>,
    snapshot_cache_max_entries: usize,
    snapshot_cache_max_bytes: usize,
    owner_epoch_floor: Arc<AtomicU64>,
    profile: Arc<MemoryProfile>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryProfileMetricKind {
    JsReadOnlyTotal,
    JsHydrateFull,
    JsHydrateKeys,
    JsTxnCommit,
    JsCacheHit,
    JsCacheMiss,
    JsCacheStale,
    OpRead,
    OpSnapshot,
    OpVersionIfNewer,
    OpApplyBatch,
    StoreRead,
    StoreSnapshot,
    StoreSnapshotKeys,
    StoreVersionIfNewer,
    StoreApplyBatch,
    StoreApplyBatchValidate,
    StoreApplyBatchWrite,
    StoreDatabaseCacheHit,
    StoreDatabaseCacheMiss,
    StoreDatabaseCacheEviction,
    StoreConnectionCreate,
    StoreConnectionReuse,
    StoreReaderPoolWait,
    StoreWriterLaneWait,
    StoreWriterBusyRetry,
    StoreConnectionDiscard,
    StoreConnectionsLive,
    StoreConnectionsPeak,
    StoreSnapshotCacheHit,
    StoreSnapshotCacheMiss,
    StoreSnapshotCacheEviction,
    EntityCacheLockWait,
    EntityCacheStripeCount,
    EntityCachePeakEntriesPerStripe,
    EntityCacheMaxEntriesPerStripe,
    RuntimeAtomicInvokeEventWait,
    RuntimeAtomicQueueWait,
    RuntimeAtomicDispatchWait,
    RuntimeAtomicExecution,
    RuntimeAtomicCompletionWait,
    RuntimeAtomicOutboxDrain,
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
    pub js_read_only_total: MemoryProfileMetricSnapshot,
    pub js_hydrate_full: MemoryProfileMetricSnapshot,
    pub js_hydrate_keys: MemoryProfileMetricSnapshot,
    pub js_txn_commit: MemoryProfileMetricSnapshot,
    pub js_cache_hit: MemoryProfileMetricSnapshot,
    pub js_cache_miss: MemoryProfileMetricSnapshot,
    pub js_cache_stale: MemoryProfileMetricSnapshot,
    pub op_read: MemoryProfileMetricSnapshot,
    pub op_snapshot: MemoryProfileMetricSnapshot,
    pub op_version_if_newer: MemoryProfileMetricSnapshot,
    pub op_apply_batch: MemoryProfileMetricSnapshot,
    pub store_read: MemoryProfileMetricSnapshot,
    pub store_snapshot: MemoryProfileMetricSnapshot,
    pub store_snapshot_keys: MemoryProfileMetricSnapshot,
    pub store_version_if_newer: MemoryProfileMetricSnapshot,
    pub store_apply_batch: MemoryProfileMetricSnapshot,
    pub store_apply_batch_validate: MemoryProfileMetricSnapshot,
    pub store_apply_batch_write: MemoryProfileMetricSnapshot,
    pub store_database_cache_hit: MemoryProfileMetricSnapshot,
    pub store_database_cache_miss: MemoryProfileMetricSnapshot,
    pub store_database_cache_eviction: MemoryProfileMetricSnapshot,
    pub store_connection_create: MemoryProfileMetricSnapshot,
    pub store_connection_reuse: MemoryProfileMetricSnapshot,
    pub store_reader_pool_wait: MemoryProfileMetricSnapshot,
    pub store_writer_lane_wait: MemoryProfileMetricSnapshot,
    pub store_writer_busy_retry: MemoryProfileMetricSnapshot,
    pub store_connection_discard: MemoryProfileMetricSnapshot,
    pub store_connections_live: MemoryProfileMetricSnapshot,
    pub store_connections_peak: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_hit: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_miss: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_eviction: MemoryProfileMetricSnapshot,
    pub entity_cache_lock_wait: MemoryProfileMetricSnapshot,
    pub entity_cache_stripe_count: MemoryProfileMetricSnapshot,
    pub entity_cache_peak_entries_per_stripe: MemoryProfileMetricSnapshot,
    pub entity_cache_max_entries_per_stripe: MemoryProfileMetricSnapshot,
    pub runtime_atomic_invoke_event_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_queue_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_dispatch_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_execution: MemoryProfileMetricSnapshot,
    pub runtime_atomic_completion_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_outbox_drain: MemoryProfileMetricSnapshot,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct MemoryCachePerformanceSnapshot {
    pub(crate) snapshot_hits: u64,
    pub(crate) snapshot_misses: u64,
    pub(crate) snapshot_evictions: u64,
}

#[derive(Default)]
pub struct MemoryProfile {
    enabled: AtomicBool,
    snapshot_cache_hits_total: AtomicU64,
    snapshot_cache_misses_total: AtomicU64,
    snapshot_cache_evictions_total: AtomicU64,
    js_read_only_total: MemoryProfileMetric,
    js_hydrate_full: MemoryProfileMetric,
    js_hydrate_keys: MemoryProfileMetric,
    js_txn_commit: MemoryProfileMetric,
    js_cache_hit: MemoryProfileMetric,
    js_cache_miss: MemoryProfileMetric,
    js_cache_stale: MemoryProfileMetric,
    op_read: MemoryProfileMetric,
    op_snapshot: MemoryProfileMetric,
    op_version_if_newer: MemoryProfileMetric,
    op_apply_batch: MemoryProfileMetric,
    store_read: MemoryProfileMetric,
    store_snapshot: MemoryProfileMetric,
    store_snapshot_keys: MemoryProfileMetric,
    store_version_if_newer: MemoryProfileMetric,
    store_apply_batch: MemoryProfileMetric,
    store_apply_batch_validate: MemoryProfileMetric,
    store_apply_batch_write: MemoryProfileMetric,
    store_database_cache_hit: MemoryProfileMetric,
    store_database_cache_miss: MemoryProfileMetric,
    store_database_cache_eviction: MemoryProfileMetric,
    store_connection_create: MemoryProfileMetric,
    store_connection_reuse: MemoryProfileMetric,
    store_reader_pool_wait: MemoryProfileMetric,
    store_writer_lane_wait: MemoryProfileMetric,
    store_writer_busy_retry: MemoryProfileMetric,
    store_connection_discard: MemoryProfileMetric,
    store_connections_live: MemoryProfileMetric,
    store_connections_peak: MemoryProfileMetric,
    store_snapshot_cache_hit: MemoryProfileMetric,
    store_snapshot_cache_miss: MemoryProfileMetric,
    store_snapshot_cache_eviction: MemoryProfileMetric,
    entity_cache_lock_wait: MemoryProfileMetric,
    entity_cache_stripe_count: MemoryProfileMetric,
    entity_cache_peak_entries_per_stripe: MemoryProfileMetric,
    entity_cache_max_entries_per_stripe: MemoryProfileMetric,
    runtime_atomic_invoke_event_wait: MemoryProfileMetric,
    runtime_atomic_queue_wait: MemoryProfileMetric,
    runtime_atomic_dispatch_wait: MemoryProfileMetric,
    runtime_atomic_execution: MemoryProfileMetric,
    runtime_atomic_completion_wait: MemoryProfileMetric,
    runtime_atomic_outbox_drain: MemoryProfileMetric,
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

struct MemoryBatchCommitOutcome {
    result: MemoryBatchApplyResult,
    cache_mutations: Vec<MemoryBatchMutation>,
}

type MemoryTransactionResult<T> = std::result::Result<T, MemoryTransactionError>;

#[derive(Debug)]
enum MemoryTransactionError {
    Storage(turso::Error),
    Platform(PlatformError),
}

impl MemoryTransactionError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Storage(error) if is_retryable_turso_error(error))
    }
}

impl From<turso::Error> for MemoryTransactionError {
    fn from(error: turso::Error) -> Self {
        Self::Storage(error)
    }
}

impl From<PlatformError> for MemoryTransactionError {
    fn from(error: PlatformError) -> Self {
        Self::Platform(error)
    }
}

impl From<MemoryTransactionError> for PlatformError {
    fn from(error: MemoryTransactionError) -> Self {
        match error {
            MemoryTransactionError::Storage(error) => memory_error_after_retry(error),
            MemoryTransactionError::Platform(error) => error,
        }
    }
}

// MemoryStore construction and state/outbox operations.
include!("memory/store.rs");

// Database handles, pooled connections, and transaction-local reads.
include!("memory/connections.rs");

// Shard routing plus shared entity snapshot-cache operations.
include!("memory/cache.rs");

// Direct persistence row helpers and transactional validation.
include!("memory/persistence.rs");

// Runtime and store profiling counters/snapshots.
include!("memory/profiling.rs");

// Schema creation, compatibility migration, and durable version floors.
include!("memory/migrations.rs");

// Persistent shard-layout manifest adoption and legacy discovery.
include!("memory/layout.rs");

// Connection setup, encodings, hashing, and small shared utilities.
include!("memory/utilities.rs");

// MemoryStore behavior coverage.
include!("memory/tests.rs");
