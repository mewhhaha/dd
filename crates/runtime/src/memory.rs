use common::{PlatformError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, OnceCell};
use turso::{Builder, Connection, Database, Value};

use crate::turso_util::{configure_turso_connection, is_retryable_turso_error};

const MEMORY_SHARD_HASH_DOMAIN: &[u8] = b"dd-memory-shard-v1\0";
const MEMORY_LAYOUT_MANIFEST_FILE: &str = "memory-layout.json";
const MEMORY_LAYOUT_FORMAT_VERSION: u32 = 1;
const LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION: u32 = 0;
const STABLE_SHA256_MEMORY_SHARD_HASH_VERSION: u32 = 1;
const MEMORY_SHARD_HASH_VERSION: u32 = STABLE_SHA256_MEMORY_SHARD_HASH_VERSION;

struct MemoryDatabaseEntry {
    slot: Arc<MemoryDatabaseSlot>,
    last_used_at: Instant,
    recency_id: u64,
}

struct MemoryDatabaseSlot {
    database: OnceCell<Arc<Database>>,
}

impl MemoryDatabaseSlot {
    fn new() -> Self {
        Self {
            database: OnceCell::new(),
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
                        && entry.slot.database.get().is_some()
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
                        && entry.slot.database.get().is_some()
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
}

impl MemoryLayoutManifest {
    fn current(namespace_shards: usize) -> Self {
        Self {
            format_version: MEMORY_LAYOUT_FORMAT_VERSION,
            shard_hash_version: MEMORY_SHARD_HASH_VERSION,
            namespace_shards,
        }
    }
}

struct MemoryShard {
    databases: Mutex<MemoryDatabaseCache>,
    memory_versions: Mutex<HashMap<String, i64>>,
    shared_snapshots: Mutex<MemorySharedSnapshotCache>,
    version: AtomicU64,
}

impl MemoryShard {
    fn new(version_floor: u64) -> Self {
        Self {
            databases: Mutex::new(MemoryDatabaseCache::default()),
            memory_versions: Mutex::new(HashMap::new()),
            shared_snapshots: Mutex::new(MemorySharedSnapshotCache::default()),
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
    namespace_shards: usize,
    shard_hash_version: u32,
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
    StoreSnapshotCacheHit,
    StoreSnapshotCacheMiss,
    StoreSnapshotCacheEviction,
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
    pub store_snapshot_cache_hit: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_miss: MemoryProfileMetricSnapshot,
    pub store_snapshot_cache_eviction: MemoryProfileMetricSnapshot,
    pub runtime_atomic_invoke_event_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_queue_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_dispatch_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_execution: MemoryProfileMetricSnapshot,
    pub runtime_atomic_completion_wait: MemoryProfileMetricSnapshot,
    pub runtime_atomic_outbox_drain: MemoryProfileMetricSnapshot,
}

#[derive(Default)]
pub struct MemoryProfile {
    enabled: AtomicBool,
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
    store_snapshot_cache_hit: MemoryProfileMetric,
    store_snapshot_cache_miss: MemoryProfileMetric,
    store_snapshot_cache_eviction: MemoryProfileMetric,
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

impl MemoryStore {
    pub async fn new(
        root_dir: PathBuf,
        namespace_shards: usize,
        db_cache_max_open: usize,
        db_idle_ttl: Duration,
    ) -> Result<Self> {
        std::fs::create_dir_all(&root_dir).map_err(memory_error)?;
        if namespace_shards == 0 {
            return Err(PlatformError::internal(
                "memory_namespace_shards must be greater than 0",
            ));
        }
        if db_cache_max_open == 0 {
            return Err(PlatformError::internal(
                "memory_db_cache_max_open must be greater than 0",
            ));
        }
        if db_idle_ttl.is_zero() {
            return Err(PlatformError::internal(
                "memory_db_idle_ttl must be greater than 0",
            ));
        }
        let layout = load_or_adopt_memory_layout(&root_dir, namespace_shards).await?;
        if layout.namespace_shards != namespace_shards {
            return Err(memory_layout_mismatch_error(
                &root_dir,
                namespace_shards,
                layout.namespace_shards,
            ));
        }
        let floors = detect_memory_floors(&root_dir, namespace_shards).await?;
        let shards = floors
            .version_floors
            .iter()
            .copied()
            .map(MemoryShard::new)
            .collect::<Vec<_>>();
        let store = Self {
            root_dir: Arc::new(root_dir),
            shards: Arc::from(shards),
            db_cache_max_open,
            db_idle_ttl,
            namespace_shards,
            shard_hash_version: layout.shard_hash_version,
            snapshot_cache_max_entries: db_cache_max_open.max(64),
            snapshot_cache_max_bytes: db_cache_max_open.max(64).saturating_mul(64 * 1024),
            owner_epoch_floor: Arc::new(AtomicU64::new(floors.owner_epoch_floor.max(1))),
            profile: Arc::new(MemoryProfile::default()),
        };
        Ok(store)
    }

    pub fn owner_epoch_floor(&self) -> u64 {
        self.owner_epoch_floor.load(Ordering::Relaxed)
    }

    pub fn set_profile_enabled(&self, enabled: bool) {
        self.profile.set_enabled(enabled);
    }

    pub fn record_profile(&self, metric: MemoryProfileMetricKind, duration_us: u64, items: u64) {
        self.profile.record(metric, duration_us, items);
    }

    pub fn take_profile_snapshot_and_reset(&self) -> MemoryProfileSnapshot {
        self.profile.take_snapshot_and_reset()
    }

    pub fn reset_profile(&self) {
        self.profile.reset();
    }

    pub fn namespace_shards(&self) -> usize {
        self.namespace_shards
    }

    pub fn shard_index_for_key(&self, memory_key: &str) -> usize {
        self.shard_index(memory_key)
    }

    pub async fn snapshot(&self, namespace: &str, memory_key: &str) -> Result<MemorySnapshot> {
        let started = Instant::now();
        if let Some(snapshot) = self.cached_full_snapshot(namespace, memory_key).await {
            self.observe_version(memory_key, snapshot.max_version);
            self.observe_memory_version(namespace, memory_key, snapshot.max_version)
                .await;
            self.record_profile(
                MemoryProfileMetricKind::StoreSnapshot,
                started.elapsed().as_micros() as u64,
                snapshot.entries.len() as u64,
            );
            return Ok(snapshot);
        }
        let conn = self.connect(namespace, memory_key).await?;
        let mut rows = conn
            .query(
                "SELECT item_key, value_blob, encoding, value, version, deleted
                 FROM memory_state
                 WHERE entity_key = ?1
                 ORDER BY item_key ASC",
                (memory_key,),
            )
            .await
            .map_err(memory_error)?;

        let mut entries = Vec::new();
        let mut max_version = -1i64;
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            let key: String = row.get::<String>(0).map_err(memory_error)?;
            let value_blob: Option<Vec<u8>> =
                row.get::<Option<Vec<u8>>>(1).map_err(memory_error)?;
            let encoding: String = row.get::<String>(2).map_err(memory_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(memory_error)?;
            let version: i64 = row.get::<i64>(4).map_err(memory_error)?;
            let deleted: i64 = row.get::<i64>(5).map_err(memory_error)?;
            max_version = max_version.max(version);
            entries.push(MemorySnapshotEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
                version,
                deleted: deleted != 0,
            });
        }
        self.observe_version(memory_key, max_version);
        self.observe_memory_version(namespace, memory_key, max_version)
            .await;
        let snapshot = MemorySnapshot {
            entries,
            max_version,
        };
        self.put_full_snapshot(namespace, memory_key, &snapshot)
            .await;
        self.record_profile(
            MemoryProfileMetricKind::StoreSnapshot,
            started.elapsed().as_micros() as u64,
            snapshot.entries.len() as u64,
        );
        Ok(snapshot)
    }

    pub async fn point_read(
        &self,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
    ) -> Result<MemoryPointRead> {
        let started = Instant::now();
        let item_key = item_key.trim();
        if item_key.is_empty() {
            return Err(PlatformError::runtime("memory item key must not be empty"));
        }
        if let Some(point) = self
            .cached_point_read(namespace, memory_key, item_key)
            .await
        {
            self.observe_version(memory_key, point.max_version);
            self.observe_memory_version(namespace, memory_key, point.max_version)
                .await;
            self.record_profile(
                MemoryProfileMetricKind::StoreRead,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(point);
        }

        let conn = self.connect(namespace, memory_key).await?;
        let record = self.record_for_key(&conn, memory_key, item_key).await?;
        let max_version = self
            .max_version_for_memory(&conn, memory_key)
            .await?
            .unwrap_or(-1);
        self.observe_version(memory_key, max_version);
        self.observe_memory_version(namespace, memory_key, max_version)
            .await;
        self.put_partial_snapshot(
            namespace,
            memory_key,
            max_version,
            record.clone().into_iter().collect::<Vec<_>>(),
            std::iter::once(item_key.to_string()),
            false,
        )
        .await;
        self.record_profile(
            MemoryProfileMetricKind::StoreRead,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok(MemoryPointRead {
            record,
            max_version,
        })
    }

    pub async fn snapshot_keys(
        &self,
        namespace: &str,
        memory_key: &str,
        keys: &[String],
    ) -> Result<MemorySnapshot> {
        if keys.is_empty() {
            return self.snapshot(namespace, memory_key).await;
        }
        let started = Instant::now();
        let filtered_keys = keys
            .iter()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty())
            .collect::<Vec<_>>();
        if filtered_keys.is_empty() {
            let conn = self.connect(namespace, memory_key).await?;
            let max_version = self
                .max_version_for_memory(&conn, memory_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(memory_key, max_version);
            return Ok(MemorySnapshot {
                entries: Vec::new(),
                max_version,
            });
        }
        if let Some(snapshot) = self
            .cached_keys_snapshot(namespace, memory_key, &filtered_keys)
            .await
        {
            self.record_profile(
                MemoryProfileMetricKind::StoreSnapshotKeys,
                started.elapsed().as_micros() as u64,
                filtered_keys.len() as u64,
            );
            return Ok(snapshot);
        }
        let conn = self.connect(namespace, memory_key).await?;
        let placeholders = (0..filtered_keys.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT item_key, value_blob, encoding, value, version, deleted
             FROM memory_state
             WHERE entity_key = ?1 AND item_key IN ({placeholders})
             ORDER BY item_key ASC"
        );
        let mut params = Vec::with_capacity(filtered_keys.len() + 1);
        params.push(Value::Text(memory_key.to_string()));
        params.extend(
            filtered_keys
                .iter()
                .map(|key| Value::Text((*key).to_string())),
        );
        let mut entries = Vec::new();
        let mut rows = conn.query(&sql, params).await.map_err(memory_error)?;
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            let key: String = row.get::<String>(0).map_err(memory_error)?;
            let value_blob: Option<Vec<u8>> =
                row.get::<Option<Vec<u8>>>(1).map_err(memory_error)?;
            let encoding: String = row.get::<String>(2).map_err(memory_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(memory_error)?;
            let version: i64 = row.get::<i64>(4).map_err(memory_error)?;
            let deleted: i64 = row.get::<i64>(5).map_err(memory_error)?;
            entries.push(MemorySnapshotEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
                version,
                deleted: deleted != 0,
            });
        }
        let max_version = self
            .max_version_for_memory(&conn, memory_key)
            .await?
            .unwrap_or(-1);
        self.observe_version(memory_key, max_version);
        self.observe_memory_version(namespace, memory_key, max_version)
            .await;
        self.put_partial_snapshot(
            namespace,
            memory_key,
            max_version,
            entries.clone(),
            filtered_keys.iter().cloned(),
            false,
        )
        .await;
        self.record_profile(
            MemoryProfileMetricKind::StoreSnapshotKeys,
            started.elapsed().as_micros() as u64,
            filtered_keys.len() as u64,
        );
        Ok(MemorySnapshot {
            entries,
            max_version,
        })
    }

    pub async fn version_if_newer(
        &self,
        namespace: &str,
        memory_key: &str,
        known_version: i64,
    ) -> Result<Option<i64>> {
        let started = Instant::now();
        let memory_key = memory_key.trim();
        if memory_key.is_empty() {
            return Err(PlatformError::runtime("memory key must not be empty"));
        }
        let version_key = Self::memory_version_key(namespace, memory_key);
        if let Some(current) = self
            .shard_for_key(memory_key)
            .memory_versions
            .lock()
            .await
            .get(&version_key)
            .copied()
        {
            self.record_profile(
                MemoryProfileMetricKind::StoreVersionIfNewer,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok((current > known_version).then_some(current));
        }
        let conn = self.connect(namespace, memory_key).await?;
        let current = self
            .max_version_for_memory(&conn, memory_key)
            .await?
            .unwrap_or(-1);
        self.observe_memory_version(namespace, memory_key, current)
            .await;
        self.record_profile(
            MemoryProfileMetricKind::StoreVersionIfNewer,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok((current > known_version).then_some(current))
    }

    pub async fn apply_batch(
        &self,
        namespace: &str,
        memory_key: &str,
        mutations: &[MemoryBatchMutation],
        command_result: Option<&MemoryCommandResultWrite>,
        outbox_effects: &[MemoryOutboxEffectWrite],
        owner_epoch: Option<i64>,
    ) -> Result<MemoryBatchApplyResult> {
        let started = Instant::now();
        let conn = self.connect(namespace, memory_key).await?;
        if mutations.is_empty() && command_result.is_none() && outbox_effects.is_empty() {
            let max_version = self
                .max_version_for_memory(&conn, memory_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(memory_key, max_version);
            self.record_profile(
                MemoryProfileMetricKind::StoreApplyBatch,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(MemoryBatchApplyResult { max_version });
        }

        for mutation in mutations {
            if mutation.key.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory batch mutation key must not be empty",
                ));
            }
            if !mutation.deleted
                && mutation.encoding != ENCODING_UTF8
                && mutation.encoding != ENCODING_V8SC
            {
                return Err(PlatformError::bad_request(format!(
                    "unsupported memory storage encoding: {}",
                    mutation.encoding
                )));
            }
        }

        if let Some(command_result) = command_result {
            if command_result.idempotency_key.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory command idempotency key must not be empty",
                ));
            }
            if command_result.idempotency_key.len() > 512 {
                return Err(PlatformError::bad_request(
                    "memory command idempotency key must be at most 512 characters",
                ));
            }
        }
        for effect in outbox_effects {
            if effect.kind.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory outbox effect kind must not be empty",
                ));
            }
        }

        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(memory_error(error)),
            }

            let outcome = async {
                let validate_started = Instant::now();
                let current = self
                    .max_version_for_memory(&conn, memory_key)
                    .await?
                    .unwrap_or(-1);
                self.validate_owner_epoch(&conn, memory_key, owner_epoch)
                    .await?;
                self.record_profile(
                    MemoryProfileMetricKind::StoreApplyBatchValidate,
                    validate_started.elapsed().as_micros() as u64,
                    1,
                );

                let write_started = Instant::now();
                let commit_version = if !mutations.is_empty() || !outbox_effects.is_empty() {
                    Some(self.reserve_version_after(memory_key, current))
                } else {
                    None
                };

                for mutation in mutations {
                    let version =
                        commit_version.expect("mutation commits must reserve a canonical version");
                    upsert_memory_state_row(
                        &conn,
                        memory_key,
                        mutation.key.as_str(),
                        mutation.value.as_slice(),
                        mutation.encoding.as_str(),
                        mutation.deleted,
                        version,
                    )
                    .await?;
                }

                let max_version = if let Some(version) = commit_version {
                    version
                } else {
                    current
                };
                if !mutations.is_empty() || !outbox_effects.is_empty() {
                    upsert_memory_meta_row(&conn, memory_key, max_version, owner_epoch).await?;
                }
                for (effect_ordinal, effect) in outbox_effects.iter().enumerate() {
                    insert_memory_outbox_row(
                        &conn,
                        memory_key,
                        effect,
                        max_version,
                        effect_ordinal,
                    )
                    .await?;
                }
                if let Some(command_result) = command_result {
                    insert_memory_command_result_row(
                        &conn,
                        memory_key,
                        command_result.idempotency_key.trim(),
                        &command_result.result,
                        max_version,
                    )
                    .await?;
                }
                let cache_mutations = mutations.to_vec();
                self.record_profile(
                    MemoryProfileMetricKind::StoreApplyBatchWrite,
                    write_started.elapsed().as_micros() as u64,
                    mutations.len() as u64 + 1,
                );
                Ok(MemoryBatchCommitOutcome {
                    result: MemoryBatchApplyResult { max_version },
                    cache_mutations,
                })
            }
            .await;

            match outcome {
                Ok(outcome) => {
                    let result = outcome.result;
                    match conn.execute("COMMIT", ()).await {
                        Ok(_) => {}
                        Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            tokio::time::sleep(std::time::Duration::from_millis(
                                5 * attempt as u64,
                            ))
                            .await;
                            continue;
                        }
                        Err(error) => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            return Err(memory_error(error));
                        }
                    }
                    self.observe_version(memory_key, result.max_version);
                    self.observe_memory_version(namespace, memory_key, result.max_version)
                        .await;
                    if !outcome.cache_mutations.is_empty() {
                        self.update_cached_snapshot_after_commit(
                            namespace,
                            memory_key,
                            result.max_version,
                            &outcome.cache_mutations,
                        )
                        .await;
                    }
                    self.record_profile(
                        MemoryProfileMetricKind::StoreApplyBatch,
                        started.elapsed().as_micros() as u64,
                        mutations.len() as u64 + 1,
                    );
                    return Ok(result);
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if is_retryable_platform_memory_error(&error) && attempt < 8 {
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    pub async fn command_result(
        &self,
        namespace: &str,
        memory_key: &str,
        idempotency_key: &str,
    ) -> Result<Option<MemoryCommandResult>> {
        let key = idempotency_key.trim();
        if key.is_empty() {
            return Ok(None);
        }
        let conn = self.connect(namespace, memory_key).await?;
        let mut rows = conn
            .query(
                "SELECT result_blob, revision
                 FROM memory_commands
                 WHERE entity_key = ?1 AND idempotency_key = ?2
                 LIMIT 1",
                (memory_key, key),
            )
            .await
            .map_err(memory_error)?;
        let Some(row) = rows.next().await.map_err(memory_error)? else {
            return Ok(None);
        };
        let result = row.get::<Vec<u8>>(0).map_err(memory_error)?;
        let revision = row.get::<i64>(1).map_err(memory_error)?;
        let _ = rows.next().await.map_err(memory_error)?;
        Ok(Some(MemoryCommandResult { result, revision }))
    }

    #[allow(dead_code)]
    pub async fn outbox_records(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Result<Vec<MemoryOutboxRecord>> {
        let conn = self.connect(namespace, memory_key).await?;
        let mut rows = conn
            .query(
                "SELECT effect_id, kind, payload_blob, revision, status, attempt_count, next_attempt_at_ms
                 FROM memory_outbox
                 WHERE entity_key = ?1
                 ORDER BY revision, effect_id",
                (memory_key,),
            )
            .await
            .map_err(memory_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            records.push(MemoryOutboxRecord {
                effect_id: row.get::<String>(0).map_err(memory_error)?,
                kind: row.get::<String>(1).map_err(memory_error)?,
                payload: row.get::<Vec<u8>>(2).map_err(memory_error)?,
                revision: row.get::<i64>(3).map_err(memory_error)?,
                status: row.get::<String>(4).map_err(memory_error)?,
                attempt_count: row.get::<i64>(5).map_err(memory_error)?,
                next_attempt_at_ms: row.get::<i64>(6).map_err(memory_error)?,
            });
        }
        Ok(records)
    }

    #[allow(dead_code)]
    pub async fn claim_outbox_records(
        &self,
        namespace: &str,
        memory_key: &str,
        limit: usize,
        lease_for: Duration,
    ) -> Result<Vec<MemoryOutboxRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now_ms = epoch_ms_i64()?;
        let lease_until_ms = now_ms.saturating_add(duration_ms_i64(lease_for)?);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let conn = self.connect(namespace, memory_key).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(memory_error(error)),
            }

            let outcome = async {
                let mut rows = conn
                    .query(
                        "SELECT effect_id, kind, payload_blob, revision, status, attempt_count, next_attempt_at_ms
                         FROM memory_outbox
                         WHERE entity_key = ?1
                           AND status IN ('pending', 'inflight')
                           AND next_attempt_at_ms <= ?2
                         ORDER BY revision, effect_id
                         LIMIT ?3",
                        (memory_key, now_ms, limit),
                    )
                    .await
                    .map_err(memory_error)?;
                let mut records = Vec::new();
                while let Some(row) = rows.next().await.map_err(memory_error)? {
                    records.push(MemoryOutboxRecord {
                        effect_id: row.get::<String>(0).map_err(memory_error)?,
                        kind: row.get::<String>(1).map_err(memory_error)?,
                        payload: row.get::<Vec<u8>>(2).map_err(memory_error)?,
                        revision: row.get::<i64>(3).map_err(memory_error)?,
                        status: "inflight".to_string(),
                        attempt_count: row.get::<i64>(5).map_err(memory_error)?.saturating_add(1),
                        next_attempt_at_ms: lease_until_ms,
                    });
                }
                for record in &records {
                    conn.execute(
                        "UPDATE memory_outbox
                         SET status = 'inflight',
                             attempt_count = ?1,
                             next_attempt_at_ms = ?2,
                             updated_at_ms = ?3
                         WHERE effect_id = ?4
                           AND entity_key = ?5
                           AND status IN ('pending', 'inflight')
                           AND next_attempt_at_ms <= ?3",
                        (
                            record.attempt_count,
                            lease_until_ms,
                            now_ms,
                            record.effect_id.as_str(),
                            memory_key,
                        ),
                    )
                    .await
                    .map_err(memory_error)?;
                }
                Ok::<_, PlatformError>(records)
            }
            .await;

            match outcome {
                Ok(records) => match conn.execute("COMMIT", ()).await {
                    Ok(_) => return Ok(records),
                    Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Err(memory_error(error));
                    }
                },
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if is_retryable_platform_memory_error(&error) && attempt < 8 {
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    pub async fn claim_due_outbox_records(
        &self,
        limit: usize,
        lease_for: Duration,
        kinds: &[&str],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || kinds.is_empty() {
            return Ok(Vec::new());
        }
        let (exact_kinds, kind_prefixes) = normalize_outbox_kind_selectors(kinds);
        if exact_kinds.is_empty() && kind_prefixes.is_empty() {
            return Ok(Vec::new());
        }
        let mut claims = Vec::new();
        for shard_index in 0..self.namespace_shards {
            if claims.len() >= limit {
                break;
            }
            let remaining = limit.saturating_sub(claims.len());
            let mut shard_claims = self
                .claim_due_outbox_records_for_shard_index_with_selectors(
                    shard_index,
                    remaining,
                    lease_for,
                    &exact_kinds,
                    &kind_prefixes,
                )
                .await?;
            claims.append(&mut shard_claims);
        }
        Ok(claims)
    }

    pub async fn claim_due_outbox_records_for_shard_index(
        &self,
        shard_index: usize,
        limit: usize,
        lease_for: Duration,
        kinds: &[&str],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || kinds.is_empty() {
            return Ok(Vec::new());
        }
        let (exact_kinds, kind_prefixes) = normalize_outbox_kind_selectors(kinds);
        if exact_kinds.is_empty() && kind_prefixes.is_empty() {
            return Ok(Vec::new());
        }
        self.claim_due_outbox_records_for_shard_index_with_selectors(
            shard_index,
            limit,
            lease_for,
            &exact_kinds,
            &kind_prefixes,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn mark_outbox_delivered(
        &self,
        namespace: &str,
        memory_key: &str,
        effect_id: &str,
    ) -> Result<()> {
        let effect_id = effect_id.trim();
        if effect_id.is_empty() {
            return Err(PlatformError::bad_request(
                "memory outbox effect_id is required",
            ));
        }
        let now_ms = epoch_ms_i64()?;
        let conn = self.connect(namespace, memory_key).await?;
        conn.execute(
            "UPDATE memory_outbox
             SET status = 'delivered',
                 updated_at_ms = ?1
             WHERE entity_key = ?2
               AND effect_id = ?3",
            (now_ms, memory_key, effect_id),
        )
        .await
        .map_err(memory_error)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn retry_outbox_record(
        &self,
        namespace: &str,
        memory_key: &str,
        effect_id: &str,
        retry_after: Duration,
    ) -> Result<()> {
        let effect_id = effect_id.trim();
        if effect_id.is_empty() {
            return Err(PlatformError::bad_request(
                "memory outbox effect_id is required",
            ));
        }
        let now_ms = epoch_ms_i64()?;
        let next_attempt_at_ms = now_ms.saturating_add(duration_ms_i64(retry_after)?);
        let conn = self.connect(namespace, memory_key).await?;
        conn.execute(
            "UPDATE memory_outbox
             SET status = 'pending',
                 next_attempt_at_ms = ?1,
                 updated_at_ms = ?2
             WHERE entity_key = ?3
               AND effect_id = ?4",
            (next_attempt_at_ms, now_ms, memory_key, effect_id),
        )
        .await
        .map_err(memory_error)?;
        Ok(())
    }

    pub async fn apply_outbox_delivery_outcomes(
        &self,
        outcomes: &[MemoryOutboxDeliveryOutcome],
    ) -> Result<()> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let mut grouped: BTreeMap<(String, String), Vec<&MemoryOutboxDeliveryOutcome>> =
            BTreeMap::new();
        for outcome in outcomes {
            if outcome.effect_id.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory outbox effect_id is required",
                ));
            }
            grouped
                .entry((outcome.namespace.clone(), outcome.memory_key.clone()))
                .or_default()
                .push(outcome);
        }

        for ((namespace, memory_key), entity_outcomes) in grouped {
            let conn = self.connect(&namespace, &memory_key).await?;
            let mut attempt = 0usize;
            loop {
                attempt += 1;
                match conn.execute("BEGIN IMMEDIATE", ()).await {
                    Ok(_) => {}
                    Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => return Err(memory_error(error)),
                }

                let outcome = async {
                    let now_ms = epoch_ms_i64()?;
                    for delivery in &entity_outcomes {
                        match delivery.action {
                            MemoryOutboxDeliveryAction::Delivered
                            | MemoryOutboxDeliveryAction::DroppedTerminal => {
                                conn.execute(
                                    "UPDATE memory_outbox
                                     SET status = 'delivered',
                                         updated_at_ms = ?1
                                     WHERE entity_key = ?2
                                       AND effect_id = ?3",
                                    (now_ms, memory_key.as_str(), delivery.effect_id.as_str()),
                                )
                                .await
                                .map_err(memory_error)?;
                            }
                            MemoryOutboxDeliveryAction::Retry { retry_after } => {
                                let next_attempt_at_ms =
                                    now_ms.saturating_add(duration_ms_i64(retry_after)?);
                                conn.execute(
                                    "UPDATE memory_outbox
                                     SET status = 'pending',
                                         next_attempt_at_ms = ?1,
                                         updated_at_ms = ?2
                                     WHERE entity_key = ?3
                                       AND effect_id = ?4",
                                    (
                                        next_attempt_at_ms,
                                        now_ms,
                                        memory_key.as_str(),
                                        delivery.effect_id.as_str(),
                                    ),
                                )
                                .await
                                .map_err(memory_error)?;
                            }
                        }
                    }
                    Ok::<_, PlatformError>(())
                }
                .await;

                match outcome {
                    Ok(()) => match conn.execute("COMMIT", ()).await {
                        Ok(_) => break,
                        Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            tokio::time::sleep(std::time::Duration::from_millis(
                                5 * attempt as u64,
                            ))
                            .await;
                            continue;
                        }
                        Err(error) => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            return Err(memory_error(error));
                        }
                    },
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        if is_retryable_platform_memory_error(&error) && attempt < 8 {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                5 * attempt as u64,
                            ))
                            .await;
                            continue;
                        }
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }

    async fn claim_due_outbox_records_for_shard_index_with_selectors(
        &self,
        shard_index: usize,
        limit: usize,
        lease_for: Duration,
        exact_kinds: &[String],
        kind_prefixes: &[String],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || (exact_kinds.is_empty() && kind_prefixes.is_empty()) {
            return Ok(Vec::new());
        }
        let shard_index = shard_index % self.namespace_shards.max(1);
        let mut claims = Vec::new();
        let namespaces = self.discover_namespaces_for_shard(shard_index).await?;
        for namespace in namespaces {
            if claims.len() >= limit {
                break;
            }
            let remaining = limit.saturating_sub(claims.len());
            let mut namespace_claims = self
                .claim_due_outbox_records_for_namespace_shard(
                    &namespace,
                    shard_index,
                    remaining,
                    lease_for,
                    exact_kinds,
                    kind_prefixes,
                )
                .await?;
            claims.append(&mut namespace_claims);
        }
        Ok(claims)
    }

    async fn claim_due_outbox_records_for_namespace_shard(
        &self,
        namespace: &str,
        shard_index: usize,
        limit: usize,
        lease_for: Duration,
        exact_kinds: &[String],
        kind_prefixes: &[String],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || (exact_kinds.is_empty() && kind_prefixes.is_empty()) {
            return Ok(Vec::new());
        }
        let now_ms = epoch_ms_i64()?;
        let lease_until_ms = now_ms.saturating_add(duration_ms_i64(lease_for)?);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut kind_selectors = Vec::with_capacity(exact_kinds.len() + kind_prefixes.len());
        let mut next_placeholder = 2usize;
        for _ in exact_kinds {
            kind_selectors.push(format!("kind = ?{next_placeholder}"));
            next_placeholder += 1;
        }
        for _ in kind_prefixes {
            kind_selectors.push(format!("kind LIKE ?{next_placeholder} ESCAPE '\\'"));
            next_placeholder += 1;
        }
        let kind_filter = kind_selectors.join(" OR ");
        let limit_placeholder = next_placeholder;
        let select_sql = format!(
            "SELECT entity_key, effect_id, kind, payload_blob, revision, status, attempt_count, next_attempt_at_ms
             FROM memory_outbox
             WHERE status IN ('pending', 'inflight')
               AND next_attempt_at_ms <= ?1
               AND ({kind_filter})
             ORDER BY revision, effect_id
             LIMIT ?{limit_placeholder}"
        );
        let mut select_params = Vec::with_capacity(exact_kinds.len() + kind_prefixes.len() + 2);
        select_params.push(Value::Integer(now_ms));
        select_params.extend(exact_kinds.iter().map(|kind| Value::Text(kind.clone())));
        select_params.extend(
            kind_prefixes
                .iter()
                .map(|prefix| Value::Text(format!("{}%", escape_sql_like_prefix(prefix)))),
        );
        select_params.push(Value::Integer(limit));
        let conn = self.connect_shard(namespace, shard_index).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(memory_error(error)),
            }

            let outcome = async {
                let mut rows = conn
                    .query(&select_sql, select_params.clone())
                    .await
                    .map_err(memory_error)?;
                let mut claims = Vec::new();
                while let Some(row) = rows.next().await.map_err(memory_error)? {
                    let memory_key = row.get::<String>(0).map_err(memory_error)?;
                    claims.push(MemoryOutboxClaim {
                        namespace: namespace.to_string(),
                        memory_key,
                        record: MemoryOutboxRecord {
                            effect_id: row.get::<String>(1).map_err(memory_error)?,
                            kind: row.get::<String>(2).map_err(memory_error)?,
                            payload: row.get::<Vec<u8>>(3).map_err(memory_error)?,
                            revision: row.get::<i64>(4).map_err(memory_error)?,
                            status: "inflight".to_string(),
                            attempt_count: row
                                .get::<i64>(6)
                                .map_err(memory_error)?
                                .saturating_add(1),
                            next_attempt_at_ms: lease_until_ms,
                        },
                    });
                }
                for claim in &claims {
                    conn.execute(
                        "UPDATE memory_outbox
                         SET status = 'inflight',
                             attempt_count = ?1,
                             next_attempt_at_ms = ?2,
                             updated_at_ms = ?3
                         WHERE effect_id = ?4
                           AND entity_key = ?5
                           AND kind = ?6
                           AND status IN ('pending', 'inflight')
                           AND next_attempt_at_ms <= ?3",
                        (
                            claim.record.attempt_count,
                            lease_until_ms,
                            now_ms,
                            claim.record.effect_id.as_str(),
                            claim.memory_key.as_str(),
                            claim.record.kind.as_str(),
                        ),
                    )
                    .await
                    .map_err(memory_error)?;
                }
                Ok::<_, PlatformError>(claims)
            }
            .await;

            match outcome {
                Ok(claims) => match conn.execute("COMMIT", ()).await {
                    Ok(_) => return Ok(claims),
                    Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Err(memory_error(error));
                    }
                },
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if is_retryable_platform_memory_error(&error) && attempt < 8 {
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    fn reserve_version_after(&self, memory_key: &str, floor: i64) -> i64 {
        let minimum = floor.saturating_add(1).max(1) as u64;
        let shard = self.shard_for_key(memory_key);
        let mut current = shard.version.load(Ordering::Relaxed);
        loop {
            let next = current.max(minimum);
            match shard.version.compare_exchange(
                current,
                next.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next as i64,
                Err(actual) => current = actual,
            }
        }
    }

    async fn connect(&self, namespace: &str, memory_key: &str) -> Result<Connection> {
        let namespace = namespace.trim();
        if namespace.is_empty() {
            return Err(PlatformError::runtime("memory namespace must not be empty"));
        }
        let memory_key = memory_key.trim();
        if memory_key.is_empty() {
            return Err(PlatformError::runtime("memory key must not be empty"));
        }

        self.connect_shard(namespace, self.shard_index(memory_key))
            .await
    }

    async fn connect_shard(&self, namespace: &str, shard_index: usize) -> Result<Connection> {
        let shard = self.shard_for_index(shard_index);
        let db_key = self.database_key(namespace, shard_index);
        let now = Instant::now();
        let slot = {
            let mut databases = shard.databases.lock().await;
            self.prune_databases_locked(shard_index, &mut databases, now);
            if let Some(slot) = databases.touch(&db_key, now) {
                self.record_profile(MemoryProfileMetricKind::StoreDatabaseCacheHit, 0, 1);
                slot
            } else {
                self.record_profile(MemoryProfileMetricKind::StoreDatabaseCacheMiss, 0, 1);
                let slot = Arc::new(MemoryDatabaseSlot::new());
                databases.insert_slot(db_key.clone(), Arc::clone(&slot), now);
                slot
            }
        };

        let path = self.db_path(namespace, shard_index);
        let database = match slot
            .database
            .get_or_try_init(|| async {
                ensure_parent_dir(&path)?;
                let path_str = path.to_string_lossy().to_string();
                let database = Builder::new_local(&path_str)
                    .build()
                    .await
                    .map_err(memory_error)?;
                let database = Arc::new(database);
                ensure_schema(&database).await?;
                Ok::<_, PlatformError>(database)
            })
            .await
        {
            Ok(database) => Arc::clone(database),
            Err(error) => {
                let mut databases = shard.databases.lock().await;
                let remove_failed_slot = databases
                    .entries
                    .get(&db_key)
                    .map(|entry| {
                        Arc::ptr_eq(&entry.slot, &slot) && entry.slot.database.get().is_none()
                    })
                    .unwrap_or(false);
                if remove_failed_slot {
                    databases.remove(&db_key);
                }
                return Err(error);
            }
        };

        {
            let mut databases = shard.databases.lock().await;
            if databases
                .entries
                .get(&db_key)
                .map(|entry| Arc::ptr_eq(&entry.slot, &slot))
                .unwrap_or(false)
            {
                databases.touch(&db_key, Instant::now());
            }
            self.prune_databases_locked(shard_index, &mut databases, Instant::now());
        }

        let conn = database.connect().map_err(memory_error)?;
        configure_connection(&conn).await?;
        Ok(conn)
    }

    async fn max_version_for_memory(
        &self,
        conn: &Connection,
        memory_key: &str,
    ) -> Result<Option<i64>> {
        let mut rows = conn
            .query(
                "SELECT max_version FROM memory_meta
                 WHERE entity_key = ?1
                 LIMIT 1",
                (memory_key,),
            )
            .await
            .map_err(memory_error)?;
        let version = if let Some(row) = rows.next().await.map_err(memory_error)? {
            row.get::<Option<i64>>(0).map_err(memory_error)?
        } else {
            return Ok(None);
        };
        let _ = rows.next().await.map_err(memory_error)?;
        Ok(version)
    }

    async fn owner_epoch_for_memory(
        &self,
        conn: &Connection,
        memory_key: &str,
    ) -> Result<Option<i64>> {
        let mut rows = conn
            .query(
                "SELECT owner_epoch FROM memory_meta
                 WHERE entity_key = ?1
                 LIMIT 1",
                (memory_key,),
            )
            .await
            .map_err(memory_error)?;
        let epoch = if let Some(row) = rows.next().await.map_err(memory_error)? {
            row.get::<Option<i64>>(0).map_err(memory_error)?
        } else {
            return Ok(None);
        };
        let _ = rows.next().await.map_err(memory_error)?;
        Ok(epoch)
    }

    async fn validate_owner_epoch(
        &self,
        conn: &Connection,
        memory_key: &str,
        owner_epoch: Option<i64>,
    ) -> Result<()> {
        let Some(owner_epoch) = owner_epoch else {
            return Ok(());
        };
        let owner_epoch = owner_epoch.max(0);
        if owner_epoch == 0 {
            return Ok(());
        }
        let current_owner_epoch = self
            .owner_epoch_for_memory(conn, memory_key)
            .await?
            .unwrap_or(0)
            .max(0);
        if current_owner_epoch > owner_epoch {
            return Err(PlatformError::runtime(format!(
                "stale memory entity owner epoch {owner_epoch}; current owner epoch is {current_owner_epoch}"
            )));
        }
        Ok(())
    }

    async fn record_for_key(
        &self,
        conn: &Connection,
        memory_key: &str,
        item_key: &str,
    ) -> Result<Option<MemorySnapshotEntry>> {
        let mut rows = conn
            .query(
                "SELECT value_blob, encoding, value, version, deleted
                 FROM memory_state
                 WHERE entity_key = ?1 AND item_key = ?2
                 LIMIT 1",
                (memory_key, item_key),
            )
            .await
            .map_err(memory_error)?;
        let Some(row) = rows.next().await.map_err(memory_error)? else {
            return Ok(None);
        };
        let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(0).map_err(memory_error)?;
        let encoding: String = row.get::<String>(1).map_err(memory_error)?;
        let legacy_value: String = row.get::<String>(2).map_err(memory_error)?;
        let version: i64 = row.get::<i64>(3).map_err(memory_error)?;
        let deleted: i64 = row.get::<i64>(4).map_err(memory_error)?;
        let _ = rows.next().await.map_err(memory_error)?;
        Ok(Some(MemorySnapshotEntry {
            key: item_key.to_string(),
            value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
            encoding: normalize_encoding(&encoding),
            version,
            deleted: deleted != 0,
        }))
    }

    async fn discover_namespaces_for_shard(&self, shard_index: usize) -> Result<Vec<String>> {
        let mut namespaces = Vec::new();
        let entries = match std::fs::read_dir(self.root_dir.as_ref()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(namespaces),
            Err(error) => return Err(memory_error(error)),
        };
        for entry in entries {
            let entry = entry.map_err(memory_error)?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let shard_path = path.join(format!("shard-{shard_index:04}.db"));
            if !shard_path.exists() {
                continue;
            }
            let raw = entry.file_name();
            let Some(namespace) = hex_decode_to_utf8(raw.to_string_lossy().as_ref()) else {
                continue;
            };
            namespaces.push(namespace);
        }
        namespaces.sort();
        namespaces.dedup();
        Ok(namespaces)
    }

    fn database_key(&self, namespace: &str, shard_index: usize) -> String {
        // The cache stores a shareable Database handle per namespace and
        // physical shard. Connections are created per request and configured
        // independently, so the handle itself does not need thread-local cache
        // identity.
        format!("{namespace}\u{1f}{shard_index}")
    }

    fn memory_version_key(namespace: &str, memory_key: &str) -> String {
        format!("{namespace}\u{1f}{memory_key}")
    }

    fn memory_snapshot_key(namespace: &str, memory_key: &str) -> String {
        format!("{namespace}\u{1f}{memory_key}")
    }

    fn db_path(&self, namespace: &str, shard_index: usize) -> PathBuf {
        let encoded_namespace = hex_encode(namespace.as_bytes());
        self.root_dir
            .join(encoded_namespace)
            .join(format!("shard-{shard_index:04}.db"))
    }

    fn shard_index(&self, memory_key: &str) -> usize {
        memory_shard_index_for_hash_version(
            memory_key,
            self.namespace_shards,
            self.shard_hash_version,
        )
        .expect("memory store must have a supported shard hash version")
    }

    fn shard_for_key(&self, memory_key: &str) -> &MemoryShard {
        self.shard_for_index(self.shard_index(memory_key))
    }

    fn shard_for_index(&self, shard_index: usize) -> &MemoryShard {
        &self.shards[shard_index % self.shards.len()]
    }

    fn observe_version(&self, memory_key: &str, version: i64) {
        self.shard_for_key(memory_key).observe_version(version);
    }

    async fn observe_memory_version(&self, namespace: &str, memory_key: &str, version: i64) {
        if version < 0 {
            return;
        }
        self.shard_for_key(memory_key)
            .memory_versions
            .lock()
            .await
            .insert(Self::memory_version_key(namespace, memory_key), version);
    }

    async fn cached_full_snapshot(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Option<MemorySnapshot> {
        let entry = self.cached_snapshot_entry(namespace, memory_key).await?;
        if !entry.complete {
            return None;
        }
        Some(MemorySnapshot {
            entries: snapshot_entries_from_records(entry.records.as_ref()),
            max_version: entry.max_version,
        })
    }

    async fn cached_point_read(
        &self,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
    ) -> Option<MemoryPointRead> {
        let entry = self.cached_snapshot_entry(namespace, memory_key).await?;
        if !entry.complete && !entry.loaded_keys.contains(item_key) {
            return None;
        }
        Some(MemoryPointRead {
            record: entry.records.get(item_key).cloned(),
            max_version: entry.max_version,
        })
    }

    async fn cached_keys_snapshot(
        &self,
        namespace: &str,
        memory_key: &str,
        keys: &[String],
    ) -> Option<MemorySnapshot> {
        let entry = self.cached_snapshot_entry(namespace, memory_key).await?;
        if !entry.complete
            && keys
                .iter()
                .any(|key| !entry.loaded_keys.contains(key.as_str()))
        {
            return None;
        }
        let mut entries = keys
            .iter()
            .filter_map(|key| entry.records.get(key).cloned())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        Some(MemorySnapshot {
            entries,
            max_version: entry.max_version,
        })
    }

    async fn cached_snapshot_entry(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Option<MemorySharedSnapshotEntry> {
        let key = Self::memory_snapshot_key(namespace, memory_key);
        let current_version = self
            .shard_for_key(memory_key)
            .memory_versions
            .lock()
            .await
            .get(&Self::memory_version_key(namespace, memory_key))
            .copied();
        let now = Instant::now();
        let shard_index = self.shard_index(memory_key);
        let mut snapshots = self
            .shard_for_index(shard_index)
            .shared_snapshots
            .lock()
            .await;
        self.prune_snapshots_locked(shard_index, &mut snapshots, now);
        let Some(entry) = snapshots.get_cloned(&key, now) else {
            self.record_profile(MemoryProfileMetricKind::StoreSnapshotCacheMiss, 0, 1);
            return None;
        };
        if current_version.is_some_and(|current| current > entry.max_version) {
            snapshots.remove(&key);
            self.record_profile(MemoryProfileMetricKind::StoreSnapshotCacheMiss, 0, 1);
            return None;
        }
        self.record_profile(MemoryProfileMetricKind::StoreSnapshotCacheHit, 0, 1);
        Some(entry)
    }

    async fn put_full_snapshot(
        &self,
        namespace: &str,
        memory_key: &str,
        snapshot: &MemorySnapshot,
    ) {
        self.put_partial_snapshot(
            namespace,
            memory_key,
            snapshot.max_version,
            snapshot.entries.clone(),
            snapshot.entries.iter().map(|entry| entry.key.clone()),
            true,
        )
        .await;
    }

    async fn put_partial_snapshot<I>(
        &self,
        namespace: &str,
        memory_key: &str,
        max_version: i64,
        entries: Vec<MemorySnapshotEntry>,
        loaded_keys: I,
        complete: bool,
    ) where
        I: IntoIterator<Item = String>,
    {
        let key = Self::memory_snapshot_key(namespace, memory_key);
        let now = Instant::now();
        let shard_index = self.shard_index(memory_key);
        let mut snapshots = self
            .shard_for_index(shard_index)
            .shared_snapshots
            .lock()
            .await;
        self.prune_snapshots_locked(shard_index, &mut snapshots, now);
        let existing = snapshots.entries.remove(&key);
        let existing = if let Some(existing) = existing {
            snapshots.approximate_bytes = snapshots
                .approximate_bytes
                .saturating_sub(existing.approximate_bytes);
            snapshots
                .recency
                .remove(&(existing.last_used_at, existing.recency_id));
            Some(existing)
        } else {
            None
        };

        if existing
            .as_ref()
            .is_some_and(|entry| entry.max_version > max_version)
        {
            let mut entry = existing.expect("checked above");
            entry.last_used_at = now;
            snapshots.insert_or_replace(key, entry);
            return;
        }

        let mut next_records = if complete
            || existing
                .as_ref()
                .is_some_and(|entry| entry.max_version < max_version)
        {
            HashMap::new()
        } else {
            existing
                .as_ref()
                .map(|entry| entry.records.as_ref().clone())
                .unwrap_or_default()
        };
        let mut next_loaded_keys = if complete
            || existing
                .as_ref()
                .is_some_and(|entry| entry.max_version < max_version)
        {
            HashSet::new()
        } else {
            existing
                .as_ref()
                .map(|entry| entry.loaded_keys.as_ref().clone())
                .unwrap_or_default()
        };

        for loaded_key in loaded_keys {
            let normalized_key = loaded_key.trim();
            if !normalized_key.is_empty() {
                next_loaded_keys.insert(normalized_key.to_string());
            }
        }
        for snapshot_entry in entries {
            let normalized_key = snapshot_entry.key.trim();
            if normalized_key.is_empty() {
                continue;
            }
            next_loaded_keys.insert(normalized_key.to_string());
            next_records.insert(normalized_key.to_string(), snapshot_entry);
        }

        let approximate_bytes =
            approximate_snapshot_cache_bytes(&key, &next_records, &next_loaded_keys);
        if approximate_bytes > self.snapshot_cache_byte_budget_for_shard(shard_index) {
            return;
        }
        snapshots.insert_or_replace(
            key,
            MemorySharedSnapshotEntry {
                records: Arc::new(next_records),
                loaded_keys: Arc::new(next_loaded_keys),
                complete,
                max_version,
                last_used_at: now,
                recency_id: 0,
                approximate_bytes,
            },
        );
        self.prune_snapshots_locked(shard_index, &mut snapshots, now);
    }

    async fn update_cached_snapshot_after_commit(
        &self,
        namespace: &str,
        memory_key: &str,
        max_version: i64,
        mutations: &[MemoryBatchMutation],
    ) {
        let key = Self::memory_snapshot_key(namespace, memory_key);
        let now = Instant::now();
        let shard_index = self.shard_index(memory_key);
        let mut snapshots = self
            .shard_for_index(shard_index)
            .shared_snapshots
            .lock()
            .await;
        self.prune_snapshots_locked(shard_index, &mut snapshots, now);
        let Some(entry) = snapshots.remove(&key) else {
            return;
        };
        let mut next_records = entry.records.as_ref().clone();
        let mut next_loaded_keys = entry.loaded_keys.as_ref().clone();
        for mutation in mutations {
            let normalized_key = mutation.key.trim();
            if normalized_key.is_empty() {
                continue;
            }
            let record = MemorySnapshotEntry {
                key: normalized_key.to_string(),
                value: mutation.value.clone(),
                encoding: mutation.encoding.clone(),
                version: max_version,
                deleted: mutation.deleted,
            };
            next_loaded_keys.insert(normalized_key.to_string());
            next_records.insert(normalized_key.to_string(), record);
        }
        let approximate_bytes =
            approximate_snapshot_cache_bytes(&key, &next_records, &next_loaded_keys);
        if approximate_bytes > self.snapshot_cache_byte_budget_for_shard(shard_index) {
            return;
        }
        snapshots.insert_or_replace(
            key,
            MemorySharedSnapshotEntry {
                records: Arc::new(next_records),
                loaded_keys: Arc::new(next_loaded_keys),
                complete: entry.complete,
                max_version,
                last_used_at: now,
                recency_id: 0,
                approximate_bytes,
            },
        );
    }

    fn prune_databases_locked(
        &self,
        shard_index: usize,
        databases: &mut MemoryDatabaseCache,
        now: Instant,
    ) {
        let evicted = databases.prune(
            now,
            self.db_idle_ttl,
            self.database_cache_budget_for_shard(shard_index),
        );
        if evicted > 0 {
            self.record_profile(
                MemoryProfileMetricKind::StoreDatabaseCacheEviction,
                0,
                evicted as u64,
            );
        }
    }

    fn prune_snapshots_locked(
        &self,
        shard_index: usize,
        snapshots: &mut MemorySharedSnapshotCache,
        now: Instant,
    ) {
        let evicted = snapshots.prune(
            now,
            self.db_idle_ttl,
            self.snapshot_cache_entry_budget_for_shard(shard_index),
            self.snapshot_cache_byte_budget_for_shard(shard_index),
        );
        if evicted > 0 {
            self.record_profile(
                MemoryProfileMetricKind::StoreSnapshotCacheEviction,
                0,
                evicted as u64,
            );
        }
    }

    fn database_cache_budget_for_shard(&self, shard_index: usize) -> usize {
        partitioned_budget(self.db_cache_max_open, self.namespace_shards, shard_index)
    }

    fn snapshot_cache_entry_budget_for_shard(&self, shard_index: usize) -> usize {
        partitioned_budget(
            self.snapshot_cache_max_entries,
            self.namespace_shards,
            shard_index,
        )
    }

    fn snapshot_cache_byte_budget_for_shard(&self, shard_index: usize) -> usize {
        partitioned_budget(
            self.snapshot_cache_max_bytes,
            self.namespace_shards,
            shard_index,
        )
    }
}

async fn upsert_memory_state_row(
    conn: &Connection,
    memory_key: &str,
    item_key: &str,
    value: &[u8],
    encoding: &str,
    deleted: bool,
    version: i64,
) -> Result<()> {
    let now_ms = epoch_ms_i64()?;
    if deleted {
        let empty_blob: &[u8] = &[];
        conn.execute(
            "INSERT INTO memory_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
             VALUES (?1, ?2, '', ?3, ?4, 1, ?5, ?6)
             ON CONFLICT(entity_key, item_key) DO UPDATE SET
               value = excluded.value,
               value_blob = excluded.value_blob,
               encoding = excluded.encoding,
               deleted = 1,
               version = excluded.version,
               updated_at_ms = excluded.updated_at_ms",
            (
                memory_key,
                item_key,
                empty_blob,
                ENCODING_UTF8,
                version,
                now_ms,
            ),
        )
        .await
        .map_err(memory_error)?;
        return Ok(());
    }

    let value_text = if encoding == ENCODING_UTF8 {
        std::str::from_utf8(value)
            .map_err(|error| PlatformError::bad_request(format!("invalid utf8 value: {error}")))?
    } else {
        ""
    };
    conn.execute(
        "INSERT INTO memory_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)
         ON CONFLICT(entity_key, item_key) DO UPDATE SET
           value = excluded.value,
           value_blob = excluded.value_blob,
           encoding = excluded.encoding,
           deleted = 0,
           version = excluded.version,
           updated_at_ms = excluded.updated_at_ms",
        (
            memory_key, item_key, value_text, value, encoding, version, now_ms,
        ),
    )
    .await
    .map_err(memory_error)?;
    Ok(())
}

async fn upsert_memory_meta_row(
    conn: &Connection,
    memory_key: &str,
    max_version: i64,
    owner_epoch: Option<i64>,
) -> Result<()> {
    let now_ms = epoch_ms_i64()?;
    let owner_epoch = owner_epoch.unwrap_or(0).max(0);
    conn.execute(
        "INSERT INTO memory_meta (entity_key, max_version, owner_epoch, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(entity_key) DO UPDATE SET
           max_version = excluded.max_version,
           owner_epoch = CASE WHEN excluded.owner_epoch > 0 THEN excluded.owner_epoch ELSE memory_meta.owner_epoch END,
           updated_at_ms = excluded.updated_at_ms",
        (memory_key, max_version, owner_epoch, now_ms),
    )
    .await
    .map_err(memory_error)?;
    Ok(())
}

async fn insert_memory_command_result_row(
    conn: &Connection,
    memory_key: &str,
    idempotency_key: &str,
    result: &[u8],
    revision: i64,
) -> Result<()> {
    let now_ms = epoch_ms_i64()?;
    conn.execute(
        "INSERT INTO memory_commands (entity_key, idempotency_key, result_blob, revision, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        (memory_key, idempotency_key, result, revision, now_ms),
    )
    .await
    .map_err(memory_error)?;
    Ok(())
}

async fn insert_memory_outbox_row(
    conn: &Connection,
    memory_key: &str,
    effect: &MemoryOutboxEffectWrite,
    revision: i64,
    ordinal: usize,
) -> Result<()> {
    let now_ms = epoch_ms_i64()?;
    let effect_id = memory_outbox_effect_id(memory_key, revision, ordinal, effect);
    conn.execute(
        "INSERT INTO memory_outbox (
           effect_id,
           entity_key,
           revision,
           kind,
           payload_blob,
           status,
           attempt_count,
           next_attempt_at_ms,
           created_at_ms,
           updated_at_ms
         )
         VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?6, ?6)",
        (
            effect_id,
            memory_key,
            revision,
            effect.kind.trim(),
            effect.payload.as_slice(),
            now_ms,
        ),
    )
    .await
    .map_err(memory_error)?;
    Ok(())
}

fn memory_outbox_effect_id(
    memory_key: &str,
    revision: i64,
    ordinal: usize,
    effect: &MemoryOutboxEffectWrite,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(memory_key.as_bytes());
    hasher.update([0]);
    hasher.update(revision.to_be_bytes());
    hasher.update((ordinal as u64).to_be_bytes());
    hasher.update(effect.kind.trim().as_bytes());
    hasher.update([0]);
    hasher.update(effect.payload.as_slice());
    let digest = hasher.finalize();
    let mut id = String::with_capacity("memfx_".len() + 64);
    id.push_str("memfx_");
    for byte in digest {
        use std::fmt::Write;
        write!(&mut id, "{byte:02x}").expect("writing to String cannot fail");
    }
    id
}

impl MemoryProfileMetric {
    fn record(&self, duration_us: u64, items: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(duration_us, Ordering::Relaxed);
        self.total_items.fetch_add(items, Ordering::Relaxed);
        let mut current = self.max_us.load(Ordering::Relaxed);
        while duration_us > current {
            match self.max_us.compare_exchange(
                current,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn snapshot(&self) -> MemoryProfileMetricSnapshot {
        MemoryProfileMetricSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            total_us: self.total_us.load(Ordering::Relaxed),
            total_items: self.total_items.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.calls.store(0, Ordering::Relaxed);
        self.total_us.store(0, Ordering::Relaxed);
        self.total_items.store(0, Ordering::Relaxed);
        self.max_us.store(0, Ordering::Relaxed);
    }
}

impl MemoryProfile {
    fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    fn record(&self, metric: MemoryProfileMetricKind, duration_us: u64, items: u64) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let target = match metric {
            MemoryProfileMetricKind::JsReadOnlyTotal => &self.js_read_only_total,
            MemoryProfileMetricKind::JsHydrateFull => &self.js_hydrate_full,
            MemoryProfileMetricKind::JsHydrateKeys => &self.js_hydrate_keys,
            MemoryProfileMetricKind::JsTxnCommit => &self.js_txn_commit,
            MemoryProfileMetricKind::JsCacheHit => &self.js_cache_hit,
            MemoryProfileMetricKind::JsCacheMiss => &self.js_cache_miss,
            MemoryProfileMetricKind::JsCacheStale => &self.js_cache_stale,
            MemoryProfileMetricKind::OpRead => &self.op_read,
            MemoryProfileMetricKind::OpSnapshot => &self.op_snapshot,
            MemoryProfileMetricKind::OpVersionIfNewer => &self.op_version_if_newer,
            MemoryProfileMetricKind::OpApplyBatch => &self.op_apply_batch,
            MemoryProfileMetricKind::StoreRead => &self.store_read,
            MemoryProfileMetricKind::StoreSnapshot => &self.store_snapshot,
            MemoryProfileMetricKind::StoreSnapshotKeys => &self.store_snapshot_keys,
            MemoryProfileMetricKind::StoreVersionIfNewer => &self.store_version_if_newer,
            MemoryProfileMetricKind::StoreApplyBatch => &self.store_apply_batch,
            MemoryProfileMetricKind::StoreApplyBatchValidate => &self.store_apply_batch_validate,
            MemoryProfileMetricKind::StoreApplyBatchWrite => &self.store_apply_batch_write,
            MemoryProfileMetricKind::StoreDatabaseCacheHit => &self.store_database_cache_hit,
            MemoryProfileMetricKind::StoreDatabaseCacheMiss => &self.store_database_cache_miss,
            MemoryProfileMetricKind::StoreDatabaseCacheEviction => {
                &self.store_database_cache_eviction
            }
            MemoryProfileMetricKind::StoreSnapshotCacheHit => &self.store_snapshot_cache_hit,
            MemoryProfileMetricKind::StoreSnapshotCacheMiss => &self.store_snapshot_cache_miss,
            MemoryProfileMetricKind::StoreSnapshotCacheEviction => {
                &self.store_snapshot_cache_eviction
            }
            MemoryProfileMetricKind::RuntimeAtomicInvokeEventWait => {
                &self.runtime_atomic_invoke_event_wait
            }
            MemoryProfileMetricKind::RuntimeAtomicQueueWait => &self.runtime_atomic_queue_wait,
            MemoryProfileMetricKind::RuntimeAtomicDispatchWait => {
                &self.runtime_atomic_dispatch_wait
            }
            MemoryProfileMetricKind::RuntimeAtomicExecution => &self.runtime_atomic_execution,
            MemoryProfileMetricKind::RuntimeAtomicCompletionWait => {
                &self.runtime_atomic_completion_wait
            }
            MemoryProfileMetricKind::RuntimeAtomicOutboxDrain => &self.runtime_atomic_outbox_drain,
        };
        target.record(duration_us, items.max(1));
    }

    fn take_snapshot_and_reset(&self) -> MemoryProfileSnapshot {
        let snapshot = MemoryProfileSnapshot {
            enabled: self.enabled.load(Ordering::Relaxed),
            js_read_only_total: self.js_read_only_total.snapshot(),
            js_hydrate_full: self.js_hydrate_full.snapshot(),
            js_hydrate_keys: self.js_hydrate_keys.snapshot(),
            js_txn_commit: self.js_txn_commit.snapshot(),
            js_cache_hit: self.js_cache_hit.snapshot(),
            js_cache_miss: self.js_cache_miss.snapshot(),
            js_cache_stale: self.js_cache_stale.snapshot(),
            op_read: self.op_read.snapshot(),
            op_snapshot: self.op_snapshot.snapshot(),
            op_version_if_newer: self.op_version_if_newer.snapshot(),
            op_apply_batch: self.op_apply_batch.snapshot(),
            store_read: self.store_read.snapshot(),
            store_snapshot: self.store_snapshot.snapshot(),
            store_snapshot_keys: self.store_snapshot_keys.snapshot(),
            store_version_if_newer: self.store_version_if_newer.snapshot(),
            store_apply_batch: self.store_apply_batch.snapshot(),
            store_apply_batch_validate: self.store_apply_batch_validate.snapshot(),
            store_apply_batch_write: self.store_apply_batch_write.snapshot(),
            store_database_cache_hit: self.store_database_cache_hit.snapshot(),
            store_database_cache_miss: self.store_database_cache_miss.snapshot(),
            store_database_cache_eviction: self.store_database_cache_eviction.snapshot(),
            store_snapshot_cache_hit: self.store_snapshot_cache_hit.snapshot(),
            store_snapshot_cache_miss: self.store_snapshot_cache_miss.snapshot(),
            store_snapshot_cache_eviction: self.store_snapshot_cache_eviction.snapshot(),
            runtime_atomic_invoke_event_wait: self.runtime_atomic_invoke_event_wait.snapshot(),
            runtime_atomic_queue_wait: self.runtime_atomic_queue_wait.snapshot(),
            runtime_atomic_dispatch_wait: self.runtime_atomic_dispatch_wait.snapshot(),
            runtime_atomic_execution: self.runtime_atomic_execution.snapshot(),
            runtime_atomic_completion_wait: self.runtime_atomic_completion_wait.snapshot(),
            runtime_atomic_outbox_drain: self.runtime_atomic_outbox_drain.snapshot(),
        };
        self.reset();
        snapshot
    }

    fn reset(&self) {
        self.js_read_only_total.reset();
        self.js_hydrate_full.reset();
        self.js_hydrate_keys.reset();
        self.js_txn_commit.reset();
        self.js_cache_hit.reset();
        self.js_cache_miss.reset();
        self.js_cache_stale.reset();
        self.op_read.reset();
        self.op_snapshot.reset();
        self.op_version_if_newer.reset();
        self.op_apply_batch.reset();
        self.store_read.reset();
        self.store_snapshot.reset();
        self.store_snapshot_keys.reset();
        self.store_version_if_newer.reset();
        self.store_apply_batch.reset();
        self.store_apply_batch_validate.reset();
        self.store_apply_batch_write.reset();
        self.store_database_cache_hit.reset();
        self.store_database_cache_miss.reset();
        self.store_database_cache_eviction.reset();
        self.store_snapshot_cache_hit.reset();
        self.store_snapshot_cache_miss.reset();
        self.store_snapshot_cache_eviction.reset();
        self.runtime_atomic_invoke_event_wait.reset();
        self.runtime_atomic_queue_wait.reset();
        self.runtime_atomic_dispatch_wait.reset();
        self.runtime_atomic_execution.reset();
        self.runtime_atomic_completion_wait.reset();
        self.runtime_atomic_outbox_drain.reset();
    }
}

async fn ensure_schema(database: &Database) -> Result<()> {
    let conn = database.connect().map_err(memory_error)?;
    configure_connection(&conn).await?;
    ensure_mvcc_mode(&conn).await?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_state (
          entity_key TEXT NOT NULL,
          item_key TEXT NOT NULL,
          value TEXT NOT NULL,
          value_blob BLOB,
          encoding TEXT NOT NULL DEFAULT 'utf8',
          deleted INTEGER NOT NULL DEFAULT 0,
          version INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          PRIMARY KEY (entity_key, item_key)
        )",
        (),
    )
    .await
    .map_err(memory_error)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_meta (
          entity_key TEXT NOT NULL PRIMARY KEY,
          max_version INTEGER NOT NULL,
          owner_epoch INTEGER NOT NULL DEFAULT 0,
          updated_at_ms INTEGER NOT NULL
        )",
        (),
    )
    .await
    .map_err(memory_error)?;
    ensure_compat_columns(&conn).await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_state_lookup
         ON memory_state(entity_key, item_key)",
        (),
    )
    .await
    .map_err(memory_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_state_list
         ON memory_state(entity_key, deleted, item_key)",
        (),
    )
    .await
    .map_err(memory_error)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_commands (
          entity_key TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          result_blob BLOB NOT NULL,
          revision INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          PRIMARY KEY (entity_key, idempotency_key)
        )",
        (),
    )
    .await
    .map_err(memory_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_commands_entity
         ON memory_commands(entity_key, updated_at_ms)",
        (),
    )
    .await
    .map_err(memory_error)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_outbox (
          effect_id TEXT PRIMARY KEY,
          entity_key TEXT NOT NULL,
          revision INTEGER NOT NULL,
          kind TEXT NOT NULL,
          payload_blob BLOB NOT NULL,
          status TEXT NOT NULL,
          attempt_count INTEGER NOT NULL,
          next_attempt_at_ms INTEGER NOT NULL,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )",
        (),
    )
    .await
    .map_err(memory_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_outbox_entity_revision
         ON memory_outbox(entity_key, revision, effect_id)",
        (),
    )
    .await
    .map_err(memory_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_outbox_due
         ON memory_outbox(entity_key, status, next_attempt_at_ms, revision, effect_id)",
        (),
    )
    .await
    .map_err(memory_error)?;
    Ok(())
}

struct MemoryDurabilityFloors {
    version_floors: Vec<u64>,
    owner_epoch_floor: u64,
}

async fn detect_memory_floors(
    root_dir: &Path,
    namespace_shards: usize,
) -> Result<MemoryDurabilityFloors> {
    let mut max_versions = vec![0u64; namespace_shards];
    let mut max_owner_epoch = 0u64;
    let entries = match std::fs::read_dir(root_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MemoryDurabilityFloors {
                version_floors: vec![1; namespace_shards],
                owner_epoch_floor: 1,
            });
        }
        Err(error) => return Err(memory_error(error)),
    };
    for entry in entries {
        let entry = entry.map_err(memory_error)?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let shard_entries = std::fs::read_dir(&path).map_err(memory_error)?;
        for shard_entry in shard_entries {
            let shard_entry = shard_entry.map_err(memory_error)?;
            let shard_path = shard_entry.path();
            if shard_path.extension().and_then(|ext| ext.to_str()) != Some("db") {
                continue;
            }
            let Some(shard_index) = shard_index_from_db_path(&shard_path) else {
                continue;
            };
            if shard_index >= namespace_shards {
                continue;
            }
            let path_str = shard_path.to_string_lossy().to_string();
            let database = Builder::new_local(&path_str)
                .build()
                .await
                .map_err(memory_error)?;
            let conn = database.connect().map_err(memory_error)?;
            configure_connection(&conn).await?;
            max_versions[shard_index] = max_versions[shard_index].max(
                read_single_i64(&conn, "SELECT MAX(max_version) FROM memory_meta").await? as u64,
            );
            max_owner_epoch = max_owner_epoch.max(
                read_single_i64(&conn, "SELECT MAX(owner_epoch) FROM memory_meta").await? as u64,
            );
        }
    }
    Ok(MemoryDurabilityFloors {
        version_floors: max_versions
            .into_iter()
            .map(|version| version.saturating_add(1).max(1))
            .collect(),
        owner_epoch_floor: max_owner_epoch.saturating_add(1).max(1),
    })
}

async fn load_or_adopt_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
) -> Result<MemoryLayoutManifest> {
    let manifest_path = memory_layout_manifest_path(root_dir);
    if manifest_path.exists() {
        let manifest = read_memory_layout(&manifest_path)?;
        validate_memory_layout(root_dir, configured_shards, &manifest)?;
        return Ok(manifest);
    }

    let manifest = adopt_legacy_memory_layout(root_dir, configured_shards).await?;
    write_memory_layout_atomic(&manifest_path, &manifest)?;
    Ok(manifest)
}

fn validate_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
    manifest: &MemoryLayoutManifest,
) -> Result<()> {
    if manifest.format_version != MEMORY_LAYOUT_FORMAT_VERSION {
        return Err(PlatformError::runtime(format!(
            "unsupported memory layout manifest format_version {} at {}; supported format_version is {}",
            manifest.format_version,
            memory_layout_manifest_path(root_dir).display(),
            MEMORY_LAYOUT_FORMAT_VERSION
        )));
    }
    if !is_supported_memory_shard_hash_version(manifest.shard_hash_version) {
        return Err(PlatformError::runtime(format!(
            "unsupported memory layout shard_hash_version {} at {}; supported shard_hash_versions are {}",
            manifest.shard_hash_version,
            memory_layout_manifest_path(root_dir).display(),
            supported_memory_shard_hash_versions_label()
        )));
    }
    if manifest.namespace_shards != configured_shards {
        return Err(memory_layout_mismatch_error(
            root_dir,
            configured_shards,
            manifest.namespace_shards,
        ));
    }
    Ok(())
}

fn memory_layout_mismatch_error(
    root_dir: &Path,
    configured_shards: usize,
    persisted_shards: usize,
) -> PlatformError {
    PlatformError::runtime(format!(
        "memory layout mismatch for root {}: configured memory_namespace_shards={} but persisted namespace_shards={}. Changing the shard count requires an explicit reshard operation before startup.",
        root_dir.display(),
        configured_shards,
        persisted_shards
    ))
}

fn memory_layout_manifest_path(root_dir: &Path) -> PathBuf {
    root_dir.join(MEMORY_LAYOUT_MANIFEST_FILE)
}

fn read_memory_layout(path: &Path) -> Result<MemoryLayoutManifest> {
    let bytes = std::fs::read(path).map_err(memory_error)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        PlatformError::runtime(format!(
            "memory store error: failed to parse memory layout manifest {}: {error}",
            path.display()
        ))
    })
}

fn write_memory_layout_atomic(path: &Path, manifest: &MemoryLayoutManifest) -> Result<()> {
    ensure_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp_path = parent.join(format!(
        ".{}.tmp-{}-{:?}",
        MEMORY_LAYOUT_MANIFEST_FILE,
        std::process::id(),
        std::thread::current().id()
    ));
    let json = serde_json::to_vec_pretty(manifest).map_err(memory_error)?;
    {
        let mut file = std::fs::File::create(&tmp_path).map_err(memory_error)?;
        use std::io::Write as _;
        file.write_all(&json).map_err(memory_error)?;
        file.write_all(b"\n").map_err(memory_error)?;
        file.sync_all().map_err(memory_error)?;
    }
    if let Err(error) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(memory_error(error));
    }
    Ok(())
}

async fn adopt_legacy_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
) -> Result<MemoryLayoutManifest> {
    let shard_files = discover_legacy_memory_shard_files(root_dir)?;
    if shard_files.is_empty() {
        return Ok(MemoryLayoutManifest::current(configured_shards));
    }

    match validate_legacy_memory_layout(
        root_dir,
        configured_shards,
        STABLE_SHA256_MEMORY_SHARD_HASH_VERSION,
        &shard_files,
    )
    .await
    {
        Ok(()) => Ok(MemoryLayoutManifest::current(configured_shards)),
        Err(stable_error) => {
            validate_legacy_memory_layout(
                root_dir,
                configured_shards,
                LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION,
                &shard_files,
            )
            .await
            .map_err(|legacy_error| {
                PlatformError::runtime(format!(
                    "memory legacy layout at {} is incompatible with supported shard hash layouts for configured memory_namespace_shards={}; stable layout error: {}; legacy layout error: {}; changing the shard count or hash layout requires an explicit reshard operation before startup",
                    root_dir.display(),
                    configured_shards,
                    stable_error,
                    legacy_error
                ))
            })?;
            Ok(MemoryLayoutManifest {
                format_version: MEMORY_LAYOUT_FORMAT_VERSION,
                shard_hash_version: LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION,
                namespace_shards: configured_shards,
            })
        }
    }
}

async fn validate_legacy_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
    shard_hash_version: u32,
    shard_files: &[LegacyMemoryShardFile],
) -> Result<()> {
    for shard_file in shard_files {
        if shard_file.shard_index >= configured_shards {
            return Err(PlatformError::runtime(format!(
                "memory legacy layout at {} contains shard index {} outside configured memory_namespace_shards={} for shard_hash_version={}; changing the shard count requires an explicit reshard operation before startup",
                root_dir.display(),
                shard_file.shard_index,
                configured_shards,
                shard_hash_version
            )));
        }
        let keys = distinct_entity_keys_in_memory_db(&shard_file.path).await?;
        for entity_key in keys {
            let expected = memory_shard_index_for_hash_version(
                &entity_key,
                configured_shards,
                shard_hash_version,
            )?;
            if expected != shard_file.shard_index {
                return Err(PlatformError::runtime(format!(
                    "memory legacy layout at {} is incompatible with configured memory_namespace_shards={} and shard_hash_version={}: entity key in {} belongs to shard {} under the configured layout but is stored in shard {}; changing the shard count or hash layout requires an explicit reshard operation before startup",
                    root_dir.display(),
                    configured_shards,
                    shard_hash_version,
                    shard_file.path.display(),
                    expected,
                    shard_file.shard_index
                )));
            }
        }
        // Empty legacy shard files cannot prove the previous shard count because
        // shard databases are created lazily. They are accepted only because all
        // discovered persisted entity keys above are compatible.
    }
    Ok(())
}

struct LegacyMemoryShardFile {
    path: PathBuf,
    shard_index: usize,
}

fn discover_legacy_memory_shard_files(root_dir: &Path) -> Result<Vec<LegacyMemoryShardFile>> {
    let mut files = Vec::new();
    let entries = match std::fs::read_dir(root_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(error) => return Err(memory_error(error)),
    };
    for entry in entries {
        let entry = entry.map_err(memory_error)?;
        let namespace_path = entry.path();
        if !namespace_path.is_dir() {
            continue;
        }
        let Some(raw_namespace) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if hex_decode_to_utf8(&raw_namespace).is_none() {
            continue;
        }
        let shard_entries = std::fs::read_dir(&namespace_path).map_err(memory_error)?;
        for shard_entry in shard_entries {
            let shard_entry = shard_entry.map_err(memory_error)?;
            let path = shard_entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(shard_index) = parse_managed_memory_shard_file_name(file_name)? else {
                continue;
            };
            files.push(LegacyMemoryShardFile { path, shard_index });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn parse_managed_memory_shard_file_name(file_name: &str) -> Result<Option<usize>> {
    let Some(rest) = file_name.strip_prefix("shard-") else {
        return Ok(None);
    };
    let Some(index) = rest.strip_suffix(".db") else {
        return Ok(None);
    };
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PlatformError::runtime(format!(
            "malformed managed memory shard database file name: {file_name}"
        )));
    }
    index.parse::<usize>().map(Some).map_err(|error| {
        PlatformError::runtime(format!(
            "malformed managed memory shard database file name {file_name}: {error}"
        ))
    })
}

async fn distinct_entity_keys_in_memory_db(path: &Path) -> Result<Vec<String>> {
    let path_str = path.to_string_lossy().to_string();
    let database = Builder::new_local(&path_str)
        .build()
        .await
        .map_err(memory_error)?;
    let conn = database.connect().map_err(memory_error)?;
    configure_connection(&conn).await?;
    let mut keys = HashSet::new();
    for table in [
        "memory_meta",
        "memory_state",
        "memory_commands",
        "memory_outbox",
    ] {
        read_distinct_entity_keys_from_table(&conn, table, &mut keys).await?;
    }
    let mut keys = keys.into_iter().collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

async fn read_distinct_entity_keys_from_table(
    conn: &Connection,
    table: &str,
    keys: &mut HashSet<String>,
) -> Result<()> {
    let sql = format!("SELECT DISTINCT entity_key FROM {table}");
    let mut rows = match conn.query(&sql, ()).await {
        Ok(rows) => rows,
        Err(error) => {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("no such table") || message.contains("no such column") {
                return Ok(());
            }
            return Err(memory_error(error));
        }
    };
    while let Some(row) = rows.next().await.map_err(memory_error)? {
        keys.insert(row.get::<String>(0).map_err(memory_error)?);
    }
    Ok(())
}

fn shard_index_from_db_path(path: &Path) -> Option<usize> {
    let name = path.file_stem()?.to_str()?;
    let index = name.strip_prefix("shard-")?;
    index.parse::<usize>().ok()
}

async fn read_single_i64(conn: &Connection, sql: &str) -> Result<i64> {
    let mut rows = match conn.query(sql, ()).await {
        Ok(rows) => rows,
        Err(error) => {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("no such table") || message.contains("no such column") {
                return Ok(0);
            }
            return Err(memory_error(error));
        }
    };
    let Some(row) = rows.next().await.map_err(memory_error)? else {
        return Ok(0);
    };
    let value = row
        .get::<Option<i64>>(0)
        .map_err(memory_error)?
        .unwrap_or(0)
        .max(0);
    let _ = rows.next().await.map_err(memory_error)?;
    Ok(value)
}

async fn configure_connection(conn: &Connection) -> Result<()> {
    configure_turso_connection(conn, memory_error)
}

async fn ensure_mvcc_mode(conn: &Connection) -> Result<()> {
    conn.pragma_update("journal_mode", "'mvcc'")
        .await
        .map_err(memory_error)?;
    let mut rows = conn
        .query("PRAGMA journal_mode", ())
        .await
        .map_err(memory_error)?;
    let Some(row) = rows.next().await.map_err(memory_error)? else {
        return Err(PlatformError::runtime(
            "memory store error: failed to read journal_mode",
        ));
    };
    let mode = row.get::<String>(0).map_err(memory_error)?;
    let _ = rows.next().await.map_err(memory_error)?;
    if !mode.eq_ignore_ascii_case("mvcc") {
        return Err(PlatformError::runtime(format!(
            "memory store error: expected mvcc journal mode, got {mode}",
        )));
    }
    Ok(())
}

async fn ensure_compat_columns(conn: &Connection) -> Result<()> {
    let mut rows = conn
        .query("PRAGMA table_info(memory_state)", ())
        .await
        .map_err(memory_error)?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await.map_err(memory_error)? {
        let name: String = row.get::<String>(1).map_err(memory_error)?;
        columns.insert(name);
    }

    if !columns.contains("value_blob") {
        conn.execute("ALTER TABLE memory_state ADD COLUMN value_blob BLOB", ())
            .await
            .map_err(memory_error)?;
    }
    if !columns.contains("encoding") {
        conn.execute(
            "ALTER TABLE memory_state ADD COLUMN encoding TEXT NOT NULL DEFAULT 'utf8'",
            (),
        )
        .await
        .map_err(memory_error)?;
    }

    let mut rows = conn
        .query("PRAGMA table_info(memory_meta)", ())
        .await
        .map_err(memory_error)?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await.map_err(memory_error)? {
        let name: String = row.get::<String>(1).map_err(memory_error)?;
        columns.insert(name);
    }
    if !columns.contains("owner_epoch") {
        conn.execute(
            "ALTER TABLE memory_meta ADD COLUMN owner_epoch INTEGER NOT NULL DEFAULT 0",
            (),
        )
        .await
        .map_err(memory_error)?;
    }
    Ok(())
}

fn is_retryable_memory_error(error: &turso::Error) -> bool {
    is_retryable_turso_error(error)
}

fn is_retryable_platform_memory_error(error: &PlatformError) -> bool {
    is_retryable_turso_error(error)
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}

#[allow(dead_code)]
fn duration_ms_i64(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PlatformError::bad_request("duration is too large"))
}

fn memory_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("memory store error: {error}"))
}

fn normalize_outbox_kind_selectors(kinds: &[&str]) -> (Vec<String>, Vec<String>) {
    let mut exact = Vec::new();
    let mut prefixes = Vec::new();
    for kind in kinds {
        let kind = kind.trim();
        if kind.is_empty() {
            continue;
        }
        if let Some(prefix) = kind.strip_suffix('*') {
            let prefix = prefix.trim();
            if !prefix.is_empty() {
                prefixes.push(prefix.to_string());
            }
        } else {
            exact.push(kind.to_string());
        }
    }
    exact.sort();
    exact.dedup();
    prefixes.sort();
    prefixes.dedup();
    (exact, prefixes)
}

fn escape_sql_like_prefix(prefix: &str) -> String {
    let mut escaped = String::with_capacity(prefix.len());
    for value in prefix.chars() {
        match value {
            '%' | '_' | '\\' => {
                escaped.push('\\');
                escaped.push(value);
            }
            _ => escaped.push(value),
        }
    }
    escaped
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(memory_error)?;
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2).max(2));
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    if out.is_empty() {
        "00".to_string()
    } else {
        out
    }
}

fn normalize_encoding(raw: &str) -> String {
    match raw {
        ENCODING_UTF8 => ENCODING_UTF8.to_string(),
        ENCODING_V8SC => ENCODING_V8SC.to_string(),
        _ => ENCODING_UTF8.to_string(),
    }
}

fn snapshot_entries_from_records(
    records: &HashMap<String, MemorySnapshotEntry>,
) -> Vec<MemorySnapshotEntry> {
    let mut entries = records.values().cloned().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

fn partitioned_budget(total: usize, shards: usize, shard_index: usize) -> usize {
    let shards = shards.max(1);
    let base = total / shards;
    let remainder = total % shards;
    base + usize::from(shard_index % shards < remainder)
}

fn approximate_snapshot_cache_bytes(
    cache_key: &str,
    records: &HashMap<String, MemorySnapshotEntry>,
    loaded_keys: &HashSet<String>,
) -> usize {
    // This intentionally approximates allocator overhead while staying cheap
    // and deterministic. It accounts for cache key bytes, loaded-key strings,
    // record keys, value buffers, encodings, and a small per-entry overhead.
    let record_bytes = records
        .values()
        .map(|entry| {
            96usize
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.len())
                .saturating_add(entry.encoding.len())
        })
        .sum::<usize>();
    let loaded_key_bytes = loaded_keys
        .iter()
        .map(|key| 48usize.saturating_add(key.len()))
        .sum::<usize>();
    128usize
        .saturating_add(cache_key.len())
        .saturating_add(record_bytes)
        .saturating_add(loaded_key_bytes)
}

fn hex_decode_to_utf8(input: &str) -> Option<String> {
    if !input.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    let chars = input.as_bytes();
    let mut index = 0usize;
    while index < chars.len() {
        let hi = char::from(chars[index]).to_digit(16)?;
        let lo = char::from(chars[index + 1]).to_digit(16)?;
        bytes.push(((hi << 4) | lo) as u8);
        index += 2;
    }
    String::from_utf8(bytes).ok()
}

/// Stable storage-format hash for persisted memory shard routing.
pub fn stable_memory_shard_index(memory_key: &str, namespace_shards: usize) -> usize {
    if namespace_shards <= 1 {
        return 0;
    }
    let mut hasher = Sha256::new();
    hasher.update(MEMORY_SHARD_HASH_DOMAIN);
    hasher.update(memory_key.as_bytes());
    let digest = hasher.finalize();
    let mut shard_bytes = [0u8; 8];
    shard_bytes.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(shard_bytes) as usize) % namespace_shards
}

fn legacy_default_hasher_memory_shard_index(memory_key: &str, namespace_shards: usize) -> usize {
    if namespace_shards <= 1 {
        return 0;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    memory_key.hash(&mut hasher);
    (hasher.finish() as usize) % namespace_shards
}

fn memory_shard_index_for_hash_version(
    memory_key: &str,
    namespace_shards: usize,
    shard_hash_version: u32,
) -> Result<usize> {
    match shard_hash_version {
        STABLE_SHA256_MEMORY_SHARD_HASH_VERSION => {
            Ok(stable_memory_shard_index(memory_key, namespace_shards))
        }
        LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION => Ok(
            legacy_default_hasher_memory_shard_index(memory_key, namespace_shards),
        ),
        version => Err(PlatformError::runtime(format!(
            "unsupported memory shard_hash_version {version}; supported shard_hash_versions are {}",
            supported_memory_shard_hash_versions_label()
        ))),
    }
}

fn is_supported_memory_shard_hash_version(version: u32) -> bool {
    matches!(
        version,
        STABLE_SHA256_MEMORY_SHARD_HASH_VERSION | LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION
    )
}

fn supported_memory_shard_hash_versions_label() -> &'static str {
    "0 (legacy DefaultHasher), 1 (dd-memory-shard-v1)"
}

const ENCODING_UTF8: &str = "utf8";
const ENCODING_V8SC: &str = "v8sc";

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-memory-store-{name}-{}", Uuid::new_v4()))
    }

    fn utf8_mutation(key: &str, value: &str) -> MemoryBatchMutation {
        MemoryBatchMutation {
            key: key.to_string(),
            value: value.as_bytes().to_vec(),
            encoding: ENCODING_UTF8.to_string(),
            deleted: false,
        }
    }

    fn key_with_different_shard(old_shards: usize, new_shards: usize) -> String {
        (0..10_000)
            .map(|index| format!("entity-{index}"))
            .find(|key| {
                let old_shard = stable_memory_shard_index(key, old_shards);
                old_shard < new_shards && old_shard != stable_memory_shard_index(key, new_shards)
            })
            .expect("test should find key with different shard")
    }

    fn key_with_different_hash_layout(shards: usize) -> (String, usize, usize) {
        (0..10_000)
            .map(|index| format!("legacy-entity-{index}"))
            .find_map(|key| {
                let legacy = legacy_default_hasher_memory_shard_index(&key, shards);
                let stable = stable_memory_shard_index(&key, shards);
                (legacy != stable).then_some((key, legacy, stable))
            })
            .expect("test should find key with different legacy and stable shards")
    }

    fn key_at_or_above_shard(shards: usize, minimum_shard: usize) -> String {
        (0..10_000)
            .map(|index| format!("entity-{index}"))
            .find(|key| stable_memory_shard_index(key, shards) >= minimum_shard)
            .expect("test should find key at or above requested shard")
    }

    async fn seed_legacy_default_hasher_memory_state(
        root: &Path,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
        value: &str,
        version: i64,
        namespace_shards: usize,
    ) -> Result<usize> {
        let shard_index = legacy_default_hasher_memory_shard_index(memory_key, namespace_shards);
        let path = root
            .join(hex_encode(namespace.as_bytes()))
            .join(format!("shard-{shard_index:04}.db"));
        ensure_parent_dir(&path)?;
        let path_str = path.to_string_lossy().to_string();
        let database = Builder::new_local(&path_str)
            .build()
            .await
            .map_err(memory_error)?;
        ensure_schema(&database).await?;
        let conn = database.connect().map_err(memory_error)?;
        configure_connection(&conn).await?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(memory_error)?;
        let outcome = async {
            upsert_memory_state_row(
                &conn,
                memory_key,
                item_key,
                value.as_bytes(),
                ENCODING_UTF8,
                false,
                version,
            )
            .await?;
            upsert_memory_meta_row(&conn, memory_key, version, None).await?;
            Ok::<(), PlatformError>(())
        }
        .await;
        match outcome {
            Ok(()) => {
                conn.execute("COMMIT", ()).await.map_err(memory_error)?;
            }
            Err(error) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(error);
            }
        }
        Ok(shard_index)
    }

    fn assert_send_sync<T: Send + Sync>() {}

    async fn expect_memory_store_new_error(
        root: PathBuf,
        namespace_shards: usize,
        message: &str,
    ) -> PlatformError {
        match MemoryStore::new(root, namespace_shards, 4, Duration::from_secs(60)).await {
            Ok(_) => panic!("{message}"),
            Err(error) => error,
        }
    }

    #[test]
    fn memory_outbox_effect_id_is_stable_and_ordinal_specific() {
        let effect = MemoryOutboxEffectWrite {
            kind: "audit.created".to_string(),
            payload: b"payload".to_vec(),
        };
        let first = memory_outbox_effect_id("entity", 7, 0, &effect);
        assert_eq!(first, memory_outbox_effect_id("entity", 7, 0, &effect));
        assert_ne!(first, memory_outbox_effect_id("entity", 7, 1, &effect));
        assert_ne!(first, memory_outbox_effect_id("entity", 8, 0, &effect));
        assert!(first.starts_with("memfx_"));
        assert_eq!(first.len(), "memfx_".len() + 64);
    }

    #[tokio::test]
    async fn memory_db_paths_are_stable_per_namespace() -> Result<()> {
        let store = MemoryStore::new(temp_root("paths"), 16, 4, Duration::from_secs(60)).await?;
        let shard = store.shard_index("memory-a");
        let ns_a = store.db_path("ns", shard);
        let ns_a_again = store.db_path("ns", shard);
        let ns_b = store.db_path("other", shard);

        assert_eq!(ns_a, ns_a_again);
        assert_ne!(ns_a, ns_b);
        Ok(())
    }

    #[test]
    fn memory_shard_index_is_pinned_to_stable_sha256_routing() {
        assert_eq!(stable_memory_shard_index("", 1), 0);
        assert_eq!(stable_memory_shard_index("memory-a", 4), 2);
        assert_eq!(stable_memory_shard_index("memory-b", 4), 3);
        assert_eq!(stable_memory_shard_index("room/42", 16), 0);
        assert_eq!(stable_memory_shard_index("alpha", 16), 6);
        assert_eq!(stable_memory_shard_index("memory-a", 4096), 3386);
        assert_eq!(stable_memory_shard_index("memory-b", 4096), 2551);
    }

    #[test]
    fn turso_database_handle_is_shareable_across_threads() {
        assert_send_sync::<Database>();
    }

    #[test]
    fn memory_cache_budget_partitioning_is_global_and_deterministic() {
        assert_eq!(
            (0..4)
                .map(|shard| partitioned_budget(2, 4, shard))
                .collect::<Vec<_>>(),
            vec![1, 1, 0, 0]
        );
        assert_eq!(
            (0..4)
                .map(|shard| partitioned_budget(6, 4, shard))
                .collect::<Vec<_>>(),
            vec![2, 2, 1, 1]
        );
    }

    #[tokio::test]
    async fn concurrent_cold_database_open_uses_shared_slot() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("concurrent-cold-open"),
            1,
            4,
            Duration::from_secs(60),
        )
        .await?;
        let store = Arc::new(store);
        let mut tasks = Vec::new();
        for index in 0..16 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                store
                    .apply_batch(
                        "ns",
                        "memory-a",
                        &[utf8_mutation("count", &index.to_string())],
                        None,
                        &[],
                        None,
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await
                .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        }
        let shard = store.shard_for_key("memory-a");
        assert_eq!(shard.databases.lock().await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn memory_database_cache_budget_is_not_multiplied_by_shards() -> Result<()> {
        let store =
            MemoryStore::new(temp_root("db-global-budget"), 4, 2, Duration::from_secs(60)).await?;
        for index in 0..64 {
            let key = format!("memory-{index}");
            store
                .apply_batch("ns", &key, &[utf8_mutation("count", "1")], None, &[], None)
                .await?;
        }
        let mut cached = 0usize;
        for shard in store.shards.iter() {
            cached = cached.saturating_add(shard.databases.lock().await.len());
        }
        assert!(
            cached <= 2,
            "cached database slots exceeded global budget: {cached}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_snapshot_cache_eviction_keeps_recent_entry() -> Result<()> {
        let mut store = MemoryStore::new(
            temp_root("snapshot-lru-budget"),
            1,
            4,
            Duration::from_secs(60),
        )
        .await?;
        store.snapshot_cache_max_entries = 1;
        store.snapshot_cache_max_bytes = usize::MAX / 2;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        store
            .apply_batch(
                "ns",
                "memory-b",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                None,
            )
            .await?;
        store.snapshot("ns", "memory-a").await?;
        store.snapshot("ns", "memory-b").await?;

        let snapshots = store
            .shard_for_key("memory-a")
            .shared_snapshots
            .lock()
            .await;
        assert!(!snapshots
            .entries
            .contains_key(&MemoryStore::memory_snapshot_key("ns", "memory-a")));
        assert!(snapshots
            .entries
            .contains_key(&MemoryStore::memory_snapshot_key("ns", "memory-b")));
        assert_eq!(snapshots.entries.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_memory_snapshot_is_not_cached() -> Result<()> {
        let mut store = MemoryStore::new(
            temp_root("snapshot-oversized"),
            1,
            4,
            Duration::from_secs(60),
        )
        .await?;
        store.snapshot_cache_max_entries = 16;
        store.snapshot_cache_max_bytes = 1;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("large", "value")],
                None,
                &[],
                None,
            )
            .await?;
        store.snapshot("ns", "memory-a").await?;
        let snapshots = store
            .shard_for_key("memory-a")
            .shared_snapshots
            .lock()
            .await;
        assert!(snapshots.entries.is_empty());
        assert_eq!(snapshots.approximate_bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_is_created_for_fresh_root() -> Result<()> {
        let root = temp_root("layout-fresh");
        let _store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;

        let manifest = read_memory_layout(&memory_layout_manifest_path(&root))?;
        assert_eq!(manifest, MemoryLayoutManifest::current(16));
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_accepts_matching_reopen() -> Result<()> {
        let root = temp_root("layout-reopen");
        let _store = MemoryStore::new(root.clone(), 8, 4, Duration::from_secs(60)).await?;
        let _store = MemoryStore::new(root, 8, 4, Duration::from_secs(60)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_rejects_shard_count_change_before_new_shard_file() -> Result<()>
    {
        let root = temp_root("layout-mismatch");
        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        let before = discover_legacy_memory_shard_files(&root)?.len();

        let error = expect_memory_store_new_error(
            root.clone(),
            4,
            "shard-count mismatch must fail startup",
        )
        .await;
        let message = error.to_string();
        assert!(message.contains("memory layout mismatch"), "{message}");
        assert!(
            message.contains("configured memory_namespace_shards=4"),
            "{message}"
        );
        assert!(
            message.contains("persisted namespace_shards=16"),
            "{message}"
        );
        assert_eq!(discover_legacy_memory_shard_files(&root)?.len(), before);
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_rejects_unsupported_format_version() -> Result<()> {
        let root = temp_root("layout-format");
        std::fs::create_dir_all(&root).map_err(memory_error)?;
        write_memory_layout_atomic(
            &memory_layout_manifest_path(&root),
            &MemoryLayoutManifest {
                format_version: 2,
                shard_hash_version: MEMORY_SHARD_HASH_VERSION,
                namespace_shards: 16,
            },
        )?;

        let error = expect_memory_store_new_error(
            root,
            16,
            "unsupported manifest format must fail startup",
        )
        .await;
        assert!(
            error
                .to_string()
                .contains("unsupported memory layout manifest format_version"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_rejects_unsupported_hash_version() -> Result<()> {
        let root = temp_root("layout-hash");
        std::fs::create_dir_all(&root).map_err(memory_error)?;
        write_memory_layout_atomic(
            &memory_layout_manifest_path(&root),
            &MemoryLayoutManifest {
                format_version: MEMORY_LAYOUT_FORMAT_VERSION,
                shard_hash_version: 2,
                namespace_shards: 16,
            },
        )?;

        let error =
            expect_memory_store_new_error(root, 16, "unsupported hash version must fail startup")
                .await;
        assert!(
            error
                .to_string()
                .contains("unsupported memory layout shard_hash_version"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_adopts_compatible_legacy_store() -> Result<()> {
        let root = temp_root("layout-legacy-compatible");
        let store = MemoryStore::new(root.clone(), 4, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        std::fs::remove_file(memory_layout_manifest_path(&root)).map_err(memory_error)?;

        let store = MemoryStore::new(root.clone(), 4, 4, Duration::from_secs(60)).await?;
        let point = store.point_read("ns", "memory-a", "count").await?;
        assert_eq!(
            point.record.expect("record should survive adoption").value,
            b"1"
        );
        assert_eq!(
            read_memory_layout(&memory_layout_manifest_path(&root))?.namespace_shards,
            4
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_adopts_legacy_default_hasher_store() -> Result<()> {
        let root = temp_root("layout-legacy-default-hasher");
        let (memory_key, expected_legacy_shard, stable_shard) = key_with_different_hash_layout(16);
        let written_shard =
            seed_legacy_default_hasher_memory_state(&root, "ns", &memory_key, "count", "1", 1, 16)
                .await?;
        assert_eq!(written_shard, expected_legacy_shard);
        assert_ne!(expected_legacy_shard, stable_shard);

        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        let manifest = read_memory_layout(&memory_layout_manifest_path(&root))?;
        assert_eq!(
            manifest.shard_hash_version,
            LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION
        );
        assert_eq!(
            store.shard_index_for_key(&memory_key),
            expected_legacy_shard
        );
        let point = store.point_read("ns", &memory_key, "count").await?;
        assert_eq!(
            point
                .record
                .expect("legacy state should remain readable")
                .value,
            b"1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_rejects_incompatible_legacy_entity_placement() -> Result<()> {
        let root = temp_root("layout-legacy-incompatible");
        let key = key_with_different_shard(6, 4);
        let store = MemoryStore::new(root.clone(), 6, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch("ns", &key, &[utf8_mutation("count", "1")], None, &[], None)
            .await?;
        std::fs::remove_file(memory_layout_manifest_path(&root)).map_err(memory_error)?;

        let error = expect_memory_store_new_error(
            root,
            4,
            "incompatible legacy placement must fail startup",
        )
        .await;
        assert!(
            error
                .to_string()
                .contains("is incompatible with configured memory_namespace_shards"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_rejects_legacy_shard_outside_configured_range() -> Result<()> {
        let root = temp_root("layout-legacy-high-shard");
        let key = key_at_or_above_shard(16, 4);
        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch("ns", &key, &[utf8_mutation("count", "1")], None, &[], None)
            .await?;
        std::fs::remove_file(memory_layout_manifest_path(&root)).map_err(memory_error)?;

        let error =
            expect_memory_store_new_error(root, 4, "higher shard files must not be ignored").await;
        assert!(error
            .to_string()
            .contains("outside configured memory_namespace_shards=4"));
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_rejects_truncated_manifest_without_overwrite() -> Result<()> {
        let root = temp_root("layout-truncated");
        std::fs::create_dir_all(&root).map_err(memory_error)?;
        let path = memory_layout_manifest_path(&root);
        std::fs::write(&path, b"{").map_err(memory_error)?;

        let error =
            expect_memory_store_new_error(root.clone(), 16, "truncated manifest must fail startup")
                .await;
        assert!(error
            .to_string()
            .contains("failed to parse memory layout manifest"));
        assert_eq!(std::fs::read(&path).map_err(memory_error)?, b"{");
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_concurrent_initialization_writes_valid_manifest() -> Result<()> {
        let root = temp_root("layout-concurrent");
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let root = root.clone();
            tasks.push(tokio::spawn(async move {
                MemoryStore::new(root, 16, 4, Duration::from_secs(60)).await
            }));
        }
        for task in tasks {
            task.await
                .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        }

        assert_eq!(
            read_memory_layout(&memory_layout_manifest_path(&root))?,
            MemoryLayoutManifest::current(16)
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_versions_are_shard_local() -> Result<()> {
        let store =
            MemoryStore::new(temp_root("shard-versions"), 4, 4, Duration::from_secs(60)).await?;
        assert_ne!(store.shard_index("memory-a"), store.shard_index("memory-b"));

        let first = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        let second = store
            .apply_batch(
                "ns",
                "memory-b",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;

        assert_eq!(first.max_version, 1);
        assert_eq!(second.max_version, 1);
        Ok(())
    }

    #[tokio::test]
    async fn memory_db_cache_eviction_keeps_persisted_state() -> Result<()> {
        let store = MemoryStore::new(temp_root("eviction"), 1, 1, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        store
            .apply_batch(
                "ns",
                "memory-b",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                None,
            )
            .await?;

        let warm_snapshot = store.snapshot("ns", "memory-b").await?;
        assert_eq!(warm_snapshot.entries.len(), 1);
        assert_eq!(
            store.shard_for_key("memory-b").databases.lock().await.len(),
            1
        );

        let snapshot = store.snapshot("ns", "memory-a").await?;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(
            String::from_utf8(snapshot.entries[0].value.clone()).expect("utf8"),
            "1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_version_if_newer_observes_commits() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("version-if-newer"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;
        assert_eq!(store.version_if_newer("ns", "memory-a", -1).await?, None);
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        assert_eq!(store.version_if_newer("ns", "memory-a", -1).await?, Some(1));
        assert_eq!(store.version_if_newer("ns", "memory-a", 1).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn memory_snapshot_cache_updates_after_transactional_commit() -> Result<()> {
        let store =
            MemoryStore::new(temp_root("snapshot-cache"), 16, 4, Duration::from_secs(60)).await?;

        let initial = store.snapshot("ns", "memory-a").await?;
        assert_eq!(initial.max_version, -1);

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;

        let snapshot = store.snapshot("ns", "memory-a").await?;
        assert_eq!(snapshot.max_version, 1);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].key, "count");
        assert_eq!(
            String::from_utf8(snapshot.entries[0].value.clone()).expect("utf8"),
            "1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_snapshot_cache_uses_committed_version() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-cache-version"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let initial = store.snapshot("ns", "memory-a").await?;
        assert_eq!(initial.max_version, -1);

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let key = MemoryStore::memory_snapshot_key("ns", "memory-a");
        let snapshots = store
            .shard_for_key("memory-a")
            .shared_snapshots
            .lock()
            .await;
        let entry = snapshots
            .entries
            .get(&key)
            .expect("shared cache entry should be updated after commit");
        assert_eq!(entry.max_version, result.max_version);
        assert_eq!(
            entry
                .records
                .get("count")
                .expect("count should be cached")
                .version,
            result.max_version
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_persists_and_advances_owner_epoch() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-owner-epoch"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                Some(42),
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let conn = store.connect("ns", "memory-a").await?;
        assert_eq!(
            read_single_i64(
                &conn,
                "SELECT owner_epoch FROM memory_meta WHERE entity_key = 'memory-a'"
            )
            .await?,
            42
        );

        let next = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                Some(43),
            )
            .await?;
        assert_eq!(next.max_version, 2);
        assert_eq!(
            read_single_i64(
                &conn,
                "SELECT owner_epoch FROM memory_meta WHERE entity_key = 'memory-a'"
            )
            .await?,
            43
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_rejects_stale_owner_epoch() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-stale-owner-epoch"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                Some(42),
            )
            .await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                Some(43),
            )
            .await?;

        let stale = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "stale")],
                None,
                &[],
                Some(42),
            )
            .await
            .expect_err("stale owner must not commit");
        assert!(
            stale
                .to_string()
                .contains("stale memory entity owner epoch 42"),
            "{stale}"
        );

        let conn = store.connect("ns", "memory-a").await?;
        assert_eq!(
            read_single_i64(
                &conn,
                "SELECT owner_epoch FROM memory_meta WHERE entity_key = 'memory-a'"
            )
            .await?,
            43
        );
        let point = store
            .point_read("ns", "memory-a", "count")
            .await?
            .record
            .expect("count should remain committed by current owner");
        assert_eq!(String::from_utf8(point.value).expect("utf8"), "2");
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_stores_command_result_atomically() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-command-result"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                Some(&MemoryCommandResultWrite {
                    idempotency_key: "command-1".to_string(),
                    result: b"ok".to_vec(),
                }),
                &[],
                None,
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let cached = store
            .command_result("ns", "memory-a", "command-1")
            .await?
            .expect("command result should be persisted with the commit");
        assert_eq!(cached.result, b"ok");
        assert_eq!(cached.revision, result.max_version);

        let duplicate = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "duplicate")],
                Some(&MemoryCommandResultWrite {
                    idempotency_key: "command-1".to_string(),
                    result: b"duplicate".to_vec(),
                }),
                &[],
                None,
            )
            .await
            .expect_err("duplicate command result must roll back the batch");
        assert!(
            duplicate
                .to_string()
                .to_ascii_lowercase()
                .contains("unique")
                || duplicate
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("constraint"),
            "{duplicate}"
        );
        let point = store
            .point_read("ns", "memory-a", "count")
            .await?
            .record
            .expect("count should remain from original command");
        assert_eq!(String::from_utf8(point.value).expect("utf8"), "1");
        let cached_after_duplicate = store
            .command_result("ns", "memory-a", "command-1")
            .await?
            .expect("original command result should remain");
        assert_eq!(cached_after_duplicate.result, b"ok");

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "owner")],
                None,
                &[],
                Some(2),
            )
            .await?;

        let stale = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "stale")],
                Some(&MemoryCommandResultWrite {
                    idempotency_key: "command-2".to_string(),
                    result: b"conflicted".to_vec(),
                }),
                &[],
                Some(1),
            )
            .await
            .expect_err("stale owner must not commit command result");
        assert!(
            stale
                .to_string()
                .contains("stale memory entity owner epoch 1"),
            "{stale}"
        );
        assert!(
            store
                .command_result("ns", "memory-a", "command-2")
                .await?
                .is_none(),
            "failed memory commit must not persist a command result",
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_stores_outbox_effects_atomically() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-outbox"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[MemoryOutboxEffectWrite {
                    kind: "audit.created".to_string(),
                    payload: br#"{"count":1}"#.to_vec(),
                }],
                None,
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let outbox = store.outbox_records("ns", "memory-a").await?;
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].kind, "audit.created");
        assert_eq!(outbox[0].payload, br#"{"count":1}"#);
        assert_eq!(outbox[0].revision, result.max_version);
        assert_eq!(outbox[0].status, "pending");
        assert_eq!(
            outbox[0].effect_id,
            memory_outbox_effect_id(
                "memory-a",
                result.max_version,
                0,
                &MemoryOutboxEffectWrite {
                    kind: "audit.created".to_string(),
                    payload: br#"{"count":1}"#.to_vec(),
                },
            )
        );

        let effect_only = store
            .apply_batch(
                "ns",
                "memory-a",
                &[],
                None,
                &[MemoryOutboxEffectWrite {
                    kind: "audit.effect-only".to_string(),
                    payload: b"ok".to_vec(),
                }],
                None,
            )
            .await?;
        assert_eq!(effect_only.max_version, 2);

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "owner")],
                None,
                &[],
                Some(2),
            )
            .await?;

        let stale = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "stale")],
                None,
                &[MemoryOutboxEffectWrite {
                    kind: "audit.conflicted".to_string(),
                    payload: b"no".to_vec(),
                }],
                Some(1),
            )
            .await
            .expect_err("stale owner must not commit outbox effect");
        assert!(
            stale
                .to_string()
                .contains("stale memory entity owner epoch 1"),
            "{stale}"
        );

        let outbox = store.outbox_records("ns", "memory-a").await?;
        assert_eq!(outbox.len(), 2);
        assert_eq!(outbox[1].kind, "audit.effect-only");
        assert_eq!(outbox[1].revision, effect_only.max_version);
        assert_eq!(
            outbox[1].effect_id,
            memory_outbox_effect_id(
                "memory-a",
                effect_only.max_version,
                0,
                &MemoryOutboxEffectWrite {
                    kind: "audit.effect-only".to_string(),
                    payload: b"ok".to_vec(),
                },
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_outbox_claims_marks_and_retries_due_effects() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-outbox-claim"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        for (idx, kind) in ["effect.one", "effect.two", "effect.three"]
            .into_iter()
            .enumerate()
        {
            let result = store
                .apply_batch(
                    "ns",
                    "memory-a",
                    &[],
                    None,
                    &[MemoryOutboxEffectWrite {
                        kind: kind.to_string(),
                        payload: kind.as_bytes().to_vec(),
                    }],
                    None,
                )
                .await?;
            assert_eq!(result.max_version, idx as i64 + 1);
        }

        let claimed = store
            .claim_outbox_records("ns", "memory-a", 2, Duration::from_secs(60))
            .await?;
        assert_eq!(claimed.len(), 2);
        assert_eq!(claimed[0].kind, "effect.one");
        assert_eq!(claimed[1].kind, "effect.two");
        assert_eq!(claimed[0].attempt_count, 1);
        assert_eq!(claimed[1].attempt_count, 1);
        assert_eq!(claimed[0].status, "inflight");

        let next_claim = store
            .claim_outbox_records("ns", "memory-a", 10, Duration::from_secs(60))
            .await?;
        assert_eq!(next_claim.len(), 1);
        assert_eq!(next_claim[0].kind, "effect.three");

        store
            .mark_outbox_delivered("ns", "memory-a", &claimed[0].effect_id)
            .await?;
        store
            .mark_outbox_delivered("ns", "memory-a", &next_claim[0].effect_id)
            .await?;
        store
            .retry_outbox_record(
                "ns",
                "memory-a",
                &claimed[1].effect_id,
                Duration::from_secs(60),
            )
            .await?;

        let no_due = store
            .claim_outbox_records("ns", "memory-a", 10, Duration::from_secs(60))
            .await?;
        assert!(no_due.is_empty());

        store
            .retry_outbox_record("ns", "memory-a", &claimed[1].effect_id, Duration::ZERO)
            .await?;
        let retry = store
            .claim_outbox_records("ns", "memory-a", 10, Duration::from_secs(60))
            .await?;
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].kind, "effect.two");
        assert_eq!(retry[0].attempt_count, 2);
        store
            .mark_outbox_delivered("ns", "memory-a", &retry[0].effect_id)
            .await?;

        let records = store.outbox_records("ns", "memory-a").await?;
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|record| record.status == "delivered"));
        Ok(())
    }

    #[tokio::test]
    async fn memory_outbox_global_claim_filters_supported_effects() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-outbox-global-claim"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        for (namespace, memory_key, kind) in [
            ("ns-a", "memory-a", "socket.send"),
            ("ns-a", "memory-b", "audit.created"),
            ("ns-b", "memory-c", "transport.close"),
        ] {
            store
                .apply_batch(
                    namespace,
                    memory_key,
                    &[],
                    None,
                    &[MemoryOutboxEffectWrite {
                        kind: kind.to_string(),
                        payload: br#"{"handle":"h"}"#.to_vec(),
                    }],
                    None,
                )
                .await?;
        }

        let mut claims = store
            .claim_due_outbox_records(
                10,
                Duration::from_secs(60),
                &["socket.send", "transport.close"],
            )
            .await?;
        claims.sort_by(|a, b| {
            (a.namespace.as_str(), a.memory_key.as_str())
                .cmp(&(b.namespace.as_str(), b.memory_key.as_str()))
        });
        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].namespace, "ns-a");
        assert_eq!(claims[0].memory_key, "memory-a");
        assert_eq!(claims[0].record.kind, "socket.send");
        assert_eq!(claims[1].namespace, "ns-b");
        assert_eq!(claims[1].memory_key, "memory-c");
        assert_eq!(claims[1].record.kind, "transport.close");

        let no_due = store
            .claim_due_outbox_records(
                10,
                Duration::from_secs(60),
                &["socket.send", "transport.close"],
            )
            .await?;
        assert!(no_due.is_empty());

        let audit = store.outbox_records("ns-a", "memory-b").await?;
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].kind, "audit.created");
        assert_eq!(audit[0].status, "pending");
        Ok(())
    }

    #[tokio::test]
    async fn memory_outbox_global_claim_supports_prefix_kinds() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-outbox-prefix-claim"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        for (namespace, memory_key, kind) in [
            ("ns-a", "memory-a", "audit.created"),
            ("ns-a", "memory-b", "audit.updated"),
            ("ns-b", "memory-c", "trace.request"),
            ("ns-b", "memory-d", "effect.unsupported"),
        ] {
            store
                .apply_batch(
                    namespace,
                    memory_key,
                    &[],
                    None,
                    &[MemoryOutboxEffectWrite {
                        kind: kind.to_string(),
                        payload: kind.as_bytes().to_vec(),
                    }],
                    None,
                )
                .await?;
        }

        let mut claims = store
            .claim_due_outbox_records(10, Duration::from_secs(60), &["audit.*", "trace.*"])
            .await?;
        claims.sort_by(|a, b| {
            (a.namespace.as_str(), a.memory_key.as_str())
                .cmp(&(b.namespace.as_str(), b.memory_key.as_str()))
        });
        assert_eq!(claims.len(), 3);
        assert_eq!(claims[0].record.kind, "audit.created");
        assert_eq!(claims[1].record.kind, "audit.updated");
        assert_eq!(claims[2].record.kind, "trace.request");

        let unsupported = store.outbox_records("ns-b", "memory-d").await?;
        assert_eq!(unsupported.len(), 1);
        assert_eq!(unsupported[0].kind, "effect.unsupported");
        assert_eq!(unsupported[0].status, "pending");
        Ok(())
    }

    #[tokio::test]
    async fn memory_commit_snapshot_cache_uses_committed_version() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("coordinator-cache-version"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let initial = store.snapshot("ns", "memory-a").await?;
        assert_eq!(initial.max_version, -1);

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                Some(1),
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let key = MemoryStore::memory_snapshot_key("ns", "memory-a");
        let snapshots = store
            .shard_for_key("memory-a")
            .shared_snapshots
            .lock()
            .await;
        let entry = snapshots
            .entries
            .get(&key)
            .expect("shared cache entry should be updated after commit");
        assert_eq!(entry.max_version, result.max_version);
        assert_eq!(
            entry
                .records
                .get("count")
                .expect("count should be cached")
                .version,
            result.max_version
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_point_read_populates_partial_shared_cache_and_tracks_misses() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("point-read-cache"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;

        let hit = store.point_read("ns", "memory-a", "count").await?;
        assert_eq!(hit.max_version, 1);
        assert_eq!(
            String::from_utf8(
                hit.record
                    .as_ref()
                    .expect("count record should be present")
                    .value
                    .clone()
            )
            .expect("utf8"),
            "1"
        );

        let key = MemoryStore::memory_snapshot_key("ns", "memory-a");
        {
            let snapshots = store
                .shard_for_key("memory-a")
                .shared_snapshots
                .lock()
                .await;
            let entry = snapshots
                .entries
                .get(&key)
                .expect("shared cache entry should exist");
            assert!(!entry.complete);
            assert!(entry.loaded_keys.contains("count"));
            assert!(entry.records.contains_key("count"));
        }

        let miss = store.point_read("ns", "memory-a", "missing").await?;
        assert_eq!(miss.max_version, 1);
        assert!(miss.record.is_none());

        let snapshots = store
            .shard_for_key("memory-a")
            .shared_snapshots
            .lock()
            .await;
        let entry = snapshots
            .entries
            .get(&key)
            .expect("shared cache entry should exist");
        assert!(entry.loaded_keys.contains("missing"));
        assert!(!entry.records.contains_key("missing"));
        Ok(())
    }

    #[tokio::test]
    async fn memory_point_read_cache_updates_after_commit() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("point-read-commit"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        let first = store.point_read("ns", "memory-a", "count").await?;
        assert_eq!(first.max_version, 1);

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                None,
            )
            .await?;

        let updated = store.point_read("ns", "memory-a", "count").await?;
        assert_eq!(updated.max_version, 2);
        assert_eq!(
            String::from_utf8(
                updated
                    .record
                    .as_ref()
                    .expect("count record should still be present")
                    .value
                    .clone()
            )
            .expect("utf8"),
            "2"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_writes_complete_past_repeated_commit_threshold() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let store = MemoryStore::new(
                temp_root("transactional-write-threshold"),
                16,
                4,
                Duration::from_secs(60),
            )
            .await?;

            for idx in 0..64 {
                let next_value = (idx + 1).to_string();
                store
                    .apply_batch(
                        "ns",
                        "memory-a",
                        &[utf8_mutation("count", &next_value)],
                        None,
                        &[],
                        Some(idx as i64 + 1),
                    )
                    .await?;
            }

            let final_value = store.point_read("ns", "memory-a", "count").await?;
            assert_eq!(
                String::from_utf8(
                    final_value
                        .record
                        .expect("count should be present after repeated transactional writes")
                        .value
                )
                .expect("utf8"),
                "64"
            );
            Ok::<(), PlatformError>(())
        })
        .await
        .map_err(|_| {
            PlatformError::runtime(
                "transactional write threshold test timed out before completing 64 commits",
            )
        })??;
        Ok(())
    }
}
