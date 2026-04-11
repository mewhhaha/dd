use common::{PlatformError, Result};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify};
use turso::{Builder, Connection, Database, Value};

struct ActorDatabaseEntry {
    database: Arc<Database>,
    last_used_at: Instant,
}

struct ActorSharedSnapshotEntry {
    records: Arc<HashMap<String, ActorSnapshotEntry>>,
    loaded_keys: Arc<HashSet<String>>,
    complete: bool,
    max_version: i64,
    last_used_at: Instant,
}

#[derive(Default)]
struct ActorWriteShardState {
    pending_namespaces: HashSet<String>,
    token_waiters: HashMap<u64, Vec<u64>>,
}

struct ActorWriteShard {
    state: Mutex<ActorWriteShardState>,
    notify: Notify,
}

#[derive(Debug, Clone)]
pub struct ActorDirectMutation {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ActorQueuedMutationKey {
    actor_key: String,
    item_key: String,
}

#[derive(Debug, Clone)]
struct ActorPendingMutationEntry {
    actor_key: String,
    mutation: ActorDirectMutation,
    token: u64,
    queue_ids: Vec<i64>,
    completion_tokens: Vec<u64>,
}

#[derive(Debug)]
struct ActorWriteSubmission {
    remaining_parts: usize,
    max_version: i64,
    result: Option<Result<i64>>,
    notify: Arc<Notify>,
}

#[derive(Debug, Clone)]
struct ActorDirectQueueRow {
    queue_id: i64,
    actor_key: String,
    item_key: String,
    value: Vec<u8>,
    encoding: String,
    deleted: bool,
    token: u64,
}

#[derive(Clone)]
pub struct ActorStore {
    root_dir: Arc<PathBuf>,
    databases: Arc<Mutex<HashMap<String, ActorDatabaseEntry>>>,
    actor_versions: Arc<Mutex<HashMap<String, i64>>>,
    shared_snapshots: Arc<Mutex<HashMap<String, ActorSharedSnapshotEntry>>>,
    write_shards: Arc<Vec<Arc<ActorWriteShard>>>,
    write_submissions: Arc<Mutex<HashMap<u64, ActorWriteSubmission>>>,
    db_cache_max_open: usize,
    db_idle_ttl: Duration,
    namespace_shards: usize,
    snapshot_cache_max_entries: usize,
    write_flush_delay: Duration,
    write_flush_batch_size: usize,
    write_max_pending_keys: usize,
    next_write_submission_id: Arc<AtomicU64>,
    next_write_token: Arc<AtomicU64>,
    version: Arc<AtomicU64>,
    profile: Arc<ActorProfile>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorProfileMetricKind {
    JsReadOnlyTotal,
    JsFreshnessCheck,
    JsHydrateFull,
    JsHydrateKeys,
    JsTxnCommit,
    JsTxnBlindCommit,
    JsTxnValidate,
    JsCacheHit,
    JsCacheMiss,
    JsCacheStale,
    OpRead,
    OpSnapshot,
    OpVersionIfNewer,
    OpValidateReads,
    OpApplyBatch,
    OpApplyBlindBatch,
    StoreDirectEnqueue,
    StoreDirectAwait,
    StoreDirectQueueLoad,
    StoreDirectQueueFlush,
    StoreDirectQueueDelete,
    StoreDirectWaiterComplete,
    StoreRead,
    StoreSnapshot,
    StoreSnapshotKeys,
    StoreVersionIfNewer,
    StoreApplyBatch,
    StoreApplyBatchValidate,
    StoreApplyBatchWrite,
    StoreApplyBlindBatch,
    StoreApplyBlindBatchWrite,
}

#[derive(Default)]
struct ActorProfileMetric {
    calls: AtomicU64,
    total_us: AtomicU64,
    total_items: AtomicU64,
    max_us: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActorProfileMetricSnapshot {
    pub calls: u64,
    pub total_us: u64,
    pub total_items: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActorProfileSnapshot {
    pub enabled: bool,
    pub js_read_only_total: ActorProfileMetricSnapshot,
    pub js_freshness_check: ActorProfileMetricSnapshot,
    pub js_hydrate_full: ActorProfileMetricSnapshot,
    pub js_hydrate_keys: ActorProfileMetricSnapshot,
    pub js_txn_commit: ActorProfileMetricSnapshot,
    pub js_txn_blind_commit: ActorProfileMetricSnapshot,
    pub js_txn_validate: ActorProfileMetricSnapshot,
    pub js_cache_hit: ActorProfileMetricSnapshot,
    pub js_cache_miss: ActorProfileMetricSnapshot,
    pub js_cache_stale: ActorProfileMetricSnapshot,
    pub op_read: ActorProfileMetricSnapshot,
    pub op_snapshot: ActorProfileMetricSnapshot,
    pub op_version_if_newer: ActorProfileMetricSnapshot,
    pub op_validate_reads: ActorProfileMetricSnapshot,
    pub op_apply_batch: ActorProfileMetricSnapshot,
    pub op_apply_blind_batch: ActorProfileMetricSnapshot,
    pub store_direct_enqueue: ActorProfileMetricSnapshot,
    pub store_direct_await: ActorProfileMetricSnapshot,
    pub store_direct_queue_load: ActorProfileMetricSnapshot,
    pub store_direct_queue_flush: ActorProfileMetricSnapshot,
    pub store_direct_queue_delete: ActorProfileMetricSnapshot,
    pub store_direct_waiter_complete: ActorProfileMetricSnapshot,
    pub store_read: ActorProfileMetricSnapshot,
    pub store_snapshot: ActorProfileMetricSnapshot,
    pub store_snapshot_keys: ActorProfileMetricSnapshot,
    pub store_version_if_newer: ActorProfileMetricSnapshot,
    pub store_apply_batch: ActorProfileMetricSnapshot,
    pub store_apply_batch_validate: ActorProfileMetricSnapshot,
    pub store_apply_batch_write: ActorProfileMetricSnapshot,
    pub store_apply_blind_batch: ActorProfileMetricSnapshot,
    pub store_apply_blind_batch_write: ActorProfileMetricSnapshot,
}

#[derive(Default)]
pub struct ActorProfile {
    enabled: AtomicBool,
    js_read_only_total: ActorProfileMetric,
    js_freshness_check: ActorProfileMetric,
    js_hydrate_full: ActorProfileMetric,
    js_hydrate_keys: ActorProfileMetric,
    js_txn_commit: ActorProfileMetric,
    js_txn_blind_commit: ActorProfileMetric,
    js_txn_validate: ActorProfileMetric,
    js_cache_hit: ActorProfileMetric,
    js_cache_miss: ActorProfileMetric,
    js_cache_stale: ActorProfileMetric,
    op_read: ActorProfileMetric,
    op_snapshot: ActorProfileMetric,
    op_version_if_newer: ActorProfileMetric,
    op_validate_reads: ActorProfileMetric,
    op_apply_batch: ActorProfileMetric,
    op_apply_blind_batch: ActorProfileMetric,
    store_direct_enqueue: ActorProfileMetric,
    store_direct_await: ActorProfileMetric,
    store_direct_queue_load: ActorProfileMetric,
    store_direct_queue_flush: ActorProfileMetric,
    store_direct_queue_delete: ActorProfileMetric,
    store_direct_waiter_complete: ActorProfileMetric,
    store_read: ActorProfileMetric,
    store_snapshot: ActorProfileMetric,
    store_snapshot_keys: ActorProfileMetric,
    store_version_if_newer: ActorProfileMetric,
    store_apply_batch: ActorProfileMetric,
    store_apply_batch_validate: ActorProfileMetric,
    store_apply_batch_write: ActorProfileMetric,
    store_apply_blind_batch: ActorProfileMetric,
    store_apply_blind_batch_write: ActorProfileMetric,
}

#[derive(Debug, Clone)]
pub struct ActorSnapshotEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub version: i64,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct ActorSnapshot {
    pub entries: Vec<ActorSnapshotEntry>,
    pub max_version: i64,
}

#[derive(Debug, Clone)]
pub struct ActorPointRead {
    pub record: Option<ActorSnapshotEntry>,
    pub max_version: i64,
}

#[derive(Debug, Clone)]
pub struct ActorBatchMutation {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub version: i64,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct ActorReadDependency {
    pub key: String,
    pub version: i64,
}

#[derive(Debug, Clone)]
pub struct ActorBatchApplyResult {
    pub conflict: bool,
    pub max_version: i64,
}

impl ActorStore {
    pub async fn new(
        root_dir: PathBuf,
        namespace_shards: usize,
        db_cache_max_open: usize,
        db_idle_ttl: Duration,
    ) -> Result<Self> {
        std::fs::create_dir_all(&root_dir).map_err(actor_error)?;
        if namespace_shards == 0 {
            return Err(PlatformError::internal(
                "actor_namespace_shards must be greater than 0",
            ));
        }
        if db_cache_max_open == 0 {
            return Err(PlatformError::internal(
                "actor_db_cache_max_open must be greater than 0",
            ));
        }
        if db_idle_ttl.is_zero() {
            return Err(PlatformError::internal(
                "actor_db_idle_ttl must be greater than 0",
            ));
        }
        let bootstrapped_version = detect_actor_version_floor(&root_dir).await?;
        let write_shards = Arc::new(
            (0..namespace_shards)
                .map(|_| {
                    Arc::new(ActorWriteShard {
                        state: Mutex::new(ActorWriteShardState::default()),
                        notify: Notify::new(),
                    })
                })
                .collect::<Vec<_>>(),
        );
        let store = Self {
            root_dir: Arc::new(root_dir),
            databases: Arc::new(Mutex::new(HashMap::new())),
            actor_versions: Arc::new(Mutex::new(HashMap::new())),
            shared_snapshots: Arc::new(Mutex::new(HashMap::new())),
            write_shards,
            write_submissions: Arc::new(Mutex::new(HashMap::new())),
            db_cache_max_open,
            db_idle_ttl,
            namespace_shards,
            snapshot_cache_max_entries: db_cache_max_open.max(64),
            write_flush_delay: Duration::from_millis(2),
            write_flush_batch_size: 128,
            write_max_pending_keys: 8_192,
            next_write_submission_id: Arc::new(AtomicU64::new(1)),
            next_write_token: Arc::new(AtomicU64::new(bootstrapped_version.max(1))),
            version: Arc::new(AtomicU64::new(bootstrapped_version.max(1))),
            profile: Arc::new(ActorProfile::default()),
        };
        for shard_index in 0..namespace_shards {
            let store_clone = store.clone();
            tokio::spawn(async move {
                store_clone.run_write_shard(shard_index).await;
            });
        }
        Ok(store)
    }

    pub fn set_profile_enabled(&self, enabled: bool) {
        self.profile.set_enabled(enabled);
    }

    pub fn record_profile(&self, metric: ActorProfileMetricKind, duration_us: u64, items: u64) {
        self.profile.record(metric, duration_us, items);
    }

    pub fn take_profile_snapshot_and_reset(&self) -> ActorProfileSnapshot {
        self.profile.take_snapshot_and_reset()
    }

    pub fn reset_profile(&self) {
        self.profile.reset();
    }

    pub async fn snapshot(&self, namespace: &str, actor_key: &str) -> Result<ActorSnapshot> {
        let started = Instant::now();
        if let Some(snapshot) = self.cached_full_snapshot(namespace, actor_key).await {
            self.observe_version(snapshot.max_version);
            self.observe_actor_version(namespace, actor_key, snapshot.max_version)
                .await;
            self.record_profile(
                ActorProfileMetricKind::StoreSnapshot,
                started.elapsed().as_micros() as u64,
                snapshot.entries.len() as u64,
            );
            return Ok(snapshot);
        }
        let conn = self.connect(namespace, actor_key).await?;
        let mut rows = conn
            .query(
                "SELECT item_key, value_blob, encoding, value, version, deleted
                 FROM actor_state
                 WHERE entity_key = ?1
                 ORDER BY item_key ASC",
                (actor_key,),
            )
            .await
            .map_err(actor_error)?;

        let mut entries = Vec::new();
        let mut max_version = -1i64;
        while let Some(row) = rows.next().await.map_err(actor_error)? {
            let key: String = row.get::<String>(0).map_err(actor_error)?;
            let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(1).map_err(actor_error)?;
            let encoding: String = row.get::<String>(2).map_err(actor_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(actor_error)?;
            let version: i64 = row.get::<i64>(4).map_err(actor_error)?;
            let deleted: i64 = row.get::<i64>(5).map_err(actor_error)?;
            max_version = max_version.max(version);
            entries.push(ActorSnapshotEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
                version,
                deleted: deleted != 0,
            });
        }
        self.observe_version(max_version);
        self.observe_actor_version(namespace, actor_key, max_version)
            .await;
        let snapshot = ActorSnapshot {
            entries,
            max_version,
        };
        self.put_full_snapshot(namespace, actor_key, &snapshot)
            .await;
        self.record_profile(
            ActorProfileMetricKind::StoreSnapshot,
            started.elapsed().as_micros() as u64,
            snapshot.entries.len() as u64,
        );
        Ok(snapshot)
    }

    pub async fn point_read(
        &self,
        namespace: &str,
        actor_key: &str,
        item_key: &str,
    ) -> Result<ActorPointRead> {
        let started = Instant::now();
        let item_key = item_key.trim();
        if item_key.is_empty() {
            return Err(PlatformError::runtime("actor item key must not be empty"));
        }
        if let Some(point) = self.cached_point_read(namespace, actor_key, item_key).await {
            self.observe_version(point.max_version);
            self.observe_actor_version(namespace, actor_key, point.max_version)
                .await;
            self.record_profile(
                ActorProfileMetricKind::StoreRead,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(point);
        }

        let conn = self.connect(namespace, actor_key).await?;
        let record = self.record_for_key(&conn, actor_key, item_key).await?;
        let max_version = self
            .max_version_for_actor(&conn, actor_key)
            .await?
            .unwrap_or(-1);
        self.observe_version(max_version);
        self.observe_actor_version(namespace, actor_key, max_version)
            .await;
        self.put_partial_snapshot(
            namespace,
            actor_key,
            max_version,
            record.clone().into_iter().collect::<Vec<_>>(),
            std::iter::once(item_key.to_string()),
            false,
        )
        .await;
        self.record_profile(
            ActorProfileMetricKind::StoreRead,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok(ActorPointRead {
            record,
            max_version,
        })
    }

    pub async fn snapshot_keys(
        &self,
        namespace: &str,
        actor_key: &str,
        keys: &[String],
    ) -> Result<ActorSnapshot> {
        if keys.is_empty() {
            return self.snapshot(namespace, actor_key).await;
        }
        let started = Instant::now();
        let filtered_keys = keys
            .iter()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty())
            .collect::<Vec<_>>();
        if filtered_keys.is_empty() {
            let conn = self.connect(namespace, actor_key).await?;
            let max_version = self
                .max_version_for_actor(&conn, actor_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(max_version);
            return Ok(ActorSnapshot {
                entries: Vec::new(),
                max_version,
            });
        }
        if let Some(snapshot) = self
            .cached_keys_snapshot(namespace, actor_key, &filtered_keys)
            .await
        {
            self.record_profile(
                ActorProfileMetricKind::StoreSnapshotKeys,
                started.elapsed().as_micros() as u64,
                filtered_keys.len() as u64,
            );
            return Ok(snapshot);
        }
        let conn = self.connect(namespace, actor_key).await?;
        let placeholders = (0..filtered_keys.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT item_key, value_blob, encoding, value, version, deleted
             FROM actor_state
             WHERE entity_key = ?1 AND item_key IN ({placeholders})
             ORDER BY item_key ASC"
        );
        let mut params = Vec::with_capacity(filtered_keys.len() + 1);
        params.push(Value::Text(actor_key.to_string()));
        params.extend(
            filtered_keys
                .iter()
                .map(|key| Value::Text((*key).to_string())),
        );
        let mut entries = Vec::new();
        let mut rows = conn.query(&sql, params).await.map_err(actor_error)?;
        while let Some(row) = rows.next().await.map_err(actor_error)? {
            let key: String = row.get::<String>(0).map_err(actor_error)?;
            let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(1).map_err(actor_error)?;
            let encoding: String = row.get::<String>(2).map_err(actor_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(actor_error)?;
            let version: i64 = row.get::<i64>(4).map_err(actor_error)?;
            let deleted: i64 = row.get::<i64>(5).map_err(actor_error)?;
            entries.push(ActorSnapshotEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
                version,
                deleted: deleted != 0,
            });
        }
        let max_version = self
            .max_version_for_actor(&conn, actor_key)
            .await?
            .unwrap_or(-1);
        self.observe_version(max_version);
        self.observe_actor_version(namespace, actor_key, max_version)
            .await;
        self.put_partial_snapshot(
            namespace,
            actor_key,
            max_version,
            entries.clone(),
            filtered_keys.iter().cloned(),
            false,
        )
        .await;
        self.record_profile(
            ActorProfileMetricKind::StoreSnapshotKeys,
            started.elapsed().as_micros() as u64,
            filtered_keys.len() as u64,
        );
        Ok(ActorSnapshot {
            entries,
            max_version,
        })
    }

    pub async fn validate_reads(
        &self,
        namespace: &str,
        actor_key: &str,
        reads: &[ActorReadDependency],
        list_gate_version: Option<i64>,
    ) -> Result<ActorBatchApplyResult> {
        let started = Instant::now();
        let conn = self.connect(namespace, actor_key).await?;
        let current = self
            .max_version_for_actor(&conn, actor_key)
            .await?
            .unwrap_or(-1);
        if let Some(expected_list_version) = list_gate_version {
            if current != expected_list_version {
                self.observe_actor_version(namespace, actor_key, current)
                    .await;
                return Ok(ActorBatchApplyResult {
                    conflict: true,
                    max_version: current,
                });
            }
        }
        if reads.is_empty() {
            self.observe_actor_version(namespace, actor_key, current)
                .await;
            return Ok(ActorBatchApplyResult {
                conflict: false,
                max_version: current,
            });
        }
        let placeholders = (0..reads.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT item_key, version
             FROM actor_state
             WHERE entity_key = ?1 AND item_key IN ({placeholders})"
        );
        let mut params = Vec::with_capacity(reads.len() + 1);
        params.push(Value::Text(actor_key.to_string()));
        params.extend(
            reads
                .iter()
                .map(|dependency| Value::Text(dependency.key.clone())),
        );
        let mut rows = conn.query(&sql, params).await.map_err(actor_error)?;
        let mut observed_versions = HashMap::with_capacity(reads.len());
        while let Some(row) = rows.next().await.map_err(actor_error)? {
            let key: String = row.get::<String>(0).map_err(actor_error)?;
            let version: i64 = row.get::<i64>(1).map_err(actor_error)?;
            observed_versions.insert(key, version);
        }
        for dependency in reads {
            let observed = observed_versions
                .get(&dependency.key)
                .copied()
                .unwrap_or(-1);
            if observed != dependency.version {
                self.observe_actor_version(namespace, actor_key, current.max(observed))
                    .await;
                return Ok(ActorBatchApplyResult {
                    conflict: true,
                    max_version: current.max(observed),
                });
            }
        }
        self.observe_actor_version(namespace, actor_key, current)
            .await;
        self.record_profile(
            ActorProfileMetricKind::OpValidateReads,
            started.elapsed().as_micros() as u64,
            reads.len() as u64,
        );
        Ok(ActorBatchApplyResult {
            conflict: false,
            max_version: current,
        })
    }

    pub async fn version_if_newer(
        &self,
        namespace: &str,
        actor_key: &str,
        known_version: i64,
    ) -> Result<Option<i64>> {
        let started = Instant::now();
        let actor_key = actor_key.trim();
        if actor_key.is_empty() {
            return Err(PlatformError::runtime("actor key must not be empty"));
        }
        let version_key = Self::actor_version_key(namespace, actor_key);
        if let Some(current) = self.actor_versions.lock().await.get(&version_key).copied() {
            self.record_profile(
                ActorProfileMetricKind::StoreVersionIfNewer,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok((current > known_version).then_some(current));
        }
        let conn = self.connect(namespace, actor_key).await?;
        let current = self
            .max_version_for_actor(&conn, actor_key)
            .await?
            .unwrap_or(-1);
        self.observe_actor_version(namespace, actor_key, current)
            .await;
        self.record_profile(
            ActorProfileMetricKind::StoreVersionIfNewer,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok((current > known_version).then_some(current))
    }

    pub async fn enqueue_direct_batch(
        &self,
        namespace: &str,
        actor_key: &str,
        mutations: &[ActorDirectMutation],
    ) -> Result<u64> {
        let started = Instant::now();
        let namespace = namespace.trim();
        if namespace.is_empty() {
            return Err(PlatformError::runtime("memory namespace must not be empty"));
        }
        let actor_key = actor_key.trim();
        if actor_key.is_empty() {
            return Err(PlatformError::runtime("actor key must not be empty"));
        }
        let coalesced = coalesce_direct_mutations(mutations)?;
        let submission_id = self.next_write_submission_id.fetch_add(1, Ordering::SeqCst);
        let notify = Arc::new(Notify::new());
        if coalesced.is_empty() {
            self.write_submissions.lock().await.insert(
                submission_id,
                ActorWriteSubmission {
                    remaining_parts: 0,
                    max_version: self
                        .actor_versions
                        .lock()
                        .await
                        .get(&Self::actor_version_key(namespace, actor_key))
                        .copied()
                        .unwrap_or(-1),
                    result: Some(Ok(-1)),
                    notify,
                },
            );
            return Ok(submission_id);
        }

        let shard_index = self.shard_index(actor_key);
        let shard = self.write_shards[shard_index].clone();
        let queued = coalesced
            .into_iter()
            .map(|mutation| {
                (
                    self.next_write_token.fetch_add(1, Ordering::SeqCst),
                    mutation,
                )
            })
            .collect::<Vec<_>>();
        let conn = self.connect_shard_uncached(namespace, shard_index).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(actor_error(error)),
            }
            let outcome = async {
                let queued_rows = self.direct_queue_len(&conn).await?;
                if queued_rows.saturating_add(queued.len()) > self.write_max_pending_keys {
                    return Err(PlatformError::runtime(
                        "actor direct write queue overloaded",
                    ));
                }
                for (token, mutation) in &queued {
                    let now_ms = epoch_ms_i64()?;
                    conn.execute(
                        "INSERT INTO actor_direct_queue (entity_key, item_key, value_blob, encoding, deleted, token, enqueued_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        (
                            actor_key,
                            mutation.key.as_str(),
                            mutation.value.as_slice(),
                            mutation.encoding.as_str(),
                            if mutation.deleted { 1 } else { 0 },
                            *token as i64,
                            now_ms,
                        ),
                    )
                    .await
                    .map_err(actor_error)?;
                }
                Ok::<(), PlatformError>(())
            }
            .await;
            match outcome {
                Ok(()) => match conn.execute("COMMIT", ()).await {
                    Ok(_) => break,
                    Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Err(actor_error(error));
                    }
                },
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(error);
                }
            }
        }
        self.write_submissions.lock().await.insert(
            submission_id,
            ActorWriteSubmission {
                remaining_parts: queued.len(),
                max_version: -1,
                result: None,
                notify,
            },
        );
        {
            let mut state = shard.state.lock().await;
            state.pending_namespaces.insert(namespace.to_string());
            for (token, _) in &queued {
                state
                    .token_waiters
                    .entry(*token)
                    .or_default()
                    .push(submission_id);
            }
        }
        shard.notify.notify_one();
        self.record_profile(
            ActorProfileMetricKind::StoreDirectEnqueue,
            started.elapsed().as_micros() as u64,
            queued.len() as u64,
        );
        Ok(submission_id)
    }

    #[allow(dead_code)]
    pub async fn wait_direct_submission(&self, submission_id: u64) -> Result<i64> {
        let started = Instant::now();
        loop {
            let notify = {
                let mut submissions = self.write_submissions.lock().await;
                let Some(entry) = submissions.get(&submission_id) else {
                    return Err(PlatformError::runtime(format!(
                        "unknown actor write submission {submission_id}"
                    )));
                };
                if let Some(result) = &entry.result {
                    let result = result.clone();
                    submissions.remove(&submission_id);
                    self.record_profile(
                        ActorProfileMetricKind::StoreDirectAwait,
                        started.elapsed().as_micros() as u64,
                        1,
                    );
                    return result;
                }
                entry.notify.clone()
            };
            tokio::select! {
                _ = notify.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
            }
        }
    }

    pub fn try_poll_direct_submission(&self, submission_id: u64) -> Result<Option<i64>> {
        let Ok(mut submissions) = self.write_submissions.try_lock() else {
            return Ok(None);
        };
        Self::poll_direct_submission_locked(&mut submissions, submission_id)
    }

    fn poll_direct_submission_locked(
        submissions: &mut HashMap<u64, ActorWriteSubmission>,
        submission_id: u64,
    ) -> Result<Option<i64>> {
        let Some(entry) = submissions.get(&submission_id) else {
            return Err(PlatformError::runtime(format!(
                "unknown actor write submission {submission_id}"
            )));
        };
        let Some(result) = &entry.result else {
            return Ok(None);
        };
        let result = result.clone();
        submissions.remove(&submission_id);
        result.map(Some)
    }

    pub async fn apply_batch(
        &self,
        namespace: &str,
        actor_key: &str,
        reads: &[ActorReadDependency],
        mutations: &[ActorBatchMutation],
        expected_base_version: Option<i64>,
        list_gate_version: Option<i64>,
        transactional: bool,
    ) -> Result<ActorBatchApplyResult> {
        let started = Instant::now();
        let conn = if mutations.is_empty() {
            self.connect(namespace, actor_key).await?
        } else {
            self.connect_uncached(namespace, actor_key).await?
        };
        if mutations.is_empty() && reads.is_empty() && list_gate_version.is_none() {
            let max_version = self
                .max_version_for_actor(&conn, actor_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(max_version);
            self.record_profile(
                ActorProfileMetricKind::StoreApplyBatch,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(ActorBatchApplyResult {
                conflict: false,
                max_version,
            });
        }

        for mutation in mutations {
            if mutation.key.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "actor batch mutation key must not be empty",
                ));
            }
            if mutation.version < 0 {
                return Err(PlatformError::bad_request(
                    "actor batch mutation version must be non-negative",
                ));
            }
            if !mutation.deleted
                && mutation.encoding != ENCODING_UTF8
                && mutation.encoding != ENCODING_V8SC
            {
                return Err(PlatformError::bad_request(format!(
                    "unsupported actor storage encoding: {}",
                    mutation.encoding
                )));
            }
        }

        if !transactional {
            let mut previous_version = expected_base_version.unwrap_or(-1);
            for mutation in mutations {
                if mutation.version <= previous_version {
                    return Err(PlatformError::bad_request(
                        "actor batch mutation versions must be strictly increasing",
                    ));
                }
                previous_version = mutation.version;
            }
        }

        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN CONCURRENT", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(actor_error(error)),
            }

            let outcome = async {
                let validate_started = Instant::now();
                let current = self.max_version_for_actor(&conn, actor_key).await?.unwrap_or(-1);
                if let Some(expected_list_version) = list_gate_version {
                    if current != expected_list_version {
                        self.observe_actor_version(namespace, actor_key, current).await;
                        self.record_profile(
                            ActorProfileMetricKind::StoreApplyBatchValidate,
                            validate_started.elapsed().as_micros() as u64,
                            reads.len() as u64 + 1,
                        );
                        return Ok(ActorBatchApplyResult {
                            conflict: true,
                            max_version: current,
                        });
                    }
                }
                if !transactional {
                    if let Some(expected) = expected_base_version {
                        if current != expected {
                            self.observe_actor_version(namespace, actor_key, current).await;
                            self.record_profile(
                                ActorProfileMetricKind::StoreApplyBatchValidate,
                                validate_started.elapsed().as_micros() as u64,
                                reads.len() as u64 + 1,
                            );
                            return Ok(ActorBatchApplyResult {
                                conflict: true,
                                max_version: current,
                            });
                        }
                    }
                }
                for dependency in reads {
                    let observed = self
                        .version_for_key(&conn, actor_key, dependency.key.as_str())
                        .await?
                        .unwrap_or(-1);
                    if observed != dependency.version {
                        self.observe_actor_version(namespace, actor_key, current.max(observed))
                            .await;
                        self.record_profile(
                            ActorProfileMetricKind::StoreApplyBatchValidate,
                            validate_started.elapsed().as_micros() as u64,
                            reads.len() as u64 + 1,
                        );
                        return Ok(ActorBatchApplyResult {
                            conflict: true,
                            max_version: current.max(observed),
                        });
                    }
                }
                self.record_profile(
                    ActorProfileMetricKind::StoreApplyBatchValidate,
                    validate_started.elapsed().as_micros() as u64,
                    reads.len() as u64 + 1,
                );

                let write_started = Instant::now();
                let commit_version = if transactional && !mutations.is_empty() {
                    Some(self.reserve_version_after(current))
                } else {
                    None
                };

                for mutation in mutations {
                    let version = commit_version.unwrap_or(mutation.version);
                    let now_ms = epoch_ms_i64()?;
                    if mutation.deleted {
                        let empty_blob: &[u8] = &[];
                        conn.execute(
                            "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                             VALUES (?1, ?2, '', ?3, ?4, 1, ?5, ?6)
                             ON CONFLICT(entity_key, item_key) DO UPDATE SET
                               value = excluded.value,
                               value_blob = excluded.value_blob,
                               encoding = excluded.encoding,
                               deleted = 1,
                               version = excluded.version,
                               updated_at_ms = excluded.updated_at_ms",
                            (
                                actor_key,
                                mutation.key.as_str(),
                                empty_blob,
                                ENCODING_UTF8,
                                version,
                                now_ms,
                            ),
                        )
                        .await
                        .map_err(actor_error)?;
                        continue;
                    }

                    let value_text = if mutation.encoding == ENCODING_UTF8 {
                        std::str::from_utf8(&mutation.value)
                            .map_err(|error| {
                                PlatformError::bad_request(format!("invalid utf8 value: {error}"))
                            })?
                            .to_string()
                    } else {
                        String::new()
                    };
                    conn.execute(
                        "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)
                         ON CONFLICT(entity_key, item_key) DO UPDATE SET
                           value = excluded.value,
                           value_blob = excluded.value_blob,
                           encoding = excluded.encoding,
                           deleted = 0,
                           version = excluded.version,
                           updated_at_ms = excluded.updated_at_ms",
                        (
                            actor_key,
                            mutation.key.as_str(),
                            value_text.as_str(),
                            mutation.value.as_slice(),
                            mutation.encoding.as_str(),
                            version,
                            now_ms,
                        ),
                    )
                    .await
                    .map_err(actor_error)?;
                }

                let max_version = if let Some(version) = commit_version {
                    version
                } else {
                    mutations
                        .last()
                        .map(|mutation| mutation.version)
                        .unwrap_or(current)
                };
                if !mutations.is_empty() {
                    let now_ms = epoch_ms_i64()?;
                    conn.execute(
                        "INSERT INTO actor_meta (entity_key, max_version, updated_at_ms)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT(entity_key) DO UPDATE SET
                           max_version = excluded.max_version,
                           updated_at_ms = excluded.updated_at_ms",
                        (actor_key, max_version, now_ms),
                    )
                    .await
                    .map_err(actor_error)?;
                }
                self.update_cached_snapshot_after_commit(namespace, actor_key, max_version, mutations)
                    .await;
                self.record_profile(
                    ActorProfileMetricKind::StoreApplyBatchWrite,
                    write_started.elapsed().as_micros() as u64,
                    mutations.len() as u64 + 1,
                );
                Ok(ActorBatchApplyResult {
                    conflict: false,
                    max_version,
                })
            }
            .await;

            match outcome {
                Ok(result) => {
                    if result.conflict {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Ok(result);
                    }
                    match conn.execute("COMMIT", ()).await {
                        Ok(_) => {}
                        Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            tokio::time::sleep(std::time::Duration::from_millis(
                                5 * attempt as u64,
                            ))
                            .await;
                            continue;
                        }
                        Err(error) => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            return Err(actor_error(error));
                        }
                    }
                    self.observe_version(result.max_version);
                    self.observe_actor_version(namespace, actor_key, result.max_version)
                        .await;
                    self.record_profile(
                        ActorProfileMetricKind::StoreApplyBatch,
                        started.elapsed().as_micros() as u64,
                        mutations.len() as u64 + reads.len() as u64 + 1,
                    );
                    return Ok(result);
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(error);
                }
            }
        }
    }

    pub async fn apply_blind_batch(
        &self,
        namespace: &str,
        actor_key: &str,
        mutations: &[ActorBatchMutation],
    ) -> Result<ActorBatchApplyResult> {
        let started = Instant::now();
        let conn = if mutations.is_empty() {
            self.connect(namespace, actor_key).await?
        } else {
            self.connect_uncached(namespace, actor_key).await?
        };
        if mutations.is_empty() {
            let max_version = self
                .max_version_for_actor(&conn, actor_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(max_version);
            self.record_profile(
                ActorProfileMetricKind::StoreApplyBlindBatch,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(ActorBatchApplyResult {
                conflict: false,
                max_version,
            });
        }

        for mutation in mutations {
            if mutation.key.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "actor batch mutation key must not be empty",
                ));
            }
            if !mutation.deleted
                && mutation.encoding != ENCODING_UTF8
                && mutation.encoding != ENCODING_V8SC
            {
                return Err(PlatformError::bad_request(format!(
                    "unsupported actor storage encoding: {}",
                    mutation.encoding
                )));
            }
        }

        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN CONCURRENT", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(actor_error(error)),
            }

            let outcome = async {
                let current = self.max_version_for_actor(&conn, actor_key).await?.unwrap_or(-1);
                let write_started = Instant::now();
                let commit_version = self.reserve_version_after(current);
                for mutation in mutations {
                    let now_ms = epoch_ms_i64()?;
                    if mutation.deleted {
                        let empty_blob: &[u8] = &[];
                        conn.execute(
                            "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                             VALUES (?1, ?2, '', ?3, ?4, 1, ?5, ?6)
                             ON CONFLICT(entity_key, item_key) DO UPDATE SET
                               value = excluded.value,
                               value_blob = excluded.value_blob,
                               encoding = excluded.encoding,
                               deleted = 1,
                               version = excluded.version,
                               updated_at_ms = excluded.updated_at_ms",
                            (
                                actor_key,
                                mutation.key.as_str(),
                                empty_blob,
                                ENCODING_UTF8,
                                commit_version,
                                now_ms,
                            ),
                        )
                        .await
                        .map_err(actor_error)?;
                    } else {
                        let value_text = if mutation.encoding == ENCODING_UTF8 {
                            std::str::from_utf8(&mutation.value)
                                .map_err(|error| {
                                    PlatformError::bad_request(format!(
                                        "invalid utf8 value: {error}"
                                    ))
                                })?
                                .to_string()
                        } else {
                            String::new()
                        };
                        conn.execute(
                            "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)
                             ON CONFLICT(entity_key, item_key) DO UPDATE SET
                               value = excluded.value,
                               value_blob = excluded.value_blob,
                               encoding = excluded.encoding,
                               deleted = 0,
                               version = excluded.version,
                               updated_at_ms = excluded.updated_at_ms",
                            (
                                actor_key,
                                mutation.key.as_str(),
                                value_text.as_str(),
                                mutation.value.as_slice(),
                                mutation.encoding.as_str(),
                                commit_version,
                                now_ms,
                            ),
                        )
                        .await
                        .map_err(actor_error)?;
                    }
                }
                let now_ms = epoch_ms_i64()?;
                conn.execute(
                    "INSERT INTO actor_meta (entity_key, max_version, updated_at_ms)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(entity_key) DO UPDATE SET
                       max_version = excluded.max_version,
                       updated_at_ms = excluded.updated_at_ms",
                    (actor_key, commit_version, now_ms),
                )
                .await
                .map_err(actor_error)?;

                let committed_mutations = mutations
                    .iter()
                    .map(|mutation| ActorBatchMutation {
                        key: mutation.key.clone(),
                        value: mutation.value.clone(),
                        encoding: mutation.encoding.clone(),
                        version: commit_version,
                        deleted: mutation.deleted,
                    })
                    .collect::<Vec<_>>();
                self.update_cached_snapshot_after_commit(
                    namespace,
                    actor_key,
                    commit_version,
                    &committed_mutations,
                )
                .await;
                self.record_profile(
                    ActorProfileMetricKind::StoreApplyBlindBatchWrite,
                    write_started.elapsed().as_micros() as u64,
                    mutations.len() as u64 + 1,
                );
                Ok::<ActorBatchApplyResult, PlatformError>(ActorBatchApplyResult {
                    conflict: false,
                    max_version: commit_version,
                })
            }
            .await;

            match outcome {
                Ok(result) => {
                    match conn.execute("COMMIT", ()).await {
                        Ok(_) => {}
                        Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            tokio::time::sleep(std::time::Duration::from_millis(
                                5 * attempt as u64,
                            ))
                            .await;
                            continue;
                        }
                        Err(error) => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            return Err(actor_error(error));
                        }
                    }
                    self.observe_version(result.max_version);
                    self.observe_actor_version(namespace, actor_key, result.max_version)
                        .await;
                    self.record_profile(
                        ActorProfileMetricKind::StoreApplyBlindBatch,
                        started.elapsed().as_micros() as u64,
                        mutations.len() as u64 + 1,
                    );
                    return Ok(result);
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(error);
                }
            }
        }
    }

    async fn run_write_shard(&self, shard_index: usize) {
        let shard = self.write_shards[shard_index].clone();
        if let Ok(namespaces) = self.discover_namespaces_for_shard(shard_index).await {
            let mut state = shard.state.lock().await;
            state.pending_namespaces.extend(namespaces);
        }
        shard.notify.notify_one();
        loop {
            shard.notify.notified().await;
            tokio::time::sleep(self.write_flush_delay).await;
            loop {
                let namespaces = {
                    let mut state = shard.state.lock().await;
                    if state.pending_namespaces.is_empty() {
                        if let Ok(namespaces) =
                            self.discover_namespaces_for_shard(shard_index).await
                        {
                            state.pending_namespaces.extend(namespaces);
                        }
                    }
                    state.pending_namespaces.iter().cloned().collect::<Vec<_>>()
                };
                if namespaces.is_empty() {
                    break;
                }
                let mut did_work = false;
                for namespace in namespaces {
                    let batch = match self
                        .load_direct_queue_batch(
                            &namespace,
                            shard_index,
                            self.write_flush_batch_size,
                        )
                        .await
                    {
                        Ok(batch) => batch,
                        Err(_) => {
                            did_work = true;
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                    };
                    if batch.is_empty() {
                        let mut state = shard.state.lock().await;
                        state.pending_namespaces.remove(&namespace);
                        continue;
                    }
                    did_work = true;
                    if let Err(error) = self
                        .flush_namespace_direct_group(&namespace, shard_index, batch.clone(), true)
                        .await
                    {
                        let message = error.to_string();
                        self.fail_pending_batch(shard_index, &batch, &message).await;
                    }
                }
                if !did_work {
                    break;
                }
            }
        }
    }

    async fn flush_namespace_direct_group(
        &self,
        namespace: &str,
        shard_index: usize,
        entries: Vec<ActorPendingMutationEntry>,
        allow_split: bool,
    ) -> Result<()> {
        let started = Instant::now();
        if entries.is_empty() {
            return Ok(());
        }
        let mut actor_groups = HashMap::<String, Vec<ActorPendingMutationEntry>>::new();
        for entry in entries {
            actor_groups
                .entry(entry.actor_key.clone())
                .or_default()
                .push(entry);
        }
        let actor_group_count = actor_groups.len() as u64;
        let conn = self.connect_shard_uncached(namespace, shard_index).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN CONCURRENT", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(actor_error(error)),
            }

            let outcome = async {
                for (actor_key, entries) in &actor_groups {
                    let mut actor_max_version = -1i64;
                    for entry in entries {
                        let version = entry.token as i64;
                        actor_max_version = actor_max_version.max(version);
                        let now_ms = epoch_ms_i64()?;
                        if entry.mutation.deleted {
                            let empty_blob: &[u8] = &[];
                            conn.execute(
                                "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                                 VALUES (?1, ?2, '', ?3, ?4, 1, ?5, ?6)
                                 ON CONFLICT(entity_key, item_key) DO UPDATE SET
                                   value = CASE WHEN excluded.version > actor_state.version THEN excluded.value ELSE actor_state.value END,
                                   value_blob = CASE WHEN excluded.version > actor_state.version THEN excluded.value_blob ELSE actor_state.value_blob END,
                                   encoding = CASE WHEN excluded.version > actor_state.version THEN excluded.encoding ELSE actor_state.encoding END,
                                   deleted = CASE WHEN excluded.version > actor_state.version THEN 1 ELSE actor_state.deleted END,
                                   version = CASE WHEN excluded.version > actor_state.version THEN excluded.version ELSE actor_state.version END,
                                   updated_at_ms = CASE WHEN excluded.version > actor_state.version THEN excluded.updated_at_ms ELSE actor_state.updated_at_ms END",
                                (
                                    actor_key.as_str(),
                                    entry.mutation.key.as_str(),
                                    empty_blob,
                                    ENCODING_UTF8,
                                    version,
                                    now_ms,
                                ),
                            )
                            .await
                            .map_err(actor_error)?;
                        } else {
                            let value_text = if entry.mutation.encoding == ENCODING_UTF8 {
                                std::str::from_utf8(&entry.mutation.value)
                                    .map_err(|error| {
                                        PlatformError::bad_request(format!(
                                            "invalid utf8 value: {error}"
                                        ))
                                    })?
                                    .to_string()
                            } else {
                                String::new()
                            };
                            conn.execute(
                                "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                                 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)
                                 ON CONFLICT(entity_key, item_key) DO UPDATE SET
                                   value = CASE WHEN excluded.version > actor_state.version THEN excluded.value ELSE actor_state.value END,
                                   value_blob = CASE WHEN excluded.version > actor_state.version THEN excluded.value_blob ELSE actor_state.value_blob END,
                                   encoding = CASE WHEN excluded.version > actor_state.version THEN excluded.encoding ELSE actor_state.encoding END,
                                   deleted = CASE WHEN excluded.version > actor_state.version THEN 0 ELSE actor_state.deleted END,
                                   version = CASE WHEN excluded.version > actor_state.version THEN excluded.version ELSE actor_state.version END,
                                   updated_at_ms = CASE WHEN excluded.version > actor_state.version THEN excluded.updated_at_ms ELSE actor_state.updated_at_ms END",
                                (
                                    actor_key.as_str(),
                                    entry.mutation.key.as_str(),
                                    value_text.as_str(),
                                    entry.mutation.value.as_slice(),
                                    entry.mutation.encoding.as_str(),
                                    version,
                                    now_ms,
                                ),
                            )
                            .await
                            .map_err(actor_error)?;
                        }
                    }
                    let now_ms = epoch_ms_i64()?;
                    conn.execute(
                        "INSERT INTO actor_meta (entity_key, max_version, updated_at_ms)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT(entity_key) DO UPDATE SET
                           max_version = CASE WHEN excluded.max_version > actor_meta.max_version THEN excluded.max_version ELSE actor_meta.max_version END,
                           updated_at_ms = CASE WHEN excluded.max_version > actor_meta.max_version THEN excluded.updated_at_ms ELSE actor_meta.updated_at_ms END",
                        (actor_key.as_str(), actor_max_version, now_ms),
                    )
                    .await
                    .map_err(actor_error)?;
                }
                let delete_started = Instant::now();
                let mut deleted = 0u64;
                for queue_id in actor_groups
                    .values()
                    .flat_map(|entries| entries.iter())
                    .flat_map(|entry| entry.queue_ids.iter().copied())
                {
                    conn.execute(
                        "DELETE FROM actor_direct_queue WHERE queue_id = ?1",
                        (queue_id,),
                    )
                    .await
                    .map_err(actor_error)?;
                    deleted += 1;
                }
                self.record_profile(
                    ActorProfileMetricKind::StoreDirectQueueDelete,
                    delete_started.elapsed().as_micros() as u64,
                    deleted,
                );
                Ok::<(), PlatformError>(())
            }
            .await;

            match outcome {
                Ok(()) => match conn.execute("COMMIT", ()).await {
                    Ok(_) => {
                        self.record_profile(
                            ActorProfileMetricKind::StoreDirectQueueFlush,
                            started.elapsed().as_micros() as u64,
                            actor_group_count,
                        );
                        for (actor_key, entries) in actor_groups {
                            let version = entries
                                .iter()
                                .map(|entry| entry.token as i64)
                                .max()
                                .unwrap_or(-1);
                            self.observe_version(version);
                            self.observe_actor_version(namespace, &actor_key, version)
                                .await;
                            let mutations = entries
                                .iter()
                                .map(|entry| ActorBatchMutation {
                                    key: entry.mutation.key.clone(),
                                    value: entry.mutation.value.clone(),
                                    encoding: entry.mutation.encoding.clone(),
                                    version: entry.token as i64,
                                    deleted: entry.mutation.deleted,
                                })
                                .collect::<Vec<_>>();
                            self.update_cached_snapshot_after_commit(
                                namespace, &actor_key, version, &mutations,
                            )
                            .await;
                            self.complete_waiters_for_tokens(
                                shard_index,
                                entries
                                    .into_iter()
                                    .flat_map(|entry| entry.completion_tokens.into_iter())
                                    .collect::<Vec<_>>(),
                                version,
                            )
                            .await;
                        }
                        return Ok(());
                    }
                    Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        if allow_split && actor_groups.len() > 1 && attempt >= 3 {
                            for (actor_key, entries) in actor_groups {
                                self.flush_single_actor_direct_group(
                                    namespace,
                                    shard_index,
                                    actor_key,
                                    entries,
                                )
                                .await?;
                            }
                            return Ok(());
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Err(actor_error(error));
                    }
                },
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(error);
                }
            }
        }
    }

    async fn flush_single_actor_direct_group(
        &self,
        namespace: &str,
        shard_index: usize,
        actor_key: String,
        entries: Vec<ActorPendingMutationEntry>,
    ) -> Result<()> {
        let started = Instant::now();
        let entry_count = entries.len() as u64;
        let conn = self.connect_shard_uncached(namespace, shard_index).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN CONCURRENT", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(actor_error(error)),
            }
            let outcome = async {
                let mut actor_max_version = -1i64;
                for entry in &entries {
                    let version = entry.token as i64;
                    actor_max_version = actor_max_version.max(version);
                    let now_ms = epoch_ms_i64()?;
                    if entry.mutation.deleted {
                        let empty_blob: &[u8] = &[];
                        conn.execute(
                            "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                             VALUES (?1, ?2, '', ?3, ?4, 1, ?5, ?6)
                             ON CONFLICT(entity_key, item_key) DO UPDATE SET
                               value = CASE WHEN excluded.version > actor_state.version THEN excluded.value ELSE actor_state.value END,
                               value_blob = CASE WHEN excluded.version > actor_state.version THEN excluded.value_blob ELSE actor_state.value_blob END,
                               encoding = CASE WHEN excluded.version > actor_state.version THEN excluded.encoding ELSE actor_state.encoding END,
                               deleted = CASE WHEN excluded.version > actor_state.version THEN 1 ELSE actor_state.deleted END,
                               version = CASE WHEN excluded.version > actor_state.version THEN excluded.version ELSE actor_state.version END,
                               updated_at_ms = CASE WHEN excluded.version > actor_state.version THEN excluded.updated_at_ms ELSE actor_state.updated_at_ms END",
                            (
                                actor_key.as_str(),
                                entry.mutation.key.as_str(),
                                empty_blob,
                                ENCODING_UTF8,
                                version,
                                now_ms,
                            ),
                        )
                        .await
                        .map_err(actor_error)?;
                    } else {
                        let value_text = if entry.mutation.encoding == ENCODING_UTF8 {
                            std::str::from_utf8(&entry.mutation.value)
                                .map_err(|error| {
                                    PlatformError::bad_request(format!(
                                        "invalid utf8 value: {error}"
                                    ))
                                })?
                                .to_string()
                        } else {
                            String::new()
                        };
                        conn.execute(
                            "INSERT INTO actor_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
                             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)
                             ON CONFLICT(entity_key, item_key) DO UPDATE SET
                               value = CASE WHEN excluded.version > actor_state.version THEN excluded.value ELSE actor_state.value END,
                               value_blob = CASE WHEN excluded.version > actor_state.version THEN excluded.value_blob ELSE actor_state.value_blob END,
                               encoding = CASE WHEN excluded.version > actor_state.version THEN excluded.encoding ELSE actor_state.encoding END,
                               deleted = CASE WHEN excluded.version > actor_state.version THEN 0 ELSE actor_state.deleted END,
                               version = CASE WHEN excluded.version > actor_state.version THEN excluded.version ELSE actor_state.version END,
                               updated_at_ms = CASE WHEN excluded.version > actor_state.version THEN excluded.updated_at_ms ELSE actor_state.updated_at_ms END",
                            (
                                actor_key.as_str(),
                                entry.mutation.key.as_str(),
                                value_text.as_str(),
                                entry.mutation.value.as_slice(),
                                entry.mutation.encoding.as_str(),
                                version,
                                now_ms,
                            ),
                        )
                        .await
                        .map_err(actor_error)?;
                    }
                }
                let now_ms = epoch_ms_i64()?;
                conn.execute(
                    "INSERT INTO actor_meta (entity_key, max_version, updated_at_ms)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(entity_key) DO UPDATE SET
                       max_version = CASE WHEN excluded.max_version > actor_meta.max_version THEN excluded.max_version ELSE actor_meta.max_version END,
                       updated_at_ms = CASE WHEN excluded.max_version > actor_meta.max_version THEN excluded.updated_at_ms ELSE actor_meta.updated_at_ms END",
                    (actor_key.as_str(), actor_max_version, now_ms),
                )
                .await
                .map_err(actor_error)?;
                let delete_started = Instant::now();
                let mut deleted = 0u64;
                for queue_id in entries.iter().flat_map(|entry| entry.queue_ids.iter().copied()) {
                    conn.execute(
                        "DELETE FROM actor_direct_queue WHERE queue_id = ?1",
                        (queue_id,),
                    )
                    .await
                    .map_err(actor_error)?;
                    deleted += 1;
                }
                self.record_profile(
                    ActorProfileMetricKind::StoreDirectQueueDelete,
                    delete_started.elapsed().as_micros() as u64,
                    deleted,
                );
                Ok::<(), PlatformError>(())
            }
            .await;

            match outcome {
                Ok(()) => match conn.execute("COMMIT", ()).await {
                    Ok(_) => {
                        self.record_profile(
                            ActorProfileMetricKind::StoreDirectQueueFlush,
                            started.elapsed().as_micros() as u64,
                            entry_count,
                        );
                        let version = entries
                            .iter()
                            .map(|entry| entry.token as i64)
                            .max()
                            .unwrap_or(-1);
                        self.observe_version(version);
                        self.observe_actor_version(namespace, &actor_key, version)
                            .await;
                        let mutations = entries
                            .iter()
                            .map(|entry| ActorBatchMutation {
                                key: entry.mutation.key.clone(),
                                value: entry.mutation.value.clone(),
                                encoding: entry.mutation.encoding.clone(),
                                version: entry.token as i64,
                                deleted: entry.mutation.deleted,
                            })
                            .collect::<Vec<_>>();
                        self.update_cached_snapshot_after_commit(
                            namespace, &actor_key, version, &mutations,
                        )
                        .await;
                        self.complete_waiters_for_tokens(
                            shard_index,
                            entries
                                .into_iter()
                                .flat_map(|entry| entry.completion_tokens.into_iter())
                                .collect::<Vec<_>>(),
                            version,
                        )
                        .await;
                        return Ok(());
                    }
                    Err(error) if is_retryable_actor_error(&error) && attempt < 8 => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Err(actor_error(error));
                    }
                },
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(error);
                }
            }
        }
    }

    async fn fail_pending_batch(
        &self,
        shard_index: usize,
        batch: &[ActorPendingMutationEntry],
        error: &str,
    ) {
        let tokens = batch
            .iter()
            .flat_map(|entry| entry.completion_tokens.iter().copied())
            .collect::<Vec<_>>();
        self.fail_waiters_for_tokens(shard_index, &tokens, error)
            .await;
    }

    async fn fail_submission_ids(&self, submission_ids: &[u64], error: &str) {
        let error = PlatformError::runtime(error.to_string());
        let mut submissions = self.write_submissions.lock().await;
        for submission_id in submission_ids {
            if let Some(entry) = submissions.get_mut(submission_id) {
                if entry.result.is_none() {
                    entry.result = Some(Err(error.clone()));
                    entry.notify.notify_waiters();
                }
            }
        }
    }

    async fn complete_waiters(&self, waiters: Vec<u64>, version: i64) {
        let started = Instant::now();
        let waiter_count = waiters.len() as u64;
        let mut submissions = self.write_submissions.lock().await;
        for submission_id in waiters {
            let Some(entry) = submissions.get_mut(&submission_id) else {
                continue;
            };
            if entry.result.is_some() {
                continue;
            }
            entry.max_version = entry.max_version.max(version);
            if entry.remaining_parts > 0 {
                entry.remaining_parts -= 1;
            }
            if entry.remaining_parts == 0 {
                entry.result = Some(Ok(entry.max_version));
                entry.notify.notify_waiters();
            }
        }
        self.record_profile(
            ActorProfileMetricKind::StoreDirectWaiterComplete,
            started.elapsed().as_micros() as u64,
            waiter_count,
        );
    }

    async fn complete_waiters_for_tokens(
        &self,
        shard_index: usize,
        tokens: Vec<u64>,
        version: i64,
    ) {
        let waiters = {
            let shard = self.write_shards[shard_index].clone();
            let mut state = shard.state.lock().await;
            let mut waiters = Vec::new();
            for token in tokens {
                if let Some(token_waiters) = state.token_waiters.remove(&token) {
                    waiters.extend(token_waiters);
                }
            }
            waiters
        };
        self.complete_waiters(waiters, version).await;
    }

    async fn fail_waiters_for_tokens(&self, shard_index: usize, tokens: &[u64], error: &str) {
        let waiters = {
            let shard = self.write_shards[shard_index].clone();
            let mut state = shard.state.lock().await;
            let mut waiters = Vec::new();
            for token in tokens {
                if let Some(token_waiters) = state.token_waiters.remove(token) {
                    waiters.extend(token_waiters);
                }
            }
            waiters
        };
        self.fail_submission_ids(&waiters, error).await;
    }

    fn reserve_version_after(&self, floor: i64) -> i64 {
        let minimum = floor.saturating_add(1).max(1) as u64;
        let mut current = self.version.load(Ordering::SeqCst);
        loop {
            let next = current.max(minimum);
            match self.version.compare_exchange(
                current,
                next.saturating_add(1),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return next as i64,
                Err(actual) => current = actual,
            }
        }
    }

    async fn connect(&self, namespace: &str, actor_key: &str) -> Result<Connection> {
        let namespace = namespace.trim();
        if namespace.is_empty() {
            return Err(PlatformError::runtime("memory namespace must not be empty"));
        }
        let actor_key = actor_key.trim();
        if actor_key.is_empty() {
            return Err(PlatformError::runtime("actor key must not be empty"));
        }

        self.connect_shard(namespace, self.shard_index(actor_key))
            .await
    }

    async fn connect_uncached(&self, namespace: &str, actor_key: &str) -> Result<Connection> {
        let namespace = namespace.trim();
        if namespace.is_empty() {
            return Err(PlatformError::runtime("memory namespace must not be empty"));
        }
        let actor_key = actor_key.trim();
        if actor_key.is_empty() {
            return Err(PlatformError::runtime("actor key must not be empty"));
        }

        self.connect_shard_uncached(namespace, self.shard_index(actor_key))
            .await
    }

    async fn connect_shard(&self, namespace: &str, shard_index: usize) -> Result<Connection> {
        let db_key = self.database_key(namespace, shard_index);
        let now = Instant::now();
        let existing = {
            let mut databases = self.databases.lock().await;
            self.prune_databases_locked(&mut databases, now);
            databases.get_mut(&db_key).map(|entry| {
                entry.last_used_at = now;
                Arc::clone(&entry.database)
            })
        };
        if let Some(existing) = existing {
            let conn = existing.connect().map_err(actor_error)?;
            configure_connection(&conn).await?;
            return Ok(conn);
        }

        let path = self.db_path(namespace, shard_index);
        ensure_parent_dir(&path)?;
        let path_str = path.to_string_lossy().to_string();
        let database = Builder::new_local(&path_str)
            .build()
            .await
            .map_err(actor_error)?;
        let database = Arc::new(database);
        ensure_schema(&database).await?;

        let database = {
            let mut databases = self.databases.lock().await;
            self.prune_databases_locked(&mut databases, now);
            let database = databases
                .entry(db_key)
                .or_insert_with(|| ActorDatabaseEntry {
                    database: database.clone(),
                    last_used_at: now,
                })
                .database
                .clone();
            self.prune_databases_locked(&mut databases, now);
            database
        };
        let conn = database.connect().map_err(actor_error)?;
        configure_connection(&conn).await?;
        Ok(conn)
    }

    async fn connect_shard_uncached(
        &self,
        namespace: &str,
        shard_index: usize,
    ) -> Result<Connection> {
        let path = self.db_path(namespace, shard_index);
        ensure_parent_dir(&path)?;
        let path_str = path.to_string_lossy().to_string();
        let database = Builder::new_local(&path_str)
            .build()
            .await
            .map_err(actor_error)?;
        ensure_schema(&database).await?;
        let conn = database.connect().map_err(actor_error)?;
        configure_connection(&conn).await?;
        Ok(conn)
    }

    async fn max_version_for_actor(
        &self,
        conn: &Connection,
        actor_key: &str,
    ) -> Result<Option<i64>> {
        let mut rows = conn
            .query(
                "SELECT max_version FROM actor_meta
                 WHERE entity_key = ?1
                 LIMIT 1",
                (actor_key,),
            )
            .await
            .map_err(actor_error)?;
        let version = if let Some(row) = rows.next().await.map_err(actor_error)? {
            row.get::<Option<i64>>(0).map_err(actor_error)?
        } else {
            return Ok(None);
        };
        let _ = rows.next().await.map_err(actor_error)?;
        Ok(version)
    }

    async fn version_for_key(
        &self,
        conn: &Connection,
        actor_key: &str,
        item_key: &str,
    ) -> Result<Option<i64>> {
        let mut rows = conn
            .query(
                "SELECT version FROM actor_state
                 WHERE entity_key = ?1 AND item_key = ?2
                 LIMIT 1",
                (actor_key, item_key),
            )
            .await
            .map_err(actor_error)?;
        let version = if let Some(row) = rows.next().await.map_err(actor_error)? {
            row.get::<i64>(0).map_err(actor_error)?
        } else {
            return Ok(None);
        };
        let _ = rows.next().await.map_err(actor_error)?;
        self.observe_version(version);
        Ok(Some(version))
    }

    async fn record_for_key(
        &self,
        conn: &Connection,
        actor_key: &str,
        item_key: &str,
    ) -> Result<Option<ActorSnapshotEntry>> {
        let mut rows = conn
            .query(
                "SELECT value_blob, encoding, value, version, deleted
                 FROM actor_state
                 WHERE entity_key = ?1 AND item_key = ?2
                 LIMIT 1",
                (actor_key, item_key),
            )
            .await
            .map_err(actor_error)?;
        let Some(row) = rows.next().await.map_err(actor_error)? else {
            return Ok(None);
        };
        let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(0).map_err(actor_error)?;
        let encoding: String = row.get::<String>(1).map_err(actor_error)?;
        let legacy_value: String = row.get::<String>(2).map_err(actor_error)?;
        let version: i64 = row.get::<i64>(3).map_err(actor_error)?;
        let deleted: i64 = row.get::<i64>(4).map_err(actor_error)?;
        let _ = rows.next().await.map_err(actor_error)?;
        Ok(Some(ActorSnapshotEntry {
            key: item_key.to_string(),
            value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
            encoding: normalize_encoding(&encoding),
            version,
            deleted: deleted != 0,
        }))
    }

    async fn direct_queue_len(&self, conn: &Connection) -> Result<usize> {
        let mut rows = conn
            .query("SELECT COUNT(*) FROM actor_direct_queue", ())
            .await
            .map_err(actor_error)?;
        let Some(row) = rows.next().await.map_err(actor_error)? else {
            return Ok(0);
        };
        let count = row.get::<i64>(0).map_err(actor_error)?;
        let _ = rows.next().await.map_err(actor_error)?;
        Ok(count.max(0) as usize)
    }

    async fn load_direct_queue_batch(
        &self,
        namespace: &str,
        shard_index: usize,
        limit: usize,
    ) -> Result<Vec<ActorPendingMutationEntry>> {
        let started = Instant::now();
        let conn = self.connect_shard(namespace, shard_index).await?;
        let mut rows = conn
            .query(
                "SELECT queue_id, entity_key, item_key, value_blob, encoding, deleted, token
                 FROM actor_direct_queue
                 ORDER BY queue_id ASC
                 LIMIT ?1",
                (limit as i64,),
            )
            .await
            .map_err(actor_error)?;
        let mut loaded = Vec::new();
        while let Some(row) = rows.next().await.map_err(actor_error)? {
            loaded.push(ActorDirectQueueRow {
                queue_id: row.get::<i64>(0).map_err(actor_error)?,
                actor_key: row.get::<String>(1).map_err(actor_error)?,
                item_key: row.get::<String>(2).map_err(actor_error)?,
                value: row.get::<Vec<u8>>(3).map_err(actor_error)?,
                encoding: row.get::<String>(4).map_err(actor_error)?,
                deleted: row.get::<i64>(5).map_err(actor_error)? != 0,
                token: row.get::<i64>(6).map_err(actor_error)? as u64,
            });
        }
        let loaded_len = loaded.len() as u64;
        let coalesced = coalesce_direct_queue_rows(loaded);
        self.record_profile(
            ActorProfileMetricKind::StoreDirectQueueLoad,
            started.elapsed().as_micros() as u64,
            loaded_len,
        );
        Ok(coalesced)
    }

    async fn discover_namespaces_for_shard(&self, shard_index: usize) -> Result<Vec<String>> {
        let mut namespaces = Vec::new();
        let entries = match std::fs::read_dir(self.root_dir.as_ref()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(namespaces),
            Err(error) => return Err(actor_error(error)),
        };
        for entry in entries {
            let entry = entry.map_err(actor_error)?;
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
        format!(
            "{:?}\u{1e}{namespace}\u{1f}{shard_index}",
            std::thread::current().id()
        )
    }

    fn actor_version_key(namespace: &str, actor_key: &str) -> String {
        format!("{namespace}\u{1f}{actor_key}")
    }

    fn actor_snapshot_key(namespace: &str, actor_key: &str) -> String {
        format!("{namespace}\u{1f}{actor_key}")
    }

    fn db_path(&self, namespace: &str, shard_index: usize) -> PathBuf {
        let encoded_namespace = hex_encode(namespace.as_bytes());
        self.root_dir
            .join(encoded_namespace)
            .join(format!("shard-{shard_index:04}.db"))
    }

    fn shard_index(&self, actor_key: &str) -> usize {
        if self.namespace_shards == 1 {
            return 0;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        actor_key.hash(&mut hasher);
        (hasher.finish() as usize) % self.namespace_shards
    }

    fn observe_version(&self, version: i64) {
        if version < 0 {
            return;
        }
        let wanted = version as u64 + 1;
        let mut current = self.version.load(Ordering::SeqCst);
        while current < wanted {
            match self
                .version
                .compare_exchange(current, wanted, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    async fn observe_actor_version(&self, namespace: &str, actor_key: &str, version: i64) {
        if version < 0 {
            return;
        }
        self.actor_versions
            .lock()
            .await
            .insert(Self::actor_version_key(namespace, actor_key), version);
    }

    async fn cached_full_snapshot(
        &self,
        namespace: &str,
        actor_key: &str,
    ) -> Option<ActorSnapshot> {
        let entry = self.cached_snapshot_entry(namespace, actor_key).await?;
        if !entry.complete {
            return None;
        }
        Some(ActorSnapshot {
            entries: snapshot_entries_from_records(entry.records.as_ref()),
            max_version: entry.max_version,
        })
    }

    async fn cached_point_read(
        &self,
        namespace: &str,
        actor_key: &str,
        item_key: &str,
    ) -> Option<ActorPointRead> {
        let entry = self.cached_snapshot_entry(namespace, actor_key).await?;
        if !entry.complete && !entry.loaded_keys.contains(item_key) {
            return None;
        }
        Some(ActorPointRead {
            record: entry.records.get(item_key).cloned(),
            max_version: entry.max_version,
        })
    }

    async fn cached_keys_snapshot(
        &self,
        namespace: &str,
        actor_key: &str,
        keys: &[String],
    ) -> Option<ActorSnapshot> {
        let entry = self.cached_snapshot_entry(namespace, actor_key).await?;
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
        Some(ActorSnapshot {
            entries,
            max_version: entry.max_version,
        })
    }

    async fn cached_snapshot_entry(
        &self,
        namespace: &str,
        actor_key: &str,
    ) -> Option<ActorSharedSnapshotEntry> {
        let key = Self::actor_snapshot_key(namespace, actor_key);
        let current_version = self
            .actor_versions
            .lock()
            .await
            .get(&Self::actor_version_key(namespace, actor_key))
            .copied();
        let now = Instant::now();
        let mut snapshots = self.shared_snapshots.lock().await;
        self.prune_snapshots_locked(&mut snapshots, now);
        let entry = snapshots.get_mut(&key)?;
        if current_version.is_some_and(|current| current > entry.max_version) {
            snapshots.remove(&key);
            return None;
        }
        entry.last_used_at = now;
        Some(ActorSharedSnapshotEntry {
            records: entry.records.clone(),
            loaded_keys: entry.loaded_keys.clone(),
            complete: entry.complete,
            max_version: entry.max_version,
            last_used_at: entry.last_used_at,
        })
    }

    async fn put_full_snapshot(&self, namespace: &str, actor_key: &str, snapshot: &ActorSnapshot) {
        self.put_partial_snapshot(
            namespace,
            actor_key,
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
        actor_key: &str,
        max_version: i64,
        entries: Vec<ActorSnapshotEntry>,
        loaded_keys: I,
        complete: bool,
    ) where
        I: IntoIterator<Item = String>,
    {
        let key = Self::actor_snapshot_key(namespace, actor_key);
        let now = Instant::now();
        let mut snapshots = self.shared_snapshots.lock().await;
        self.prune_snapshots_locked(&mut snapshots, now);
        let entry = snapshots
            .entry(key)
            .or_insert_with(|| ActorSharedSnapshotEntry {
                records: Arc::new(HashMap::new()),
                loaded_keys: Arc::new(HashSet::new()),
                complete: false,
                max_version,
                last_used_at: now,
            });

        if entry.max_version > max_version {
            entry.last_used_at = now;
            return;
        }

        let mut next_records = if complete || entry.max_version < max_version {
            HashMap::new()
        } else {
            entry.records.as_ref().clone()
        };
        let mut next_loaded_keys = if complete || entry.max_version < max_version {
            HashSet::new()
        } else {
            entry.loaded_keys.as_ref().clone()
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

        entry.records = Arc::new(next_records);
        entry.loaded_keys = Arc::new(next_loaded_keys);
        entry.complete = complete;
        entry.max_version = max_version;
        entry.last_used_at = now;
        self.prune_snapshots_locked(&mut snapshots, now);
    }

    async fn update_cached_snapshot_after_commit(
        &self,
        namespace: &str,
        actor_key: &str,
        max_version: i64,
        mutations: &[ActorBatchMutation],
    ) {
        let key = Self::actor_snapshot_key(namespace, actor_key);
        let now = Instant::now();
        let mut snapshots = self.shared_snapshots.lock().await;
        self.prune_snapshots_locked(&mut snapshots, now);
        let Some(entry) = snapshots.get_mut(&key) else {
            return;
        };
        let mut next_records = entry.records.as_ref().clone();
        let mut next_loaded_keys = entry.loaded_keys.as_ref().clone();
        for mutation in mutations {
            let normalized_key = mutation.key.trim();
            if normalized_key.is_empty() {
                continue;
            }
            let record = ActorSnapshotEntry {
                key: normalized_key.to_string(),
                value: mutation.value.clone(),
                encoding: mutation.encoding.clone(),
                version: mutation.version,
                deleted: mutation.deleted,
            };
            next_loaded_keys.insert(normalized_key.to_string());
            next_records.insert(normalized_key.to_string(), record);
        }
        entry.records = Arc::new(next_records);
        entry.loaded_keys = Arc::new(next_loaded_keys);
        entry.max_version = max_version;
        entry.last_used_at = now;
    }

    fn prune_databases_locked(
        &self,
        databases: &mut HashMap<String, ActorDatabaseEntry>,
        now: Instant,
    ) {
        databases.retain(|_, entry| now.duration_since(entry.last_used_at) < self.db_idle_ttl);
        if databases.len() <= self.db_cache_max_open {
            return;
        }
        let mut keys_by_age = databases
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_used_at))
            .collect::<Vec<_>>();
        keys_by_age.sort_by_key(|(_, last_used_at)| *last_used_at);
        let excess = databases.len().saturating_sub(self.db_cache_max_open);
        for (key, _) in keys_by_age.into_iter().take(excess) {
            databases.remove(&key);
        }
    }

    fn prune_snapshots_locked(
        &self,
        snapshots: &mut HashMap<String, ActorSharedSnapshotEntry>,
        now: Instant,
    ) {
        snapshots.retain(|_, entry| now.duration_since(entry.last_used_at) < self.db_idle_ttl);
        if snapshots.len() <= self.snapshot_cache_max_entries {
            return;
        }
        let mut keys_by_age = snapshots
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_used_at))
            .collect::<Vec<_>>();
        keys_by_age.sort_by_key(|(_, last_used_at)| *last_used_at);
        let excess = snapshots
            .len()
            .saturating_sub(self.snapshot_cache_max_entries);
        for (key, _) in keys_by_age.into_iter().take(excess) {
            snapshots.remove(&key);
        }
    }
}

impl ActorProfileMetric {
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

    fn snapshot(&self) -> ActorProfileMetricSnapshot {
        ActorProfileMetricSnapshot {
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

impl ActorProfile {
    fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    fn record(&self, metric: ActorProfileMetricKind, duration_us: u64, items: u64) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let target = match metric {
            ActorProfileMetricKind::JsReadOnlyTotal => &self.js_read_only_total,
            ActorProfileMetricKind::JsFreshnessCheck => &self.js_freshness_check,
            ActorProfileMetricKind::JsHydrateFull => &self.js_hydrate_full,
            ActorProfileMetricKind::JsHydrateKeys => &self.js_hydrate_keys,
            ActorProfileMetricKind::JsTxnCommit => &self.js_txn_commit,
            ActorProfileMetricKind::JsTxnBlindCommit => &self.js_txn_blind_commit,
            ActorProfileMetricKind::JsTxnValidate => &self.js_txn_validate,
            ActorProfileMetricKind::JsCacheHit => &self.js_cache_hit,
            ActorProfileMetricKind::JsCacheMiss => &self.js_cache_miss,
            ActorProfileMetricKind::JsCacheStale => &self.js_cache_stale,
            ActorProfileMetricKind::OpRead => &self.op_read,
            ActorProfileMetricKind::OpSnapshot => &self.op_snapshot,
            ActorProfileMetricKind::OpVersionIfNewer => &self.op_version_if_newer,
            ActorProfileMetricKind::OpValidateReads => &self.op_validate_reads,
            ActorProfileMetricKind::OpApplyBatch => &self.op_apply_batch,
            ActorProfileMetricKind::OpApplyBlindBatch => &self.op_apply_blind_batch,
            ActorProfileMetricKind::StoreDirectEnqueue => &self.store_direct_enqueue,
            ActorProfileMetricKind::StoreDirectAwait => &self.store_direct_await,
            ActorProfileMetricKind::StoreDirectQueueLoad => &self.store_direct_queue_load,
            ActorProfileMetricKind::StoreDirectQueueFlush => &self.store_direct_queue_flush,
            ActorProfileMetricKind::StoreDirectQueueDelete => &self.store_direct_queue_delete,
            ActorProfileMetricKind::StoreDirectWaiterComplete => &self.store_direct_waiter_complete,
            ActorProfileMetricKind::StoreRead => &self.store_read,
            ActorProfileMetricKind::StoreSnapshot => &self.store_snapshot,
            ActorProfileMetricKind::StoreSnapshotKeys => &self.store_snapshot_keys,
            ActorProfileMetricKind::StoreVersionIfNewer => &self.store_version_if_newer,
            ActorProfileMetricKind::StoreApplyBatch => &self.store_apply_batch,
            ActorProfileMetricKind::StoreApplyBatchValidate => &self.store_apply_batch_validate,
            ActorProfileMetricKind::StoreApplyBatchWrite => &self.store_apply_batch_write,
            ActorProfileMetricKind::StoreApplyBlindBatch => &self.store_apply_blind_batch,
            ActorProfileMetricKind::StoreApplyBlindBatchWrite => {
                &self.store_apply_blind_batch_write
            }
        };
        target.record(duration_us, items.max(1));
    }

    fn take_snapshot_and_reset(&self) -> ActorProfileSnapshot {
        let snapshot = ActorProfileSnapshot {
            enabled: self.enabled.load(Ordering::Relaxed),
            js_read_only_total: self.js_read_only_total.snapshot(),
            js_freshness_check: self.js_freshness_check.snapshot(),
            js_hydrate_full: self.js_hydrate_full.snapshot(),
            js_hydrate_keys: self.js_hydrate_keys.snapshot(),
            js_txn_commit: self.js_txn_commit.snapshot(),
            js_txn_blind_commit: self.js_txn_blind_commit.snapshot(),
            js_txn_validate: self.js_txn_validate.snapshot(),
            js_cache_hit: self.js_cache_hit.snapshot(),
            js_cache_miss: self.js_cache_miss.snapshot(),
            js_cache_stale: self.js_cache_stale.snapshot(),
            op_read: self.op_read.snapshot(),
            op_snapshot: self.op_snapshot.snapshot(),
            op_version_if_newer: self.op_version_if_newer.snapshot(),
            op_validate_reads: self.op_validate_reads.snapshot(),
            op_apply_batch: self.op_apply_batch.snapshot(),
            op_apply_blind_batch: self.op_apply_blind_batch.snapshot(),
            store_direct_enqueue: self.store_direct_enqueue.snapshot(),
            store_direct_await: self.store_direct_await.snapshot(),
            store_direct_queue_load: self.store_direct_queue_load.snapshot(),
            store_direct_queue_flush: self.store_direct_queue_flush.snapshot(),
            store_direct_queue_delete: self.store_direct_queue_delete.snapshot(),
            store_direct_waiter_complete: self.store_direct_waiter_complete.snapshot(),
            store_read: self.store_read.snapshot(),
            store_snapshot: self.store_snapshot.snapshot(),
            store_snapshot_keys: self.store_snapshot_keys.snapshot(),
            store_version_if_newer: self.store_version_if_newer.snapshot(),
            store_apply_batch: self.store_apply_batch.snapshot(),
            store_apply_batch_validate: self.store_apply_batch_validate.snapshot(),
            store_apply_batch_write: self.store_apply_batch_write.snapshot(),
            store_apply_blind_batch: self.store_apply_blind_batch.snapshot(),
            store_apply_blind_batch_write: self.store_apply_blind_batch_write.snapshot(),
        };
        self.reset();
        snapshot
    }

    fn reset(&self) {
        self.js_read_only_total.reset();
        self.js_freshness_check.reset();
        self.js_hydrate_full.reset();
        self.js_hydrate_keys.reset();
        self.js_txn_commit.reset();
        self.js_txn_blind_commit.reset();
        self.js_txn_validate.reset();
        self.js_cache_hit.reset();
        self.js_cache_miss.reset();
        self.js_cache_stale.reset();
        self.op_read.reset();
        self.op_snapshot.reset();
        self.op_version_if_newer.reset();
        self.op_validate_reads.reset();
        self.op_apply_batch.reset();
        self.op_apply_blind_batch.reset();
        self.store_direct_enqueue.reset();
        self.store_direct_await.reset();
        self.store_direct_queue_load.reset();
        self.store_direct_queue_flush.reset();
        self.store_direct_queue_delete.reset();
        self.store_direct_waiter_complete.reset();
        self.store_read.reset();
        self.store_snapshot.reset();
        self.store_snapshot_keys.reset();
        self.store_version_if_newer.reset();
        self.store_apply_batch.reset();
        self.store_apply_batch_validate.reset();
        self.store_apply_batch_write.reset();
        self.store_apply_blind_batch.reset();
        self.store_apply_blind_batch_write.reset();
    }
}

async fn ensure_schema(database: &Database) -> Result<()> {
    let conn = database.connect().map_err(actor_error)?;
    configure_connection(&conn).await?;
    ensure_mvcc_mode(&conn).await?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS actor_state (
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
    .map_err(actor_error)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS actor_meta (
          entity_key TEXT NOT NULL PRIMARY KEY,
          max_version INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )",
        (),
    )
    .await
    .map_err(actor_error)?;
    ensure_compat_columns(&conn).await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_actor_state_lookup
         ON actor_state(entity_key, item_key)",
        (),
    )
    .await
    .map_err(actor_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_actor_state_list
         ON actor_state(entity_key, deleted, item_key)",
        (),
    )
    .await
    .map_err(actor_error)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS actor_direct_queue (
          queue_id INTEGER PRIMARY KEY,
          entity_key TEXT NOT NULL,
          item_key TEXT NOT NULL,
          value_blob BLOB NOT NULL,
          encoding TEXT NOT NULL DEFAULT 'utf8',
          deleted INTEGER NOT NULL DEFAULT 0,
          token INTEGER NOT NULL,
          enqueued_at_ms INTEGER NOT NULL
        )",
        (),
    )
    .await
    .map_err(actor_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_actor_direct_queue_order
         ON actor_direct_queue(queue_id)",
        (),
    )
    .await
    .map_err(actor_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_actor_direct_queue_key
         ON actor_direct_queue(entity_key, item_key, queue_id)",
        (),
    )
    .await
    .map_err(actor_error)?;
    Ok(())
}

async fn detect_actor_version_floor(root_dir: &Path) -> Result<u64> {
    let mut max_version = 0u64;
    let entries = match std::fs::read_dir(root_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(1),
        Err(error) => return Err(actor_error(error)),
    };
    for entry in entries {
        let entry = entry.map_err(actor_error)?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let shard_entries = std::fs::read_dir(&path).map_err(actor_error)?;
        for shard_entry in shard_entries {
            let shard_entry = shard_entry.map_err(actor_error)?;
            let shard_path = shard_entry.path();
            if shard_path.extension().and_then(|ext| ext.to_str()) != Some("db") {
                continue;
            }
            let path_str = shard_path.to_string_lossy().to_string();
            let database = Builder::new_local(&path_str)
                .build()
                .await
                .map_err(actor_error)?;
            let conn = database.connect().map_err(actor_error)?;
            configure_connection(&conn).await?;
            max_version = max_version.max(
                read_single_i64(&conn, "SELECT MAX(max_version) FROM actor_meta").await? as u64,
            );
            max_version = max_version.max(
                read_single_i64(&conn, "SELECT MAX(token) FROM actor_direct_queue").await? as u64,
            );
        }
    }
    Ok(max_version.saturating_add(1).max(1))
}

async fn read_single_i64(conn: &Connection, sql: &str) -> Result<i64> {
    let mut rows = match conn.query(sql, ()).await {
        Ok(rows) => rows,
        Err(error) => {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("no such table") {
                return Ok(0);
            }
            return Err(actor_error(error));
        }
    };
    let Some(row) = rows.next().await.map_err(actor_error)? else {
        return Ok(0);
    };
    let value = row
        .get::<Option<i64>>(0)
        .map_err(actor_error)?
        .unwrap_or(0)
        .max(0);
    let _ = rows.next().await.map_err(actor_error)?;
    Ok(value)
}

async fn configure_connection(conn: &Connection) -> Result<()> {
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(actor_error)?;
    Ok(())
}

async fn ensure_mvcc_mode(conn: &Connection) -> Result<()> {
    conn.pragma_update("journal_mode", "'mvcc'")
        .await
        .map_err(actor_error)?;
    let mut rows = conn
        .query("PRAGMA journal_mode", ())
        .await
        .map_err(actor_error)?;
    let Some(row) = rows.next().await.map_err(actor_error)? else {
        return Err(PlatformError::runtime(
            "actor store error: failed to read journal_mode",
        ));
    };
    let mode = row.get::<String>(0).map_err(actor_error)?;
    let _ = rows.next().await.map_err(actor_error)?;
    if !mode.eq_ignore_ascii_case("mvcc") {
        return Err(PlatformError::runtime(format!(
            "actor store error: expected mvcc journal mode, got {mode}",
        )));
    }
    Ok(())
}

async fn ensure_compat_columns(conn: &Connection) -> Result<()> {
    let mut rows = conn
        .query("PRAGMA table_info(actor_state)", ())
        .await
        .map_err(actor_error)?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await.map_err(actor_error)? {
        let name: String = row.get::<String>(1).map_err(actor_error)?;
        columns.insert(name);
    }

    if !columns.contains("value_blob") {
        conn.execute("ALTER TABLE actor_state ADD COLUMN value_blob BLOB", ())
            .await
            .map_err(actor_error)?;
    }
    if !columns.contains("encoding") {
        conn.execute(
            "ALTER TABLE actor_state ADD COLUMN encoding TEXT NOT NULL DEFAULT 'utf8'",
            (),
        )
        .await
        .map_err(actor_error)?;
    }
    Ok(())
}

fn is_retryable_actor_error(error: &turso::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database is busy")
        || message.contains("busy")
        || message.contains("conflict")
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}

fn actor_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("actor store error: {error}"))
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(actor_error)?;
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
    records: &HashMap<String, ActorSnapshotEntry>,
) -> Vec<ActorSnapshotEntry> {
    let mut entries = records.values().cloned().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

fn coalesce_direct_mutations(
    mutations: &[ActorDirectMutation],
) -> Result<Vec<ActorDirectMutation>> {
    let mut coalesced = HashMap::<String, ActorDirectMutation>::new();
    for mutation in mutations {
        let key = mutation.key.trim();
        if key.is_empty() {
            return Err(PlatformError::bad_request(
                "actor direct mutation key must not be empty",
            ));
        }
        let encoding = normalize_encoding(&mutation.encoding);
        if !mutation.deleted && encoding != ENCODING_UTF8 && encoding != ENCODING_V8SC {
            return Err(PlatformError::bad_request(format!(
                "unsupported actor storage encoding: {}",
                mutation.encoding
            )));
        }
        coalesced.insert(
            key.to_string(),
            ActorDirectMutation {
                key: key.to_string(),
                value: mutation.value.clone(),
                encoding,
                deleted: mutation.deleted,
            },
        );
    }
    Ok(coalesced.into_values().collect())
}

fn coalesce_direct_queue_rows(rows: Vec<ActorDirectQueueRow>) -> Vec<ActorPendingMutationEntry> {
    let mut coalesced = HashMap::<ActorQueuedMutationKey, ActorPendingMutationEntry>::new();
    for row in rows {
        let key = ActorQueuedMutationKey {
            actor_key: row.actor_key.clone(),
            item_key: row.item_key.clone(),
        };
        let mutation = ActorDirectMutation {
            key: row.item_key.clone(),
            value: row.value.clone(),
            encoding: normalize_encoding(&row.encoding),
            deleted: row.deleted,
        };
        if let Some(entry) = coalesced.get_mut(&key) {
            entry.queue_ids.push(row.queue_id);
            entry.completion_tokens.push(row.token);
            if row.token >= entry.token {
                entry.token = row.token;
                entry.mutation = mutation;
            }
        } else {
            coalesced.insert(
                key,
                ActorPendingMutationEntry {
                    actor_key: row.actor_key,
                    mutation,
                    token: row.token,
                    queue_ids: vec![row.queue_id],
                    completion_tokens: vec![row.token],
                },
            );
        }
    }
    coalesced.into_values().collect()
}

fn hex_decode_to_utf8(input: &str) -> Option<String> {
    if input.len() % 2 != 0 {
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

const ENCODING_UTF8: &str = "utf8";
const ENCODING_V8SC: &str = "v8sc";

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-actor-store-{name}-{}", Uuid::new_v4()))
    }

    fn utf8_mutation(key: &str, value: &str, version: i64) -> ActorBatchMutation {
        ActorBatchMutation {
            key: key.to_string(),
            value: value.as_bytes().to_vec(),
            encoding: ENCODING_UTF8.to_string(),
            version,
            deleted: false,
        }
    }

    async fn seed_direct_queue_row(
        root: &Path,
        namespace: &str,
        shard_index: usize,
        actor_key: &str,
        item_key: &str,
        value: &str,
        token: i64,
    ) -> Result<()> {
        let path = root
            .join(hex_encode(namespace.as_bytes()))
            .join(format!("shard-{shard_index:04}.db"));
        ensure_parent_dir(&path)?;
        let path_str = path.to_string_lossy().to_string();
        let database = Builder::new_local(&path_str)
            .build()
            .await
            .map_err(actor_error)?;
        ensure_schema(&database).await?;
        let conn = database.connect().map_err(actor_error)?;
        configure_connection(&conn).await?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(actor_error)?;
        let outcome = async {
            conn.execute(
                "INSERT INTO actor_direct_queue (entity_key, item_key, value_blob, encoding, deleted, token, enqueued_at_ms)
                 VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)",
                (
                    actor_key,
                    item_key,
                    value.as_bytes(),
                    ENCODING_UTF8,
                    token,
                    epoch_ms_i64()?,
                ),
            )
            .await
            .map_err(actor_error)?;
            Ok::<(), PlatformError>(())
        }
        .await;
        match outcome {
            Ok(()) => {
                conn.execute("COMMIT", ()).await.map_err(actor_error)?;
            }
            Err(error) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(error);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn actor_db_paths_are_stable_per_namespace() -> Result<()> {
        let store = ActorStore::new(temp_root("paths"), 16, 4, Duration::from_secs(60)).await?;
        let shard = store.shard_index("actor-a");
        let ns_a = store.db_path("ns", shard);
        let ns_a_again = store.db_path("ns", shard);
        let ns_b = store.db_path("other", shard);

        assert_eq!(ns_a, ns_a_again);
        assert_ne!(ns_a, ns_b);
        Ok(())
    }

    #[tokio::test]
    async fn actor_db_cache_eviction_keeps_persisted_state() -> Result<()> {
        let store = ActorStore::new(temp_root("eviction"), 1, 1, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "actor-a",
                &[],
                &[utf8_mutation("count", "1", 1)],
                Some(-1),
                None,
                false,
            )
            .await?;
        store
            .apply_batch(
                "ns",
                "actor-b",
                &[],
                &[utf8_mutation("count", "2", 2)],
                Some(-1),
                None,
                false,
            )
            .await?;

        assert_eq!(store.databases.lock().await.len(), 1);

        let snapshot = store.snapshot("ns", "actor-a").await?;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(
            String::from_utf8(snapshot.entries[0].value.clone()).expect("utf8"),
            "1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn actor_version_if_newer_observes_commits() -> Result<()> {
        let store = ActorStore::new(
            temp_root("version-if-newer"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;
        assert_eq!(store.version_if_newer("ns", "actor-a", -1).await?, None);
        store
            .apply_batch(
                "ns",
                "actor-a",
                &[],
                &[utf8_mutation("count", "1", 1)],
                Some(-1),
                None,
                false,
            )
            .await?;
        assert_eq!(store.version_if_newer("ns", "actor-a", -1).await?, Some(1));
        assert_eq!(store.version_if_newer("ns", "actor-a", 1).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn actor_snapshot_cache_updates_after_transactional_commit() -> Result<()> {
        let store =
            ActorStore::new(temp_root("snapshot-cache"), 16, 4, Duration::from_secs(60)).await?;

        let initial = store.snapshot("ns", "actor-a").await?;
        assert_eq!(initial.max_version, -1);

        store
            .apply_batch(
                "ns",
                "actor-a",
                &[],
                &[utf8_mutation("count", "1", 1)],
                Some(-1),
                None,
                true,
            )
            .await?;

        let snapshot = store.snapshot("ns", "actor-a").await?;
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
    async fn actor_point_read_populates_partial_shared_cache_and_tracks_misses() -> Result<()> {
        let store = ActorStore::new(
            temp_root("point-read-cache"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        store
            .apply_batch(
                "ns",
                "actor-a",
                &[],
                &[utf8_mutation("count", "1", 1)],
                Some(-1),
                None,
                true,
            )
            .await?;

        let hit = store.point_read("ns", "actor-a", "count").await?;
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

        let key = ActorStore::actor_snapshot_key("ns", "actor-a");
        {
            let snapshots = store.shared_snapshots.lock().await;
            let entry = snapshots
                .get(&key)
                .expect("shared cache entry should exist");
            assert!(!entry.complete);
            assert!(entry.loaded_keys.contains("count"));
            assert!(entry.records.contains_key("count"));
        }

        let miss = store.point_read("ns", "actor-a", "missing").await?;
        assert_eq!(miss.max_version, 1);
        assert!(miss.record.is_none());

        let snapshots = store.shared_snapshots.lock().await;
        let entry = snapshots
            .get(&key)
            .expect("shared cache entry should exist");
        assert!(entry.loaded_keys.contains("missing"));
        assert!(!entry.records.contains_key("missing"));
        Ok(())
    }

    #[tokio::test]
    async fn actor_point_read_cache_updates_after_commit() -> Result<()> {
        let store = ActorStore::new(
            temp_root("point-read-commit"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        store
            .apply_batch(
                "ns",
                "actor-a",
                &[],
                &[utf8_mutation("count", "1", 1)],
                Some(-1),
                None,
                true,
            )
            .await?;
        let first = store.point_read("ns", "actor-a", "count").await?;
        assert_eq!(first.max_version, 1);

        store
            .apply_batch(
                "ns",
                "actor-a",
                &[ActorReadDependency {
                    key: "count".to_string(),
                    version: 1,
                }],
                &[utf8_mutation("count", "2", 2)],
                Some(-1),
                None,
                true,
            )
            .await?;

        let updated = store.point_read("ns", "actor-a", "count").await?;
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
    async fn actor_transactional_writes_complete_past_repeated_commit_threshold() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let store = ActorStore::new(
                temp_root("transactional-write-threshold"),
                16,
                4,
                Duration::from_secs(60),
            )
            .await?;

            let mut observed_version = -1;
            for idx in 0..64 {
                let next_value = (idx + 1).to_string();
                let reads = if observed_version < 0 {
                    Vec::new()
                } else {
                    vec![ActorReadDependency {
                        key: "count".to_string(),
                        version: observed_version,
                    }]
                };
                let result = store
                    .apply_batch(
                        "ns",
                        "actor-a",
                        &reads,
                        &[utf8_mutation("count", &next_value, idx as i64 + 1)],
                        Some(-1),
                        None,
                        true,
                    )
                    .await?;
                assert!(!result.conflict, "unexpected conflict at iteration {idx}");
                observed_version = result.max_version;
            }

            let final_value = store.point_read("ns", "actor-a", "count").await?;
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

    #[tokio::test]
    async fn actor_blind_writes_complete_past_repeated_commit_threshold() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let store = ActorStore::new(
                temp_root("blind-write-threshold"),
                16,
                4,
                Duration::from_secs(60),
            )
            .await?;

            for idx in 0..64 {
                let next_value = (idx + 1).to_string();
                let result = store
                    .apply_blind_batch(
                        "ns",
                        "actor-a",
                        &[utf8_mutation("count", &next_value, idx as i64 + 1)],
                    )
                    .await?;
                assert!(
                    !result.conflict,
                    "unexpected blind-write conflict at iteration {idx}"
                );
            }

            let final_value = store.point_read("ns", "actor-a", "count").await?;
            assert_eq!(
                String::from_utf8(
                    final_value
                        .record
                        .expect("count should be present after repeated blind writes")
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
                "blind write threshold test timed out before completing 64 commits",
            )
        })??;
        Ok(())
    }

    #[tokio::test]
    async fn actor_direct_queue_submissions_complete_past_repeated_threshold() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let store = ActorStore::new(
                temp_root("direct-queue-threshold"),
                16,
                4,
                Duration::from_secs(60),
            )
            .await?;

            let mut observed_version = -1;
            for idx in 0..64 {
                let next_value = (idx + 1).to_string();
                let mutation = ActorDirectMutation {
                    key: "count".to_string(),
                    value: next_value.as_bytes().to_vec(),
                    encoding: ENCODING_UTF8.to_string(),
                    deleted: false,
                };
                let submission_id =
                    tokio::time::timeout(Duration::from_secs(2), store.enqueue_direct_batch(
                        "ns",
                        "actor-a",
                        &[mutation],
                    ))
                    .await
                    .map_err(|_| {
                        PlatformError::runtime(format!(
                            "direct queue enqueue timed out at iteration {idx}"
                        ))
                    })??;
                let version =
                    tokio::time::timeout(Duration::from_secs(2), store.wait_direct_submission(
                        submission_id,
                    ))
                    .await
                    .map_err(|_| {
                        PlatformError::runtime(format!(
                            "direct queue submission wait timed out at iteration {idx}"
                        ))
                    })??;
                assert!(
                    version >= observed_version,
                    "direct queue version regressed at iteration {idx}: {version} < {observed_version}"
                );
                observed_version = version;
            }

            let final_value = store.point_read("ns", "actor-a", "count").await?;
            assert_eq!(
                String::from_utf8(
                    final_value
                        .record
                        .expect("count should be present after repeated direct queue writes")
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
                "direct queue threshold test timed out before completing 64 submissions",
            )
        })??;
        Ok(())
    }

    #[tokio::test]
    async fn actor_version_floor_bootstraps_from_direct_queue_tokens() -> Result<()> {
        let root = temp_root("version-floor");
        seed_direct_queue_row(&root, "ns", 0, "actor-a", "count", "9", 41).await?;

        let store = ActorStore::new(root, 1, 4, Duration::from_secs(60)).await?;
        assert_eq!(store.next_write_token.load(Ordering::SeqCst), 42);
        assert_eq!(store.version.load(Ordering::SeqCst), 42);
        Ok(())
    }

    #[tokio::test]
    async fn actor_direct_queue_replays_on_store_startup() -> Result<()> {
        let root = temp_root("queue-replay");
        seed_direct_queue_row(&root, "ns", 0, "actor-a", "count", "9", 7).await?;

        let store = ActorStore::new(root, 1, 4, Duration::from_secs(60)).await?;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let snapshot = store.snapshot("ns", "actor-a").await?;
            let current = snapshot
                .entries
                .iter()
                .find(|entry| entry.key == "count" && !entry.deleted)
                .map(|entry| String::from_utf8(entry.value.clone()).expect("utf8"));
            if snapshot.max_version == 7 && current.as_deref() == Some("9") {
                let conn = store.connect_shard("ns", 0).await?;
                assert_eq!(store.direct_queue_len(&conn).await?, 0);
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(PlatformError::runtime(format!(
                    "expected queued write to replay before timeout, got snapshot version {} and value {:?}",
                    snapshot.max_version,
                    current
                )));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
