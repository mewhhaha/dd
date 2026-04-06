use common::{PlatformError, Result};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value};

struct ActorDatabaseEntry {
    database: Arc<Database>,
    last_used_at: Instant,
}

#[derive(Clone)]
pub struct ActorStore {
    root_dir: Arc<PathBuf>,
    databases: Arc<Mutex<HashMap<String, ActorDatabaseEntry>>>,
    actor_versions: Arc<Mutex<HashMap<String, i64>>>,
    db_cache_max_open: usize,
    db_idle_ttl: Duration,
    version: Arc<AtomicU64>,
    profile: Arc<ActorProfile>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorProfileMetricKind {
    JsReadOnlyTotal,
    JsFreshnessCheck,
    JsHydrateFull,
    JsHydrateKeys,
    JsCacheHit,
    JsCacheMiss,
    JsCacheStale,
    OpSnapshot,
    OpVersionIfNewer,
    OpValidateReads,
    StoreSnapshot,
    StoreSnapshotKeys,
    StoreVersionIfNewer,
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
    pub js_cache_hit: ActorProfileMetricSnapshot,
    pub js_cache_miss: ActorProfileMetricSnapshot,
    pub js_cache_stale: ActorProfileMetricSnapshot,
    pub op_snapshot: ActorProfileMetricSnapshot,
    pub op_version_if_newer: ActorProfileMetricSnapshot,
    pub op_validate_reads: ActorProfileMetricSnapshot,
    pub store_snapshot: ActorProfileMetricSnapshot,
    pub store_snapshot_keys: ActorProfileMetricSnapshot,
    pub store_version_if_newer: ActorProfileMetricSnapshot,
}

#[derive(Default)]
pub struct ActorProfile {
    enabled: AtomicBool,
    js_read_only_total: ActorProfileMetric,
    js_freshness_check: ActorProfileMetric,
    js_hydrate_full: ActorProfileMetric,
    js_hydrate_keys: ActorProfileMetric,
    js_cache_hit: ActorProfileMetric,
    js_cache_miss: ActorProfileMetric,
    js_cache_stale: ActorProfileMetric,
    op_snapshot: ActorProfileMetric,
    op_version_if_newer: ActorProfileMetric,
    op_validate_reads: ActorProfileMetric,
    store_snapshot: ActorProfileMetric,
    store_snapshot_keys: ActorProfileMetric,
    store_version_if_newer: ActorProfileMetric,
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
        db_cache_max_open: usize,
        db_idle_ttl: Duration,
    ) -> Result<Self> {
        std::fs::create_dir_all(&root_dir).map_err(actor_error)?;
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
        Ok(Self {
            root_dir: Arc::new(root_dir),
            databases: Arc::new(Mutex::new(HashMap::new())),
            actor_versions: Arc::new(Mutex::new(HashMap::new())),
            db_cache_max_open,
            db_idle_ttl,
            version: Arc::new(AtomicU64::new(1)),
            profile: Arc::new(ActorProfile::default()),
        })
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
        self.record_profile(
            ActorProfileMetricKind::StoreSnapshot,
            started.elapsed().as_micros() as u64,
            entries.len() as u64,
        );
        Ok(ActorSnapshot {
            entries,
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
        let conn = self.connect(namespace, actor_key).await?;
        let filtered_keys = keys
            .iter()
            .map(|key| key.trim())
            .filter(|key| !key.is_empty())
            .collect::<Vec<_>>();
        if filtered_keys.is_empty() {
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
        let conn = self.connect(namespace, actor_key).await?;
        if mutations.is_empty() && reads.is_empty() && list_gate_version.is_none() {
            let max_version = self
                .max_version_for_actor(&conn, actor_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(max_version);
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
                let current = self.max_version_for_actor(&conn, actor_key).await?.unwrap_or(-1);
                if let Some(expected_list_version) = list_gate_version {
                    if current != expected_list_version {
                        self.observe_actor_version(namespace, actor_key, current).await;
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
                        return Ok(ActorBatchApplyResult {
                            conflict: true,
                            max_version: current.max(observed),
                        });
                    }
                }

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
                self.observe_actor_version(namespace, actor_key, max_version).await;
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
                    return Ok(result);
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(error);
                }
            }
        }
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
            return Err(PlatformError::runtime("actor namespace must not be empty"));
        }
        let actor_key = actor_key.trim();
        if actor_key.is_empty() {
            return Err(PlatformError::runtime("actor key must not be empty"));
        }

        let db_key = Self::database_key(namespace);
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

        let path = self.db_path(namespace);
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
        if let Some(row) = rows.next().await.map_err(actor_error)? {
            let version = row.get::<Option<i64>>(0).map_err(actor_error)?;
            return Ok(version);
        }
        Ok(None)
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
        if let Some(row) = rows.next().await.map_err(actor_error)? {
            let version = row.get::<i64>(0).map_err(actor_error)?;
            self.observe_version(version);
            return Ok(Some(version));
        }
        Ok(None)
    }

    fn database_key(namespace: &str) -> String {
        namespace.to_string()
    }

    fn actor_version_key(namespace: &str, actor_key: &str) -> String {
        format!("{namespace}\u{1f}{actor_key}")
    }

    fn db_path(&self, namespace: &str) -> PathBuf {
        let encoded_namespace = hex_encode(namespace.as_bytes());
        self.root_dir.join(format!("{encoded_namespace}.db"))
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
            ActorProfileMetricKind::JsCacheHit => &self.js_cache_hit,
            ActorProfileMetricKind::JsCacheMiss => &self.js_cache_miss,
            ActorProfileMetricKind::JsCacheStale => &self.js_cache_stale,
            ActorProfileMetricKind::OpSnapshot => &self.op_snapshot,
            ActorProfileMetricKind::OpVersionIfNewer => &self.op_version_if_newer,
            ActorProfileMetricKind::OpValidateReads => &self.op_validate_reads,
            ActorProfileMetricKind::StoreSnapshot => &self.store_snapshot,
            ActorProfileMetricKind::StoreSnapshotKeys => &self.store_snapshot_keys,
            ActorProfileMetricKind::StoreVersionIfNewer => &self.store_version_if_newer,
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
            js_cache_hit: self.js_cache_hit.snapshot(),
            js_cache_miss: self.js_cache_miss.snapshot(),
            js_cache_stale: self.js_cache_stale.snapshot(),
            op_snapshot: self.op_snapshot.snapshot(),
            op_version_if_newer: self.op_version_if_newer.snapshot(),
            op_validate_reads: self.op_validate_reads.snapshot(),
            store_snapshot: self.store_snapshot.snapshot(),
            store_snapshot_keys: self.store_snapshot_keys.snapshot(),
            store_version_if_newer: self.store_version_if_newer.snapshot(),
        };
        self.reset();
        snapshot
    }

    fn reset(&self) {
        self.js_read_only_total.reset();
        self.js_freshness_check.reset();
        self.js_hydrate_full.reset();
        self.js_hydrate_keys.reset();
        self.js_cache_hit.reset();
        self.js_cache_miss.reset();
        self.js_cache_stale.reset();
        self.op_snapshot.reset();
        self.op_version_if_newer.reset();
        self.op_validate_reads.reset();
        self.store_snapshot.reset();
        self.store_snapshot_keys.reset();
        self.store_version_if_newer.reset();
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
    Ok(())
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

    #[tokio::test]
    async fn actor_db_paths_are_stable_per_namespace() -> Result<()> {
        let store = ActorStore::new(temp_root("paths"), 4, Duration::from_secs(60)).await?;
        let ns_a = store.db_path("ns");
        let ns_a_again = store.db_path("ns");
        let ns_b = store.db_path("other");

        assert_eq!(ns_a, ns_a_again);
        assert_ne!(ns_a, ns_b);
        Ok(())
    }

    #[tokio::test]
    async fn actor_db_cache_eviction_keeps_persisted_state() -> Result<()> {
        let store = ActorStore::new(temp_root("eviction"), 1, Duration::from_secs(60)).await?;
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
        let store =
            ActorStore::new(temp_root("version-if-newer"), 4, Duration::from_secs(60)).await?;
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
}
