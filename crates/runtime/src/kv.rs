use common::{PlatformError, Result};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use turso::{transaction::TransactionBehavior, Builder, Connection, Database, Value};

const ENCODING_UTF8: &str = "utf8";
const ENCODING_V8SC: &str = "v8sc";

#[derive(Clone)]
pub struct KvStore {
    database: Arc<Database>,
    connections: Arc<Mutex<Vec<Connection>>>,
    version: Arc<AtomicU64>,
    profile: Arc<KvProfile>,
    write_scheduler: KvWriteScheduler,
    failed_versions: Arc<Mutex<HashSet<i64>>>,
}

struct KvConnectionGuard {
    connections: Arc<Mutex<Vec<Connection>>>,
    conn: Option<Connection>,
}

impl std::ops::Deref for KvConnectionGuard {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("kv pooled connection must be present")
    }
}

impl std::ops::DerefMut for KvConnectionGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("kv pooled connection must be present")
    }
}

impl Drop for KvConnectionGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.connections
                .lock()
                .expect("kv connection pool lock poisoned")
                .push(conn);
        }
    }
}

#[derive(Debug, Clone)]
pub struct KvValue {
    pub value: Vec<u8>,
    pub encoding: String,
}

#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
}

#[derive(Debug, Clone)]
pub struct KvBatchMutation {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvUtf8Lookup {
    Missing,
    WrongEncoding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvProfileMetricKind {
    JsRequestTotal,
    JsBatchFlush,
    OpGet,
    OpGetManyUtf8,
    OpGetValue,
    StoreGetUtf8,
    StoreGetUtf8Many,
    StoreGetValue,
    WriteEnqueue,
    WriteSuperseded,
    WriteRejected,
    WriteFlush,
    WriteRetry,
    WriteQueueWait,
    JsCacheHit,
    JsCacheMiss,
    JsCacheStale,
    JsCacheFill,
    JsCacheInvalidate,
}

#[derive(Default)]
struct KvProfileMetric {
    calls: AtomicU64,
    total_us: AtomicU64,
    total_items: AtomicU64,
    max_us: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KvProfileMetricSnapshot {
    pub calls: u64,
    pub total_us: u64,
    pub total_items: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KvProfileSnapshot {
    pub enabled: bool,
    pub js_request_total: KvProfileMetricSnapshot,
    pub js_batch_flush: KvProfileMetricSnapshot,
    pub op_get: KvProfileMetricSnapshot,
    pub op_get_many_utf8: KvProfileMetricSnapshot,
    pub op_get_value: KvProfileMetricSnapshot,
    pub store_get_utf8: KvProfileMetricSnapshot,
    pub store_get_utf8_many: KvProfileMetricSnapshot,
    pub store_get_value: KvProfileMetricSnapshot,
    pub write_enqueue: KvProfileMetricSnapshot,
    pub write_superseded: KvProfileMetricSnapshot,
    pub write_rejected: KvProfileMetricSnapshot,
    pub write_flush: KvProfileMetricSnapshot,
    pub write_retry: KvProfileMetricSnapshot,
    pub write_queue_wait: KvProfileMetricSnapshot,
    pub js_cache_hit: KvProfileMetricSnapshot,
    pub js_cache_miss: KvProfileMetricSnapshot,
    pub js_cache_stale: KvProfileMetricSnapshot,
    pub js_cache_fill: KvProfileMetricSnapshot,
    pub js_cache_invalidate: KvProfileMetricSnapshot,
}

#[derive(Default)]
pub struct KvProfile {
    enabled: AtomicBool,
    js_request_total: KvProfileMetric,
    js_batch_flush: KvProfileMetric,
    op_get: KvProfileMetric,
    op_get_many_utf8: KvProfileMetric,
    op_get_value: KvProfileMetric,
    store_get_utf8: KvProfileMetric,
    store_get_utf8_many: KvProfileMetric,
    store_get_value: KvProfileMetric,
    write_enqueue: KvProfileMetric,
    write_superseded: KvProfileMetric,
    write_rejected: KvProfileMetric,
    write_flush: KvProfileMetric,
    write_retry: KvProfileMetric,
    write_queue_wait: KvProfileMetric,
    js_cache_hit: KvProfileMetric,
    js_cache_miss: KvProfileMetric,
    js_cache_stale: KvProfileMetric,
    js_cache_fill: KvProfileMetric,
    js_cache_invalidate: KvProfileMetric,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvWriteMode {
    EnqueueBestEffort,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct KvWriteKey {
    worker_name: String,
    binding: String,
    key: String,
}

#[derive(Debug, Clone)]
struct KvScheduledMutation {
    key: KvWriteKey,
    value: Vec<u8>,
    encoding: String,
    deleted: bool,
    version: i64,
    enqueued_at: Instant,
    size_bytes: usize,
}

#[derive(Default)]
struct KvWriteShardState {
    pending: HashMap<KvWriteKey, KvScheduledMutation>,
    pending_bytes: usize,
    shutting_down: bool,
}

struct KvWriteShard {
    state: Mutex<KvWriteShardState>,
    wake: Condvar,
}

struct KvWriteSchedulerInner {
    shards: Vec<Arc<KvWriteShard>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    max_pending_keys: usize,
    max_pending_bytes: usize,
    flush_interval: Duration,
    flush_threshold: usize,
    flush_gate: Arc<Mutex<()>>,
    database: Arc<Database>,
    profile: Arc<KvProfile>,
    failed_versions: Arc<Mutex<HashSet<i64>>>,
}

#[derive(Clone)]
struct KvWriteScheduler {
    inner: Arc<KvWriteSchedulerInner>,
}

impl KvProfileMetric {
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

    fn snapshot(&self) -> KvProfileMetricSnapshot {
        KvProfileMetricSnapshot {
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

impl KvProfile {
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if !enabled {
            self.reset();
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn record(&self, metric: KvProfileMetricKind, duration_us: u64, items: u64) {
        if !self.enabled() {
            return;
        }
        self.metric(metric).record(duration_us, items.max(1));
    }

    pub fn snapshot(&self) -> KvProfileSnapshot {
        KvProfileSnapshot {
            enabled: self.enabled(),
            js_request_total: self.js_request_total.snapshot(),
            js_batch_flush: self.js_batch_flush.snapshot(),
            op_get: self.op_get.snapshot(),
            op_get_many_utf8: self.op_get_many_utf8.snapshot(),
            op_get_value: self.op_get_value.snapshot(),
            store_get_utf8: self.store_get_utf8.snapshot(),
            store_get_utf8_many: self.store_get_utf8_many.snapshot(),
            store_get_value: self.store_get_value.snapshot(),
            write_enqueue: self.write_enqueue.snapshot(),
            write_superseded: self.write_superseded.snapshot(),
            write_rejected: self.write_rejected.snapshot(),
            write_flush: self.write_flush.snapshot(),
            write_retry: self.write_retry.snapshot(),
            write_queue_wait: self.write_queue_wait.snapshot(),
            js_cache_hit: self.js_cache_hit.snapshot(),
            js_cache_miss: self.js_cache_miss.snapshot(),
            js_cache_stale: self.js_cache_stale.snapshot(),
            js_cache_fill: self.js_cache_fill.snapshot(),
            js_cache_invalidate: self.js_cache_invalidate.snapshot(),
        }
    }

    pub fn take_snapshot_and_reset(&self) -> KvProfileSnapshot {
        let snapshot = self.snapshot();
        self.reset();
        snapshot
    }

    pub fn reset(&self) {
        self.js_request_total.reset();
        self.js_batch_flush.reset();
        self.op_get.reset();
        self.op_get_many_utf8.reset();
        self.op_get_value.reset();
        self.store_get_utf8.reset();
        self.store_get_utf8_many.reset();
        self.store_get_value.reset();
        self.write_enqueue.reset();
        self.write_superseded.reset();
        self.write_rejected.reset();
        self.write_flush.reset();
        self.write_retry.reset();
        self.write_queue_wait.reset();
        self.js_cache_hit.reset();
        self.js_cache_miss.reset();
        self.js_cache_stale.reset();
        self.js_cache_fill.reset();
        self.js_cache_invalidate.reset();
    }

    fn metric(&self, metric: KvProfileMetricKind) -> &KvProfileMetric {
        match metric {
            KvProfileMetricKind::JsRequestTotal => &self.js_request_total,
            KvProfileMetricKind::JsBatchFlush => &self.js_batch_flush,
            KvProfileMetricKind::OpGet => &self.op_get,
            KvProfileMetricKind::OpGetManyUtf8 => &self.op_get_many_utf8,
            KvProfileMetricKind::OpGetValue => &self.op_get_value,
            KvProfileMetricKind::StoreGetUtf8 => &self.store_get_utf8,
            KvProfileMetricKind::StoreGetUtf8Many => &self.store_get_utf8_many,
            KvProfileMetricKind::StoreGetValue => &self.store_get_value,
            KvProfileMetricKind::WriteEnqueue => &self.write_enqueue,
            KvProfileMetricKind::WriteSuperseded => &self.write_superseded,
            KvProfileMetricKind::WriteRejected => &self.write_rejected,
            KvProfileMetricKind::WriteFlush => &self.write_flush,
            KvProfileMetricKind::WriteRetry => &self.write_retry,
            KvProfileMetricKind::WriteQueueWait => &self.write_queue_wait,
            KvProfileMetricKind::JsCacheHit => &self.js_cache_hit,
            KvProfileMetricKind::JsCacheMiss => &self.js_cache_miss,
            KvProfileMetricKind::JsCacheStale => &self.js_cache_stale,
            KvProfileMetricKind::JsCacheFill => &self.js_cache_fill,
            KvProfileMetricKind::JsCacheInvalidate => &self.js_cache_invalidate,
        }
    }
}

impl KvScheduledMutation {
    fn from_batch_mutation(
        worker_name: &str,
        binding: &str,
        mutation: KvBatchMutation,
        version: i64,
    ) -> Result<Self> {
        let encoding = normalize_encoding(&mutation.encoding);
        if encoding != ENCODING_UTF8 && encoding != ENCODING_V8SC {
            return Err(PlatformError::bad_request(format!(
                "unsupported kv encoding: {}",
                mutation.encoding
            )));
        }
        if !mutation.deleted && encoding == ENCODING_UTF8 {
            std::str::from_utf8(&mutation.value).map_err(|error| {
                PlatformError::bad_request(format!("invalid utf8 value: {error}"))
            })?;
        }
        let size_bytes = worker_name.len()
            + binding.len()
            + mutation.key.len()
            + mutation.value.len()
            + encoding.len()
            + 64;
        Ok(Self {
            key: KvWriteKey {
                worker_name: worker_name.to_string(),
                binding: binding.to_string(),
                key: mutation.key,
            },
            value: mutation.value,
            encoding,
            deleted: mutation.deleted,
            version,
            enqueued_at: Instant::now(),
            size_bytes,
        })
    }
}

impl KvWriteScheduler {
    fn new(
        database: Arc<Database>,
        profile: Arc<KvProfile>,
        failed_versions: Arc<Mutex<HashSet<i64>>>,
    ) -> Self {
        const SHARD_COUNT: usize = 16;
        let shards = (0..SHARD_COUNT)
            .map(|_| {
                Arc::new(KvWriteShard {
                    state: Mutex::new(KvWriteShardState::default()),
                    wake: Condvar::new(),
                })
            })
            .collect::<Vec<_>>();
        let inner = Arc::new(KvWriteSchedulerInner {
            shards,
            handles: Mutex::new(Vec::new()),
            max_pending_keys: 4_096,
            max_pending_bytes: 16 * 1024 * 1024,
            flush_interval: Duration::from_millis(2),
            flush_threshold: 128,
            flush_gate: Arc::new(Mutex::new(())),
            database,
            profile,
            failed_versions,
        });
        inner.spawn_flushers();
        Self { inner }
    }

    fn enqueue_batch(
        &self,
        next_version: impl Fn() -> i64,
        worker_name: &str,
        binding: &str,
        mutations: &[KvBatchMutation],
    ) -> Result<Vec<i64>> {
        if mutations.is_empty() {
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let mut versions = Vec::with_capacity(mutations.len());
        let mut grouped = HashMap::<usize, HashMap<KvWriteKey, KvScheduledMutation>>::new();
        for mutation in mutations.iter().cloned() {
            let version = next_version();
            let scheduled =
                KvScheduledMutation::from_batch_mutation(worker_name, binding, mutation, version)?;
            versions.push(version);
            let shard_index = self.inner.shard_index(&scheduled.key);
            grouped
                .entry(shard_index)
                .or_default()
                .insert(scheduled.key.clone(), scheduled);
        }

        let mut shard_indexes = grouped.keys().copied().collect::<Vec<_>>();
        shard_indexes.sort_unstable();

        let mut guards = Vec::with_capacity(shard_indexes.len());
        for shard_index in &shard_indexes {
            let guard = self.inner.shards[*shard_index]
                .state
                .lock()
                .expect("kv write shard lock poisoned");
            guards.push((*shard_index, guard));
        }

        for (shard_index, state) in &guards {
            let shard_mutations = grouped
                .get(shard_index)
                .expect("grouped mutations should exist for locked shard");
            let mut pending_keys = state.pending.len();
            let mut pending_bytes = state.pending_bytes;
            for mutation in shard_mutations.values() {
                match state.pending.get(&mutation.key) {
                    Some(existing) => {
                        pending_bytes = pending_bytes
                            .saturating_sub(existing.size_bytes)
                            .saturating_add(mutation.size_bytes);
                    }
                    None => {
                        pending_keys += 1;
                        pending_bytes = pending_bytes.saturating_add(mutation.size_bytes);
                    }
                }
            }
            if pending_keys > self.inner.max_pending_keys
                || pending_bytes > self.inner.max_pending_bytes
            {
                self.inner.profile.record(
                    KvProfileMetricKind::WriteRejected,
                    0,
                    mutations.len() as u64,
                );
                return Err(PlatformError::runtime(
                    "kv write queue overloaded: enqueue rejected",
                ));
            }
        }

        let mut superseded = 0u64;
        for (shard_index, mut state) in guards {
            let shard_mutations = grouped
                .remove(&shard_index)
                .expect("grouped mutations should exist for locked shard");
            for mutation in shard_mutations.into_values() {
                if let Some(existing) = state.pending.insert(mutation.key.clone(), mutation.clone())
                {
                    superseded += 1;
                    state.pending_bytes = state.pending_bytes.saturating_sub(existing.size_bytes);
                }
                state.pending_bytes = state.pending_bytes.saturating_add(mutation.size_bytes);
            }
        }

        for shard_index in shard_indexes {
            self.inner.shards[shard_index].wake.notify_one();
        }
        self.inner.profile.record(
            KvProfileMetricKind::WriteEnqueue,
            started.elapsed().as_micros() as u64,
            mutations.len() as u64,
        );
        if superseded > 0 {
            self.inner
                .profile
                .record(KvProfileMetricKind::WriteSuperseded, 0, superseded);
        }
        Ok(versions)
    }
}

impl KvWriteSchedulerInner {
    fn spawn_flushers(self: &Arc<Self>) {
        let mut handles = self
            .handles
            .lock()
            .expect("kv write scheduler handles lock poisoned");
        for shard_index in 0..self.shards.len() {
            let shard = Arc::clone(&self.shards[shard_index]);
            let database = Arc::clone(&self.database);
            let profile = Arc::clone(&self.profile);
            let flush_interval = self.flush_interval;
            let flush_threshold = self.flush_threshold;
            let flush_gate = Arc::clone(&self.flush_gate);
            let failed_versions = Arc::clone(&self.failed_versions);
            let handle = std::thread::Builder::new()
                .name(format!("kv-write-shard-{shard_index}"))
                .spawn(move || {
                    Self::run_shard_loop(
                        database,
                        profile,
                        flush_interval,
                        flush_threshold,
                        flush_gate,
                        failed_versions,
                        shard,
                    )
                })
                .expect("kv write shard thread should start");
            handles.push(handle);
        }
    }

    fn shard_index(&self, key: &KvWriteKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn run_shard_loop(
        database: Arc<Database>,
        profile: Arc<KvProfile>,
        flush_interval: Duration,
        flush_threshold: usize,
        flush_gate: Arc<Mutex<()>>,
        failed_versions: Arc<Mutex<HashSet<i64>>>,
        shard: Arc<KvWriteShard>,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("kv write shard runtime should build");
        loop {
            let batch = {
                let mut state = shard.state.lock().expect("kv write shard lock poisoned");
                while state.pending.is_empty() && !state.shutting_down {
                    state = shard
                        .wake
                        .wait(state)
                        .expect("kv write shard condvar wait should succeed");
                }
                if state.shutting_down && state.pending.is_empty() {
                    return;
                }
                if state.pending.len() < flush_threshold && !state.shutting_down {
                    let (next_state, _) = shard
                        .wake
                        .wait_timeout(state, flush_interval)
                        .expect("kv write shard timed wait should succeed");
                    state = next_state;
                    if state.pending.is_empty() && !state.shutting_down {
                        continue;
                    }
                }
                let pending = std::mem::take(&mut state.pending);
                state.pending_bytes = 0;
                pending.into_values().collect::<Vec<_>>()
            };
            if batch.is_empty() {
                continue;
            }
            let oldest = batch
                .iter()
                .map(|mutation| mutation.enqueued_at)
                .min()
                .unwrap_or_else(Instant::now);
            profile.record(
                KvProfileMetricKind::WriteQueueWait,
                oldest.elapsed().as_micros() as u64,
                batch.len() as u64,
            );
            let flush_guard = flush_gate
                .lock()
                .expect("kv write flush gate lock poisoned");
            let flush_result = runtime.block_on(Self::flush_batch(&database, &profile, &batch));
            drop(flush_guard);
            match flush_result {
                Ok(()) => {}
                Err(_) => {
                    let mut failed = failed_versions
                        .lock()
                        .expect("kv failed versions lock poisoned");
                    for mutation in &batch {
                        failed.insert(mutation.version);
                    }
                }
            }
        }
    }

    async fn flush_batch(
        database: &Arc<Database>,
        profile: &Arc<KvProfile>,
        batch: &[KvScheduledMutation],
    ) -> Result<()> {
        const MAX_ATTEMPTS: usize = 8;
        let started = Instant::now();
        for attempt in 0..MAX_ATTEMPTS {
            let mut conn = database.connect().map_err(kv_error)?;
            configure_connection(&conn).await?;
            let mut tx = Some(
                conn.transaction_with_behavior(TransactionBehavior::Immediate)
                    .await
                    .map_err(kv_error)?,
            );
            let now_ms = epoch_ms_i64()?;
            let mut should_retry = false;
            for mutation in batch {
                let value_text = if !mutation.deleted && mutation.encoding == ENCODING_UTF8 {
                    std::str::from_utf8(&mutation.value)
                        .map_err(|error| {
                            PlatformError::bad_request(format!("invalid utf8 value: {error}"))
                        })?
                        .to_string()
                } else {
                    String::new()
                };
                let value_blob = if mutation.deleted || mutation.encoding == ENCODING_UTF8 {
                    None
                } else {
                    Some(mutation.value.clone())
                };
                match tx
                    .as_ref()
                    .expect("kv write transaction should be present")
                    .execute(
                        "INSERT INTO worker_kv (worker_name, binding, key, value, value_blob, encoding, deleted, version, updated_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                         ON CONFLICT(worker_name, binding, key) DO UPDATE SET
                           value = excluded.value,
                           value_blob = excluded.value_blob,
                           encoding = excluded.encoding,
                           deleted = excluded.deleted,
                           version = excluded.version,
                           updated_at_ms = excluded.updated_at_ms
                         WHERE excluded.version > worker_kv.version",
                        (
                            mutation.key.worker_name.as_str(),
                            mutation.key.binding.as_str(),
                            mutation.key.key.as_str(),
                            value_text.as_str(),
                            value_blob.as_deref(),
                            mutation.encoding.as_str(),
                            if mutation.deleted { 1 } else { 0 },
                            mutation.version,
                            now_ms,
                        ),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(error) => {
                        if let Some(tx) = tx.take() {
                            let _ = tx.rollback().await;
                        }
                        if is_retryable_kv_error(&error) && attempt + 1 < MAX_ATTEMPTS {
                            profile.record(KvProfileMetricKind::WriteRetry, 0, 1);
                            tokio::time::sleep(Duration::from_millis(5 * (attempt + 1) as u64))
                                .await;
                            should_retry = true;
                            break;
                        }
                        return Err(kv_error(error));
                    }
                }
            }
            if should_retry {
                continue;
            }
            if let Err(error) = tx
                .take()
                .expect("kv write transaction should be present")
                .commit()
                .await
            {
                if is_retryable_kv_error(&error) && attempt + 1 < MAX_ATTEMPTS {
                    profile.record(KvProfileMetricKind::WriteRetry, 0, 1);
                    tokio::time::sleep(Duration::from_millis(5 * (attempt + 1) as u64)).await;
                    continue;
                }
                return Err(kv_error(error));
            }
            profile.record(
                KvProfileMetricKind::WriteFlush,
                started.elapsed().as_micros() as u64,
                batch.len() as u64,
            );
            return Ok(());
        }
        Err(PlatformError::runtime(
            "kv background flush failed after retries",
        ))
    }
}

impl Drop for KvWriteSchedulerInner {
    fn drop(&mut self) {
        for shard in &self.shards {
            let mut state = shard.state.lock().expect("kv write shard lock poisoned");
            state.shutting_down = true;
            shard.wake.notify_all();
        }
        self.handles
            .lock()
            .expect("kv write scheduler handles lock poisoned")
            .clear();
    }
}

impl KvStore {
    pub async fn from_database_url(database_url: &str) -> Result<Self> {
        let local_path = database_url
            .strip_prefix("file:")
            .unwrap_or(database_url)
            .to_string();
        ensure_parent_dir(Path::new(&local_path))?;
        let database = Builder::new_local(&local_path)
            .build()
            .await
            .map_err(kv_error)?;
        let database = Arc::new(database);
        let profile = Arc::new(KvProfile::default());
        let failed_versions = Arc::new(Mutex::new(HashSet::new()));
        let store = Self {
            database: Arc::clone(&database),
            connections: Arc::new(Mutex::new(Vec::new())),
            version: Arc::new(AtomicU64::new(1)),
            profile: Arc::clone(&profile),
            write_scheduler: KvWriteScheduler::new(database, profile, Arc::clone(&failed_versions)),
            failed_versions,
        };
        store.ensure_schema().await?;
        store.sync_version_counter_from_db().await?;
        Ok(store)
    }

    pub fn take_failed_write_version(&self, version: i64) -> bool {
        self.failed_versions
            .lock()
            .expect("kv failed versions lock poisoned")
            .remove(&version)
    }

    pub fn set_profile_enabled(&self, enabled: bool) {
        self.profile.set_enabled(enabled);
    }

    pub fn record_profile(&self, metric: KvProfileMetricKind, duration_us: u64, items: u64) {
        self.profile.record(metric, duration_us, items);
    }

    pub fn take_profile_snapshot_and_reset(&self) -> KvProfileSnapshot {
        self.profile.take_snapshot_and_reset()
    }

    pub fn reset_profile(&self) {
        self.profile.reset();
    }

    pub fn write_mode(&self) -> KvWriteMode {
        KvWriteMode::EnqueueBestEffort
    }

    pub async fn get(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
    ) -> Result<Option<KvValue>> {
        let started = Instant::now();
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT value_blob, encoding, value, deleted
                 FROM worker_kv
                 WHERE worker_name = ?1 AND binding = ?2 AND key = ?3",
                (worker_name, binding, key),
            )
            .await
            .map_err(kv_error)?;
        if let Some(row) = rows.next().await.map_err(kv_error)? {
            let deleted: i64 = row.get::<i64>(3).map_err(kv_error)?;
            if deleted != 0 {
                return Ok(None);
            }
            let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(0).map_err(kv_error)?;
            let encoding: String = row.get::<String>(1).map_err(kv_error)?;
            let legacy_value: String = row.get::<String>(2).map_err(kv_error)?;
            let value = value_blob.unwrap_or_else(|| legacy_value.into_bytes());
            self.record_profile(
                KvProfileMetricKind::StoreGetValue,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(Some(KvValue {
                value,
                encoding: normalize_encoding(&encoding),
            }));
        }
        self.record_profile(
            KvProfileMetricKind::StoreGetValue,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok(None)
    }

    pub async fn get_utf8(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
    ) -> Result<std::result::Result<String, KvUtf8Lookup>> {
        let started = Instant::now();
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT value, encoding, deleted
                 FROM worker_kv
                 WHERE worker_name = ?1 AND binding = ?2 AND key = ?3",
                (worker_name, binding, key),
            )
            .await
            .map_err(kv_error)?;
        if let Some(row) = rows.next().await.map_err(kv_error)? {
            let deleted: i64 = row.get::<i64>(2).map_err(kv_error)?;
            if deleted != 0 {
                return Ok(Err(KvUtf8Lookup::Missing));
            }
            let encoding: String = row.get::<String>(1).map_err(kv_error)?;
            if normalize_encoding(&encoding) != ENCODING_UTF8 {
                self.record_profile(
                    KvProfileMetricKind::StoreGetUtf8,
                    started.elapsed().as_micros() as u64,
                    1,
                );
                return Ok(Err(KvUtf8Lookup::WrongEncoding));
            }
            let value = row.get::<String>(0).map_err(kv_error)?;
            self.record_profile(
                KvProfileMetricKind::StoreGetUtf8,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(Ok(value));
        }
        self.record_profile(
            KvProfileMetricKind::StoreGetUtf8,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok(Err(KvUtf8Lookup::Missing))
    }

    pub async fn get_utf8_many(
        &self,
        worker_name: &str,
        binding: &str,
        keys: &[String],
    ) -> Result<Vec<std::result::Result<String, KvUtf8Lookup>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let started = Instant::now();
        let conn = self.connect().await?;
        let placeholders = (0..keys.len())
            .map(|index| format!("?{}", index + 3))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT key, value, encoding, deleted
             FROM worker_kv
             WHERE worker_name = ?1 AND binding = ?2 AND key IN ({placeholders})"
        );
        let mut params = Vec::with_capacity(keys.len() + 2);
        params.push(Value::Text(worker_name.to_string()));
        params.push(Value::Text(binding.to_string()));
        params.extend(keys.iter().cloned().map(Value::Text));
        let mut rows = conn.query(&sql, params).await.map_err(kv_error)?;
        let mut values = std::collections::HashMap::with_capacity(keys.len());
        while let Some(row) = rows.next().await.map_err(kv_error)? {
            let key: String = row.get::<String>(0).map_err(kv_error)?;
            let value: String = row.get::<String>(1).map_err(kv_error)?;
            let encoding: String = row.get::<String>(2).map_err(kv_error)?;
            let deleted: i64 = row.get::<i64>(3).map_err(kv_error)?;
            values.insert(key, (value, normalize_encoding(&encoding), deleted != 0));
        }
        let result = keys
            .iter()
            .map(|key| match values.get(key) {
                None => Err(KvUtf8Lookup::Missing),
                Some((_, _, true)) => Err(KvUtf8Lookup::Missing),
                Some((_, encoding, false)) if encoding != ENCODING_UTF8 => {
                    Err(KvUtf8Lookup::WrongEncoding)
                }
                Some((value, _, false)) => Ok(value.clone()),
            })
            .collect();
        self.record_profile(
            KvProfileMetricKind::StoreGetUtf8Many,
            started.elapsed().as_micros() as u64,
            keys.len() as u64,
        );
        Ok(result)
    }

    pub async fn put(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let conn = self.connect().await?;
        const MAX_VERSION_RETRIES: usize = 8;
        for _ in 0..MAX_VERSION_RETRIES {
            let version = self.next_version();
            let now_ms = epoch_ms_i64()?;
            let affected = execute_with_retry(|| {
                conn.execute(
                    "INSERT INTO worker_kv (worker_name, binding, key, value, value_blob, encoding, deleted, version, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, NULL, ?5, 0, ?6, ?7)
                     ON CONFLICT(worker_name, binding, key) DO UPDATE SET
                       value = excluded.value,
                       value_blob = excluded.value_blob,
                       encoding = excluded.encoding,
                       deleted = 0,
                       version = excluded.version,
                       updated_at_ms = excluded.updated_at_ms
                     WHERE excluded.version > worker_kv.version",
                    (worker_name, binding, key, value, ENCODING_UTF8, version, now_ms),
                )
            })
            .await?;
            if affected > 0 {
                return Ok(());
            }
            self.sync_version_floor_from_conn(&conn).await?;
        }
        Err(PlatformError::runtime(
            "kv write conflict: failed to resolve version race",
        ))
    }

    pub async fn put_value(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
        value: &[u8],
        encoding: &str,
    ) -> Result<()> {
        if encoding != ENCODING_UTF8 && encoding != ENCODING_V8SC {
            return Err(PlatformError::bad_request(format!(
                "unsupported kv encoding: {encoding}"
            )));
        }
        let conn = self.connect().await?;
        const MAX_VERSION_RETRIES: usize = 8;
        let value_blob = value.to_vec();
        let value_text = if encoding == ENCODING_UTF8 {
            std::str::from_utf8(value)
                .map_err(|error| {
                    PlatformError::bad_request(format!("invalid utf8 value: {error}"))
                })?
                .to_string()
        } else {
            String::new()
        };
        let encoding = encoding.to_string();

        for _ in 0..MAX_VERSION_RETRIES {
            let version = self.next_version();
            let now_ms = epoch_ms_i64()?;
            let affected = execute_with_retry(|| {
                conn.execute(
                    "INSERT INTO worker_kv (worker_name, binding, key, value, value_blob, encoding, deleted, version, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8)
                     ON CONFLICT(worker_name, binding, key) DO UPDATE SET
                       value = excluded.value,
                       value_blob = excluded.value_blob,
                       encoding = excluded.encoding,
                       deleted = 0,
                       version = excluded.version,
                       updated_at_ms = excluded.updated_at_ms
                     WHERE excluded.version > worker_kv.version",
                    (
                        worker_name,
                        binding,
                        key,
                        value_text.as_str(),
                        value_blob.as_slice(),
                        encoding.as_str(),
                        version,
                        now_ms,
                    ),
                )
            })
            .await?;
            if affected > 0 {
                return Ok(());
            }
            self.sync_version_floor_from_conn(&conn).await?;
        }
        Err(PlatformError::runtime(
            "kv write conflict: failed to resolve version race",
        ))
    }

    pub async fn delete(&self, worker_name: &str, binding: &str, key: &str) -> Result<()> {
        let conn = self.connect().await?;
        const MAX_VERSION_RETRIES: usize = 8;
        for _ in 0..MAX_VERSION_RETRIES {
            let version = self.next_version();
            let now_ms = epoch_ms_i64()?;
            let affected = execute_with_retry(|| {
                conn.execute(
                    "INSERT INTO worker_kv (worker_name, binding, key, value, value_blob, encoding, deleted, version, updated_at_ms)
                     VALUES (?1, ?2, ?3, '', NULL, ?4, 1, ?5, ?6)
                     ON CONFLICT(worker_name, binding, key) DO UPDATE SET
                       value = excluded.value,
                       value_blob = excluded.value_blob,
                       encoding = excluded.encoding,
                       deleted = 1,
                       version = excluded.version,
                       updated_at_ms = excluded.updated_at_ms
                     WHERE excluded.version > worker_kv.version",
                    (worker_name, binding, key, ENCODING_UTF8, version, now_ms),
                )
            })
            .await?;
            if affected > 0 {
                return Ok(());
            }
            self.sync_version_floor_from_conn(&conn).await?;
        }
        Err(PlatformError::runtime(
            "kv delete conflict: failed to resolve version race",
        ))
    }

    pub fn apply_batch(
        &self,
        worker_name: &str,
        binding: &str,
        mutations: &[KvBatchMutation],
    ) -> Result<()> {
        self.write_scheduler
            .enqueue_batch(|| self.next_version(), worker_name, binding, mutations)
            .map(|_| ())
    }

    pub fn enqueue_batch_versions(
        &self,
        worker_name: &str,
        binding: &str,
        mutations: &[KvBatchMutation],
    ) -> Result<Vec<i64>> {
        self.write_scheduler
            .enqueue_batch(|| self.next_version(), worker_name, binding, mutations)
    }

    pub async fn list(
        &self,
        worker_name: &str,
        binding: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<KvEntry>> {
        let conn = self.connect().await?;
        let pattern = format!("{prefix}%");
        let mut rows = conn
            .query(
                "SELECT key, value_blob, encoding, value
                 FROM worker_kv
                 WHERE worker_name = ?1 AND binding = ?2 AND deleted = 0 AND key LIKE ?3
                 ORDER BY key ASC
                 LIMIT ?4",
                (worker_name, binding, pattern, limit as i64),
            )
            .await
            .map_err(kv_error)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(kv_error)? {
            let key: String = row.get::<String>(0).map_err(kv_error)?;
            let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(1).map_err(kv_error)?;
            let encoding: String = row.get::<String>(2).map_err(kv_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(kv_error)?;
            out.push(KvEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
            });
        }
        Ok(out)
    }

    async fn ensure_schema(&self) -> Result<()> {
        let conn = self.connect().await?;
        conn.pragma_update("journal_mode", "'WAL'")
            .await
            .map_err(kv_error)?;
        conn.pragma_update("synchronous", "'NORMAL'")
            .await
            .map_err(kv_error)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS worker_kv (
              worker_name TEXT NOT NULL,
              binding TEXT NOT NULL,
              key TEXT NOT NULL,
              value TEXT NOT NULL,
              value_blob BLOB,
              encoding TEXT NOT NULL DEFAULT 'utf8',
              deleted INTEGER NOT NULL DEFAULT 0,
              version INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL,
              PRIMARY KEY (worker_name, binding, key)
            )",
            (),
        )
        .await
        .map_err(kv_error)?;
        ensure_compat_columns(&conn).await?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_worker_kv_lookup ON worker_kv(worker_name, binding, key)",
            (),
        )
        .await
        .map_err(kv_error)?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_worker_kv_list ON worker_kv(worker_name, binding, deleted, key)",
            (),
        )
        .await
        .map_err(kv_error)?;
        Ok(())
    }

    async fn sync_version_counter_from_db(&self) -> Result<()> {
        let conn = self.connect().await?;
        self.sync_version_floor_from_conn(&conn).await
    }

    async fn sync_version_floor_from_conn(&self, conn: &Connection) -> Result<()> {
        let next = self.next_version_floor(conn).await?;
        self.set_version_floor(next);
        Ok(())
    }

    async fn next_version_floor(&self, conn: &Connection) -> Result<u64> {
        let mut rows = conn
            .query("SELECT COALESCE(MAX(version), 0) FROM worker_kv", ())
            .await
            .map_err(kv_error)?;
        let max_version = if let Some(row) = rows.next().await.map_err(kv_error)? {
            row.get::<i64>(0).map_err(kv_error)?
        } else {
            0
        };
        Ok(max_version.saturating_add(1).max(1) as u64)
    }

    async fn connect(&self) -> Result<KvConnectionGuard> {
        if let Some(conn) = self
            .connections
            .lock()
            .expect("kv connection pool lock poisoned")
            .pop()
        {
            return Ok(KvConnectionGuard {
                connections: Arc::clone(&self.connections),
                conn: Some(conn),
            });
        }
        let conn = self.database.connect().map_err(kv_error)?;
        configure_connection(&conn).await?;
        Ok(KvConnectionGuard {
            connections: Arc::clone(&self.connections),
            conn: Some(conn),
        })
    }

    fn next_version(&self) -> i64 {
        self.version.fetch_add(1, Ordering::SeqCst) as i64
    }

    fn set_version_floor(&self, floor: u64) {
        let mut current = self.version.load(Ordering::SeqCst);
        while current < floor {
            match self
                .version
                .compare_exchange(current, floor, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

fn normalize_encoding(raw: &str) -> String {
    match raw {
        ENCODING_UTF8 => ENCODING_UTF8.to_string(),
        ENCODING_V8SC => ENCODING_V8SC.to_string(),
        _ => ENCODING_UTF8.to_string(),
    }
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}

fn kv_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("kv error: {error}"))
}

async fn configure_connection(conn: &Connection) -> Result<()> {
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(kv_error)?;
    Ok(())
}

async fn ensure_compat_columns(conn: &Connection) -> Result<()> {
    let mut rows = conn
        .query("PRAGMA table_info(worker_kv)", ())
        .await
        .map_err(kv_error)?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await.map_err(kv_error)? {
        let name: String = row.get::<String>(1).map_err(kv_error)?;
        columns.insert(name);
    }

    if !columns.contains("value_blob") {
        conn.execute("ALTER TABLE worker_kv ADD COLUMN value_blob BLOB", ())
            .await
            .map_err(kv_error)?;
    }
    if !columns.contains("encoding") {
        conn.execute(
            "ALTER TABLE worker_kv ADD COLUMN encoding TEXT NOT NULL DEFAULT 'utf8'",
            (),
        )
        .await
        .map_err(kv_error)?;
    }
    Ok(())
}

async fn execute_with_retry<F, Fut>(mut execute: F) -> Result<u64>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = turso::Result<u64>>,
{
    const MAX_ATTEMPTS: usize = 8;
    let mut attempt = 0usize;
    loop {
        match execute().await {
            Ok(affected) => return Ok(affected),
            Err(error) => {
                attempt += 1;
                let is_locked = is_retryable_kv_error(&error);
                if is_locked && attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                return Err(kv_error(error));
            }
        }
    }
}

fn is_retryable_kv_error(error: &impl std::fmt::Display) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database is busy")
        || message.contains("busy")
        || message.contains("conflict")
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent)
        .map_err(|error| PlatformError::runtime(format!("kv error: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use uuid::Uuid;

    async fn test_store(path: &Path) -> Result<KvStore> {
        ensure_parent_dir(path)?;
        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(kv_error)?;
        let database = Arc::new(database);
        let profile = Arc::new(KvProfile::default());
        let failed_versions = Arc::new(Mutex::new(HashSet::new()));
        let store = KvStore {
            database: Arc::clone(&database),
            connections: Arc::new(Mutex::new(Vec::new())),
            version: Arc::new(AtomicU64::new(1)),
            profile: Arc::clone(&profile),
            write_scheduler: KvWriteScheduler::new(database, profile, Arc::clone(&failed_versions)),
            failed_versions,
        };
        store.ensure_schema().await?;
        store.sync_version_counter_from_db().await?;
        Ok(store)
    }

    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-kv-{name}-{}.db", Uuid::new_v4()))
    }

    fn decode_utf8(value: KvValue) -> String {
        assert_eq!(value.encoding, ENCODING_UTF8);
        String::from_utf8(value.value).expect("utf8")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_put_writes_succeed() -> Result<()> {
        let path = temp_db_path("concurrent-set");
        let store = test_store(&path).await?;

        let mut tasks = Vec::new();
        for idx in 0..64usize {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .put("worker-a", "MY_KV", "hot-key", &format!("value-{idx}"))
                    .await
            }));
        }
        for task in tasks {
            let result = task.await.expect("task join should succeed");
            assert!(result.is_ok(), "kv put should succeed under contention");
        }

        let value = store.get("worker-a", "MY_KV", "hot-key").await?;
        assert!(
            value.is_some(),
            "hot-key should exist after concurrent writes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn version_counter_is_restored_from_disk() -> Result<()> {
        let path = temp_db_path("version-restore");
        let store = test_store(&path).await?;
        store.put("worker-a", "MY_KV", "k", "v1").await?;
        store.put("worker-a", "MY_KV", "k", "v2").await?;
        drop(store);

        let restored = test_store(&path).await?;
        restored.put("worker-a", "MY_KV", "k", "v3").await?;
        let value = restored.get("worker-a", "MY_KV", "k").await?;
        assert_eq!(value.map(decode_utf8), Some("v3".to_string()));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_from_two_store_instances_succeed() -> Result<()> {
        let path = temp_db_path("multi-store");
        let store_a = test_store(&path).await?;
        let store_b = test_store(&path).await?;

        let mut tasks = Vec::new();
        for idx in 0..32usize {
            let a = store_a.clone();
            tasks.push(tokio::spawn(async move {
                a.put("worker-a", "MY_KV", "same-key", &format!("a-{idx}"))
                    .await
            }));
            let b = store_b.clone();
            tasks.push(tokio::spawn(async move {
                b.put("worker-a", "MY_KV", "same-key", &format!("b-{idx}"))
                    .await
            }));
        }

        for task in tasks {
            let result = task.await.expect("task join should succeed");
            assert!(result.is_ok(), "multi-store kv write should succeed");
        }

        let value = store_a.get("worker-a", "MY_KV", "same-key").await?;
        assert!(
            value.is_some(),
            "key should be present after multi-store contention"
        );
        Ok(())
    }

    #[tokio::test]
    async fn put_value_roundtrips_structured_payload() -> Result<()> {
        let path = temp_db_path("typed-roundtrip");
        let store = test_store(&path).await?;
        let payload = vec![1u8, 7, 9, 11];
        store
            .put_value("worker-a", "MY_KV", "typed", &payload, ENCODING_V8SC)
            .await?;
        let value = store
            .get("worker-a", "MY_KV", "typed")
            .await?
            .expect("typed value should exist");
        assert_eq!(value.encoding, ENCODING_V8SC);
        assert_eq!(value.value, payload);
        Ok(())
    }

    #[tokio::test]
    async fn utf8_put_uses_text_fast_path_without_blob_duplication() -> Result<()> {
        let path = temp_db_path("utf8-fast-path");
        let store = test_store(&path).await?;
        store.put("worker-a", "MY_KV", "greeting", "hello").await?;

        let conn = store.connect().await?;
        let mut rows = conn
            .query(
                "SELECT value, value_blob, encoding FROM worker_kv WHERE worker_name = ?1 AND binding = ?2 AND key = ?3",
                ("worker-a", "MY_KV", "greeting"),
            )
            .await
            .map_err(kv_error)?;
        let row = rows.next().await.map_err(kv_error)?.expect("row");
        let value = row.get::<String>(0).map_err(kv_error)?;
        let value_blob = row.get::<Option<Vec<u8>>>(1).map_err(kv_error)?;
        let encoding = row.get::<String>(2).map_err(kv_error)?;
        assert_eq!(value, "hello");
        assert!(
            value_blob.is_none(),
            "utf8 fast path should not duplicate blob storage"
        );
        assert_eq!(encoding, ENCODING_UTF8);
        Ok(())
    }

    #[tokio::test]
    async fn get_utf8_reports_wrong_encoding_for_serialized_values() -> Result<()> {
        let path = temp_db_path("utf8-lookup-wrong-encoding");
        let store = test_store(&path).await?;
        store
            .put_value("worker-a", "MY_KV", "obj", &[1, 2, 3], ENCODING_V8SC)
            .await?;

        let lookup = store.get_utf8("worker-a", "MY_KV", "obj").await?;
        assert_eq!(lookup, Err(KvUtf8Lookup::WrongEncoding));
        Ok(())
    }

    #[tokio::test]
    async fn get_utf8_many_preserves_order_and_encoding_state() -> Result<()> {
        let path = temp_db_path("utf8-many");
        let store = test_store(&path).await?;
        store.put("worker-a", "MY_KV", "a", "one").await?;
        store
            .put_value("worker-a", "MY_KV", "b", &[1, 2, 3], ENCODING_V8SC)
            .await?;

        let values = store
            .get_utf8_many(
                "worker-a",
                "MY_KV",
                &[
                    "missing".to_string(),
                    "a".to_string(),
                    "b".to_string(),
                    "a".to_string(),
                ],
            )
            .await?;
        assert_eq!(
            values,
            vec![
                Err(KvUtf8Lookup::Missing),
                Ok("one".to_string()),
                Err(KvUtf8Lookup::WrongEncoding),
                Ok("one".to_string()),
            ]
        );
        Ok(())
    }
}
