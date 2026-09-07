use crate::state::{STATE_SHARDS, StateStore, storage_error};
use crate::turso_util::{execute_cached, query_cached};
use common::Result;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct KvStore {
    state: Arc<StateStore>,
    profile: Arc<KvProfile>,
    read_cache: Arc<Mutex<CommittedKvCache>>,
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
    OpGet,
    OpGetManyUtf8,
    OpGetValue,
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
    pub op_get: KvProfileMetricSnapshot,
    pub op_get_many_utf8: KvProfileMetricSnapshot,
    pub op_get_value: KvProfileMetricSnapshot,
}

#[derive(Default)]
pub struct KvProfile {
    enabled: AtomicBool,
    js_request_total: KvProfileMetric,
    op_get: KvProfileMetric,
    op_get_many_utf8: KvProfileMetric,
    op_get_value: KvProfileMetric,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct KvWriteKey {
    worker_name: String,
    binding: String,
    key: String,
}

struct CachedKvValue {
    value: Option<KvValue>,
    ordinal: u64,
    shard_epoch: u64,
    bytes: usize,
}

struct CommittedKvCache {
    entries: HashMap<KvWriteKey, CachedKvValue>,
    order: BTreeMap<u64, KvWriteKey>,
    epoch: u64,
    next_ordinal: u64,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl Default for CommittedKvCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: BTreeMap::new(),
            epoch: 0,
            next_ordinal: 0,
            bytes: 0,
            max_entries: 16_384,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

impl CommittedKvCache {
    fn remove(&mut self, key: &KvWriteKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.order.remove(&entry.ordinal);
            self.bytes -= entry.bytes;
        }
    }

    fn insert(&mut self, key: KvWriteKey, value: Option<KvValue>, shard_epoch: u64) {
        self.remove(&key);
        let bytes = key.worker_name.len()
            + key.binding.len()
            + key.key.len()
            + 128
            + value
                .as_ref()
                .map_or(0, |value| value.value.len() + value.encoding.len());
        if bytes > self.max_bytes || self.max_entries == 0 {
            return;
        }
        while self.entries.len() >= self.max_entries || self.bytes + bytes > self.max_bytes {
            let oldest = self
                .order
                .first_key_value()
                .expect("nonempty cache exceeds limit")
                .1
                .clone();
            self.remove(&oldest);
        }
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.order.insert(ordinal, key.clone());
        self.entries.insert(
            key,
            CachedKvValue {
                value,
                ordinal,
                shard_epoch,
                bytes,
            },
        );
        self.bytes += bytes;
    }
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
            op_get: self.op_get.snapshot(),
            op_get_many_utf8: self.op_get_many_utf8.snapshot(),
            op_get_value: self.op_get_value.snapshot(),
        }
    }

    pub fn take_snapshot_and_reset(&self) -> KvProfileSnapshot {
        let snapshot = self.snapshot();
        self.reset();
        snapshot
    }

    pub fn reset(&self) {
        self.js_request_total.reset();
        self.op_get.reset();
        self.op_get_many_utf8.reset();
        self.op_get_value.reset();
    }

    fn metric(&self, metric: KvProfileMetricKind) -> &KvProfileMetric {
        match metric {
            KvProfileMetricKind::JsRequestTotal => &self.js_request_total,
            KvProfileMetricKind::OpGet => &self.op_get,
            KvProfileMetricKind::OpGetManyUtf8 => &self.op_get_many_utf8,
            KvProfileMetricKind::OpGetValue => &self.op_get_value,
        }
    }
}

impl KvStore {
    pub fn from_state(state: Arc<StateStore>) -> Self {
        Self {
            state,
            profile: Arc::new(KvProfile::default()),
            read_cache: Arc::new(Mutex::new(CommittedKvCache::default())),
        }
    }

    pub fn set_read_cache_limits(&self, max_entries: usize, max_bytes: usize) {
        let mut cache = self.read_cache.lock().expect("kv cache lock poisoned");
        cache.entries.clear();
        cache.order.clear();
        cache.bytes = 0;
        cache.epoch += 1;
        cache.max_entries = max_entries;
        cache.max_bytes = max_bytes;
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
    pub async fn checkpoint(&self) -> Result<()> {
        self.state.checkpoint().await
    }
    pub async fn health_check(&self) -> Result<()> {
        self.state.health_check().await
    }

    pub async fn get(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
    ) -> Result<Option<KvValue>> {
        let shard = StateStore::shard_index(worker_name, binding, key);
        let epoch = self.state.epoch(shard);
        let cache_key = KvWriteKey {
            worker_name: worker_name.into(),
            binding: binding.into(),
            key: key.into(),
        };
        {
            let mut cache = self.read_cache.lock().expect("kv cache lock poisoned");
            if let Some(entry) = cache.entries.get(&cache_key)
                && entry.shard_epoch == epoch
            {
                let value = entry.value.clone();
                cache.insert(cache_key, value.clone(), epoch);
                return Ok(value);
            }
        }
        let conn = self.state.read(shard).await?;
        let mut rows = query_cached(&conn, "SELECT value, encoding FROM worker_kv WHERE worker = ?1 AND binding = ?2 AND key = ?3 AND deleted = 0", (worker_name, binding, key)).await.map_err(storage_error)?;
        let value = rows
            .next()
            .await
            .map_err(storage_error)?
            .map(|row| {
                Ok::<_, turso::Error>(KvValue {
                    value: row.get(0)?,
                    encoding: row.get(1)?,
                })
            })
            .transpose()
            .map_err(storage_error)?;
        if self.state.epoch(shard) == epoch {
            self.read_cache
                .lock()
                .expect("kv cache lock poisoned")
                .insert(cache_key, value.clone(), epoch);
        }
        Ok(value)
    }
    pub async fn get_utf8(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
    ) -> Result<std::result::Result<String, KvUtf8Lookup>> {
        match self.get(worker_name, binding, key).await? {
            None => Ok(Err(KvUtf8Lookup::Missing)),
            Some(value) if value.encoding != "utf8" => Ok(Err(KvUtf8Lookup::WrongEncoding)),
            Some(value) => Ok(Ok(String::from_utf8(value.value).map_err(storage_error)?)),
        }
    }
    pub async fn get_utf8_many(
        &self,
        worker_name: &str,
        binding: &str,
        keys: &[String],
    ) -> Result<Vec<std::result::Result<String, KvUtf8Lookup>>> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_utf8(worker_name, binding, key).await?);
        }
        Ok(values)
    }
    pub async fn put(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
        value: &str,
    ) -> Result<i64> {
        self.put_value(worker_name, binding, key, value.as_bytes(), "utf8")
            .await
    }
    pub async fn put_value(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
        value: &[u8],
        encoding: &str,
    ) -> Result<i64> {
        crate::memory::validate_value(value, encoding)?;
        self.commit(
            worker_name,
            binding,
            KvBatchMutation {
                key: key.into(),
                value: value.into(),
                encoding: encoding.into(),
                deleted: false,
            },
        )
        .await
    }
    pub async fn delete(&self, worker_name: &str, binding: &str, key: &str) -> Result<i64> {
        self.commit(
            worker_name,
            binding,
            KvBatchMutation {
                key: key.into(),
                value: Vec::new(),
                encoding: "utf8".into(),
                deleted: true,
            },
        )
        .await
    }
    async fn commit(
        &self,
        worker_name: &str,
        binding: &str,
        mutation: KvBatchMutation,
    ) -> Result<i64> {
        let shard = StateStore::shard_index(worker_name, binding, &mutation.key);
        let bytes = worker_name.len()
            + binding.len()
            + mutation.key.len()
            + mutation.value.len()
            + mutation.encoding.len()
            + 128;
        let worker = worker_name.to_owned();
        let binding = binding.to_owned();
        self.state
            .write(
                shard,
                bytes,
                crate::state::WriteOptions::default(),
                move |conn, version| {
                    let worker = worker.clone();
                    let binding = binding.clone();
                    let mutation = mutation.clone();
                    Box::pin(async move {
                        execute_cached(
                        conn,
                        "INSERT INTO worker_kv(worker,binding,key,value,encoding,deleted,version)
                     VALUES (?1,?2,?3,?4,?5,?6,?7)
                     ON CONFLICT(worker,binding,key) DO UPDATE SET
                     value=excluded.value,encoding=excluded.encoding,
                     deleted=excluded.deleted,version=excluded.version",
                        (
                            worker,
                            binding,
                            mutation.key,
                            mutation.value,
                            mutation.encoding,
                            i64::from(mutation.deleted),
                            version,
                        ),
                    )
                    .await?;
                        Ok(version.into())
                    })
                },
            )
            .await
    }
    pub async fn list(
        &self,
        worker_name: &str,
        binding: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<KvEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        let pattern = format!(
            "{}%",
            prefix
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
        );
        for shard in 0..STATE_SHARDS {
            let conn = self.state.read(shard).await?;
            let mut rows = query_cached(&conn, "SELECT key, value, encoding FROM worker_kv WHERE worker = ?1 AND binding = ?2 AND deleted = 0 AND key LIKE ?3 ESCAPE '\\' ORDER BY key LIMIT ?4", (worker_name, binding, pattern.as_str(), i64::try_from(limit).unwrap_or(i64::MAX))).await.map_err(storage_error)?;
            while let Some(row) = rows.next().await.map_err(storage_error)? {
                entries.push(KvEntry {
                    key: row.get(0).map_err(storage_error)?,
                    value: row.get(1).map_err(storage_error)?,
                    encoding: row.get(2).map_err(storage_error)?,
                });
            }
        }
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        entries.truncate(limit);
        Ok(entries)
    }
}
