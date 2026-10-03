use crate::state::{STATE_SHARDS, StateStore, storage_error};
use crate::turso_util::{execute_cached, query_cached};
use bytes::Bytes;
use common::Result;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    pub value: Bytes,
    pub encoding: String,
}

#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: Bytes,
    pub encoding: String,
}

#[derive(Debug, Clone)]
pub struct KvBatchMutation {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub deleted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvProfileMetricKind {
    JsRequestTotal,
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
    pub op_get_value: KvProfileMetricSnapshot,
}

#[derive(Default)]
pub struct KvProfile {
    enabled: AtomicBool,
    js_request_total: KvProfileMetric,
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
    fn lookup(&mut self, key: &KvWriteKey, shard_epoch: u64) -> Option<Option<KvValue>> {
        let entry = self.entries.get_mut(key)?;
        if entry.shard_epoch != shard_epoch {
            return None;
        }
        self.order.remove(&entry.ordinal);
        entry.ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.order.insert(entry.ordinal, key.clone());
        Some(entry.value.clone())
    }

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
        self.op_get_value.reset();
    }

    fn metric(&self, metric: KvProfileMetricKind) -> &KvProfileMetric {
        match metric {
            KvProfileMetricKind::JsRequestTotal => &self.js_request_total,
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
            if let Some(value) = cache.lookup(&cache_key, epoch) {
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
                    value: row.get::<Vec<u8>>(0)?.into(),
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
        let mut keys = BTreeSet::new();
        let upper = prefix_upper_bound(prefix);
        let sql_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        for shard in 0..STATE_SHARDS {
            let conn = self.state.read(shard).await?;
            let mut rows = if let Some(upper) = &upper {
                query_cached(&conn, "SELECT key FROM worker_kv WHERE worker = ?1 AND binding = ?2 AND deleted = 0 AND key >= ?3 AND key < ?4 ORDER BY key LIMIT ?5", (worker_name, binding, prefix, upper.as_str(), sql_limit)).await
            } else {
                query_cached(&conn, "SELECT key FROM worker_kv WHERE worker = ?1 AND binding = ?2 AND deleted = 0 AND key >= ?3 ORDER BY key LIMIT ?4", (worker_name, binding, prefix, sql_limit)).await
            }.map_err(storage_error)?;
            while let Some(row) = rows.next().await.map_err(storage_error)? {
                keys.insert(row.get::<String>(0).map_err(storage_error)?);
                if keys.len() > limit {
                    keys.pop_last();
                }
            }
        }
        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(worker_name, binding, &key).await? {
                entries.push(KvEntry {
                    key,
                    value: value.value,
                    encoding: value.encoding,
                });
            }
        }
        Ok(entries)
    }
}

fn prefix_upper_bound(prefix: &str) -> Option<String> {
    for (index, character) in prefix.char_indices().rev() {
        let next = match character {
            '\u{10ffff}' => continue,
            '\u{d7ff}' => '\u{e000}',
            _ => char::from_u32(character as u32 + 1).expect("next Unicode scalar"),
        };
        let mut upper = prefix[..index].to_owned();
        upper.push(next);
        return Some(upper);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_hits_share_bytes_refresh_recency_and_observe_writes() {
        let root = std::env::temp_dir().join(format!("dd-kv-cache-{}", uuid::Uuid::new_v4()));
        let state = StateStore::open(&root).await.unwrap();
        let store = KvStore::from_state(Arc::clone(&state));
        store.set_read_cache_limits(2, 1024 * 1024);
        for key in ["a", "b", "c"] {
            store
                .put_value("worker", "KV", key, &vec![b'x'; 128 * 1024], "utf8")
                .await
                .unwrap();
        }
        let first = store.get("worker", "KV", "a").await.unwrap().unwrap();
        let second = store.get("worker", "KV", "b").await.unwrap().unwrap();
        let hit = store.get("worker", "KV", "a").await.unwrap().unwrap();
        assert_eq!(
            first.value.as_ptr(),
            hit.value.as_ptr(),
            "a hit must share the cached payload"
        );
        store.get("worker", "KV", "c").await.unwrap();
        let retained = store.get("worker", "KV", "a").await.unwrap().unwrap();
        assert_eq!(
            first.value.as_ptr(),
            retained.value.as_ptr(),
            "recently accessed values must survive eviction"
        );
        let reloaded = store.get("worker", "KV", "b").await.unwrap().unwrap();
        assert_ne!(
            second.value.as_ptr(),
            reloaded.value.as_ptr(),
            "the least recently used entry must be reloaded"
        );
        store.put("worker", "KV", "a", "updated").await.unwrap();
        assert_eq!(
            store.get("worker", "KV", "a").await.unwrap().unwrap().value,
            b"updated".as_slice()
        );
        assert_eq!(first.value.len(), 128 * 1024);
        assert!(first.value.iter().all(|byte| *byte == b'x'));
        store.delete("worker", "KV", "a").await.unwrap();
        assert!(store.get("worker", "KV", "a").await.unwrap().is_none());
        assert!(store.get("worker", "KV", "a").await.unwrap().is_none());
        drop(store);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn listing_projects_values_only_after_the_global_key_limit() {
        let root = std::env::temp_dir().join(format!("dd-kv-projection-{}", uuid::Uuid::new_v4()));
        let state = StateStore::open(&root).await.unwrap();
        let store = KvStore::from_state(Arc::clone(&state));
        store
            .put("worker", "KV", "a-winner", "selected value")
            .await
            .unwrap();
        let winner_shard = StateStore::shard_index("worker", "KV", "a-winner");
        let mut seeded = std::collections::HashSet::new();
        for index in 0..10_000 {
            let key = format!("z-discard-{index:05}");
            let shard = StateStore::shard_index("worker", "KV", &key);
            if shard == winner_shard || !seeded.insert(shard) {
                continue;
            }
            let conn = state.read(shard).await.unwrap();
            // A discarded row deliberately has a value that cannot be decoded
            // as BLOB, proving key selection never projects that value.
            conn.execute("INSERT INTO worker_kv(worker,binding,key,value,encoding,deleted,version) VALUES ('worker','KV',?1,123,'utf8',0,0)",(key,)).await.unwrap();
            if seeded.len() == STATE_SHARDS - 1 {
                break;
            }
        }
        assert_eq!(seeded.len(), STATE_SHARDS - 1);
        let entries = store.list("worker", "KV", "", 1).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "a-winner");
        assert_eq!(entries[0].value, b"selected value".as_slice());
        assert!(
            store.list("worker", "KV", "z-discard", 1).await.is_err(),
            "fault injection remains observable when its row wins"
        );
        drop(store);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }
}
