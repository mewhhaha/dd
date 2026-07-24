use crate::blob::BlobStore;
use crate::turso_util::{
    checkpoint_database, configure_turso_connection, ensure_storage_migration_table,
    execute_cached, health_check_database, is_retryable_turso_error, query_cached,
    record_storage_retry, record_storage_schema_version, retry_turso_busy, storage_schema_version,
};
use bytes::Bytes;
use common::{PlatformError, Result};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;
#[cfg(test)]
use tokio::sync::{Notify, oneshot};
#[cfg(test)]
use turso::Builder;
use turso::{Connection, Database, transaction::TransactionBehavior};
use uuid::Uuid;

const DEFAULT_TTL: Duration = Duration::from_secs(60);
const MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const HOT_CACHE_MAX_ENTRIES: usize = 1_024;
const HOT_CACHE_MAX_BYTES: usize = 16 * 1024 * 1024;
const TOUCH_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const TOUCH_FLUSH_BATCH_SIZE: usize = 256;
const CACHE_SCHEMA_VERSION: i64 = 1;
const CREATE_CACHE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS worker_cache_entries (
  id TEXT PRIMARY KEY,
  cache_name TEXT NOT NULL,
  method TEXT NOT NULL,
  url TEXT NOT NULL,
  vary_headers_json TEXT NOT NULL,
  vary_values_json TEXT NOT NULL,
  status INTEGER NOT NULL,
  headers_json TEXT NOT NULL,
  body_blob BLOB NOT NULL,
  body_size INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  last_access_seq INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
)";
const CREATE_CACHE_MIGRATION_TABLE_SQL: &str = "CREATE TABLE worker_cache_entries_migrating (
  id TEXT PRIMARY KEY,
  cache_name TEXT NOT NULL,
  method TEXT NOT NULL,
  url TEXT NOT NULL,
  vary_headers_json TEXT NOT NULL,
  vary_values_json TEXT NOT NULL,
  status INTEGER NOT NULL,
  headers_json TEXT NOT NULL,
  body_blob BLOB NOT NULL,
  body_size INTEGER NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  last_access_seq INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
)";

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_entries: usize,
    pub max_bytes: usize,
    pub default_ttl: Duration,
    /// Legacy tuning knob retained for configuration compatibility. Cache bodies
    /// are always stored directly as Turso BLOBs.
    #[allow(dead_code)]
    pub inline_body_limit_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 2048,
            max_bytes: 64 * 1024 * 1024,
            default_ttl: DEFAULT_TTL,
            inline_body_limit_bytes: 64 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CacheRequest {
    pub cache_name: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub bypass_stale: bool,
}

#[derive(Clone, Debug)]
pub struct CacheResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

#[derive(Clone, Debug)]
pub enum CacheLookup {
    Fresh(CacheResponse),
    StaleWhileRevalidate(CacheResponse),
    StaleIfError(CacheResponse),
    Miss,
}

#[derive(Clone)]
pub struct CacheStore {
    database: Arc<Database>,
    config: CacheConfig,
    access_seq: Arc<AtomicU64>,
    mutation_epoch: Arc<AtomicU64>,
    mutation_lock: Arc<AsyncMutex<()>>,
    hot_cache: Arc<HotCache>,
    #[cfg(test)]
    test_hooks: Arc<CacheTestHooks>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheBaseKey {
    cache_name: String,
    method: String,
    url: String,
}

impl CacheBaseKey {
    fn new(cache_name: String, method: String, url: String) -> Self {
        Self {
            cache_name,
            method,
            url,
        }
    }
}

#[derive(Clone)]
struct HotCacheEntry {
    id: String,
    base_key: CacheBaseKey,
    vary_headers: Vec<String>,
    vary_values: Vec<String>,
    response: CacheResponse,
    expires_at_ms: i64,
    swr_until_ms: i64,
    sie_until_ms: i64,
    size_bytes: usize,
}

impl HotCacheEntry {
    fn new(
        id: String,
        base_key: CacheBaseKey,
        vary_headers: Vec<String>,
        vary_values: Vec<String>,
        response: CacheResponse,
        expires_at_ms: i64,
    ) -> Self {
        let response_headers = to_header_map(&response.headers);
        let response_control =
            parse_cache_control(header_value(&response_headers, "cache-control"));
        let swr_until_ms = stale_deadline_ms(
            expires_at_ms,
            response_control.stale_while_revalidate.unwrap_or(0),
        );
        let sie_until_ms =
            stale_deadline_ms(expires_at_ms, response_control.stale_if_error.unwrap_or(0));
        let size_bytes = hot_entry_size(&id, &base_key, &vary_headers, &vary_values, &response);
        Self {
            id,
            base_key,
            vary_headers,
            vary_values,
            response,
            expires_at_ms,
            swr_until_ms,
            sie_until_ms,
            size_bytes,
        }
    }
}

struct HotCache {
    database: Arc<Database>,
    flush_runtime: tokio::runtime::Handle,
    max_entries: usize,
    max_bytes: usize,
    state: Mutex<HotCacheState>,
    flush_lock: AsyncMutex<()>,
    flush_failures: AtomicU64,
}

struct ScheduledFlushGuard {
    hot_cache: Arc<HotCache>,
    armed: bool,
}

struct PendingTouchesGuard<'a> {
    hot_cache: &'a HotCache,
    touches: Vec<(String, i64)>,
    persisted: usize,
}

impl Drop for PendingTouchesGuard<'_> {
    fn drop(&mut self) {
        self.hot_cache
            .requeue_touches(&self.touches[self.persisted..]);
    }
}

impl ScheduledFlushGuard {
    fn new(hot_cache: Arc<HotCache>) -> Self {
        Self {
            hot_cache,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ScheduledFlushGuard {
    fn drop(&mut self) {
        if self.armed {
            self.hot_cache.lock_state().flush_scheduled = false;
        }
    }
}

#[derive(Default)]
struct HotCacheState {
    entries: HashMap<String, Arc<HotCacheEntry>>,
    base_index: HashMap<CacheBaseKey, Vec<String>>,
    lru: VecDeque<(String, u64)>,
    lru_recency: HashMap<String, u64>,
    lru_clock: u64,
    total_bytes: usize,
    pending_touches: HashMap<String, i64>,
    flush_scheduled: bool,
}

#[cfg(test)]
struct CacheTestPause {
    reached: oneshot::Sender<()>,
    resume: Arc<Notify>,
}

#[cfg(test)]
#[derive(Default)]
struct CacheTestHooks {
    after_disk_read: Mutex<Option<CacheTestPause>>,
    before_hot_insert: Mutex<Option<CacheTestPause>>,
}

#[cfg(test)]
impl CacheTestHooks {
    fn install(slot: &Mutex<Option<CacheTestPause>>) -> (oneshot::Receiver<()>, Arc<Notify>) {
        let (reached, wait_reached) = oneshot::channel();
        let resume = Arc::new(Notify::new());
        *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CacheTestPause {
            reached,
            resume: Arc::clone(&resume),
        });
        (wait_reached, resume)
    }

    fn install_after_disk_read(&self) -> (oneshot::Receiver<()>, Arc<Notify>) {
        Self::install(&self.after_disk_read)
    }

    fn install_before_hot_insert(&self) -> (oneshot::Receiver<()>, Arc<Notify>) {
        Self::install(&self.before_hot_insert)
    }

    async fn pause(slot: &Mutex<Option<CacheTestPause>>) {
        let pause = slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            pause.resume.notified().await;
        }
    }
}

struct CacheRecord {
    id: String,
    vary_headers_json: String,
    vary_values_json: String,
    status: i64,
    headers_json: String,
    body: Vec<u8>,
    expires_at_ms: i64,
}

struct CacheDeleteCandidate {
    id: String,
}

struct CacheCleanupCandidate {
    id: String,
    headers_json: String,
    expires_at_ms: i64,
}

struct LegacyCacheRecord {
    id: String,
    cache_name: String,
    method: String,
    url: String,
    vary_headers_json: String,
    vary_values_json: String,
    status: i64,
    headers_json: String,
    body_storage: String,
    body_inline_hex: String,
    body_ref: String,
    body_size: i64,
    expires_at_ms: i64,
    last_access_seq: i64,
    updated_at_ms: i64,
}

#[derive(Default)]
struct CacheControl {
    no_store: bool,
    no_cache: bool,
    private: bool,
    max_age: Option<u64>,
    s_maxage: Option<u64>,
    stale_while_revalidate: Option<u64>,
    stale_if_error: Option<u64>,
}

impl HotCache {
    fn new(database: Arc<Database>, config: &CacheConfig) -> Self {
        Self {
            database,
            flush_runtime: tokio::runtime::Handle::current(),
            max_entries: config.max_entries.min(HOT_CACHE_MAX_ENTRIES),
            max_bytes: config.max_bytes.min(HOT_CACHE_MAX_BYTES),
            state: Mutex::new(HotCacheState::default()),
            flush_lock: AsyncMutex::new(()),
            flush_failures: AtomicU64::new(0),
        }
    }

    fn lookup(
        &self,
        base_key: &CacheBaseKey,
        request_headers: &HashMap<String, String>,
        now_ms: i64,
        bypass_stale: bool,
    ) -> Option<(String, CacheLookup)> {
        let mut candidates = {
            let state = self.lock_state();
            state
                .base_index
                .get(base_key)?
                .iter()
                .filter_map(|id| {
                    Some((
                        *state.lru_recency.get(id)?,
                        Arc::clone(state.entries.get(id)?),
                    ))
                })
                .collect::<Vec<_>>()
        };
        candidates.sort_unstable_by(|left, right| right.0.cmp(&left.0));

        for (_, entry) in candidates {
            if !vary_values_match(&entry.vary_headers, &entry.vary_values, request_headers) {
                continue;
            }

            if now_ms <= entry.expires_at_ms {
                self.touch_if_current(&entry);
                return Some((entry.id.clone(), CacheLookup::Fresh(entry.response.clone())));
            }

            if now_ms > entry.swr_until_ms.max(entry.sie_until_ms) {
                self.remove_if_current(&entry);
                continue;
            }
            if bypass_stale {
                continue;
            }

            if now_ms <= entry.swr_until_ms && entry.swr_until_ms > entry.expires_at_ms {
                self.touch_if_current(&entry);
                return Some((
                    entry.id.clone(),
                    CacheLookup::StaleWhileRevalidate(entry.response.clone()),
                ));
            }
            if now_ms <= entry.sie_until_ms && entry.sie_until_ms > entry.expires_at_ms {
                self.touch_if_current(&entry);
                return Some((
                    entry.id.clone(),
                    CacheLookup::StaleIfError(entry.response.clone()),
                ));
            }
        }

        None
    }

    fn insert(&self, entry: HotCacheEntry) {
        let mut state = self.lock_state();
        state.insert(entry, self.max_entries, self.max_bytes);
    }

    fn touch_if_current(&self, entry: &Arc<HotCacheEntry>) {
        let mut state = self.lock_state();
        if state
            .entries
            .get(&entry.id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            state.touch_lru(&entry.id);
        }
    }

    fn remove_if_current(&self, entry: &Arc<HotCacheEntry>) {
        let mut state = self.lock_state();
        if state
            .entries
            .get(&entry.id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            state.remove_entry(&entry.id);
        }
    }

    fn invalidate_ids<'a>(&self, ids: impl IntoIterator<Item = &'a str>) {
        let mut state = self.lock_state();
        for id in ids {
            state.remove_entry(id);
            state.pending_touches.remove(id);
        }
    }

    fn invalidate_base(&self, base_key: &CacheBaseKey) {
        let mut state = self.lock_state();
        let ids = state.base_index.get(base_key).cloned().unwrap_or_default();
        for id in ids {
            state.remove_entry(&id);
            state.pending_touches.remove(&id);
        }
    }

    fn queue_touch(self: &Arc<Self>, id: String, access_seq: i64) {
        let should_spawn = {
            let mut state = self.lock_state();
            state
                .pending_touches
                .entry(id)
                .and_modify(|pending| *pending = (*pending).max(access_seq))
                .or_insert(access_seq);
            if state.flush_scheduled {
                false
            } else {
                state.flush_scheduled = true;
                true
            }
        };

        if should_spawn {
            let hot_cache = Arc::clone(self);
            let schedule_guard = ScheduledFlushGuard::new(Arc::clone(self));
            self.flush_runtime.spawn(async move {
                hot_cache.run_scheduled_flushes(schedule_guard).await;
            });
        }
    }

    async fn run_scheduled_flushes(self: Arc<Self>, mut schedule_guard: ScheduledFlushGuard) {
        const MAX_BACKGROUND_FAILURES: usize = 8;
        let mut consecutive_failures = 0usize;
        loop {
            tokio::time::sleep(TOUCH_FLUSH_INTERVAL).await;
            match self.flush_pending_touches().await {
                Ok(()) => consecutive_failures = 0,
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    self.flush_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %error, "failed to flush cache recency touches");
                }
            }

            let mut state = self.lock_state();
            if state.pending_touches.is_empty() || consecutive_failures >= MAX_BACKGROUND_FAILURES {
                state.flush_scheduled = false;
                schedule_guard.disarm();
                break;
            }
        }
    }

    async fn flush_pending_touches(&self) -> Result<()> {
        let _flush_guard = self.flush_lock.lock().await;
        let touches = {
            let mut state = self.lock_state();
            state.pending_touches.drain().collect::<Vec<_>>()
        };
        if touches.is_empty() {
            return Ok(());
        }

        let mut pending = PendingTouchesGuard {
            hot_cache: self,
            touches,
            persisted: 0,
        };
        while pending.persisted < pending.touches.len() {
            let end = (pending.persisted + TOUCH_FLUSH_BATCH_SIZE).min(pending.touches.len());
            self.persist_touch_batch(&pending.touches[pending.persisted..end])
                .await?;
            pending.persisted = end;
        }
        Ok(())
    }

    async fn persist_touch_batch(&self, touches: &[(String, i64)]) -> Result<()> {
        const MAX_ATTEMPTS: usize = 8;
        let mut conn = self.database.connect().map_err(cache_error)?;
        configure_cache_connection(&conn).await?;

        for attempt in 0..MAX_ATTEMPTS {
            let tx = match conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
            {
                Ok(tx) => tx,
                Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    sleep_cache_retry(attempt).await;
                    continue;
                }
                Err(error) => return Err(cache_error_after_retry(error)),
            };

            let mut execute_error = None;
            for (id, access_seq) in touches {
                if let Err(error) = execute_cached(
                    &tx,
                    "UPDATE worker_cache_entries
                         SET last_access_seq = MAX(last_access_seq, ?1)
                         WHERE id = ?2",
                    (*access_seq, id.as_str()),
                )
                .await
                {
                    execute_error = Some(error);
                    break;
                }
            }
            if let Some(error) = execute_error {
                let retry = is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS;
                let _ = tx.rollback().await;
                if retry {
                    sleep_cache_retry(attempt).await;
                    continue;
                }
                return Err(cache_error_after_retry(error));
            }

            match tx.commit().await {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    sleep_cache_retry(attempt).await;
                }
                Err(error) => return Err(cache_error_after_retry(error)),
            }
        }

        Err(PlatformError::storage_unavailable(
            "cache error: recency flush failed after retries",
        ))
    }

    fn requeue_touches(&self, touches: &[(String, i64)]) {
        let mut state = self.lock_state();
        for (id, access_seq) in touches {
            state
                .pending_touches
                .entry(id.clone())
                .and_modify(|pending| *pending = (*pending).max(*access_seq))
                .or_insert(*access_seq);
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, HotCacheState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl HotCacheState {
    fn insert(&mut self, entry: HotCacheEntry, max_entries: usize, max_bytes: usize) {
        self.remove_entry(&entry.id);
        if max_entries == 0 || max_bytes == 0 || entry.size_bytes > max_bytes {
            return;
        }

        while self.entries.len() >= max_entries
            || self.total_bytes.saturating_add(entry.size_bytes) > max_bytes
        {
            let Some(victim_id) = self.oldest_live_entry() else {
                break;
            };
            self.remove_entry(&victim_id);
        }

        let entry = Arc::new(entry);
        self.total_bytes = self.total_bytes.saturating_add(entry.size_bytes);
        self.base_index
            .entry(entry.base_key.clone())
            .or_default()
            .push(entry.id.clone());
        self.entries.insert(entry.id.clone(), Arc::clone(&entry));
        self.mark_recent(&entry.id);
    }

    fn remove_entry(&mut self, id: &str) {
        let Some(entry) = self.entries.remove(id) else {
            return;
        };
        self.total_bytes = self.total_bytes.saturating_sub(entry.size_bytes);
        self.lru_recency.remove(id);
        let remove_base = if let Some(ids) = self.base_index.get_mut(&entry.base_key) {
            ids.retain(|candidate| candidate != id);
            ids.is_empty()
        } else {
            false
        };
        if remove_base {
            self.base_index.remove(&entry.base_key);
        }
    }

    fn touch_lru(&mut self, id: &str) {
        if !self.entries.contains_key(id) {
            return;
        }
        self.mark_recent(id);
    }

    fn mark_recent(&mut self, id: &str) {
        if self.lru_clock == u64::MAX {
            self.renumber_lru();
        }
        self.lru_clock += 1;
        let sequence = self.lru_clock;
        self.lru_recency.insert(id.to_string(), sequence);
        self.lru.push_back((id.to_string(), sequence));
        if self.lru.len() > self.entries.len().max(64).saturating_mul(16) {
            self.rebuild_lru();
        }
    }

    fn oldest_live_entry(&mut self) -> Option<String> {
        while let Some((id, sequence)) = self.lru.pop_front() {
            if self.lru_recency.get(&id) == Some(&sequence) {
                return Some(id);
            }
        }
        None
    }

    fn rebuild_lru(&mut self) {
        let mut live = self
            .lru_recency
            .iter()
            .map(|(id, sequence)| (id.clone(), *sequence))
            .collect::<Vec<_>>();
        live.sort_unstable_by_key(|(_, sequence)| *sequence);
        self.lru = live.into();
    }

    fn renumber_lru(&mut self) {
        let mut ids = self
            .lru_recency
            .iter()
            .map(|(id, sequence)| (id.clone(), *sequence))
            .collect::<Vec<_>>();
        ids.sort_unstable_by_key(|(_, sequence)| *sequence);
        self.lru.clear();
        self.lru_recency.clear();
        for (index, (id, _)) in ids.into_iter().enumerate() {
            let sequence = index as u64 + 1;
            self.lru_recency.insert(id.clone(), sequence);
            self.lru.push_back((id, sequence));
        }
        self.lru_clock = self.lru_recency.len() as u64;
    }
}

impl CacheStore {
    pub async fn from_database(
        config: CacheConfig,
        database: Arc<Database>,
        legacy_blobs: BlobStore,
    ) -> Result<Self> {
        migrate_cache_schema(&database, &legacy_blobs).await?;
        let store = Self {
            database: Arc::clone(&database),
            hot_cache: Arc::new(HotCache::new(database, &config)),
            config,
            access_seq: Arc::new(AtomicU64::new(1)),
            mutation_epoch: Arc::new(AtomicU64::new(0)),
            mutation_lock: Arc::new(AsyncMutex::new(())),
            #[cfg(test)]
            test_hooks: Arc::new(CacheTestHooks::default()),
        };
        store.initialize_access_seq().await?;
        Ok(store)
    }

    #[cfg(test)]
    async fn from_local_path(
        config: CacheConfig,
        local_path: String,
        legacy_blobs: BlobStore,
    ) -> Result<Self> {
        let database = Builder::new_local(&local_path)
            .build()
            .await
            .map_err(cache_error)?;
        Self::from_database(config, Arc::new(database), legacy_blobs).await
    }

    pub async fn get(&self, request: &CacheRequest) -> Result<CacheLookup> {
        let Some(method) = normalize_cache_method(&request.method) else {
            return Ok(CacheLookup::Miss);
        };
        let request_headers = to_header_map(&request.headers);
        let request_control = parse_cache_control(header_value(&request_headers, "cache-control"));
        if request_control.no_store || request_control.no_cache {
            return Ok(CacheLookup::Miss);
        }

        let cache_name = normalize_cache_name(&request.cache_name);
        let now_ms = epoch_ms_i64()?;
        let base_key = CacheBaseKey::new(cache_name.clone(), method.clone(), request.url.clone());
        if let Some((id, lookup)) =
            self.hot_cache
                .lookup(&base_key, &request_headers, now_ms, request.bypass_stale)
        {
            self.hot_cache.queue_touch(id, self.next_access_seq());
            return Ok(lookup);
        }

        let mutation_epoch = self.mutation_epoch.load(Ordering::Acquire);
        let conn = self.connect().await?;
        let mut rows = query_cached(
            &conn,
                "SELECT id, vary_headers_json, vary_values_json, status, headers_json, body_blob, expires_at_ms
                 FROM worker_cache_entries
                 WHERE cache_name = ?1 AND method = ?2 AND url = ?3
                 ORDER BY last_access_seq DESC",
                (cache_name.as_str(), method.as_str(), request.url.as_str()),
            )
            .await
            .map_err(cache_error)?;

        let mut expired = Vec::new();
        let mut stale = Vec::new();
        let mut selected_id = String::new();
        let mut selected_hot_entry = None;
        let mut lookup = CacheLookup::Miss;

        while let Some(row) = rows.next().await.map_err(cache_error)? {
            let mut record = row_to_record(&row)?;
            let vary_headers: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_headers_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate { id: record.id });
                        continue;
                    }
                };
            let stored_vary_values: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_values_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate { id: record.id });
                        continue;
                    }
                };
            let request_vary_values = derive_vary_values(&vary_headers, &request_headers);
            if request_vary_values != stored_vary_values {
                continue;
            }

            let headers: Vec<(String, String)> =
                match crate::json::from_string(std::mem::take(&mut record.headers_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate { id: record.id });
                        continue;
                    }
                };

            let response = CacheResponse {
                status: record.status as u16,
                headers,
                body: Bytes::from(record.body),
            };
            let response_headers = to_header_map(&response.headers);
            let response_control =
                parse_cache_control(header_value(&response_headers, "cache-control"));
            let swr_until_ms = stale_deadline_ms(
                record.expires_at_ms,
                response_control.stale_while_revalidate.unwrap_or(0),
            );
            let sie_until_ms = stale_deadline_ms(
                record.expires_at_ms,
                response_control.stale_if_error.unwrap_or(0),
            );
            let max_stale_until_ms = swr_until_ms.max(sie_until_ms);

            if now_ms <= record.expires_at_ms {
                selected_id = record.id.clone();
                selected_hot_entry = Some(HotCacheEntry::new(
                    record.id,
                    base_key.clone(),
                    vary_headers,
                    stored_vary_values,
                    response.clone(),
                    record.expires_at_ms,
                ));
                lookup = CacheLookup::Fresh(response);
                break;
            }

            if now_ms > max_stale_until_ms {
                expired.push(CacheDeleteCandidate { id: record.id });
                continue;
            }

            if request.bypass_stale {
                continue;
            }

            if now_ms <= swr_until_ms && response_control.stale_while_revalidate.unwrap_or(0) > 0 {
                selected_id = record.id.clone();
                selected_hot_entry = Some(HotCacheEntry::new(
                    record.id,
                    base_key.clone(),
                    vary_headers,
                    stored_vary_values,
                    response.clone(),
                    record.expires_at_ms,
                ));
                lookup = CacheLookup::StaleWhileRevalidate(response);
            } else if now_ms <= sie_until_ms && response_control.stale_if_error.unwrap_or(0) > 0 {
                selected_id = record.id.clone();
                selected_hot_entry = Some(HotCacheEntry::new(
                    record.id,
                    base_key.clone(),
                    vary_headers,
                    stored_vary_values,
                    response.clone(),
                    record.expires_at_ms,
                ));
                lookup = CacheLookup::StaleIfError(response);
            } else {
                continue;
            }
            break;
        }
        drop(rows);
        #[cfg(test)]
        CacheTestHooks::pause(&self.test_hooks.after_disk_read).await;

        if !expired.is_empty() || !stale.is_empty() || selected_hot_entry.is_some() {
            let _mutation_guard = self.mutation_lock.lock().await;
            if self.mutation_epoch.load(Ordering::Acquire) == mutation_epoch {
                if !expired.is_empty() {
                    self.remove_records(&conn, &expired).await?;
                }
                if !stale.is_empty() {
                    self.remove_records(&conn, &stale).await?;
                }
                #[cfg(test)]
                if selected_hot_entry.is_some() {
                    CacheTestHooks::pause(&self.test_hooks.before_hot_insert).await;
                }
                if let Some(entry) = selected_hot_entry.take() {
                    self.hot_cache.insert(entry);
                }
            }
        }
        if !selected_id.is_empty() {
            self.hot_cache
                .queue_touch(selected_id, self.next_access_seq());
        }

        Ok(lookup)
    }

    pub async fn put(&self, request: &CacheRequest, response: CacheResponse) -> Result<bool> {
        let Some(method) = normalize_cache_method(&request.method) else {
            return Ok(false);
        };
        let request_headers = to_header_map(&request.headers);
        if request_headers.contains_key("authorization") || request_headers.contains_key("cookie") {
            return Ok(false);
        }
        let request_control = parse_cache_control(header_value(&request_headers, "cache-control"));
        if request_control.no_store {
            return Ok(false);
        }

        if !(200..300).contains(&response.status) {
            return Ok(false);
        }

        let response_headers = to_header_map(&response.headers);
        let response_control =
            parse_cache_control(header_value(&response_headers, "cache-control"));
        if response_control.no_store || response_control.no_cache || response_control.private {
            return Ok(false);
        }
        if response_headers.contains_key("set-cookie") {
            return Ok(false);
        }

        let Some(vary_headers) = parse_vary_headers(&response.headers) else {
            return Ok(false);
        };
        let vary_values = derive_vary_values(&vary_headers, &request_headers);

        let ttl = choose_ttl(response_control, self.config.default_ttl);
        if ttl.is_zero() {
            return Ok(false);
        }

        let cache_name = normalize_cache_name(&request.cache_name);
        let size_bytes =
            estimate_entry_size(&cache_name, &method, &request.url, &response, &vary_values);
        if size_bytes > self.config.max_bytes {
            return Ok(false);
        }
        let base_key = CacheBaseKey::new(cache_name.clone(), method.clone(), request.url.clone());

        let vary_headers_json = crate::json::to_string(&vary_headers)
            .map_err(|error| PlatformError::runtime(format!("cache error: {error}")))?;
        let vary_values_json = crate::json::to_string(&vary_values)
            .map_err(|error| PlatformError::runtime(format!("cache error: {error}")))?;
        let headers_json = crate::json::to_string(&response.headers)
            .map_err(|error| PlatformError::runtime(format!("cache error: {error}")))?;

        let mut conn = self.connect().await?;
        let now_ms = epoch_ms_i64()?;
        let expires_at_ms = now_ms + ttl.as_millis() as i64;
        let access_seq = self.next_access_seq();
        let entry_id = Uuid::new_v4().to_string();
        let _mutation_guard = self.mutation_lock.lock().await;

        const MAX_ATTEMPTS: usize = 8;
        let existing_variant = 'write: {
            for attempt in 0..MAX_ATTEMPTS {
                let tx = match conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .await
                {
                    Ok(tx) => tx,
                    Err(error)
                        if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS =>
                    {
                        sleep_cache_retry(attempt).await;
                        continue;
                    }
                    Err(error) => return Err(cache_error_after_retry(error)),
                };
                let existing_variant = match self
                    .load_variant_candidates(
                        &tx,
                        &cache_name,
                        &method,
                        &request.url,
                        &vary_headers_json,
                        &vary_values_json,
                    )
                    .await
                {
                    Ok(existing_variant) => existing_variant,
                    Err(error) => {
                        let _ = tx.rollback().await;
                        return Err(error);
                    }
                };
                let put_result = execute_cached(
                    &tx,
                    "INSERT INTO worker_cache_entries (
                           id, cache_name, method, url, vary_headers_json, vary_values_json,
                           status, headers_json, body_blob, body_size,
                           expires_at_ms, last_access_seq, updated_at_ms
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                         ON CONFLICT(cache_name, method, url, vary_headers_json, vary_values_json)
                         DO UPDATE SET
                           status = excluded.status,
                           headers_json = excluded.headers_json,
                           body_blob = excluded.body_blob,
                           body_size = excluded.body_size,
                           expires_at_ms = excluded.expires_at_ms,
                           last_access_seq = excluded.last_access_seq,
                           updated_at_ms = excluded.updated_at_ms",
                    (
                        entry_id.as_str(),
                        cache_name.as_str(),
                        method.as_str(),
                        request.url.as_str(),
                        vary_headers_json.as_str(),
                        vary_values_json.as_str(),
                        response.status as i64,
                        headers_json.as_str(),
                        response.body.as_ref(),
                        size_bytes as i64,
                        expires_at_ms,
                        access_seq,
                        now_ms,
                    ),
                )
                .await;
                if let Err(error) = put_result {
                    let _ = tx.rollback().await;
                    if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS {
                        sleep_cache_retry(attempt).await;
                        continue;
                    }
                    return Err(cache_error_after_retry(error));
                }
                if let Err(error) = tx.commit().await {
                    // A commit error can be ambiguous. Invalidate conservatively
                    // so a prior decoded response cannot outlive a disk commit.
                    self.hot_cache.invalidate_base(&base_key);
                    self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
                    if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS {
                        sleep_cache_retry(attempt).await;
                        continue;
                    }
                    return Err(cache_error_after_retry(error));
                }
                break 'write existing_variant;
            }
            return Err(PlatformError::storage_unavailable(
                "cache error: put failed after retries",
            ));
        };
        let committed_id = existing_variant
            .first()
            .map(|existing| existing.id.clone())
            .unwrap_or_else(|| entry_id.clone());
        self.hot_cache.invalidate_base(&base_key);
        self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
        if existing_variant.len() > 1 {
            self.remove_records(&conn, &existing_variant[1..]).await?;
        }
        self.cleanup_expired(&conn, now_ms).await?;
        self.evict_if_needed(&conn).await?;
        self.hot_cache.insert(HotCacheEntry::new(
            committed_id,
            base_key,
            vary_headers,
            vary_values,
            response,
            expires_at_ms,
        ));
        Ok(true)
    }

    pub async fn delete(&self, request: &CacheRequest) -> Result<bool> {
        let Some(method) = normalize_cache_method(&request.method) else {
            return Ok(false);
        };
        let request_headers = to_header_map(&request.headers);
        let cache_name = normalize_cache_name(&request.cache_name);
        let base_key = CacheBaseKey::new(cache_name.clone(), method.clone(), request.url.clone());
        let _mutation_guard = self.mutation_lock.lock().await;
        self.hot_cache.invalidate_base(&base_key);
        let conn = self.connect().await?;
        let records = self
            .load_candidates(&conn, &cache_name, &method, &request.url)
            .await?;

        let mut to_delete = Vec::new();
        for mut record in records {
            let vary_headers: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_headers_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        to_delete.push(CacheDeleteCandidate { id: record.id });
                        continue;
                    }
                };
            let stored_vary_values: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_values_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        to_delete.push(CacheDeleteCandidate { id: record.id });
                        continue;
                    }
                };
            if derive_vary_values(&vary_headers, &request_headers) == stored_vary_values {
                to_delete.push(CacheDeleteCandidate { id: record.id });
            }
        }

        if to_delete.is_empty() {
            return Ok(false);
        }

        self.remove_records(&conn, &to_delete).await?;
        self.hot_cache.invalidate_base(&base_key);
        Ok(true)
    }

    #[cfg(test)]
    async fn ensure_schema(&self) -> Result<()> {
        let conn = self.connect().await?;
        execute_with_retry(|| conn.execute(CREATE_CACHE_TABLE_SQL, ())).await?;
        ensure_cache_indexes(&conn).await?;
        Ok(())
    }

    async fn initialize_access_seq(&self) -> Result<()> {
        let conn = self.connect().await?;
        let mut rows = query_cached(
            &conn,
            "SELECT COALESCE(MAX(last_access_seq), 0)
                 FROM worker_cache_entries",
            (),
        )
        .await
        .map_err(cache_error)?;
        let persisted_max = rows
            .next()
            .await
            .map_err(cache_error)?
            .map(|row| row.get::<i64>(0).map_err(cache_error))
            .transpose()?
            .unwrap_or(0)
            .max(0) as u64;
        self.access_seq
            .store(persisted_max.saturating_add(1), Ordering::Release);
        Ok(())
    }

    async fn load_candidates(
        &self,
        conn: &Connection,
        cache_name: &str,
        method: &str,
        url: &str,
    ) -> Result<Vec<CacheRecord>> {
        let mut rows = query_cached(
            conn,
                "SELECT id, vary_headers_json, vary_values_json, status, headers_json, body_blob, expires_at_ms
                 FROM worker_cache_entries
                 WHERE cache_name = ?1 AND method = ?2 AND url = ?3",
                (cache_name, method, url),
            )
            .await
            .map_err(cache_error)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(cache_error)? {
            out.push(row_to_record(&row)?);
        }
        Ok(out)
    }

    async fn load_variant_candidates(
        &self,
        conn: &Connection,
        cache_name: &str,
        method: &str,
        url: &str,
        vary_headers_json: &str,
        vary_values_json: &str,
    ) -> Result<Vec<CacheDeleteCandidate>> {
        let mut rows = query_cached(
            conn,
            "SELECT id
                 FROM worker_cache_entries
                 WHERE cache_name = ?1 AND method = ?2 AND url = ?3
                   AND vary_headers_json = ?4 AND vary_values_json = ?5",
            (cache_name, method, url, vary_headers_json, vary_values_json),
        )
        .await
        .map_err(cache_error)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(cache_error)? {
            out.push(CacheDeleteCandidate {
                id: row.get::<String>(0).map_err(cache_error)?,
            });
        }
        Ok(out)
    }

    async fn cleanup_expired(&self, conn: &Connection, now_ms: i64) -> Result<()> {
        let mut cursor_expires_at = i64::MIN;
        let mut cursor_id = String::new();
        loop {
            let mut rows = query_cached(
                conn,
                "SELECT id, headers_json, expires_at_ms
                     FROM worker_cache_entries
                     WHERE expires_at_ms <= ?1
                       AND (expires_at_ms > ?2 OR (expires_at_ms = ?2 AND id > ?3))
                     ORDER BY expires_at_ms ASC, id ASC
                     LIMIT 64",
                (now_ms, cursor_expires_at, cursor_id.as_str()),
            )
            .await
            .map_err(cache_error)?;
            let mut expired = Vec::new();
            let mut scanned_any = false;
            while let Some(row) = rows.next().await.map_err(cache_error)? {
                scanned_any = true;
                let candidate = CacheCleanupCandidate {
                    id: row.get::<String>(0).map_err(cache_error)?,
                    headers_json: row.get::<String>(1).map_err(cache_error)?,
                    expires_at_ms: row.get::<i64>(2).map_err(cache_error)?,
                };
                cursor_expires_at = candidate.expires_at_ms;
                cursor_id = candidate.id.clone();
                let should_delete = match crate::json::from_string::<Vec<(String, String)>>(
                    candidate.headers_json.clone(),
                ) {
                    Ok(headers) => {
                        let response_headers = to_header_map(&headers);
                        let response_control =
                            parse_cache_control(header_value(&response_headers, "cache-control"));
                        now_ms > max_stale_deadline_ms(candidate.expires_at_ms, &response_control)
                    }
                    Err(_) => true,
                };
                if should_delete {
                    expired.push(CacheDeleteCandidate { id: candidate.id });
                }
            }
            drop(rows);
            if !scanned_any {
                break;
            }
            if expired.is_empty() {
                continue;
            }
            self.remove_records(conn, &expired).await?;
        }
        Ok(())
    }

    async fn evict_if_needed(&self, conn: &Connection) -> Result<()> {
        self.flush_pending_touches().await?;
        loop {
            let (count, total_bytes) = self.cache_usage(conn).await?;
            if count <= self.config.max_entries && total_bytes <= self.config.max_bytes {
                break;
            }
            let mut rows = query_cached(
                conn,
                "SELECT id
                     FROM worker_cache_entries
                     ORDER BY last_access_seq ASC, updated_at_ms ASC
                     LIMIT 1",
                (),
            )
            .await
            .map_err(cache_error)?;
            let Some(row) = rows.next().await.map_err(cache_error)? else {
                break;
            };
            let victim = CacheDeleteCandidate {
                id: row.get::<String>(0).map_err(cache_error)?,
            };
            drop(rows);
            self.remove_records(conn, &[victim]).await?;
        }
        Ok(())
    }

    async fn cache_usage(&self, conn: &Connection) -> Result<(usize, usize)> {
        let mut rows = query_cached(
            conn,
            "SELECT COUNT(*), COALESCE(SUM(body_size), 0)
                 FROM worker_cache_entries",
            (),
        )
        .await
        .map_err(cache_error)?;
        if let Some(row) = rows.next().await.map_err(cache_error)? {
            let count = row.get::<i64>(0).map_err(cache_error)?.max(0) as usize;
            let bytes = row.get::<i64>(1).map_err(cache_error)?.max(0) as usize;
            return Ok((count, bytes));
        }
        Ok((0, 0))
    }

    async fn remove_records(
        &self,
        conn: &Connection,
        records: &[CacheDeleteCandidate],
    ) -> Result<()> {
        for record in records {
            self.hot_cache.invalidate_ids([record.id.as_str()]);
            let changed = execute_with_retry(|| {
                execute_cached(
                    conn,
                    "DELETE FROM worker_cache_entries WHERE id = ?1",
                    (record.id.as_str(),),
                )
            })
            .await?;
            if changed > 0 {
                self.mutation_epoch.fetch_add(1, Ordering::AcqRel);
            }
        }
        Ok(())
    }

    async fn connect(&self) -> Result<Connection> {
        let conn = self.database.connect().map_err(cache_error)?;
        configure_cache_connection(&conn).await?;
        Ok(conn)
    }

    fn next_access_seq(&self) -> i64 {
        self.access_seq
            .fetch_add(1, Ordering::Relaxed)
            .min(i64::MAX as u64) as i64
    }

    pub async fn flush_pending_touches(&self) -> Result<()> {
        let result = self.hot_cache.flush_pending_touches().await;
        if result.is_err() {
            self.hot_cache
                .flush_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub fn flush_failure_count(&self) -> u64 {
        self.hot_cache.flush_failures.load(Ordering::Relaxed)
    }

    pub fn pending_touch_count(&self) -> usize {
        self.hot_cache.lock_state().pending_touches.len()
    }

    pub async fn checkpoint(&self) -> Result<()> {
        checkpoint_database(&self.database)
            .await
            .map_err(cache_error)
    }

    pub async fn health_check(&self) -> Result<()> {
        health_check_database(&self.database)
            .await
            .map_err(cache_error)?;
        let conn = self.database.connect().map_err(cache_error)?;
        configure_cache_connection(&conn).await?;
        let version = storage_schema_version(&conn, "cache")
            .await
            .map_err(cache_error)?;
        if version != CACHE_SCHEMA_VERSION {
            return Err(PlatformError::runtime(format!(
                "cache error: schema version {version} is not ready; expected {CACHE_SCHEMA_VERSION}"
            )));
        }
        Ok(())
    }
}

fn row_to_record(row: &turso::Row) -> Result<CacheRecord> {
    Ok(CacheRecord {
        id: row.get::<String>(0).map_err(cache_error)?,
        vary_headers_json: row.get::<String>(1).map_err(cache_error)?,
        vary_values_json: row.get::<String>(2).map_err(cache_error)?,
        status: row.get::<i64>(3).map_err(cache_error)?,
        headers_json: row.get::<String>(4).map_err(cache_error)?,
        body: row.get::<Vec<u8>>(5).map_err(cache_error)?,
        expires_at_ms: row.get::<i64>(6).map_err(cache_error)?,
    })
}

fn normalize_cache_method(method: &str) -> Option<String> {
    let method = method.trim().to_ascii_uppercase();
    match method.as_str() {
        "GET" => Some(method),
        "HEAD" => Some("GET".to_string()),
        _ => None,
    }
}

fn normalize_cache_name(value: &str) -> String {
    let normalized = value.trim();
    if normalized.is_empty() {
        "default".to_string()
    } else {
        normalized.to_string()
    }
}

fn to_header_map(headers: &[(String, String)]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (name, value) in headers {
        let normalized = name.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        let trimmed = value.trim();
        map.entry(normalized)
            .and_modify(|existing: &mut String| {
                if !existing.is_empty() {
                    existing.push_str(", ");
                }
                existing.push_str(trimmed);
            })
            .or_insert_with(|| trimmed.to_string());
    }
    map
}

fn header_value<'a>(headers: &'a HashMap<String, String>, name: &str) -> &'a str {
    headers
        .get(&name.trim().to_ascii_lowercase())
        .map(|value| value.as_str())
        .unwrap_or("")
}

fn parse_cache_control(value: &str) -> CacheControl {
    let mut control = CacheControl::default();
    for raw_directive in value.split(',') {
        let directive = raw_directive.trim().to_ascii_lowercase();
        if directive.is_empty() {
            continue;
        }

        match directive.as_str() {
            "no-store" => {
                control.no_store = true;
                continue;
            }
            "no-cache" => {
                control.no_cache = true;
                continue;
            }
            "private" => {
                control.private = true;
                continue;
            }
            _ => {}
        }

        if let Some(value) = directive.strip_prefix("s-maxage=") {
            control.s_maxage = parse_u64_token(value);
            continue;
        }
        if let Some(value) = directive.strip_prefix("max-age=") {
            control.max_age = parse_u64_token(value);
            continue;
        }
        if let Some(value) = directive.strip_prefix("stale-while-revalidate=") {
            control.stale_while_revalidate = parse_u64_token(value);
            continue;
        }
        if let Some(value) = directive.strip_prefix("stale-if-error=") {
            control.stale_if_error = parse_u64_token(value);
        }
    }

    control
}

fn parse_u64_token(value: &str) -> Option<u64> {
    value.trim().trim_matches('"').parse::<u64>().ok()
}

fn stale_deadline_ms(expires_at_ms: i64, stale_seconds: u64) -> i64 {
    let capped_seconds = stale_seconds.min((i64::MAX / 1000) as u64);
    expires_at_ms.saturating_add((capped_seconds as i64).saturating_mul(1000))
}

fn max_stale_deadline_ms(expires_at_ms: i64, control: &CacheControl) -> i64 {
    stale_deadline_ms(
        expires_at_ms,
        control
            .stale_while_revalidate
            .unwrap_or(0)
            .max(control.stale_if_error.unwrap_or(0)),
    )
}

fn parse_vary_headers(response_headers: &[(String, String)]) -> Option<Vec<String>> {
    let mut seen = HashSet::new();
    let mut vary = Vec::new();
    for (name, value) in response_headers {
        if !name.eq_ignore_ascii_case("vary") {
            continue;
        }
        for token in value.split(',') {
            let normalized = token.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                continue;
            }
            if normalized == "*" {
                return None;
            }
            if seen.insert(normalized.clone()) {
                vary.push(normalized);
            }
        }
    }
    Some(vary)
}

fn derive_vary_values(
    vary_headers: &[String],
    request_headers: &HashMap<String, String>,
) -> Vec<String> {
    vary_headers
        .iter()
        .map(|name| header_value(request_headers, name).to_string())
        .collect()
}

fn vary_values_match(
    vary_headers: &[String],
    vary_values: &[String],
    request_headers: &HashMap<String, String>,
) -> bool {
    vary_headers.len() == vary_values.len()
        && vary_headers
            .iter()
            .zip(vary_values)
            .all(|(name, value)| header_value(request_headers, name) == value)
}

fn choose_ttl(control: CacheControl, fallback: Duration) -> Duration {
    let secs = control
        .s_maxage
        .or(control.max_age)
        .unwrap_or(fallback.as_secs());
    Duration::from_secs(secs)
        .min(MAX_TTL)
        .max(Duration::from_secs(0))
}

fn estimate_entry_size(
    cache_name: &str,
    method: &str,
    url: &str,
    response: &CacheResponse,
    vary_values: &[String],
) -> usize {
    let headers_bytes = response
        .headers
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum::<usize>();
    let vary_bytes = vary_values.iter().map(String::len).sum::<usize>();
    cache_name.len() + method.len() + url.len() + headers_bytes + vary_bytes + response.body.len()
}

fn hot_entry_size(
    id: &str,
    base_key: &CacheBaseKey,
    vary_headers: &[String],
    vary_values: &[String],
    response: &CacheResponse,
) -> usize {
    let headers_bytes = response
        .headers
        .iter()
        .map(|(name, value)| name.len().saturating_add(value.len()))
        .sum::<usize>();
    id.len()
        .saturating_add(base_key.cache_name.len())
        .saturating_add(base_key.method.len())
        .saturating_add(base_key.url.len())
        .saturating_add(vary_headers.iter().map(String::len).sum::<usize>())
        .saturating_add(vary_values.iter().map(String::len).sum::<usize>())
        .saturating_add(headers_bytes)
        .saturating_add(response.body.len())
}

async fn configure_cache_connection(conn: &Connection) -> Result<()> {
    configure_turso_connection(conn, cache_error)?;
    conn.pragma_update("synchronous", "'NORMAL'")
        .await
        .map_err(cache_error)?;
    Ok(())
}

async fn ensure_cache_indexes(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_cache_variant
         ON worker_cache_entries(cache_name, method, url, vary_headers_json, vary_values_json)",
        (),
    )
    .await
    .map_err(cache_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_worker_cache_lookup
         ON worker_cache_entries(cache_name, method, url)",
        (),
    )
    .await
    .map_err(cache_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_worker_cache_expiry
         ON worker_cache_entries(expires_at_ms)",
        (),
    )
    .await
    .map_err(cache_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_worker_cache_lru
         ON worker_cache_entries(last_access_seq, updated_at_ms)",
        (),
    )
    .await
    .map_err(cache_error)?;
    Ok(())
}

async fn migrate_cache_schema(database: &Database, legacy_blobs: &BlobStore) -> Result<()> {
    let mut conn = database.connect().map_err(cache_error)?;
    configure_cache_connection(&conn).await?;

    const MAX_ATTEMPTS: usize = 8;
    let mut migrated_blob_refs = HashSet::new();
    for attempt in 0..MAX_ATTEMPTS {
        let tx = match conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
        {
            Ok(tx) => tx,
            Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                sleep_cache_retry(attempt).await;
                continue;
            }
            Err(error) => return Err(cache_error_after_retry(error)),
        };

        let outcome = migrate_cache_schema_transaction(&tx, legacy_blobs).await;
        match outcome {
            Ok(result) => match tx.commit().await {
                Ok(()) => {
                    migrated_blob_refs.extend(result.blob_refs);
                    if result.migrated_rows > 0 || result.dropped_rows > 0 {
                        tracing::info!(
                            migrated_rows = result.migrated_rows,
                            dropped_rows = result.dropped_rows,
                            "migrated cache bodies into Turso BLOB storage"
                        );
                    }
                    break;
                }
                Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    // A commit error is ambiguous. Retain the refs and only
                    // remove them after a subsequent transaction confirms the
                    // migrated schema is committed.
                    migrated_blob_refs.extend(result.blob_refs);
                    sleep_cache_retry(attempt).await;
                    continue;
                }
                Err(error) => return Err(cache_error(error)),
            },
            Err(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        }
    }

    for blob_ref in migrated_blob_refs {
        if let Err(error) = legacy_blobs.delete_legacy(&blob_ref).await {
            tracing::warn!(blob_ref, error = %error, "failed to remove migrated cache blob");
        }
    }
    Ok(())
}

#[derive(Default)]
struct CacheMigrationResult {
    migrated_rows: usize,
    dropped_rows: usize,
    blob_refs: HashSet<String>,
}

async fn migrate_cache_schema_transaction(
    conn: &Connection,
    legacy_blobs: &BlobStore,
) -> Result<CacheMigrationResult> {
    ensure_storage_migration_table(conn)
        .await
        .map_err(cache_error)?;
    let applied_version = storage_schema_version(conn, "cache")
        .await
        .map_err(cache_error)?;
    if applied_version > CACHE_SCHEMA_VERSION {
        return Err(PlatformError::runtime(format!(
            "cache error: unsupported cache schema version {applied_version}; maximum supported version is {CACHE_SCHEMA_VERSION}"
        )));
    }

    let columns = cache_table_columns(conn).await?;
    if columns.is_empty() {
        conn.execute(CREATE_CACHE_TABLE_SQL, ())
            .await
            .map_err(cache_error)?;
    } else if !columns.contains("body_blob") {
        return migrate_legacy_cache_table(conn, legacy_blobs).await;
    }

    ensure_cache_indexes(conn).await?;
    record_cache_schema_version(conn).await?;
    Ok(CacheMigrationResult::default())
}

async fn cache_table_columns(conn: &Connection) -> Result<HashSet<String>> {
    let mut rows = conn
        .query("PRAGMA table_info(worker_cache_entries)", ())
        .await
        .map_err(cache_error)?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await.map_err(cache_error)? {
        columns.insert(row.get::<String>(1).map_err(cache_error)?);
    }
    Ok(columns)
}

async fn migrate_legacy_cache_table(
    conn: &Connection,
    legacy_blobs: &BlobStore,
) -> Result<CacheMigrationResult> {
    let mut rows = conn
        .query(
            "SELECT id, cache_name, method, url, vary_headers_json, vary_values_json,
                    status, headers_json, body_storage, body_inline_hex, body_ref,
                    body_size, expires_at_ms, last_access_seq, updated_at_ms
             FROM worker_cache_entries
             ORDER BY id",
            (),
        )
        .await
        .map_err(cache_error)?;
    let mut legacy_rows = Vec::new();
    while let Some(row) = rows.next().await.map_err(cache_error)? {
        legacy_rows.push(LegacyCacheRecord {
            id: row.get::<String>(0).map_err(cache_error)?,
            cache_name: row.get::<String>(1).map_err(cache_error)?,
            method: row.get::<String>(2).map_err(cache_error)?,
            url: row.get::<String>(3).map_err(cache_error)?,
            vary_headers_json: row.get::<String>(4).map_err(cache_error)?,
            vary_values_json: row.get::<String>(5).map_err(cache_error)?,
            status: row.get::<i64>(6).map_err(cache_error)?,
            headers_json: row.get::<String>(7).map_err(cache_error)?,
            body_storage: row.get::<String>(8).map_err(cache_error)?,
            body_inline_hex: row.get::<String>(9).map_err(cache_error)?,
            body_ref: row.get::<String>(10).map_err(cache_error)?,
            body_size: row.get::<i64>(11).map_err(cache_error)?,
            expires_at_ms: row.get::<i64>(12).map_err(cache_error)?,
            last_access_seq: row.get::<i64>(13).map_err(cache_error)?,
            updated_at_ms: row.get::<i64>(14).map_err(cache_error)?,
        });
    }
    drop(rows);

    conn.execute(CREATE_CACHE_MIGRATION_TABLE_SQL, ())
        .await
        .map_err(cache_error)?;
    let mut result = CacheMigrationResult::default();
    for record in legacy_rows {
        let body = match record.body_storage.as_str() {
            "inline" => hex_to_bytes(&record.body_inline_hex),
            "blob" => {
                if !record.body_ref.is_empty() {
                    result.blob_refs.insert(record.body_ref.clone());
                }
                legacy_blobs.read_legacy(&record.body_ref).await
            }
            other => Err(PlatformError::runtime(format!(
                "cache error: unsupported legacy body storage {other}"
            ))),
        };
        let body = match body {
            Ok(body) => body,
            Err(error) => {
                result.dropped_rows = result.dropped_rows.saturating_add(1);
                tracing::warn!(
                    cache_entry_id = %record.id,
                    error = %error,
                    "dropping unreadable legacy cache entry during BLOB migration"
                );
                continue;
            }
        };

        execute_cached(
            conn,
            "INSERT INTO worker_cache_entries_migrating (
               id, cache_name, method, url, vary_headers_json, vary_values_json,
               status, headers_json, body_blob, body_size, expires_at_ms,
               last_access_seq, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            (
                record.id.as_str(),
                record.cache_name.as_str(),
                record.method.as_str(),
                record.url.as_str(),
                record.vary_headers_json.as_str(),
                record.vary_values_json.as_str(),
                record.status,
                record.headers_json.as_str(),
                body.as_slice(),
                record.body_size,
                record.expires_at_ms,
                record.last_access_seq,
                record.updated_at_ms,
            ),
        )
        .await
        .map_err(cache_error)?;
        result.migrated_rows = result.migrated_rows.saturating_add(1);
    }

    conn.execute("DROP TABLE worker_cache_entries", ())
        .await
        .map_err(cache_error)?;
    conn.execute(
        "ALTER TABLE worker_cache_entries_migrating RENAME TO worker_cache_entries",
        (),
    )
    .await
    .map_err(cache_error)?;
    ensure_cache_indexes(conn).await?;
    record_cache_schema_version(conn).await?;
    Ok(result)
}

async fn record_cache_schema_version(conn: &Connection) -> Result<()> {
    record_storage_schema_version(conn, "cache", CACHE_SCHEMA_VERSION, epoch_ms_i64()?)
        .await
        .map_err(cache_error)?;
    Ok(())
}

#[cfg(test)]
fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_to_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(PlatformError::runtime(
            "cache error: invalid inline body hex",
        ));
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        let hi = from_hex_nibble(bytes[idx])?;
        let lo = from_hex_nibble(bytes[idx + 1])?;
        out.push((hi << 4) | lo);
        idx += 2;
    }
    Ok(out)
}

fn from_hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(PlatformError::runtime(
            "cache error: invalid inline body hex",
        )),
    }
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}

async fn execute_with_retry<F, Fut>(execute: F) -> Result<u64>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<u64, turso::Error>>,
{
    retry_turso_busy(execute, cache_error_after_retry).await
}

async fn sleep_cache_retry(attempt: usize) {
    record_storage_retry();
    tokio::time::sleep(Duration::from_millis(5 * (attempt + 1) as u64)).await;
}

fn cache_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("cache error: {error}"))
}

fn cache_error_after_retry(error: turso::Error) -> PlatformError {
    if is_retryable_turso_error(&error) {
        PlatformError::storage_unavailable(format!("cache error: {error}"))
    } else {
        cache_error(error)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CacheBaseKey, CacheConfig, CacheLookup, CacheRequest, CacheResponse, CacheStore,
        HOT_CACHE_MAX_BYTES, HOT_CACHE_MAX_ENTRIES, HotCacheEntry, HotCacheState,
        PendingTouchesGuard, bytes_to_hex,
    };
    use crate::blob::local_blob_store_for_tests;
    use crate::kv::KvStore;
    use bytes::Bytes;
    use common::Result;
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Barrier;
    use turso::Builder;
    use uuid::Uuid;

    async fn test_store(config: CacheConfig) -> CacheStore {
        let blob_dir = format!("/tmp/dd-cache-blob-test-{}", Uuid::new_v4());
        test_store_with_blob_dir(config, &blob_dir).await
    }

    async fn test_store_with_blob_dir(config: CacheConfig, blob_dir: &str) -> CacheStore {
        let database_path = format!("/tmp/dd-cache-test-{}.db", Uuid::new_v4());
        test_store_at_path(config, &database_path, blob_dir).await
    }

    async fn test_store_at_path(
        config: CacheConfig,
        database_path: &str,
        blob_dir: &str,
    ) -> CacheStore {
        let blob_store = local_blob_store_for_tests(blob_dir)
            .await
            .expect("blob store");
        CacheStore::from_local_path(config, database_path.to_string(), blob_store)
            .await
            .expect("cache store")
    }

    async fn persisted_access_seq(store: &CacheStore, url: &str) -> i64 {
        let conn = store.connect().await.expect("cache connection");
        let mut rows = conn
            .query(
                "SELECT last_access_seq FROM worker_cache_entries WHERE url = ?1",
                (url,),
            )
            .await
            .expect("query cache recency");
        rows.next()
            .await
            .expect("read cache recency")
            .expect("cache entry")
            .get::<i64>(0)
            .expect("last access sequence")
    }

    fn request(path: &str) -> CacheRequest {
        CacheRequest {
            cache_name: "default".to_string(),
            method: "GET".to_string(),
            url: format!("http://worker{path}"),
            headers: Vec::new(),
            bypass_stale: false,
        }
    }

    fn response(body: &str) -> CacheResponse {
        CacheResponse {
            status: 200,
            headers: vec![("cache-control".to_string(), "max-age=60".to_string())],
            body: Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    fn local_blob_file_count(blob_dir: &str) -> usize {
        fs::read_dir(blob_dir)
            .expect("blob dir should exist")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "blob"))
            .count()
    }

    #[test]
    fn hot_cache_enforces_entry_and_byte_bounds() {
        fn hot_entry(id: usize) -> HotCacheEntry {
            HotCacheEntry::new(
                id.to_string(),
                CacheBaseKey::new(
                    "default".into(),
                    "GET".into(),
                    format!("http://worker/{id}"),
                ),
                Vec::new(),
                Vec::new(),
                CacheResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from(vec![id as u8]),
                },
                i64::MAX,
            )
        }

        let mut state = HotCacheState::default();
        for id in 0..=HOT_CACHE_MAX_ENTRIES {
            state.insert(hot_entry(id), HOT_CACHE_MAX_ENTRIES, HOT_CACHE_MAX_BYTES);
        }
        assert_eq!(state.entries.len(), HOT_CACHE_MAX_ENTRIES);
        assert!(!state.entries.contains_key("0"));
        assert!(state.total_bytes <= HOT_CACHE_MAX_BYTES);

        let mut oversized = hot_entry(HOT_CACHE_MAX_ENTRIES + 1);
        oversized.size_bytes = HOT_CACHE_MAX_BYTES + 1;
        let oversized_id = oversized.id.clone();
        state.insert(oversized, HOT_CACHE_MAX_ENTRIES, HOT_CACHE_MAX_BYTES);
        assert!(!state.entries.contains_key(&oversized_id));
        assert_eq!(state.entries.len(), HOT_CACHE_MAX_ENTRIES);
    }

    #[tokio::test]
    async fn put_then_get_hits() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(5),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/x");
        assert!(store.put(&req, response("one")).await?);
        let hit = store.get(&req).await?;
        match hit {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"one"),
            other => panic!("expected fresh hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn repeated_hot_hits_share_response_body_storage() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(5),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/shared-body");
        assert!(store.put(&req, response("shared")).await?);

        let CacheLookup::Fresh(first) = store.get(&req).await? else {
            panic!("first cache lookup should be fresh");
        };
        let CacheLookup::Fresh(second) = store.get(&req).await? else {
            panic!("second cache lookup should be fresh");
        };

        assert_eq!(first.body.as_ptr(), second.body.as_ptr());
        Ok(())
    }

    #[tokio::test]
    async fn hot_hits_coalesce_recency_until_flush() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/coalesced-recency");
        assert!(store.put(&req, response("hot")).await?);
        let before = persisted_access_seq(&store, &req.url).await;

        for _ in 0..32 {
            assert!(matches!(store.get(&req).await?, CacheLookup::Fresh(_)));
        }
        {
            let state = store.hot_cache.lock_state();
            assert_eq!(state.entries.len(), 1);
            assert_eq!(state.pending_touches.len(), 1);
        }
        assert_eq!(persisted_access_seq(&store, &req.url).await, before);

        let expected = store.access_seq.load(std::sync::atomic::Ordering::Relaxed) as i64 - 1;
        store.flush_pending_touches().await?;
        assert_eq!(persisted_access_seq(&store, &req.url).await, expected);
        assert!(store.hot_cache.lock_state().pending_touches.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_delete_cannot_be_overwritten_by_inflight_disk_read() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/delete-read-race");
        assert!(store.put(&req, response("old")).await?);
        store.hot_cache.invalidate_base(&CacheBaseKey::new(
            "default".into(),
            "GET".into(),
            req.url.clone(),
        ));

        let (reached, resume) = store.test_hooks.install_before_hot_insert();
        let get_store = store.clone();
        let get_request = req.clone();
        let get = tokio::spawn(async move { get_store.get(&get_request).await });
        reached.await.expect("disk read should reach hot insert");

        let delete_store = store.clone();
        let delete_request = req.clone();
        let delete = tokio::spawn(async move { delete_store.delete(&delete_request).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !delete.is_finished(),
            "delete must wait for the disk-read insertion boundary"
        );

        resume.notify_one();
        assert!(matches!(
            get.await.expect("get task")?,
            CacheLookup::Fresh(_)
        ));
        assert!(delete.await.expect("delete task")?);
        assert!(matches!(store.get(&req).await?, CacheLookup::Miss));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_disk_cleanup_cannot_delete_concurrent_refresh() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/cleanup-refresh-race");
        assert!(store.put(&req, response("old")).await?);
        store.hot_cache.invalidate_base(&CacheBaseKey::new(
            "default".into(),
            "GET".into(),
            req.url.clone(),
        ));
        let conn = store.connect().await?;
        conn.execute(
            "UPDATE worker_cache_entries SET expires_at_ms = 0 WHERE url = ?1",
            (req.url.as_str(),),
        )
        .await
        .expect("expire cache row");

        let (reached, resume) = store.test_hooks.install_after_disk_read();
        let get_store = store.clone();
        let get_request = req.clone();
        let stale_get = tokio::spawn(async move { get_store.get(&get_request).await });
        reached.await.expect("stale disk read should pause");

        assert!(store.put(&req, response("fresh")).await?);
        resume.notify_one();
        assert!(matches!(
            stale_get.await.expect("stale get task")?,
            CacheLookup::Miss
        ));
        match store.get(&req).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"fresh"),
            other => panic!("expected refreshed cache row, got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn failed_recency_flush_is_requeued_and_recovers() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/flush-recovery");
        assert!(store.put(&req, response("hot")).await?);
        assert!(matches!(store.get(&req).await?, CacheLookup::Fresh(_)));

        let conn = store.connect().await?;
        conn.execute("DROP TABLE worker_cache_entries", ())
            .await
            .expect("drop cache table");
        assert!(store.flush_pending_touches().await.is_err());
        assert_eq!(store.hot_cache.lock_state().pending_touches.len(), 1);

        store.ensure_schema().await?;
        store.flush_pending_touches().await?;
        assert!(store.hot_cache.lock_state().pending_touches.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn canceled_recency_batch_requeues_unpersisted_touches() {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        {
            let _pending = PendingTouchesGuard {
                hot_cache: store.hot_cache.as_ref(),
                touches: vec![
                    ("persisted".into(), 1),
                    ("retry-a".into(), 2),
                    ("retry-b".into(), 3),
                ],
                persisted: 1,
            };
        }
        let state = store.hot_cache.lock_state();
        assert!(!state.pending_touches.contains_key("persisted"));
        assert_eq!(state.pending_touches.get("retry-a"), Some(&2));
        assert_eq!(state.pending_touches.get("retry-b"), Some(&3));
    }

    #[tokio::test]
    async fn restart_initializes_access_sequence_from_persisted_max() -> Result<()> {
        let database_path = format!("/tmp/dd-cache-restart-test-{}.db", Uuid::new_v4());
        let blob_dir = format!("/tmp/dd-cache-restart-blob-test-{}", Uuid::new_v4());
        let config = CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        };
        let first = test_store_at_path(config.clone(), &database_path, &blob_dir).await;
        let first_request = request("/before-restart");
        assert!(first.put(&first_request, response("first")).await?);
        assert!(matches!(
            first.get(&first_request).await?,
            CacheLookup::Fresh(_)
        ));
        first.flush_pending_touches().await?;
        let persisted_max = persisted_access_seq(&first, &first_request.url).await;
        drop(first);

        let restarted = test_store_at_path(config, &database_path, &blob_dir).await;
        assert_eq!(
            restarted
                .access_seq
                .load(std::sync::atomic::Ordering::Acquire),
            persisted_max as u64 + 1
        );
        let second_request = request("/after-restart");
        assert!(restarted.put(&second_request, response("second")).await?);
        assert!(persisted_access_seq(&restarted, &second_request.url).await > persisted_max);
        Ok(())
    }

    #[tokio::test]
    async fn vary_header_selects_variant() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(5),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let mut req_a = request("/vary");
        req_a
            .headers
            .push(("accept-language".to_string(), "en".to_string()));
        let mut req_b = request("/vary");
        req_b
            .headers
            .push(("accept-language".to_string(), "fr".to_string()));

        let mut res_a = response("hello");
        res_a
            .headers
            .push(("vary".to_string(), "accept-language".to_string()));
        let mut res_b = response("salut");
        res_b
            .headers
            .push(("vary".to_string(), "accept-language".to_string()));

        assert!(store.put(&req_a, res_a).await?);
        assert!(store.put(&req_b, res_b).await?);
        match store.get(&req_a).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"hello"),
            other => panic!("expected fresh en hit, got {:?}", other),
        }
        match store.get(&req_b).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"salut"),
            other => panic!("expected fresh fr hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn lru_eviction_respects_size_limits() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 2,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req_a = request("/a");
        let req_b = request("/b");
        let req_c = request("/c");

        assert!(store.put(&req_a, response("a")).await?);
        assert!(store.put(&req_b, response("b")).await?);
        let _ = store.get(&req_b).await?;
        assert!(store.put(&req_c, response("c")).await?);

        assert!(matches!(store.get(&req_a).await?, CacheLookup::Miss));
        match store.get(&req_b).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"b"),
            other => panic!("expected fresh b hit, got {:?}", other),
        }
        match store.get(&req_c).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"c"),
            other => panic!("expected fresh c hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn cache_hit_refreshes_lru_recency() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 2,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req_a = request("/lru-a");
        let req_b = request("/lru-b");
        let req_c = request("/lru-c");

        assert!(store.put(&req_a, response("a")).await?);
        assert!(store.put(&req_b, response("b")).await?);
        match store.get(&req_a).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"a"),
            other => panic!("expected fresh a hit, got {:?}", other),
        }
        assert!(store.put(&req_c, response("c")).await?);

        assert!(matches!(store.get(&req_b).await?, CacheLookup::Miss));
        match store.get(&req_a).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"a"),
            other => panic!("expected fresh a hit after eviction, got {:?}", other),
        }
        match store.get(&req_c).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"c"),
            other => panic!("expected fresh c hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn large_body_uses_database_blob_storage() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 16,
        })
        .await;
        let req = request("/blob");
        let body = "x".repeat(1024);
        let cached_response = CacheResponse {
            status: 200,
            headers: vec![("cache-control".to_string(), "max-age=60".to_string())],
            body: Bytes::copy_from_slice(body.as_bytes()),
        };

        assert!(store.put(&req, cached_response).await?);
        match store.get(&req).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.len(), 1024),
            other => panic!("expected BLOB fresh hit, got {:?}", other),
        }
        let conn = store.connect().await?;
        let mut rows = conn
            .query(
                "SELECT length(body_blob) FROM worker_cache_entries WHERE url = ?1",
                (req.url.as_str(),),
            )
            .await
            .map_err(super::cache_error)?;
        let row = rows
            .next()
            .await
            .map_err(super::cache_error)?
            .expect("cache row");
        assert_eq!(row.get::<i64>(0).map_err(super::cache_error)?, 1024);
        Ok(())
    }

    #[tokio::test]
    async fn put_overwrites_existing_variant_without_unique_conflict() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/overwrite");

        assert!(store.put(&req, response("one")).await?);
        assert!(store.put(&req, response("two")).await?);

        match store.get(&req).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"two"),
            other => panic!("expected overwritten cache entry, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_same_variant_are_normal_overwrites() -> Result<()> {
        let blob_dir = format!("/tmp/dd-cache-blob-test-{}", Uuid::new_v4());
        let store = test_store_with_blob_dir(
            CacheConfig {
                max_entries: 8,
                max_bytes: 1024 * 1024,
                default_ttl: Duration::from_secs(60),
                inline_body_limit_bytes: 1,
            },
            &blob_dir,
        )
        .await;
        let req = request("/concurrent-overwrite");
        let barrier = Arc::new(Barrier::new(16));
        let mut handles = Vec::new();

        for index in 0..16 {
            let store = store.clone();
            let req = req.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                store.put(&req, response(&format!("body-{index}"))).await
            }));
        }

        for handle in handles {
            assert!(handle.await.expect("put task should join")?);
        }

        match store.get(&req).await? {
            CacheLookup::Fresh(value) => {
                let body =
                    String::from_utf8(value.body.to_vec()).expect("cache body should be utf8");
                assert!(body.starts_with("body-"));
            }
            other => panic!("expected fresh concurrent overwrite hit, got {:?}", other),
        }
        assert_eq!(local_blob_file_count(&blob_dir), 0);
        Ok(())
    }

    #[tokio::test]
    async fn legacy_inline_and_external_bodies_migrate_to_database_blobs() -> Result<()> {
        let database_path = format!("/tmp/dd-cache-migration-test-{}.db", Uuid::new_v4());
        let blob_dir = format!("/tmp/dd-cache-migration-blobs-{}", Uuid::new_v4());
        let blob_store = local_blob_store_for_tests(&blob_dir).await?;
        let external_body = b"legacy external body";
        let blob_ref = blob_store.put(external_body).await?;

        let database = Builder::new_local(&database_path)
            .build()
            .await
            .map_err(super::cache_error)?;
        let conn = database.connect().map_err(super::cache_error)?;
        conn.execute(
            "CREATE TABLE worker_cache_entries (
               id TEXT PRIMARY KEY,
               cache_name TEXT NOT NULL,
               method TEXT NOT NULL,
               url TEXT NOT NULL,
               vary_headers_json TEXT NOT NULL,
               vary_values_json TEXT NOT NULL,
               status INTEGER NOT NULL,
               headers_json TEXT NOT NULL,
               body_storage TEXT NOT NULL,
               body_inline_hex TEXT NOT NULL,
               body_ref TEXT NOT NULL,
               body_size INTEGER NOT NULL,
               expires_at_ms INTEGER NOT NULL,
               last_access_seq INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             )",
            (),
        )
        .await
        .map_err(super::cache_error)?;
        let headers_json = r#"[["cache-control","max-age=60"]]"#;
        conn.execute(
            "INSERT INTO worker_cache_entries (
               id, cache_name, method, url, vary_headers_json, vary_values_json,
               status, headers_json, body_storage, body_inline_hex, body_ref,
               body_size, expires_at_ms, last_access_seq, updated_at_ms
             ) VALUES (?1, 'default', 'GET', ?2, '[]', '[]', 200, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            (
                "inline-id",
                "http://worker/legacy-inline",
                headers_json,
                "inline",
                bytes_to_hex(b"legacy inline body"),
                "",
                18_i64,
                i64::MAX / 2,
                7_i64,
            ),
        )
        .await
        .map_err(super::cache_error)?;
        conn.execute(
            "INSERT INTO worker_cache_entries (
               id, cache_name, method, url, vary_headers_json, vary_values_json,
               status, headers_json, body_storage, body_inline_hex, body_ref,
               body_size, expires_at_ms, last_access_seq, updated_at_ms
             ) VALUES (?1, 'default', 'GET', ?2, '[]', '[]', 200, ?3, 'blob', '', ?4, ?5, ?6, ?7, 1)",
            (
                "blob-id",
                "http://worker/legacy-external",
                headers_json,
                blob_ref.as_str(),
                external_body.len() as i64,
                i64::MAX / 2,
                8_i64,
            ),
        )
        .await
        .map_err(super::cache_error)?;
        drop(conn);
        drop(database);

        let store =
            CacheStore::from_local_path(CacheConfig::default(), database_path, blob_store).await?;
        match store.get(&request("/legacy-inline")).await? {
            CacheLookup::Fresh(value) => {
                assert_eq!(value.body.as_ref(), b"legacy inline body")
            }
            other => panic!("expected migrated inline hit, got {other:?}"),
        }
        match store.get(&request("/legacy-external")).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), external_body),
            other => panic!("expected migrated external hit, got {other:?}"),
        }

        let conn = store.connect().await?;
        let mut columns = conn
            .query("PRAGMA table_info(worker_cache_entries)", ())
            .await
            .map_err(super::cache_error)?;
        let mut names = std::collections::HashSet::new();
        while let Some(row) = columns.next().await.map_err(super::cache_error)? {
            names.insert(row.get::<String>(1).map_err(super::cache_error)?);
        }
        assert!(names.contains("body_blob"));
        assert!(!names.contains("body_inline_hex"));
        assert!(!names.contains("body_ref"));

        let mut versions = conn
            .query(
                "SELECT version FROM dd_storage_schema_migrations WHERE component = 'cache'",
                (),
            )
            .await
            .map_err(super::cache_error)?;
        assert_eq!(
            versions
                .next()
                .await
                .map_err(super::cache_error)?
                .expect("cache migration version")
                .get::<i64>(0)
                .map_err(super::cache_error)?,
            super::CACHE_SCHEMA_VERSION
        );
        assert_eq!(local_blob_file_count(&blob_dir), 0);
        Ok(())
    }

    #[tokio::test]
    async fn current_cache_schema_without_migration_metadata_is_adopted() -> Result<()> {
        let database_path = format!("/tmp/dd-cache-current-test-{}.db", Uuid::new_v4());
        let blob_dir = format!("/tmp/dd-cache-current-blobs-{}", Uuid::new_v4());
        let store = test_store_at_path(
            CacheConfig::default(),
            database_path.as_str(),
            blob_dir.as_str(),
        )
        .await;
        assert!(
            store
                .put(&request("/current"), response("preserved"))
                .await?
        );
        drop(store);

        let database = Builder::new_local(&database_path)
            .build()
            .await
            .map_err(super::cache_error)?;
        let conn = database.connect().map_err(super::cache_error)?;
        conn.execute(
            "DELETE FROM dd_storage_schema_migrations WHERE component = 'cache'",
            (),
        )
        .await
        .map_err(super::cache_error)?;
        drop(conn);
        drop(database);

        let store = test_store_at_path(
            CacheConfig::default(),
            database_path.as_str(),
            blob_dir.as_str(),
        )
        .await;
        match store.get(&request("/current")).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"preserved"),
            other => panic!("expected current-schema cache hit, got {other:?}"),
        }
        store.health_check().await?;
        Ok(())
    }

    #[tokio::test]
    async fn future_cache_schema_version_is_rejected_without_mutating_data() -> Result<()> {
        let database_path = format!("/tmp/dd-cache-future-test-{}.db", Uuid::new_v4());
        let blob_dir = format!("/tmp/dd-cache-future-blobs-{}", Uuid::new_v4());
        let store = test_store_at_path(
            CacheConfig::default(),
            database_path.as_str(),
            blob_dir.as_str(),
        )
        .await;
        assert!(
            store
                .put(&request("/future"), response("future-guard"))
                .await?
        );
        drop(store);

        let database = Builder::new_local(&database_path)
            .build()
            .await
            .map_err(super::cache_error)?;
        let conn = database.connect().map_err(super::cache_error)?;
        conn.execute(
            "INSERT INTO dd_storage_schema_migrations (component, version, applied_at_ms)
             VALUES ('cache', ?1, 1)",
            (super::CACHE_SCHEMA_VERSION + 1,),
        )
        .await
        .map_err(super::cache_error)?;
        drop(conn);
        drop(database);

        let blob_store = local_blob_store_for_tests(&blob_dir).await?;
        let error = match CacheStore::from_local_path(
            CacheConfig::default(),
            database_path.clone(),
            blob_store,
        )
        .await
        {
            Ok(_) => panic!("future cache schema must fail startup"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("unsupported cache schema version")
        );

        let database = Builder::new_local(&database_path)
            .build()
            .await
            .map_err(super::cache_error)?;
        let conn = database.connect().map_err(super::cache_error)?;
        let mut rows = conn
            .query(
                "SELECT body_blob FROM worker_cache_entries WHERE url = 'http://worker/future'",
                (),
            )
            .await
            .map_err(super::cache_error)?;
        assert_eq!(
            rows.next()
                .await
                .map_err(super::cache_error)?
                .expect("preserved cache row")
                .get::<Vec<u8>>(0)
                .map_err(super::cache_error)?,
            b"future-guard"
        );
        Ok(())
    }

    #[tokio::test]
    async fn kv_and_cache_share_one_database_owner_and_migration_ledger() -> Result<()> {
        let database_path = format!("/tmp/dd-shared-storage-test-{}.db", Uuid::new_v4());
        let blob_dir = format!("/tmp/dd-shared-storage-blobs-{}", Uuid::new_v4());
        let database = Arc::new(
            Builder::new_local(&database_path)
                .build()
                .await
                .map_err(super::cache_error)?,
        );
        let kv = KvStore::from_database(Arc::clone(&database)).await?;
        let cache = CacheStore::from_database(
            CacheConfig::default(),
            Arc::clone(&database),
            local_blob_store_for_tests(&blob_dir).await?,
        )
        .await?;

        assert!(kv.shares_database_owner(&database));
        assert!(Arc::ptr_eq(&cache.database, &database));
        kv.health_check().await?;
        cache.health_check().await?;

        let conn = database.connect().map_err(super::cache_error)?;
        let mut rows = conn
            .query(
                "SELECT COUNT(DISTINCT component)
                 FROM dd_storage_schema_migrations
                 WHERE component IN ('kv', 'cache')",
                (),
            )
            .await
            .map_err(super::cache_error)?;
        assert_eq!(
            rows.next()
                .await
                .map_err(super::cache_error)?
                .expect("shared migration ledger row")
                .get::<i64>(0)
                .map_err(super::cache_error)?,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn cache_connections_use_normal_synchronous_durability() -> Result<()> {
        let store = test_store(CacheConfig::default()).await;
        let conn = store.connect().await?;
        let mut rows = conn
            .query("PRAGMA synchronous", ())
            .await
            .map_err(super::cache_error)?;
        let row = rows
            .next()
            .await
            .map_err(super::cache_error)?
            .expect("synchronous row");
        assert_eq!(row.get::<i64>(0).map_err(super::cache_error)?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_recently_overwritten_variant() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/delete");

        assert!(store.put(&req, response("temp")).await?);
        assert!(store.put(&req, response("temp-2")).await?);
        assert!(store.delete(&req).await?);
        assert!(matches!(store.get(&req).await?, CacheLookup::Miss));
        Ok(())
    }

    #[tokio::test]
    async fn stale_while_revalidate_lookup_serves_stale() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/swr");
        let cached_response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-while-revalidate=30".to_string(),
            )],
            body: Bytes::from_static(b"stale"),
        };

        assert!(store.put(&req, cached_response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(matches!(
            store.get(&req).await?,
            CacheLookup::StaleWhileRevalidate(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn swr_entry_survives_unrelated_put_after_expiry() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/swr-cleanup");
        let cached_response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-while-revalidate=30".to_string(),
            )],
            body: Bytes::from_static(b"stale"),
        };

        assert!(store.put(&req, cached_response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(
            store
                .put(&request("/unrelated-swr"), response("fresh"))
                .await?
        );
        assert!(matches!(
            store.get(&req).await?,
            CacheLookup::StaleWhileRevalidate(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stale_if_error_lookup_serves_stale() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/sie");
        let cached_response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-if-error=30".to_string(),
            )],
            body: Bytes::from_static(b"stale"),
        };

        assert!(store.put(&req, cached_response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(matches!(
            store.get(&req).await?,
            CacheLookup::StaleIfError(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sie_entry_survives_unrelated_put_after_expiry() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/sie-cleanup");
        let cached_response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-if-error=30".to_string(),
            )],
            body: Bytes::from_static(b"stale"),
        };

        assert!(store.put(&req, cached_response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(
            store
                .put(&request("/unrelated-sie"), response("fresh"))
                .await?
        );
        assert!(matches!(
            store.get(&req).await?,
            CacheLookup::StaleIfError(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn bypass_stale_skips_stale_windows() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let mut req = request("/bypass");
        let response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-while-revalidate=30, stale-if-error=30".to_string(),
            )],
            body: Bytes::from_static(b"stale"),
        };

        assert!(store.put(&req, response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        req.bypass_stale = true;
        assert!(matches!(store.get(&req).await?, CacheLookup::Miss));
        Ok(())
    }

    #[tokio::test]
    async fn put_same_variant_replaces_previous_entry() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/replace");

        assert!(store.put(&req, response("first")).await?);
        assert!(store.put(&req, response("second")).await?);

        match store.get(&req).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.as_ref(), b"second"),
            other => panic!("expected fresh second value, got {:?}", other),
        }
        assert!(store.delete(&req).await?);
        assert!(matches!(store.get(&req).await?, CacheLookup::Miss));

        Ok(())
    }
}
