use common::{PlatformError, Result};
use serde::Serialize;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use turso::{Builder, Connection, Database, Value};

const ENCODING_UTF8: &str = "utf8";
const ENCODING_V8SC: &str = "v8sc";

#[derive(Clone)]
pub struct KvStore {
    database: Arc<Database>,
    connections: Arc<Mutex<Vec<Connection>>>,
    version: Arc<AtomicU64>,
    profile: Arc<KvProfile>,
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
        }
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
        let store = Self {
            database: Arc::new(database),
            connections: Arc::new(Mutex::new(Vec::new())),
            version: Arc::new(AtomicU64::new(1)),
            profile: Arc::new(KvProfile::default()),
        };
        store.ensure_schema().await?;
        store.sync_version_counter_from_db().await?;
        Ok(store)
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

    pub async fn set(
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

    pub async fn set_value(
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
                let message = error.to_string().to_ascii_lowercase();
                let is_locked = message.contains("database is locked")
                    || message.contains("database table is locked")
                    || message.contains("database is busy");
                if is_locked && attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                return Err(kv_error(error));
            }
        }
    }
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
        let store = KvStore {
            database: Arc::new(database),
            connections: Arc::new(Mutex::new(Vec::new())),
            version: Arc::new(AtomicU64::new(1)),
            profile: Arc::new(KvProfile::default()),
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
    async fn concurrent_set_writes_succeed() -> Result<()> {
        let path = temp_db_path("concurrent-set");
        let store = test_store(&path).await?;

        let mut tasks = Vec::new();
        for idx in 0..64usize {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .set("worker-a", "MY_KV", "hot-key", &format!("value-{idx}"))
                    .await
            }));
        }
        for task in tasks {
            let result = task.await.expect("task join should succeed");
            assert!(result.is_ok(), "kv set should succeed under contention");
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
        store.set("worker-a", "MY_KV", "k", "v1").await?;
        store.set("worker-a", "MY_KV", "k", "v2").await?;
        drop(store);

        let restored = test_store(&path).await?;
        restored.set("worker-a", "MY_KV", "k", "v3").await?;
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
                a.set("worker-a", "MY_KV", "same-key", &format!("a-{idx}"))
                    .await
            }));
            let b = store_b.clone();
            tasks.push(tokio::spawn(async move {
                b.set("worker-a", "MY_KV", "same-key", &format!("b-{idx}"))
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
    async fn set_value_roundtrips_structured_payload() -> Result<()> {
        let path = temp_db_path("typed-roundtrip");
        let store = test_store(&path).await?;
        let payload = vec![1u8, 7, 9, 11];
        store
            .set_value("worker-a", "MY_KV", "typed", &payload, ENCODING_V8SC)
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
    async fn utf8_set_uses_text_fast_path_without_blob_duplication() -> Result<()> {
        let path = temp_db_path("utf8-fast-path");
        let store = test_store(&path).await?;
        store.set("worker-a", "MY_KV", "greeting", "hello").await?;

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
            .set_value("worker-a", "MY_KV", "obj", &[1, 2, 3], ENCODING_V8SC)
            .await?;

        let lookup = store.get_utf8("worker-a", "MY_KV", "obj").await?;
        assert_eq!(lookup, Err(KvUtf8Lookup::WrongEncoding));
        Ok(())
    }

    #[tokio::test]
    async fn get_utf8_many_preserves_order_and_encoding_state() -> Result<()> {
        let path = temp_db_path("utf8-many");
        let store = test_store(&path).await?;
        store.set("worker-a", "MY_KV", "a", "one").await?;
        store
            .set_value("worker-a", "MY_KV", "b", &[1, 2, 3], ENCODING_V8SC)
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
