use common::{PlatformError, Result};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use turso::{Builder, Connection, Database};

const ENCODING_UTF8: &str = "utf8";
const ENCODING_V8SC: &str = "v8sc";

#[derive(Clone)]
pub struct KvStore {
    database: Arc<Database>,
    connections: Arc<Mutex<Vec<Connection>>>,
    version: Arc<AtomicU64>,
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
        };
        store.ensure_schema().await?;
        store.sync_version_counter_from_db().await?;
        Ok(store)
    }

    pub async fn get(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
    ) -> Result<Option<KvValue>> {
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
            return Ok(Some(KvValue {
                value,
                encoding: normalize_encoding(&encoding),
            }));
        }
        Ok(None)
    }

    pub async fn get_utf8(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
    ) -> Result<std::result::Result<String, KvUtf8Lookup>> {
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
                return Ok(Err(KvUtf8Lookup::WrongEncoding));
            }
            return Ok(Ok(row.get::<String>(0).map_err(kv_error)?));
        }
        Ok(Err(KvUtf8Lookup::Missing))
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
}
