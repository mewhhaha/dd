use common::{PlatformError, Result};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use turso::{Builder, Connection, Database, transaction::TransactionBehavior};

use crate::turso_util::{
    VersionFloor, checkpoint_database, configure_turso_connection, ensure_storage_migration_table,
    execute_cached, health_check_database, is_retryable_turso_error, query_cached,
    record_storage_retry, record_storage_schema_version, storage_schema_version,
};

const ENCODING_UTF8: &str = "utf8";
const KV_CONNECTION_LIMIT: usize = 32;
const KV_SCHEMA_VERSION: i64 = 1;

#[derive(Clone)]
pub struct KvStore {
    database: Arc<Database>,
    connections: Arc<Mutex<Vec<Connection>>>,
    connection_permits: Arc<Semaphore>,
    version: Arc<AtomicU64>,
}

struct KvConnectionGuard {
    connections: Arc<Mutex<Vec<Connection>>>,
    _permit: OwnedSemaphorePermit,
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
    pub async fn open_database(database_url: &str) -> Result<Arc<Database>> {
        let local_path = database_url
            .strip_prefix("file:")
            .unwrap_or(database_url)
            .to_string();
        ensure_parent_dir(Path::new(&local_path))?;
        let database = Builder::new_local(&local_path)
            .build()
            .await
            .map_err(kv_error)?;
        Ok(Arc::new(database))
    }

    pub async fn from_database(database: Arc<Database>) -> Result<Self> {
        migrate_kv_schema(&database).await?;
        let store = Self {
            database,
            connections: Arc::new(Mutex::new(Vec::new())),
            connection_permits: Arc::new(Semaphore::new(KV_CONNECTION_LIMIT)),
            version: Arc::new(AtomicU64::new(1)),
        };
        store.sync_version_counter_from_db().await?;
        Ok(store)
    }

    pub async fn checkpoint(&self) -> Result<()> {
        checkpoint_database(&self.database).await.map_err(kv_error)
    }

    pub async fn health_check(&self) -> Result<()> {
        health_check_database(&self.database)
            .await
            .map_err(kv_error)?;
        let conn = self.database.connect().map_err(kv_error)?;
        configure_connection(&conn).await?;
        let version = storage_schema_version(&conn, "kv")
            .await
            .map_err(kv_error)?;
        if version != KV_SCHEMA_VERSION {
            return Err(PlatformError::runtime(format!(
                "kv error: schema version {version} is not ready; expected {KV_SCHEMA_VERSION}"
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn shares_database_owner(&self, database: &Arc<Database>) -> bool {
        Arc::ptr_eq(&self.database, database)
    }

    pub async fn get_utf8(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
    ) -> Result<std::result::Result<String, KvUtf8Lookup>> {
        let conn = self.connect().await?;
        let mut rows = query_cached(
            &conn,
            "SELECT value, encoding, deleted
                 FROM worker_kv
                 WHERE worker_name = ?1 AND binding = ?2 AND key = ?3",
            (worker_name, binding, key),
        )
        .await
        .map_err(kv_error)?;
        let Some(row) = rows.next().await.map_err(kv_error)? else {
            return Ok(Err(KvUtf8Lookup::Missing));
        };
        let deleted: i64 = row.get::<i64>(2).map_err(kv_error)?;
        if deleted != 0 {
            return Ok(Err(KvUtf8Lookup::Missing));
        }
        let encoding: String = row.get::<String>(1).map_err(kv_error)?;
        if encoding != ENCODING_UTF8 {
            return Ok(Err(KvUtf8Lookup::WrongEncoding));
        }
        let value = row.get::<String>(0).map_err(kv_error)?;
        Ok(Ok(value))
    }

    pub async fn put(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
        value: &str,
    ) -> Result<i64> {
        self.commit_single_version(worker_name, binding, key, Some(value))
            .await
    }

    pub async fn delete(&self, worker_name: &str, binding: &str, key: &str) -> Result<i64> {
        self.commit_single_version(worker_name, binding, key, None)
            .await
    }

    async fn commit_single_version(
        &self,
        worker_name: &str,
        binding: &str,
        key: &str,
        value: Option<&str>,
    ) -> Result<i64> {
        const MAX_ATTEMPTS: usize = 8;
        let version = self.next_version();
        let mut conn = self.connect().await?;
        for attempt in 0..MAX_ATTEMPTS {
            let tx = match conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
            {
                Ok(tx) => tx,
                Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    sleep_kv_storage_retry(attempt).await;
                    continue;
                }
                Err(error) => return Err(kv_error_after_retry(error)),
            };
            let now_ms = epoch_ms_i64()?;
            let write = execute_cached(
                &tx,
                "INSERT INTO worker_kv (worker_name, binding, key, value, value_blob, encoding, deleted, version, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8)
                 ON CONFLICT(worker_name, binding, key) DO UPDATE SET
                   value = excluded.value,
                   value_blob = excluded.value_blob,
                   encoding = excluded.encoding,
                   deleted = excluded.deleted,
                   version = excluded.version,
                   updated_at_ms = excluded.updated_at_ms
                 WHERE excluded.version > worker_kv.version",
                (
                    worker_name,
                    binding,
                    key,
                    value.unwrap_or(""),
                    ENCODING_UTF8,
                    if value.is_some() { 0 } else { 1 },
                    version,
                    now_ms,
                ),
            )
            .await;
            if let Err(error) = write {
                let _ = tx.rollback().await;
                if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS {
                    sleep_kv_storage_retry(attempt).await;
                    continue;
                }
                return Err(kv_error_after_retry(error));
            }
            match tx.commit().await {
                Ok(()) => return Ok(version),
                Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    sleep_kv_storage_retry(attempt).await;
                }
                Err(error) => return Err(kv_error_after_retry(error)),
            }
        }
        Err(PlatformError::storage_unavailable(
            "kv error: write failed after retries",
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
        let mut rows = query_cached(
            &conn,
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
                encoding,
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    async fn ensure_schema(&self) -> Result<()> {
        migrate_kv_schema(&self.database).await
    }

    async fn sync_version_counter_from_db(&self) -> Result<()> {
        let conn = self.connect().await?;
        let mut rows = query_cached(&conn, "SELECT COALESCE(MAX(version), 0) FROM worker_kv", ())
            .await
            .map_err(kv_error)?;
        let max_version = if let Some(row) = rows.next().await.map_err(kv_error)? {
            row.get::<i64>(0).map_err(kv_error)?
        } else {
            0
        };
        self.set_version_floor(max_version.saturating_add(1).max(1) as u64);
        Ok(())
    }

    async fn connect(&self) -> Result<KvConnectionGuard> {
        let permit = Arc::clone(&self.connection_permits)
            .acquire_owned()
            .await
            .map_err(|_| PlatformError::runtime("kv connection pool is closed"))?;
        if let Some(conn) = self
            .connections
            .lock()
            .expect("kv connection pool lock poisoned")
            .pop()
        {
            return Ok(KvConnectionGuard {
                connections: Arc::clone(&self.connections),
                _permit: permit,
                conn: Some(conn),
            });
        }
        let conn = self.database.connect().map_err(kv_error)?;
        configure_connection(&conn).await?;
        Ok(KvConnectionGuard {
            connections: Arc::clone(&self.connections),
            _permit: permit,
            conn: Some(conn),
        })
    }

    fn next_version(&self) -> i64 {
        VersionFloor::next_i64(&self.version)
    }

    fn set_version_floor(&self, floor: u64) {
        VersionFloor::set_floor(&self.version, floor);
    }
}

async fn migrate_kv_schema(database: &Database) -> Result<()> {
    const MAX_ATTEMPTS: usize = 8;

    let mut conn = database.connect().map_err(kv_error)?;
    configure_connection(&conn).await?;
    conn.pragma_update("journal_mode", "'WAL'")
        .await
        .map_err(kv_error)?;
    let applied_at_ms = epoch_ms_i64()?;

    for attempt in 0..MAX_ATTEMPTS {
        let tx = match conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
        {
            Ok(tx) => tx,
            Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                sleep_kv_storage_retry(attempt).await;
                continue;
            }
            Err(error) => return Err(kv_error_after_retry(error)),
        };

        match migrate_kv_schema_transaction(&tx, applied_at_ms).await {
            Ok(()) => match tx.commit().await {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    sleep_kv_storage_retry(attempt).await;
                }
                Err(error) => return Err(kv_error_after_retry(error)),
            },
            Err(error) => {
                let retryable = is_retryable_turso_error(&error);
                let _ = tx.rollback().await;
                if retryable && attempt + 1 < MAX_ATTEMPTS {
                    sleep_kv_storage_retry(attempt).await;
                    continue;
                }
                return Err(kv_error_after_retry(error));
            }
        }
    }

    Err(PlatformError::storage_unavailable(
        "kv error: schema migration failed after retries",
    ))
}

async fn migrate_kv_schema_transaction(conn: &Connection, applied_at_ms: i64) -> turso::Result<()> {
    ensure_storage_migration_table(conn).await?;
    let applied_version = storage_schema_version(conn, "kv").await?;
    if applied_version > KV_SCHEMA_VERSION {
        return Err(turso::Error::Error(format!(
            "unsupported kv schema version {applied_version}; maximum supported version is {KV_SCHEMA_VERSION}"
        )));
    }

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
    .await?;
    ensure_compat_columns(conn).await?;
    conn.execute("DROP INDEX IF EXISTS idx_worker_kv_lookup", ())
        .await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_worker_kv_list
         ON worker_kv(worker_name, binding, deleted, key)",
        (),
    )
    .await?;
    record_storage_schema_version(conn, "kv", KV_SCHEMA_VERSION, applied_at_ms).await?;
    Ok(())
}

async fn sleep_kv_storage_retry(attempt: usize) {
    record_storage_retry();
    tokio::time::sleep(Duration::from_millis(5 * (attempt + 1) as u64)).await;
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

fn kv_error_after_retry(error: turso::Error) -> PlatformError {
    if is_retryable_turso_error(&error) {
        PlatformError::storage_unavailable(format!("kv error: {error}"))
    } else {
        kv_error(error)
    }
}

async fn configure_connection(conn: &Connection) -> Result<()> {
    configure_turso_connection(conn, kv_error)?;
    conn.pragma_update("synchronous", "'FULL'")
        .await
        .map_err(kv_error)?;
    Ok(())
}

async fn ensure_compat_columns(conn: &Connection) -> turso::Result<()> {
    let mut rows = conn.query("PRAGMA table_info(worker_kv)", ()).await?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await? {
        let name: String = row.get::<String>(1)?;
        columns.insert(name);
    }

    if !columns.contains("value_blob") {
        conn.execute("ALTER TABLE worker_kv ADD COLUMN value_blob BLOB", ())
            .await?;
    }
    if !columns.contains("encoding") {
        conn.execute(
            "ALTER TABLE worker_kv ADD COLUMN encoding TEXT NOT NULL DEFAULT 'utf8'",
            (),
        )
        .await?;
    }
    Ok(())
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
        let database = KvStore::open_database(&path.to_string_lossy()).await?;
        KvStore::from_database(database).await
    }

    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-kv-{name}-{}.db", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn connection_pool_is_bounded() -> Result<()> {
        let path = temp_db_path("connection-pool-bound");
        let store = test_store(&path).await?;

        let mut guards = Vec::new();
        for _ in 0..KV_CONNECTION_LIMIT {
            guards.push(store.connect().await?);
        }

        assert!(
            tokio::time::timeout(Duration::from_millis(25), store.connect())
                .await
                .is_err(),
            "connection request should wait once the pool limit is reached"
        );

        guards.pop();
        tokio::time::timeout(Duration::from_secs(1), store.connect())
            .await
            .expect("connection should become available after a guard is dropped")?;
        Ok(())
    }

    #[tokio::test]
    async fn kv_connections_use_full_synchronous_durability() -> Result<()> {
        let path = temp_db_path("full-synchronous");
        let store = test_store(&path).await?;
        let conn = store.connect().await?;
        let mut rows = conn
            .query("PRAGMA synchronous", ())
            .await
            .map_err(kv_error)?;
        let row = rows
            .next()
            .await
            .map_err(kv_error)?
            .expect("synchronous row");
        assert_eq!(row.get::<i64>(0).map_err(kv_error)?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn legacy_schema_migrates_transactionally_and_preserves_version_floor() -> Result<()> {
        let path = temp_db_path("legacy-schema");
        ensure_parent_dir(&path)?;
        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(kv_error)?;
        let conn = database.connect().map_err(kv_error)?;
        configure_connection(&conn).await?;
        conn.execute(
            "CREATE TABLE worker_kv (
               worker_name TEXT NOT NULL,
               binding TEXT NOT NULL,
               key TEXT NOT NULL,
               value TEXT NOT NULL,
               deleted INTEGER NOT NULL DEFAULT 0,
               version INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL,
               PRIMARY KEY (worker_name, binding, key)
             )",
            (),
        )
        .await
        .map_err(kv_error)?;
        conn.execute(
            "INSERT INTO worker_kv
               (worker_name, binding, key, value, deleted, version, updated_at_ms)
             VALUES ('worker-a', 'MY_KV', 'legacy', 'preserved', 0, 41, 1)",
            (),
        )
        .await
        .map_err(kv_error)?;
        conn.execute(
            "CREATE INDEX idx_worker_kv_lookup
             ON worker_kv(worker_name, binding, key)",
            (),
        )
        .await
        .map_err(kv_error)?;
        drop(conn);
        drop(database);

        let store = test_store(&path).await?;
        assert_eq!(
            store.get_utf8("worker-a", "MY_KV", "legacy").await?,
            Ok("preserved".to_string())
        );
        assert!(store.put("worker-a", "MY_KV", "next", "value").await? > 41);

        let conn = store.connect().await?;
        let mut rows = conn
            .query(
                "SELECT MAX(version) FROM dd_storage_schema_migrations WHERE component = 'kv'",
                (),
            )
            .await
            .map_err(kv_error)?;
        assert_eq!(
            rows.next()
                .await
                .map_err(kv_error)?
                .expect("migration version row")
                .get::<i64>(0)
                .map_err(kv_error)?,
            KV_SCHEMA_VERSION
        );
        drop(rows);
        let mut columns = conn
            .query("PRAGMA table_info(worker_kv)", ())
            .await
            .map_err(kv_error)?;
        let mut names = HashSet::new();
        while let Some(row) = columns.next().await.map_err(kv_error)? {
            names.insert(row.get::<String>(1).map_err(kv_error)?);
        }
        assert!(names.contains("value_blob"));
        assert!(names.contains("encoding"));
        Ok(())
    }

    #[tokio::test]
    async fn current_schema_without_migration_metadata_is_adopted() -> Result<()> {
        let path = temp_db_path("current-schema-adoption");
        let store = test_store(&path).await?;
        store
            .put("worker-a", "MY_KV", "preserved", "current")
            .await?;
        drop(store);

        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(kv_error)?;
        let conn = database.connect().map_err(kv_error)?;
        conn.execute(
            "DELETE FROM dd_storage_schema_migrations WHERE component = 'kv'",
            (),
        )
        .await
        .map_err(kv_error)?;
        drop(conn);
        drop(database);

        let store = test_store(&path).await?;
        assert_eq!(
            store.get_utf8("worker-a", "MY_KV", "preserved").await?,
            Ok("current".to_string())
        );
        store.health_check().await?;
        Ok(())
    }

    #[tokio::test]
    async fn future_schema_version_is_rejected_without_mutating_data() -> Result<()> {
        let path = temp_db_path("future-schema");
        let store = test_store(&path).await?;
        store
            .put("worker-a", "MY_KV", "preserved", "future-guard")
            .await?;
        drop(store);

        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(kv_error)?;
        let conn = database.connect().map_err(kv_error)?;
        conn.execute(
            "INSERT INTO dd_storage_schema_migrations (component, version, applied_at_ms)
             VALUES ('kv', ?1, 1)",
            (KV_SCHEMA_VERSION + 1,),
        )
        .await
        .map_err(kv_error)?;
        drop(conn);
        drop(database);

        let error = match test_store(&path).await {
            Ok(_) => panic!("future KV schema must fail startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unsupported kv schema version"));

        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(kv_error)?;
        let conn = database.connect().map_err(kv_error)?;
        let mut rows = conn
            .query(
                "SELECT value FROM worker_kv
                 WHERE worker_name = 'worker-a' AND binding = 'MY_KV' AND key = 'preserved'",
                (),
            )
            .await
            .map_err(kv_error)?;
        assert_eq!(
            rows.next()
                .await
                .map_err(kv_error)?
                .expect("preserved row")
                .get::<String>(0)
                .map_err(kv_error)?,
            "future-guard"
        );
        Ok(())
    }

    #[tokio::test]
    async fn schema_migration_drops_redundant_lookup_index() -> Result<()> {
        let path = temp_db_path("drop-redundant-index");
        let store = test_store(&path).await?;
        let conn = store.connect().await?;
        conn.execute(
            "CREATE INDEX idx_worker_kv_lookup ON worker_kv(worker_name, binding, key)",
            (),
        )
        .await
        .map_err(kv_error)?;
        drop(conn);

        store.ensure_schema().await?;

        let conn = store.connect().await?;
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                ("idx_worker_kv_lookup",),
            )
            .await
            .map_err(kv_error)?;
        let row = rows.next().await.map_err(kv_error)?.expect("count row");
        assert_eq!(row.get::<i64>(0).map_err(kv_error)?, 0);
        drop(rows);
        drop(conn);

        store.put("worker-a", "MY_KV", "key", "value").await?;
        assert_eq!(
            store.get_utf8("worker-a", "MY_KV", "key").await?,
            Ok("value".to_string())
        );
        Ok(())
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

        assert!(
            store
                .get_utf8("worker-a", "MY_KV", "hot-key")
                .await?
                .is_ok(),
            "hot-key should exist after concurrent writes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn version_counter_is_restored_from_disk() -> Result<()> {
        let path = temp_db_path("version-restore");
        let store = test_store(&path).await?;
        let first_version = store.put("worker-a", "MY_KV", "k", "v1").await?;
        let second_version = store.put("worker-a", "MY_KV", "k", "v2").await?;
        assert!(
            second_version > first_version,
            "versions should be monotonic before restart"
        );
        drop(store);

        let restored = test_store(&path).await?;
        let restored_version = restored.put("worker-a", "MY_KV", "k", "v3").await?;
        assert!(
            restored_version > second_version,
            "version floor should be restored before accepting post-restart writes"
        );
        assert_eq!(
            restored.get_utf8("worker-a", "MY_KV", "k").await?,
            Ok("v3".to_string())
        );
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

        assert!(
            store_a
                .get_utf8("worker-a", "MY_KV", "same-key")
                .await?
                .is_ok(),
            "key should be present after multi-store contention"
        );
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
    async fn get_utf8_reports_wrong_encoding_for_non_utf8_values_on_disk() -> Result<()> {
        let path = temp_db_path("utf8-lookup-wrong-encoding");
        let store = test_store(&path).await?;
        let conn = store.connect().await?;
        conn.execute(
            "INSERT INTO worker_kv
               (worker_name, binding, key, value, value_blob, encoding, deleted, version, updated_at_ms)
             VALUES ('worker-a', 'MY_KV', 'obj', '', ?1, 'v8sc', 0, 1, 1)",
            (&[1u8, 2, 3][..],),
        )
        .await
        .map_err(kv_error)?;
        drop(conn);

        let lookup = store.get_utf8("worker-a", "MY_KV", "obj").await?;
        assert_eq!(lookup, Err(KvUtf8Lookup::WrongEncoding));
        Ok(())
    }

    #[tokio::test]
    async fn delete_hides_value_from_get_utf8_and_list() -> Result<()> {
        let path = temp_db_path("delete-hides-value");
        let store = test_store(&path).await?;
        let put_version = store.put("worker-a", "MY_KV", "gone", "value").await?;
        let delete_version = store.delete("worker-a", "MY_KV", "gone").await?;
        assert!(delete_version > put_version);

        assert_eq!(
            store.get_utf8("worker-a", "MY_KV", "gone").await?,
            Err(KvUtf8Lookup::Missing)
        );
        assert!(store.list("worker-a", "MY_KV", "", 10).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn list_returns_prefix_matches_in_key_order_up_to_limit() -> Result<()> {
        let path = temp_db_path("list-prefix");
        let store = test_store(&path).await?;
        store.put("worker-a", "MY_KV", "a/1", "one").await?;
        store.put("worker-a", "MY_KV", "a/2", "two").await?;
        store.put("worker-a", "MY_KV", "b/1", "other").await?;

        let entries = store.list("worker-a", "MY_KV", "a/", 10).await?;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a/1", "a/2"]
        );
        assert_eq!(entries[0].value, b"one");
        assert_eq!(entries[0].encoding, ENCODING_UTF8);

        let limited = store.list("worker-a", "MY_KV", "", 2).await?;
        assert_eq!(limited.len(), 2);
        Ok(())
    }
}
