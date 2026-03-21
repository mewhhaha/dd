use common::{PlatformError, Result};
use std::env;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use turso::{Builder, Connection, Database};

#[derive(Clone)]
pub struct KvStore {
    database: Arc<Database>,
    version: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: String,
}

impl KvStore {
    pub async fn from_env() -> Result<Self> {
        let store_dir = env::var("DD_STORE_DIR").unwrap_or_else(|_| "./store".to_string());
        let database_url =
            env::var("TURSO_DATABASE_URL").unwrap_or_else(|_| format!("file:{store_dir}/dd-kv.db"));
        let local_path = database_url
            .strip_prefix("file:")
            .unwrap_or(&database_url)
            .to_string();
        ensure_parent_dir(Path::new(&local_path))?;
        let database = Builder::new_local(&local_path)
            .build()
            .await
            .map_err(kv_error)?;
        let store = Self {
            database: Arc::new(database),
            version: Arc::new(AtomicU64::new(1)),
        };
        store.ensure_schema().await?;
        store.sync_version_counter_from_db().await?;
        Ok(store)
    }

    pub async fn get(&self, worker_name: &str, binding: &str, key: &str) -> Result<Option<String>> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT value, deleted FROM worker_kv WHERE worker_name = ?1 AND binding = ?2 AND key = ?3",
                (worker_name, binding, key),
            )
            .await
            .map_err(kv_error)?;
        if let Some(row) = rows.next().await.map_err(kv_error)? {
            let deleted: i64 = row.get::<i64>(1).map_err(kv_error)?;
            if deleted != 0 {
                return Ok(None);
            }
            let value: String = row.get::<String>(0).map_err(kv_error)?;
            return Ok(Some(value));
        }
        Ok(None)
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
                    "INSERT INTO worker_kv (worker_name, binding, key, value, deleted, version, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)
                     ON CONFLICT(worker_name, binding, key) DO UPDATE SET
                       value = excluded.value,
                       deleted = 0,
                       version = excluded.version,
                       updated_at_ms = excluded.updated_at_ms
                     WHERE excluded.version > worker_kv.version",
                    (worker_name, binding, key, value, version, now_ms),
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
                    "INSERT INTO worker_kv (worker_name, binding, key, value, deleted, version, updated_at_ms)
                     VALUES (?1, ?2, ?3, '', 1, ?4, ?5)
                     ON CONFLICT(worker_name, binding, key) DO UPDATE SET
                       value = excluded.value,
                       deleted = 1,
                       version = excluded.version,
                       updated_at_ms = excluded.updated_at_ms
                     WHERE excluded.version > worker_kv.version",
                    (worker_name, binding, key, version, now_ms),
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
                "SELECT key, value FROM worker_kv
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
            let value: String = row.get::<String>(1).map_err(kv_error)?;
            out.push(KvEntry { key, value });
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

    async fn connect(&self) -> Result<Connection> {
        let conn = self.database.connect().map_err(kv_error)?;
        configure_connection(&conn).await?;
        Ok(conn)
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
            version: Arc::new(AtomicU64::new(1)),
        };
        store.ensure_schema().await?;
        store.sync_version_counter_from_db().await?;
        Ok(store)
    }

    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-kv-{name}-{}.db", Uuid::new_v4()))
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
        assert_eq!(value, Some("v3".to_string()));
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
}
