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
        let store_dir = env::var("GRUGD_STORE_DIR").unwrap_or_else(|_| "./store".to_string());
        let database_url = env::var("TURSO_DATABASE_URL")
            .unwrap_or_else(|_| format!("file:{store_dir}/grugd-kv.db"));
        let local_path = database_url
            .strip_prefix("file:")
            .unwrap_or(&database_url)
            .to_string();
        ensure_parent_dir(&local_path)?;
        let database = Builder::new_local(&local_path)
            .build()
            .await
            .map_err(kv_error)?;
        let store = Self {
            database: Arc::new(database),
            version: Arc::new(AtomicU64::new(1)),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    pub async fn get(&self, worker_name: &str, binding: &str, key: &str) -> Result<Option<String>> {
        let conn = self.connect()?;
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
        let conn = self.connect()?;
        let version = self.next_version();
        let now_ms = epoch_ms_i64()?;
        conn.execute(
            "INSERT INTO worker_kv (worker_name, binding, key, value, deleted, version, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)
             ON CONFLICT(worker_name, binding, key) DO UPDATE SET
               value = excluded.value,
               deleted = 0,
               version = excluded.version,
               updated_at_ms = excluded.updated_at_ms
             WHERE excluded.version >= worker_kv.version",
            (worker_name, binding, key, value, version, now_ms),
        )
        .await
        .map_err(kv_error)?;
        Ok(())
    }

    pub async fn delete(&self, worker_name: &str, binding: &str, key: &str) -> Result<()> {
        let conn = self.connect()?;
        let version = self.next_version();
        let now_ms = epoch_ms_i64()?;
        conn.execute(
            "INSERT INTO worker_kv (worker_name, binding, key, value, deleted, version, updated_at_ms)
             VALUES (?1, ?2, ?3, '', 1, ?4, ?5)
             ON CONFLICT(worker_name, binding, key) DO UPDATE SET
               value = excluded.value,
               deleted = 1,
               version = excluded.version,
               updated_at_ms = excluded.updated_at_ms
             WHERE excluded.version >= worker_kv.version",
            (worker_name, binding, key, version, now_ms),
        )
        .await
        .map_err(kv_error)?;
        Ok(())
    }

    pub async fn list(
        &self,
        worker_name: &str,
        binding: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<KvEntry>> {
        let conn = self.connect()?;
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
        let conn = self.connect()?;
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

    fn connect(&self) -> Result<Connection> {
        self.database.connect().map_err(kv_error)
    }

    fn next_version(&self) -> i64 {
        self.version.fetch_add(1, Ordering::SeqCst) as i64
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

fn ensure_parent_dir(path: &str) -> Result<()> {
    let Some(parent) = Path::new(path).parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent)
        .map_err(|error| PlatformError::runtime(format!("kv error: {error}")))?;
    Ok(())
}
