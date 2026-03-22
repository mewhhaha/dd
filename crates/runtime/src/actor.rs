use common::{PlatformError, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database};

#[derive(Clone)]
pub struct ActorStore {
    root_dir: Arc<PathBuf>,
    shards_per_namespace: usize,
    databases: Arc<Mutex<HashMap<String, Arc<Database>>>>,
    version: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct ActorStateValue {
    pub value: Vec<u8>,
    pub encoding: String,
    pub version: i64,
}

#[derive(Debug, Clone)]
pub struct ActorStateEntry {
    pub key: String,
    pub value: String,
    pub version: i64,
}

#[derive(Debug, Clone)]
pub struct ActorWriteResult {
    pub conflict: bool,
    pub version: i64,
}

impl ActorStore {
    pub async fn new(root_dir: PathBuf, shards_per_namespace: usize) -> Result<Self> {
        std::fs::create_dir_all(&root_dir).map_err(actor_error)?;
        if shards_per_namespace == 0 {
            return Err(PlatformError::internal(
                "actor_shards_per_namespace must be greater than 0",
            ));
        }
        Ok(Self {
            root_dir: Arc::new(root_dir),
            shards_per_namespace,
            databases: Arc::new(Mutex::new(HashMap::new())),
            version: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn get(
        &self,
        namespace: &str,
        actor_key: &str,
        key: &str,
    ) -> Result<Option<ActorStateValue>> {
        let conn = self.connect(namespace, actor_key).await?;
        let mut rows = conn
            .query(
                "SELECT value_blob, encoding, value, version, deleted
                 FROM actor_state
                 WHERE entity_key = ?1 AND item_key = ?2",
                (actor_key, key),
            )
            .await
            .map_err(actor_error)?;
        if let Some(row) = rows.next().await.map_err(actor_error)? {
            let deleted: i64 = row.get::<i64>(4).map_err(actor_error)?;
            if deleted != 0 {
                return Ok(None);
            }
            let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(0).map_err(actor_error)?;
            let encoding: String = row.get::<String>(1).map_err(actor_error)?;
            let legacy_value: String = row.get::<String>(2).map_err(actor_error)?;
            let version: i64 = row.get::<i64>(3).map_err(actor_error)?;
            let value = value_blob.unwrap_or_else(|| legacy_value.into_bytes());
            return Ok(Some(ActorStateValue {
                value,
                encoding: normalize_encoding(&encoding),
                version,
            }));
        }
        Ok(None)
    }

    pub async fn put(
        &self,
        namespace: &str,
        actor_key: &str,
        key: &str,
        value: &str,
        expected_version: Option<i64>,
    ) -> Result<ActorWriteResult> {
        self.put_value(
            namespace,
            actor_key,
            key,
            value.as_bytes(),
            ENCODING_UTF8,
            expected_version,
        )
        .await
    }

    pub async fn put_value(
        &self,
        namespace: &str,
        actor_key: &str,
        key: &str,
        value: &[u8],
        encoding: &str,
        expected_version: Option<i64>,
    ) -> Result<ActorWriteResult> {
        if encoding != ENCODING_UTF8 && encoding != ENCODING_V8SC {
            return Err(PlatformError::bad_request(format!(
                "unsupported actor storage encoding: {encoding}"
            )));
        }
        let conn = self.connect(namespace, actor_key).await?;
        let version = self.next_version();
        let now_ms = epoch_ms_i64()?;
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

        let affected = if let Some(expected_version) = expected_version {
            execute_with_retry(|| {
                conn.execute(
                    "UPDATE actor_state
                     SET value = ?1, value_blob = ?2, encoding = ?3, deleted = 0, version = ?4, updated_at_ms = ?5
                     WHERE entity_key = ?6 AND item_key = ?7 AND version = ?8",
                    (
                        value_text.as_str(),
                        value_blob.as_slice(),
                        encoding.as_str(),
                        version,
                        now_ms,
                        actor_key,
                        key,
                        expected_version,
                    ),
                )
            })
            .await?
        } else {
            execute_with_retry(|| {
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
                        key,
                        value_text.as_str(),
                        value_blob.as_slice(),
                        encoding.as_str(),
                        version,
                        now_ms,
                    ),
                )
            })
            .await?
        };

        if affected == 0 {
            let current_version = self
                .current_version(&conn, actor_key, key)
                .await?
                .unwrap_or(-1);
            return Ok(ActorWriteResult {
                conflict: true,
                version: current_version,
            });
        }

        Ok(ActorWriteResult {
            conflict: false,
            version,
        })
    }

    pub async fn delete(
        &self,
        namespace: &str,
        actor_key: &str,
        key: &str,
        expected_version: Option<i64>,
    ) -> Result<ActorWriteResult> {
        let conn = self.connect(namespace, actor_key).await?;
        let version = self.next_version();
        let now_ms = epoch_ms_i64()?;
        let empty_blob: &[u8] = &[];

        let affected = if let Some(expected_version) = expected_version {
            execute_with_retry(|| {
                conn.execute(
                    "UPDATE actor_state
                     SET value = '', value_blob = ?1, encoding = ?2, deleted = 1, version = ?3, updated_at_ms = ?4
                     WHERE entity_key = ?5 AND item_key = ?6 AND version = ?7",
                    (
                        empty_blob,
                        ENCODING_UTF8,
                        version,
                        now_ms,
                        actor_key,
                        key,
                        expected_version,
                    ),
                )
            })
            .await?
        } else {
            execute_with_retry(|| {
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
                        key,
                        empty_blob,
                        ENCODING_UTF8,
                        version,
                        now_ms,
                    ),
                )
            })
            .await?
        };

        if affected == 0 {
            let current_version = self
                .current_version(&conn, actor_key, key)
                .await?
                .unwrap_or(-1);
            return Ok(ActorWriteResult {
                conflict: true,
                version: current_version,
            });
        }

        Ok(ActorWriteResult {
            conflict: false,
            version,
        })
    }

    pub async fn list(
        &self,
        namespace: &str,
        actor_key: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<ActorStateEntry>> {
        let conn = self.connect(namespace, actor_key).await?;
        let pattern = format!("{prefix}%");
        let mut rows = conn
            .query(
                "SELECT item_key, value, version
                 FROM actor_state
                 WHERE entity_key = ?1 AND deleted = 0 AND (encoding = ?2 OR encoding IS NULL) AND item_key LIKE ?3
                 ORDER BY item_key ASC
                 LIMIT ?4",
                (actor_key, ENCODING_UTF8, pattern, limit as i64),
            )
            .await
            .map_err(actor_error)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(actor_error)? {
            out.push(ActorStateEntry {
                key: row.get::<String>(0).map_err(actor_error)?,
                value: row.get::<String>(1).map_err(actor_error)?,
                version: row.get::<i64>(2).map_err(actor_error)?,
            });
        }
        Ok(out)
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

        let shard = self.shard_for(actor_key);
        let db_key = format!("{namespace}:{shard}");
        if let Some(existing) = self.databases.lock().await.get(&db_key).cloned() {
            let conn = existing.connect().map_err(actor_error)?;
            configure_connection(&conn).await?;
            return Ok(conn);
        }

        let path = self.db_path(namespace, shard);
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
            databases
                .entry(db_key)
                .or_insert_with(|| database.clone())
                .clone()
        };
        let conn = database.connect().map_err(actor_error)?;
        configure_connection(&conn).await?;
        Ok(conn)
    }

    async fn current_version(
        &self,
        conn: &Connection,
        actor_key: &str,
        key: &str,
    ) -> Result<Option<i64>> {
        let mut rows = conn
            .query(
                "SELECT version FROM actor_state
                 WHERE entity_key = ?1 AND item_key = ?2",
                (actor_key, key),
            )
            .await
            .map_err(actor_error)?;
        if let Some(row) = rows.next().await.map_err(actor_error)? {
            let version: i64 = row.get::<i64>(0).map_err(actor_error)?;
            return Ok(Some(version));
        }
        Ok(None)
    }

    fn shard_for(&self, actor_key: &str) -> usize {
        let hash = fnv1a64(actor_key.as_bytes());
        (hash as usize) % self.shards_per_namespace
    }

    fn db_path(&self, namespace: &str, shard: usize) -> PathBuf {
        let encoded_namespace = hex_encode(namespace.as_bytes());
        self.root_dir
            .join(encoded_namespace)
            .join(format!("shard-{shard:04}.db"))
    }

    fn next_version(&self) -> i64 {
        self.version.fetch_add(1, Ordering::SeqCst) as i64
    }
}

async fn ensure_schema(database: &Database) -> Result<()> {
    let conn = database.connect().map_err(actor_error)?;
    configure_connection(&conn).await?;
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
                return Err(actor_error(error));
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

fn fnv1a64(input: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in input {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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
