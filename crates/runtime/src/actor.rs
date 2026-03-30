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
pub struct ActorSnapshotEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub version: i64,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct ActorSnapshot {
    pub entries: Vec<ActorSnapshotEntry>,
    pub max_version: i64,
}

#[derive(Debug, Clone)]
pub struct ActorBatchMutation {
    pub key: String,
    pub value: Vec<u8>,
    pub encoding: String,
    pub version: i64,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct ActorBatchApplyResult {
    pub conflict: bool,
    pub max_version: i64,
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

    pub async fn snapshot(&self, namespace: &str, actor_key: &str) -> Result<ActorSnapshot> {
        let conn = self.connect(namespace, actor_key).await?;
        let mut rows = conn
            .query(
                "SELECT item_key, value_blob, encoding, value, version, deleted
                 FROM actor_state
                 WHERE entity_key = ?1
                 ORDER BY item_key ASC",
                (actor_key,),
            )
            .await
            .map_err(actor_error)?;

        let mut entries = Vec::new();
        let mut max_version = -1i64;
        while let Some(row) = rows.next().await.map_err(actor_error)? {
            let key: String = row.get::<String>(0).map_err(actor_error)?;
            let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(1).map_err(actor_error)?;
            let encoding: String = row.get::<String>(2).map_err(actor_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(actor_error)?;
            let version: i64 = row.get::<i64>(4).map_err(actor_error)?;
            let deleted: i64 = row.get::<i64>(5).map_err(actor_error)?;
            max_version = max_version.max(version);
            entries.push(ActorSnapshotEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
                version,
                deleted: deleted != 0,
            });
        }
        self.observe_version(max_version);
        Ok(ActorSnapshot {
            entries,
            max_version,
        })
    }

    pub async fn apply_batch(
        &self,
        namespace: &str,
        actor_key: &str,
        mutations: &[ActorBatchMutation],
        expected_base_version: Option<i64>,
    ) -> Result<ActorBatchApplyResult> {
        let conn = self.connect(namespace, actor_key).await?;
        if mutations.is_empty() {
            let max_version = self
                .max_version_for_actor(&conn, actor_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(max_version);
            return Ok(ActorBatchApplyResult {
                conflict: false,
                max_version,
            });
        }

        for mutation in mutations {
            if mutation.key.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "actor batch mutation key must not be empty",
                ));
            }
            if mutation.version < 0 {
                return Err(PlatformError::bad_request(
                    "actor batch mutation version must be non-negative",
                ));
            }
            if !mutation.deleted
                && mutation.encoding != ENCODING_UTF8
                && mutation.encoding != ENCODING_V8SC
            {
                return Err(PlatformError::bad_request(format!(
                    "unsupported actor storage encoding: {}",
                    mutation.encoding
                )));
            }
        }

        let mut previous_version = expected_base_version.unwrap_or(-1);
        for mutation in mutations {
            if mutation.version <= previous_version {
                return Err(PlatformError::bad_request(
                    "actor batch mutation versions must be strictly increasing",
                ));
            }
            previous_version = mutation.version;
        }

        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_locked_error(&error) && attempt < 8 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => return Err(actor_error(error)),
            }

            let outcome = async {
                let current = self.max_version_for_actor(&conn, actor_key).await?.unwrap_or(-1);
                if let Some(expected) = expected_base_version {
                    if current != expected {
                        return Ok(ActorBatchApplyResult {
                            conflict: true,
                            max_version: current,
                        });
                    }
                }

                for mutation in mutations {
                    let version = mutation.version;
                    let now_ms = epoch_ms_i64()?;
                    if mutation.deleted {
                        let empty_blob: &[u8] = &[];
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
                                mutation.key.as_str(),
                                empty_blob,
                                ENCODING_UTF8,
                                version,
                                now_ms,
                            ),
                        )
                        .await
                        .map_err(actor_error)?;
                        continue;
                    }

                    let value_text = if mutation.encoding == ENCODING_UTF8 {
                        std::str::from_utf8(&mutation.value)
                            .map_err(|error| {
                                PlatformError::bad_request(format!("invalid utf8 value: {error}"))
                            })?
                            .to_string()
                    } else {
                        String::new()
                    };
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
                            mutation.key.as_str(),
                            value_text.as_str(),
                            mutation.value.as_slice(),
                            mutation.encoding.as_str(),
                            version,
                            now_ms,
                        ),
                    )
                    .await
                    .map_err(actor_error)?;
                }

                let max_version = mutations.last().map(|mutation| mutation.version).unwrap_or(current);
                Ok(ActorBatchApplyResult {
                    conflict: false,
                    max_version,
                })
            }
            .await;

            match outcome {
                Ok(result) => {
                    if result.conflict {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        return Ok(result);
                    }
                    conn.execute("COMMIT", ()).await.map_err(actor_error)?;
                    self.observe_version(result.max_version);
                    return Ok(result);
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    return Err(error);
                }
            }
        }
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

    async fn max_version_for_actor(
        &self,
        conn: &Connection,
        actor_key: &str,
    ) -> Result<Option<i64>> {
        let mut rows = conn
            .query(
                "SELECT MAX(version) FROM actor_state
                 WHERE entity_key = ?1",
                (actor_key,),
            )
            .await
            .map_err(actor_error)?;
        if let Some(row) = rows.next().await.map_err(actor_error)? {
            let version = row.get::<Option<i64>>(0).map_err(actor_error)?;
            return Ok(version);
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

    fn observe_version(&self, version: i64) {
        if version < 0 {
            return;
        }
        let wanted = version as u64 + 1;
        let mut current = self.version.load(Ordering::SeqCst);
        while current < wanted {
            match self
                .version
                .compare_exchange(current, wanted, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
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

fn is_locked_error(error: &turso::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database is busy")
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
