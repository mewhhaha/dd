use common::{PlatformError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use turso::{Builder, Connection, Database};

use crate::turso_util::{
    configure_turso_connection, execute_cached, is_retryable_turso_error, query_cached,
    record_storage_retry,
};

pub const STATE_SHARDS: usize = 32;
const QUEUE_COMMANDS: usize = 4096;
const QUEUE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MANIFEST: &str = "state-layout.json";
pub(crate) const COMPLETE: &str = "state-complete";

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Manifest {
    format: u32,
    shards: usize,
    routing: String,
}

pub struct StateStore {
    shards: Vec<StateShard>,
    _lock: std::fs::File,
    owner_epoch: AtomicI64,
    leases: Mutex<EntityLeases>,
    lease_admissions: Arc<Semaphore>,
}

type EntityLeases = HashMap<(String, String), Weak<tokio::sync::Mutex<()>>>;

#[derive(Debug, Clone, Default, Serialize)]
pub struct StatePerformanceSnapshot {
    pub committed_groups: u64,
    pub committed_commands: u64,
    pub rollbacks: u64,
    pub discarded_connections: u64,
    pub busy_retries: u64,
    pub pending_commands: usize,
    pub pending_bytes: usize,
}

#[derive(Default)]
struct WriterMetrics {
    committed_groups: AtomicU64,
    committed_commands: AtomicU64,
    rollbacks: AtomicU64,
    discarded_connections: AtomicU64,
    busy_retries: AtomicU64,
}

struct StateShard {
    database: Arc<Database>,
    readers: Arc<ReadPool>,
    sender: Option<mpsc::Sender<WriteCommand>>,
    writer: Option<JoinHandle<()>>,
    bytes: Arc<Semaphore>,
    commands: Arc<Semaphore>,
    epoch: Arc<AtomicU64>,
    metrics: Arc<WriterMetrics>,
}

struct ReadPool {
    idle: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
}

pub(crate) struct ReadConnection {
    conn: Option<Connection>,
    pool: Arc<ReadPool>,
    _permit: OwnedSemaphorePermit,
}

impl std::ops::Deref for ReadConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn.as_ref().expect("checked out state reader")
    }
}

impl Drop for ReadConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool
                .idle
                .lock()
                .expect("state readers lock poisoned")
                .push(conn);
        }
    }
}

pub(crate) enum WriteError {
    Database(turso::Error),
    Rejected(PlatformError),
}

impl From<turso::Error> for WriteError {
    fn from(error: turso::Error) -> Self {
        Self::Database(error)
    }
}
impl From<PlatformError> for WriteError {
    fn from(error: PlatformError) -> Self {
        Self::Rejected(error)
    }
}
impl From<WriteError> for PlatformError {
    fn from(error: WriteError) -> Self {
        match error {
            WriteError::Database(error) if is_retryable_turso_error(&error) => {
                PlatformError::storage_unavailable(format!(
                    "state writer exhausted lock retries: {error}"
                ))
            }
            WriteError::Database(error) => storage_error(error),
            WriteError::Rejected(error) => error,
        }
    }
}

pub(crate) type WriteFuture<'a, T> =
    Pin<Box<dyn Future<Output = std::result::Result<T, WriteError>> + 'a>>;
type WriteReply = Box<dyn Any + Send>;
type WriteOperation =
    Box<dyn for<'a> Fn(&'a Connection, i64) -> WriteFuture<'a, WriteReply> + Send>;
struct WriteCommand {
    operation: WriteOperation,
    reply: oneshot::Sender<Result<WriteReply>>,
    _bytes: OwnedSemaphorePermit,
    _command: OwnedSemaphorePermit,
}

impl StateStore {
    pub async fn open(root: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open_layout(root.as_ref(), false).await
    }

    pub(crate) async fn create_conversion(root: &Path) -> Result<Arc<Self>> {
        Self::open_layout(root, true).await
    }

    async fn open_layout(root: &Path, converting: bool) -> Result<Arc<Self>> {
        if !converting
            && root.parent().is_some_and(|parent| {
                parent.join("conversion-incomplete").exists()
                    || (parent.join("archive").exists()
                        && std::fs::read(parent.join("conversion-complete"))
                            .ok()
                            .as_deref()
                            != Some(b"2\n"))
            })
        {
            return Err(storage_error(format!(
                "conversion of {} is incomplete",
                root.display()
            )));
        }
        let existing = root.join(MANIFEST).exists();
        let expected = Manifest {
            format: 2,
            shards: STATE_SHARDS,
            routing: "sha256-length-prefixed-v1".into(),
        };
        if root.join(MANIFEST).exists() {
            let bytes = std::fs::read(root.join(MANIFEST)).map_err(storage_error)?;
            let actual: Manifest = serde_json::from_slice(&bytes).map_err(storage_error)?;
            if actual != expected
                || std::fs::read(root.join(COMPLETE)).ok().as_deref() != Some(b"2\n")
            {
                return Err(PlatformError::runtime(format!(
                    "state directory {} has an incompatible or incomplete layout; use dd storage convert into a new destination",
                    root.display()
                )));
            }
        } else {
            if root.exists()
                && std::fs::read_dir(root)
                    .map_err(storage_error)?
                    .next()
                    .is_some()
            {
                return Err(PlatformError::runtime(format!(
                    "state directory {} is not empty and has no routing manifest; use dd storage convert",
                    root.display()
                )));
            }
            std::fs::create_dir_all(root).map_err(storage_error)?;
            write_file_synced(
                &root.join(MANIFEST),
                &serde_json::to_vec_pretty(&expected).map_err(storage_error)?,
            )?;
        }
        let lock = std::fs::File::open(root.join(MANIFEST)).map_err(storage_error)?;
        lock.try_lock().map_err(|error| {
            storage_error(format!(
                "state directory {} is already open: {error}",
                root.display()
            ))
        })?;
        let mut shards = Vec::with_capacity(STATE_SHARDS);
        let mut owner_epoch = 0i64;
        for index in 0..STATE_SHARDS {
            let path = root.join(format!("shard-{index:02}.db"));
            if existing && !path.exists() {
                return Err(storage_error(format!(
                    "state shard {} is missing from completed layout",
                    path.display()
                )));
            }
            let database = Arc::new(
                Builder::new_local(path.to_string_lossy().as_ref())
                    .build()
                    .await
                    .map_err(storage_error)?,
            );
            let conn = database.connect().map_err(storage_error)?;
            configure_connection(&conn).await?;
            if existing {
                let mut rows = query_cached(&conn, "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name IN ('state_floor','worker_kv','memory_state','memory_meta','memory_commands','memory_outbox')", ()).await.map_err(storage_error)?;
                let count = rows
                    .next()
                    .await
                    .map_err(storage_error)?
                    .expect("schema count")
                    .get::<i64>(0)
                    .map_err(storage_error)?;
                if count != 6 {
                    return Err(storage_error(format!(
                        "state shard {} has {count} required tables, expected 6",
                        path.display()
                    )));
                }
            }
            if existing {
                let mut rows = query_cached(
                    &conn,
                    "SELECT version FROM state_floor WHERE singleton = 1",
                    (),
                )
                .await
                .map_err(storage_error)?;
                if rows.next().await.map_err(storage_error)?.is_none() {
                    return Err(storage_error(format!(
                        "state shard {} is missing its version floor",
                        path.display()
                    )));
                }
            } else {
                for statement in include_str!("state_schema.sql")
                    .split(';')
                    .filter(|sql| !sql.trim().is_empty())
                {
                    conn.execute(statement, ()).await.map_err(storage_error)?;
                }
            }
            let mut rows = query_cached(
                &conn,
                "SELECT COALESCE(MAX(owner_epoch), 0) FROM memory_meta",
                (),
            )
            .await
            .map_err(storage_error)?;
            if let Some(row) = rows.next().await.map_err(storage_error)? {
                owner_epoch = owner_epoch.max(row.get::<i64>(0).map_err(storage_error)?);
            }
            drop(rows);
            drop(conn);
            let epoch = Arc::new(AtomicU64::new(0));
            let (sender, receiver) = mpsc::channel(QUEUE_COMMANDS);
            let writer_database = Arc::clone(&database);
            let writer_epoch = Arc::clone(&epoch);
            let metrics = Arc::new(WriterMetrics::default());
            let writer_metrics = Arc::clone(&metrics);
            let writer = std::thread::Builder::new()
                .name(format!("state-{index:02}"))
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("state writer runtime");
                    runtime.block_on(run_writer(
                        writer_database,
                        writer_epoch,
                        writer_metrics,
                        receiver,
                    ));
                })
                .map_err(storage_error)?;
            shards.push(StateShard {
                database,
                readers: Arc::new(ReadPool {
                    idle: Mutex::new(Vec::new()),
                    permits: Arc::new(Semaphore::new(2)),
                }),
                sender: Some(sender),
                writer: Some(writer),
                bytes: Arc::new(Semaphore::new(QUEUE_BYTES)),
                commands: Arc::new(Semaphore::new(QUEUE_COMMANDS)),
                epoch,
                metrics,
            });
        }
        if !converting && !root.join(COMPLETE).exists() {
            write_file_synced(&root.join(COMPLETE), b"2\n")?;
        }
        Ok(Arc::new(Self {
            shards,
            _lock: lock,
            owner_epoch: AtomicI64::new(owner_epoch),
            leases: Mutex::new(HashMap::new()),
            lease_admissions: Arc::new(Semaphore::new(4096)),
        }))
    }

    pub fn shard_index(worker: &str, binding: &str, entity: &str) -> usize {
        let mut hash = Sha256::new();
        hash.update(b"dd-state-shard-v1\0");
        for component in [worker, binding, entity] {
            hash.update((component.len() as u64).to_be_bytes());
            hash.update(component.as_bytes());
        }
        let digest = hash.finalize();
        u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix")) as usize % STATE_SHARDS
    }

    pub async fn acquire_lease(&self, namespace: &str, entity: &str) -> Result<Arc<MemoryLease>> {
        if namespace.len() + entity.len() > 1024 || entity.is_empty() {
            return Err(PlatformError::bad_request(format!(
                "memory lease identity length {} exceeds 1024 or entity is empty",
                namespace.len() + entity.len()
            )));
        }
        let admission = Arc::clone(&self.lease_admissions)
            .try_acquire_owned()
            .map_err(|_| PlatformError::overloaded("memory lease admission limit 4096 exceeded"))?;
        let key = (namespace.to_owned(), entity.to_owned());
        let mutex = {
            let mut leases = self.leases.lock().expect("memory lease map poisoned");
            if leases.len() >= 4096 {
                leases.retain(|_, lease| lease.strong_count() > 0);
            }
            if let Some(mutex) = leases.get(&key).and_then(Weak::upgrade) {
                mutex
            } else {
                let mutex = Arc::new(tokio::sync::Mutex::new(()));
                leases.insert(key, Arc::downgrade(&mutex));
                mutex
            }
        };
        let guard = mutex.lock_owned().await;
        Ok(Arc::new(MemoryLease {
            owner_epoch: self.next_owner_epoch()?,
            _guard: guard,
            _admission: admission,
        }))
    }

    pub fn next_owner_epoch(&self) -> Result<i64> {
        self.owner_epoch
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |epoch| {
                epoch.checked_add(1)
            })
            .map(|epoch| epoch + 1)
            .map_err(|epoch| {
                PlatformError::runtime(format!("memory owner epoch exhausted at {epoch}"))
            })
    }

    pub fn owner_epoch_floor(&self) -> u64 {
        self.owner_epoch.load(Ordering::SeqCst) as u64
    }

    pub(crate) fn epoch(&self, shard: usize) -> u64 {
        self.shards[shard].epoch.load(Ordering::Acquire)
    }

    pub(crate) async fn read(&self, shard: usize) -> Result<ReadConnection> {
        let shard = &self.shards[shard];
        let permit = Arc::clone(&shard.readers.permits)
            .acquire_owned()
            .await
            .map_err(storage_error)?;
        let conn = shard
            .readers
            .idle
            .lock()
            .expect("state readers lock poisoned")
            .pop();
        let conn = match conn {
            Some(conn) => conn,
            None => {
                let conn = shard.database.connect().map_err(storage_error)?;
                configure_connection(&conn).await?;
                conn
            }
        };
        Ok(ReadConnection {
            conn: Some(conn),
            pool: Arc::clone(&shard.readers),
            _permit: permit,
        })
    }

    pub(crate) async fn write<T: Any + Send, F>(
        &self,
        shard: usize,
        bytes: usize,
        operation: F,
    ) -> Result<T>
    where
        F: for<'a> Fn(&'a Connection, i64) -> WriteFuture<'a, T> + Send + Sync + 'static,
    {
        let shard = &self.shards[shard];
        if bytes > QUEUE_BYTES {
            return Err(PlatformError::bad_request(format!(
                "state mutation size {bytes} exceeds maximum {QUEUE_BYTES} bytes"
            )));
        }
        let bytes = bytes.max(1) as u32;
        let byte_permit = Arc::clone(&shard.bytes)
            .try_acquire_many_owned(bytes)
            .map_err(|_| {
                PlatformError::overloaded(format!(
                    "state writer byte limit {QUEUE_BYTES} exceeded by {bytes}-byte request"
                ))
            })?;
        let command_permit = Arc::clone(&shard.commands)
            .try_acquire_owned()
            .map_err(|_| {
                PlatformError::overloaded(format!(
                    "state writer command limit {QUEUE_COMMANDS} exceeded"
                ))
            })?;
        let (reply, completion) = oneshot::channel();
        let operation = Arc::new(operation);
        let command = WriteCommand {
            operation: Box::new(move |conn, version| {
                let operation = Arc::clone(&operation);
                Box::pin(async move {
                    operation(conn, version)
                        .await
                        .map(|value| Box::new(value) as WriteReply)
                })
            }),
            reply,
            _bytes: byte_permit,
            _command: command_permit,
        };
        shard
            .sender
            .as_ref()
            .expect("live state writer")
            .try_send(command)
            .map_err(|error| {
                PlatformError::runtime(format!("state writer admission failed: {error}"))
            })?;
        let result = completion.await.map_err(|error| {
            PlatformError::runtime(format!("state writer stopped before commit: {error}"))
        })??;
        Ok(*result.downcast::<T>().expect("state writer response type"))
    }

    pub fn performance_snapshot(&self) -> StatePerformanceSnapshot {
        let mut snapshot = StatePerformanceSnapshot::default();
        for shard in &self.shards {
            snapshot.committed_groups += shard.metrics.committed_groups.load(Ordering::Relaxed);
            snapshot.committed_commands += shard.metrics.committed_commands.load(Ordering::Relaxed);
            snapshot.rollbacks += shard.metrics.rollbacks.load(Ordering::Relaxed);
            snapshot.discarded_connections +=
                shard.metrics.discarded_connections.load(Ordering::Relaxed);
            snapshot.busy_retries += shard.metrics.busy_retries.load(Ordering::Relaxed);
            snapshot.pending_commands += QUEUE_COMMANDS - shard.commands.available_permits();
            snapshot.pending_bytes += QUEUE_BYTES - shard.bytes.available_permits();
        }
        snapshot
    }

    pub async fn checkpoint(&self) -> Result<()> {
        for index in 0..STATE_SHARDS {
            let conn = self.read(index).await?;
            let mut rows = conn
                .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
                .await
                .map_err(storage_error)?;
            while let Some(row) = rows.next().await.map_err(storage_error)? {
                if row.get::<i64>(0).map_err(storage_error)? != 0 {
                    return Err(PlatformError::storage_unavailable(format!(
                        "state shard {index} checkpoint is busy"
                    )));
                }
            }
        }
        Ok(())
    }
    pub async fn health_check(&self) -> Result<()> {
        for index in 0..STATE_SHARDS {
            let conn = self.read(index).await?;
            let mut rows = query_cached(
                &conn,
                "SELECT version FROM state_floor WHERE singleton = 1",
                (),
            )
            .await
            .map_err(storage_error)?;
            if rows.next().await.map_err(storage_error)?.is_none() {
                return Err(storage_error(format!(
                    "state shard {index} is missing its version floor"
                )));
            }
        }
        Ok(())
    }
}

impl Drop for StateShard {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(writer) = self.writer.take() {
            writer.join().expect("state writer panicked");
        }
    }
}

async fn run_writer(
    database: Arc<Database>,
    epoch: Arc<AtomicU64>,
    metrics: Arc<WriterMetrics>,
    mut receiver: mpsc::Receiver<WriteCommand>,
) {
    let mut conn = None;
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        while batch.len() < 128 {
            match receiver.try_recv() {
                Ok(command) => batch.push(command),
                Err(_) => break,
            }
        }
        let result = commit_group(&database, &mut conn, &metrics, &batch).await;
        epoch.fetch_add(1, Ordering::AcqRel);
        match result {
            Ok(replies) => {
                for (command, reply) in batch.into_iter().zip(replies) {
                    let _ = command.reply.send(Ok(reply));
                }
            }
            Err(_) if batch.len() > 1 => {
                for command in batch {
                    let result = commit_group(
                        &database,
                        &mut conn,
                        &metrics,
                        std::slice::from_ref(&command),
                    )
                    .await
                    .map(|mut replies| replies.remove(0));
                    epoch.fetch_add(1, Ordering::AcqRel);
                    let _ = command.reply.send(result);
                }
            }
            Err(error) => {
                let _ = batch.remove(0).reply.send(Err(error));
            }
        }
    }
}

async fn commit_group(
    database: &Database,
    slot: &mut Option<Connection>,
    metrics: &WriterMetrics,
    batch: &[WriteCommand],
) -> Result<Vec<WriteReply>> {
    for attempt in 0..8 {
        if slot.is_none() {
            let conn = database.connect().map_err(storage_error)?;
            configure_connection(&conn).await?;
            *slot = Some(conn);
        }
        let conn = slot.as_ref().expect("state writer connection");
        let result: std::result::Result<Vec<WriteReply>, WriteError> = async {
            execute_cached(conn, "BEGIN IMMEDIATE", ()).await?;
            let mut rows = query_cached(
                conn,
                "SELECT version FROM state_floor WHERE singleton = 1",
                (),
            )
            .await?;
            let mut version = rows
                .next()
                .await?
                .expect("state version floor")
                .get::<i64>(0)?;
            drop(rows);
            let mut replies = Vec::with_capacity(batch.len());
            for command in batch {
                version = version.checked_add(1).ok_or_else(|| {
                    PlatformError::runtime(format!("state version exhausted at {version}"))
                })?;
                replies.push((command.operation)(conn, version).await?);
            }
            execute_cached(
                conn,
                "UPDATE state_floor SET version = ?1 WHERE singleton = 1",
                (version,),
            )
            .await?;
            execute_cached(conn, "COMMIT", ()).await?;
            Ok(replies)
        }
        .await;
        match result {
            Ok(replies) => {
                metrics.committed_groups.fetch_add(1, Ordering::Relaxed);
                metrics
                    .committed_commands
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                return Ok(replies);
            }
            Err(error) => {
                let retry = matches!(&error, WriteError::Database(error) if is_retryable_turso_error(error));
                if execute_cached(conn, "ROLLBACK", ()).await.is_err() {
                    slot.take();
                    metrics
                        .discarded_connections
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics.rollbacks.fetch_add(1, Ordering::Relaxed);
                }
                if !retry || attempt == 7 {
                    return Err(error.into());
                }
                record_storage_retry();
                metrics.busy_retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5 * (attempt + 1))).await;
            }
        }
    }
    unreachable!("bounded state retry loop returns")
}

pub(crate) async fn configure_connection(conn: &Connection) -> Result<()> {
    configure_turso_connection(conn, storage_error)?;
    conn.execute("PRAGMA synchronous = FULL", ())
        .await
        .map_err(storage_error)?;
    Ok(())
}

pub(crate) fn write_file_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(storage_error)?;
    file.write_all(bytes).map_err(storage_error)?;
    file.sync_all().map_err(storage_error)?;
    std::fs::File::open(path.parent().expect("state file parent"))
        .and_then(|parent| parent.sync_all())
        .map_err(storage_error)
}

pub(crate) fn storage_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("state storage: {error}"))
}

pub struct MemoryLease {
    owner_epoch: i64,
    _guard: tokio::sync::OwnedMutexGuard<()>,
    _admission: OwnedSemaphorePermit,
}
impl MemoryLease {
    pub fn owner_epoch(&self) -> i64 {
        self.owner_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{MemoryBatchMutation, MemoryStore, worker_namespace};

    #[tokio::test]
    async fn queued_bytes_reject_retryably_and_release_after_commit() {
        let root = std::env::temp_dir().join(format!("dd-writer-budget-{}", uuid::Uuid::new_v4()));
        let state = StateStore::open(&root).await.unwrap();
        let kv = crate::kv::KvStore::from_state(Arc::clone(&state));
        let blocker = state
            .read(StateStore::shard_index("worker", "KV", "key"))
            .await
            .unwrap();
        blocker.execute("BEGIN IMMEDIATE", ()).await.unwrap();
        let value = vec![b'a'; 9 * 1024 * 1024];
        let mut commit = Box::pin(kv.put_value("worker", "KV", "key", &value, "utf8"));
        std::future::poll_fn(|context| {
            assert!(commit.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let error = kv
            .put_value("worker", "KV", "key", &value, "utf8")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), common::ErrorKind::Overloaded);
        blocker.execute("ROLLBACK", ()).await.unwrap();
        drop(blocker);
        commit.await.unwrap();
        kv.put("worker", "KV", "key", "after pressure")
            .await
            .unwrap();
        assert_eq!(
            kv.get_utf8("worker", "KV", "key").await.unwrap().unwrap(),
            "after pressure"
        );
        drop(kv);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn canceled_commit_keeps_the_entity_leased_until_its_queued_write_finishes() {
        let root =
            std::env::temp_dir().join(format!("dd-canceled-commit-{}", uuid::Uuid::new_v4()));
        let state = StateStore::open(&root).await.unwrap();
        let memory = MemoryStore::from_state(Arc::clone(&state));
        let namespace = worker_namespace("worker", "MEMORY");
        let lease = memory.acquire_lease(&namespace, "entity").await.unwrap();
        let owner = lease.owner_epoch();
        let blocker = state
            .read(StateStore::shard_index("worker", "MEMORY", "entity"))
            .await
            .unwrap();
        blocker.execute("BEGIN IMMEDIATE", ()).await.unwrap();
        let mutations = [MemoryBatchMutation {
            key: "key".into(),
            value: b"committed".to_vec(),
            encoding: "utf8".into(),
            deleted: false,
        }];
        let mut commit = Box::pin(memory.apply_batch(
            &namespace,
            "entity",
            crate::memory::MemoryCommit {
                mutations: &mutations,
                owner_epoch: Some(owner),
                lease: Some(lease),
                ..Default::default()
            },
        ));
        std::future::poll_fn(|context| {
            assert!(
                commit.as_mut().poll(context).is_pending(),
                "external write lock must delay commit"
            );
            std::task::Poll::Ready(())
        })
        .await;
        drop(commit);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                memory.acquire_lease(&namespace, "entity")
            )
            .await
            .is_err(),
            "canceled caller must leave the queued commit holding its entity lease"
        );
        blocker.execute("ROLLBACK", ()).await.unwrap();
        drop(blocker);
        let next = tokio::time::timeout(
            Duration::from_secs(5),
            memory.acquire_lease(&namespace, "entity"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            memory.snapshot(&namespace, "entity").await.unwrap().entries[0].value,
            b"committed"
        );
        drop(next);
        drop(memory);
        drop(state);
        std::fs::remove_dir_all(root).unwrap();
    }
}
