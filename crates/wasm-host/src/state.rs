//! Per-instance host state carried in the wasmtime `Store`, plus the
//! per-worker context shared by all instances of one deployed worker.

use crate::heap::Heap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use storage::blob::BlobStore;
use storage::cache::{CacheConfig, CacheStore};
use storage::kv::KvStore;
use storage::memory::MemoryStore;
use wasmtime::{Memory, Table};

/// Headers of the request currently being dispatched, served to the guest
/// through `ffi.dd_header`.
pub struct CurrentRequest {
    pub headers: Vec<(String, String)>,
}

pub struct Timer {
    pub id: u32,
    pub due: Instant,
    /// `Some` for intervals; the timer is rescheduled after firing.
    pub every: Option<std::time::Duration>,
    pub callback_bits: u64,
}

/// A queued promise reaction. `callback_bits == TAG_UNDEFINED` means
/// pass-through: the value resolves `downstream` directly (used for chaining
/// a promise returned from a `.then` callback).
pub struct Microtask {
    pub callback_bits: u64,
    pub value_bits: u64,
    pub downstream: u32,
}

/// Disk-backed stores shared by every worker in one server process, mirroring
/// dd_server's layout: KV and cache share one turso database, memory
/// namespaces get their own sharded databases, blobs a directory.
pub struct WorkerStores {
    pub kv: KvStore,
    pub memory: MemoryStore,
    pub cache: CacheStore,
}

impl WorkerStores {
    pub async fn open(store_dir: &Path) -> common::Result<Arc<WorkerStores>> {
        tokio::fs::create_dir_all(store_dir)
            .await
            .map_err(|error| {
                common::PlatformError::internal(format!(
                    "cannot create store dir {}: {error}",
                    store_dir.display()
                ))
            })?;
        let database_url = store_dir.join("kv.db").display().to_string();
        let database = KvStore::open_database(&database_url).await?;
        let kv = KvStore::from_database(Arc::clone(&database)).await?;
        let blobs = BlobStore::for_legacy_root(store_dir.join("blobs")).await?;
        let cache = CacheStore::from_database(CacheConfig::default(), database, blobs).await?;
        let memory = MemoryStore::new(
            store_dir.join("memory"),
            8,
            64,
            std::time::Duration::from_secs(30),
        )
        .await?;
        Ok(Arc::new(WorkerStores { kv, memory, cache }))
    }
}

/// All deployed workers by name; `dd_service_fetch` resolves through this,
/// so bindings can point at workers deployed later (or cyclically).
pub type WorkerRegistry = Arc<RwLock<HashMap<String, Arc<crate::engine::WorkerModule>>>>;

/// Live websocket connections of one worker: outbound senders keyed by
/// connection id. Shared between the websocket dispatcher instance and
/// regular fetch instances so any handler can push to any connection.
#[derive(Default)]
pub struct WsConnections {
    next_id: std::sync::atomic::AtomicU64,
    senders:
        std::sync::Mutex<HashMap<u64, tokio::sync::mpsc::UnboundedSender<crate::ws::WsOutbound>>>,
}

impl WsConnections {
    pub fn register(
        &self,
    ) -> (
        u64,
        tokio::sync::mpsc::UnboundedReceiver<crate::ws::WsOutbound>,
    ) {
        let id = 1 + self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.senders
            .lock()
            .expect("ws registry is never poisoned")
            .insert(id, sender);
        (id, receiver)
    }

    pub fn remove(&self, id: u64) {
        self.senders
            .lock()
            .expect("ws registry is never poisoned")
            .remove(&id);
    }

    pub fn send(&self, id: u64, outbound: crate::ws::WsOutbound) -> bool {
        let senders = self.senders.lock().expect("ws registry is never poisoned");
        match senders.get(&id) {
            Some(sender) => sender.send(outbound).is_ok(),
            None => false,
        }
    }
}

/// Shared by every instance of one deployed worker.
pub struct WorkerContext {
    pub worker_name: String,
    pub stores: Option<Arc<WorkerStores>>,
    /// Service binding name -> target worker name.
    pub service_bindings: HashMap<String, String>,
    pub workers: WorkerRegistry,
    pub http: reqwest::Client,
    pub ws_connections: Arc<WsConnections>,
}

impl Default for WorkerContext {
    fn default() -> Self {
        WorkerContext {
            worker_name: "worker".to_string(),
            stores: None,
            service_bindings: HashMap::new(),
            workers: Arc::new(RwLock::new(HashMap::new())),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("static reqwest client configuration cannot fail"),
            ws_connections: Arc::new(WsConnections::default()),
        }
    }
}

/// One `dd_memory_atomic` in progress: the loaded tvar values for the locked
/// key, plus which of them the command wrote.
pub struct ActiveAtomic {
    pub tvars: HashMap<String, serde_json::Value>,
    pub dirty: HashSet<String>,
}

pub struct HostState {
    pub heap: Heap,
    pub class_methods: HashMap<String, HashMap<String, u32>>,
    pub class_parents: HashMap<String, String>,
    pub class_statics: HashMap<String, HashMap<String, u64>>,
    pub registered_handler: Option<u64>,
    pub current_request: Option<CurrentRequest>,
    pub pending_exception: Option<u64>,
    pub try_depth: u32,
    pub timers: Vec<Timer>,
    pub microtasks: VecDeque<Microtask>,
    pub next_timer_id: u32,
    pub table: Option<Table>,
    pub memory: Option<Memory>,
    pub context: Arc<WorkerContext>,
    pub active_atomic: Option<ActiveAtomic>,
    /// Depth of the `dd_service_fetch` chain that led to this invocation.
    pub service_depth: u32,
    /// Handlers object installed by `dd_ws_register` ({open, message, close}).
    pub ws_handlers: Option<u64>,
}

impl HostState {
    pub fn with_context(context: Arc<WorkerContext>) -> Self {
        HostState {
            heap: Heap::default(),
            class_methods: HashMap::new(),
            class_parents: HashMap::new(),
            class_statics: HashMap::new(),
            registered_handler: None,
            current_request: None,
            pending_exception: None,
            try_depth: 0,
            timers: Vec::new(),
            microtasks: VecDeque::new(),
            next_timer_id: 0,
            table: None,
            memory: None,
            context,
            active_atomic: None,
            service_depth: 0,
            ws_handlers: None,
        }
    }
}

impl Default for HostState {
    fn default() -> Self {
        HostState::with_context(Arc::new(WorkerContext::default()))
    }
}
