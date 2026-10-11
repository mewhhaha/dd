//! Runtime and storage configuration.

use super::*;

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub min_isolates: usize,
    pub max_global_isolates: usize,
    pub max_isolates_per_worker: usize,
    pub max_inflight_per_isolate: usize,
    pub max_queued_requests_per_worker: usize,
    pub reserved_internal_queued_requests_per_worker: usize,
    pub max_global_queued_requests: usize,
    pub max_global_queued_bytes: usize,
    pub max_queue_wait: Duration,
    pub request_wall_timeout: Duration,
    pub max_request_body_bytes: usize,
    pub max_buffered_request_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_buffered_response_bytes: usize,
    pub max_buffered_websocket_bytes: usize,
    pub max_buffered_websocket_bytes_per_session: usize,
    pub max_isolate_heap_bytes: usize,
    pub isolate_startup_timeout: Duration,
    pub idle_ttl: Duration,
    pub scale_tick: Duration,
    pub queue_warn_thresholds: Vec<usize>,
    pub cache_max_entries: usize,
    pub cache_max_bytes: usize,
    pub cache_default_ttl: Duration,
    pub kv_read_cache_max_entries: usize,
    pub kv_read_cache_max_bytes: usize,
    pub v8_flags: Vec<String>,
    pub debug_code_generation: bool,
    /// Give workers `globalThis.__dd_raw_host_fetch`, which fetches outside
    /// any request and egress rule. Only the local development runtime sets
    /// this, for dd-vite's dev module loading.
    pub dev_unscoped_fetch: bool,
    /// Give workers `globalThis.__dd_internals`: the runtime's bootstrap
    /// object, every op included. Only the runtime's own tests and
    /// benchmarks set this, to reach behind the worker API.
    pub expose_internals: bool,
    /// Chrome DevTools debugging of worker isolates. Only the local
    /// development runtime turns it on; production never creates an
    /// inspector.
    pub inspector: InspectorMode,
    pub kv_profile_enabled: bool,
    pub memory_profile_enabled: bool,
    pub temporary_worker_ttl: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            min_isolates: 0,
            max_global_isolates: default_global_isolate_budget(),
            max_isolates_per_worker: default_global_isolate_budget(),
            max_inflight_per_isolate: 4,
            max_queued_requests_per_worker: 1024,
            reserved_internal_queued_requests_per_worker: 64,
            max_global_queued_requests: 16 * 1024,
            max_global_queued_bytes: 64 * 1024 * 1024,
            max_queue_wait: Duration::from_secs(30),
            request_wall_timeout: Duration::from_secs(30),
            max_request_body_bytes: 64 * 1024 * 1024,
            max_buffered_request_bytes: 64 * 1024 * 1024,
            max_response_body_bytes: 64 * 1024 * 1024,
            max_buffered_response_bytes: 64 * 1024 * 1024,
            max_buffered_websocket_bytes: 64 * 1024 * 1024,
            max_buffered_websocket_bytes_per_session: 1024 * 1024,
            max_isolate_heap_bytes: 128 * 1024 * 1024,
            isolate_startup_timeout: Duration::from_secs(5),
            idle_ttl: Duration::from_secs(30),
            scale_tick: Duration::from_secs(1),
            queue_warn_thresholds: vec![10, 100, 1000],
            cache_max_entries: 2048,
            cache_max_bytes: 64 * 1024 * 1024,
            cache_default_ttl: Duration::from_secs(60),
            kv_read_cache_max_entries: 16_384,
            kv_read_cache_max_bytes: 16 * 1024 * 1024,
            v8_flags: Vec::new(),
            debug_code_generation: false,
            dev_unscoped_fetch: false,
            expose_internals: false,
            inspector: InspectorMode::Off,
            kv_profile_enabled: false,
            memory_profile_enabled: false,
            temporary_worker_ttl: Duration::from_secs(60 * 60),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeStorageConfig {
    pub store_dir: PathBuf,
    pub memory_outbox_max_concurrent_shards: usize,
    pub memory_outbox_max_claimed_bytes: usize,
    pub memory_snapshot_cache_max_entries: usize,
    pub memory_snapshot_cache_max_bytes: usize,
    pub worker_store_enabled: bool,
}

impl Default for RuntimeStorageConfig {
    fn default() -> Self {
        let store_dir = PathBuf::from("./store");
        Self {
            store_dir,
            memory_outbox_max_concurrent_shards: default_memory_outbox_parallelism(
                ::storage::state::STATE_SHARDS,
            ),
            memory_outbox_max_claimed_bytes: ::storage::memory::DEFAULT_OUTBOX_CLAIM_BYTE_LIMIT,
            memory_snapshot_cache_max_entries: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES,
            memory_snapshot_cache_max_bytes: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_BYTES,
            worker_store_enabled: !cfg!(test),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeServiceConfig {
    pub runtime: RuntimeConfig,
    pub storage: RuntimeStorageConfig,
}
