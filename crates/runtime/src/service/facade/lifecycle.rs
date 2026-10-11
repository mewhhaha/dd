//! Runtime startup, restoration and coordinated shutdown.

use super::*;
use tracing::info;

pub(in crate::service) struct RuntimeShutdownState {
    started: AtomicBool,
    thread: StdMutex<Option<thread::JoinHandle<()>>>,
    result: StdMutex<Option<Result<()>>>,
    completed: Notify,
    completed_blocking: std::sync::Condvar,
}

impl RuntimeShutdownState {
    fn new(thread: thread::JoinHandle<()>) -> Self {
        Self {
            started: AtomicBool::new(false),
            thread: StdMutex::new(Some(thread)),
            result: StdMutex::new(None),
            completed: Notify::new(),
            completed_blocking: std::sync::Condvar::new(),
        }
    }

    fn start(self: &Arc<Self>, sender: RuntimeCommandSender) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let state = Arc::clone(self);
        let shutdown_sender = sender.clone();
        let shutdown_worker = thread::Builder::new()
            .name("dd-runtime-shutdown".to_string())
            .spawn(move || {
                state.shutdown_and_join_from_worker(shutdown_sender);
            });

        if let Err(error) = shutdown_worker {
            tracing::warn!(error = %error, "failed to spawn runtime shutdown worker; shutting down inline");
            self.shutdown_and_join_inline(sender);
        }
    }

    fn shutdown_and_join_from_worker(&self, sender: RuntimeCommandSender) {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command_result = if sender
            .blocking_send(RuntimeCommand::Shutdown { reply: reply_tx })
            .is_err()
        {
            // The runtime may already have stopped. The thread join below
            // determines whether that was a clean exit or a panic.
            Ok(())
        } else {
            match reply_rx.blocking_recv() {
                Ok(result) => result,
                Err(_) => Err(PlatformError::internal("runtime shutdown channel closed")),
            }
        };
        self.finish_after_runtime_thread_join(command_result);
    }

    /// This path can run from a Tokio task while handling a failed thread
    /// spawn, so it uses `try_send` rather than Tokio's panicking
    /// `blocking_send`. The runtime manager consumes this dedicated fast lane
    /// on another OS thread.
    fn shutdown_and_join_inline(&self, sender: RuntimeCommandSender) {
        let (reply_tx, mut reply_rx) = oneshot::channel();
        let mut command = RuntimeCommand::Shutdown { reply: reply_tx };
        loop {
            match sender.try_send(command) {
                Ok(()) => break,
                Err(error) => match *error {
                    mpsc::error::TrySendError::Full(returned) => {
                        command = returned;
                        thread::yield_now();
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        self.finish_after_runtime_thread_join(Ok(()));
                        return;
                    }
                },
            }
        }

        // Joining the dedicated runtime thread guarantees it has either sent
        // this reply or dropped its sender, so no Tokio blocking receive is
        // needed on this inline path.
        let join_result = self.join_runtime_thread();
        let command_result = reply_rx.try_recv().map_err(|error| {
            PlatformError::internal(format!("runtime shutdown reply unavailable: {error}"))
        });
        self.finish(command_result.and(join_result));
    }

    fn finish_after_runtime_thread_join(&self, command_result: Result<()>) {
        self.finish(command_result.and(self.join_runtime_thread()));
    }

    fn join_runtime_thread(&self) -> Result<()> {
        let (runtime_thread, lock_result) = match self.thread.lock() {
            Ok(mut thread) => (thread.take(), Ok(())),
            Err(poisoned) => (
                poisoned.into_inner().take(),
                Err(PlatformError::internal(
                    "runtime thread join state is poisoned",
                )),
            ),
        };
        let join_result = match runtime_thread {
            Some(runtime_thread) => match runtime_thread.join() {
                Ok(()) => Ok(()),
                Err(_) => Err(PlatformError::internal(
                    "runtime thread panicked during shutdown",
                )),
            },
            None => Err(PlatformError::internal(
                "runtime thread join handle is unavailable",
            )),
        };
        join_result.and(lock_result)
    }

    fn finish(&self, result: Result<()>) {
        match self.result.lock() {
            Ok(mut stored) => *stored = Some(result),
            Err(mut poisoned) => **poisoned.get_mut() = Some(result),
        }
        self.completed.notify_waiters();
        self.completed_blocking.notify_all();
    }

    pub(in crate::service) async fn wait(&self) -> Result<()> {
        loop {
            let notified = self.completed.notified();
            let result = match self.result.lock() {
                Ok(result) => result.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            };
            if let Some(result) = result {
                return result;
            }
            notified.await;
        }
    }

    fn wait_blocking(&self) -> Result<()> {
        let mut result = match self.result.lock() {
            Ok(result) => result,
            Err(poisoned) => poisoned.into_inner(),
        };
        loop {
            if let Some(result) = result.clone() {
                return result;
            }
            result = match self.completed_blocking.wait(result) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    #[cfg(test)]
    pub(in crate::service) fn is_complete(&self) -> bool {
        match self.result.lock() {
            Ok(result) => result.is_some(),
            Err(poisoned) => poisoned.into_inner().is_some(),
        }
    }
}

/// Owns automatic shutdown for the complete set of `RuntimeService` clones.
///
/// The worker is deliberately a standard thread: dropping a service often
/// happens while a `#[tokio::test]` runtime is winding down, when spawned
/// Tokio work would be cancelled before it can stop the runtime thread.
pub(super) struct RuntimeServiceLifetime {
    cancel_sender: RuntimeCommandSender,
    shutdown: Arc<RuntimeShutdownState>,
}

impl Drop for RuntimeServiceLifetime {
    fn drop(&mut self) {
        self.shutdown.start(self.cancel_sender.clone());
        if let Err(error) = self.shutdown.wait_blocking() {
            tracing::warn!(error = %error, "runtime service automatic shutdown failed");
        }
    }
}

impl RuntimeService {
    pub async fn start(config: RuntimeServiceConfig) -> Result<Self> {
        ensure_rustls_crypto_provider();
        let RuntimeServiceConfig { runtime, storage } = config;
        validate_runtime_config(&runtime)?;
        ensure_v8_flags(&runtime.v8_flags)?;
        if storage.memory_snapshot_cache_max_entries == 0
            || storage.memory_snapshot_cache_max_bytes == 0
        {
            return Err(PlatformError::internal(
                "memory snapshot cache limits must be greater than zero",
            ));
        }
        if storage.memory_outbox_max_concurrent_shards == 0 {
            return Err(PlatformError::internal(
                "memory_outbox_max_concurrent_shards must be greater than zero",
            ));
        }
        if storage.store_dir.join("conversion-incomplete").exists()
            || (!storage.store_dir.join("state").exists()
                && ["dd-kv.db", "memory", "workers", "control.db", "tokens.json"]
                    .iter()
                    .any(|name| storage.store_dir.join(name).exists()))
        {
            return Err(PlatformError::runtime(format!(
                "store {} requires offline conversion into a new destination; use dd storage convert",
                storage.store_dir.display()
            )));
        }
        tokio::fs::create_dir_all(&storage.store_dir)
            .await
            .map_err(|error| {
                PlatformError::internal(format!(
                    "failed to create store directory {}: {error}",
                    storage.store_dir.display()
                ))
            })?;

        let state_store =
            ::storage::state::StateStore::open(storage.store_dir.join("state")).await?;
        let control_store = ControlStore::open(&storage.store_dir).await?;
        let bootstrap_snapshot = build_bootstrap_snapshot().await?;
        let kv_store = KvStore::from_state(Arc::clone(&state_store));
        kv_store.set_profile_enabled(runtime.kv_profile_enabled);
        kv_store.set_read_cache_limits(
            runtime.kv_read_cache_max_entries,
            runtime.kv_read_cache_max_bytes,
        );
        let mut memory_store = MemoryStore::from_state(Arc::clone(&state_store));
        memory_store.set_outbox_claim_byte_limit(storage.memory_outbox_max_claimed_bytes)?;
        memory_store.set_snapshot_cache_limits(
            storage.memory_snapshot_cache_max_entries,
            storage.memory_snapshot_cache_max_bytes,
        );
        memory_store.set_profile_enabled(runtime.memory_profile_enabled);
        let cache_store = CacheStore::open(
            storage.store_dir.join("cache.db"),
            CacheConfig {
                max_entries: runtime.cache_max_entries,
                max_bytes: runtime.cache_max_bytes,
                default_ttl: runtime.cache_default_ttl,
            },
        )
        .await?;
        let (sender, receiver) = mpsc::channel(256);
        let (fast_sender, cancel_receiver) = mpsc::channel(RUNTIME_FAST_COMMAND_CHANNEL_CAPACITY);
        let (cancel_sender, cancellation_receiver) = mpsc::unbounded_channel();
        let admission = RuntimeAdmission::new(&runtime);
        let routes = WorkerRoutes::default();
        let sender = RuntimeCommandSender::new(
            sender,
            routes.clone(),
            CommandLane::Request,
            Arc::clone(&admission),
        );
        let fast_sender = RuntimeCommandSender::new(
            fast_sender,
            routes.clone(),
            CommandLane::Internal,
            Arc::clone(&admission),
        );
        let cancel_sender = RuntimeCancellationSender::new(cancel_sender, routes.clone());
        let asset_catalog = AssetCatalog::default();
        let module_registry = crate::module_registry::ModuleRegistry::default();
        let deployment_validator = Arc::new(crate::service::deployment::DeploymentValidator {
            snapshot: bootstrap_snapshot,
            max_heap_bytes: runtime.max_isolate_heap_bytes,
            timeout: runtime.isolate_startup_timeout,
            policy: crate::engine::IsolatePolicy::for_config(&runtime),
            slots: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        let temporary_worker_ttl = runtime.temporary_worker_ttl;
        let request_body_budget = Arc::new(tokio::sync::Semaphore::new(
            runtime.max_buffered_request_bytes,
        ));
        let request_body_chunk_bytes = runtime.max_buffered_request_bytes.min(64 * 1024);
        let console = crate::ops::WorkerConsoleSink::new();
        let inspector = InspectorRegistry::new(runtime.inspector);
        let runtime_thread = spawn_runtime_thread(RuntimeThreadStart {
            admission,
            routes,
            receiver,
            cancel_receiver,
            cancellation_receiver,
            runtime_fast_sender: fast_sender.clone(),
            asset_catalog: asset_catalog.clone(),
            bootstrap_snapshot,
            console: console.clone(),
            inspector: inspector.clone(),
            kv_store: kv_store.clone(),
            memory_store: memory_store.clone(),
            cache_store: cache_store.clone(),
            config: runtime,
            storage: storage.clone(),
            control_store: control_store.clone(),
            module_registry: module_registry.clone(),
        })?;
        let shutdown = Arc::new(RuntimeShutdownState::new(runtime_thread));
        let lifetime = Arc::new(RuntimeServiceLifetime {
            cancel_sender: fast_sender.clone(),
            shutdown: Arc::clone(&shutdown),
        });
        let service = Self {
            sender,
            console,
            inspector,
            cancel_sender,
            fast_sender,
            asset_catalog,
            state_store,
            memory_store,
            cache_store,
            request_body_budget,
            request_body_chunk_bytes,
            control_store,
            deployment_validator,
            deployment_admission: Arc::new(tokio::sync::Semaphore::new(MAX_ACCEPTED_DEPLOYMENTS)),
            deployment_order: Arc::new(tokio::sync::Mutex::new(())),
            deployment_stopping: tokio::sync::watch::channel(false).0,
            temporary_worker_ttl,
            _module_registry: module_registry,
            storage,
            shutdown,
            _lifetime: lifetime,
        };
        if let Err(error) = service.restore_workers_from_store().await {
            let _ = service.shutdown().await;
            return Err(error);
        }
        Ok(service)
    }

    pub fn request_body_budget(&self) -> (Arc<tokio::sync::Semaphore>, usize) {
        (
            Arc::clone(&self.request_body_budget),
            self.request_body_chunk_bytes,
        )
    }

    async fn restore_workers_from_store(&self) -> Result<()> {
        if !self.storage.worker_store_enabled {
            return Ok(());
        }
        let now_ms = epoch_ms_i64()?;
        let mut restored = 0usize;
        for stored in self.control_store.active_deployments().await? {
            if stored
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
            {
                info!(
                    worker = %stored.worker,
                    deployment_id = %stored.deployment_id,
                    "deactivating expired temporary worker from control store"
                );
                self.control_store.deactivate_worker(&stored.worker).await?;
                continue;
            }
            let worker = stored.worker.clone();
            let deployment_id = stored.deployment_id.clone();
            let restore_result = self
                .deploy_with_config_internal(DeployWithConfigRequest {
                    worker_name: stored.worker,
                    source: stored.source,
                    config: stored.config,
                    assets: stored.assets,
                    server_modules: stored.server_modules,
                    asset_headers: stored.asset_headers,
                    deployment_id: Some(deployment_id.clone()),
                    persistence: DeploymentPersistence::Restore,
                    temporary: stored.expires_at_ms.is_some(),
                    expires_at_ms: stored.expires_at_ms,
                    enforce_temporary_transition: false,
                })
                .await;
            let diagnostic = restore_result.as_ref().map(|_| ()).map_err(Clone::clone);
            self.control_store
                .record_restore_result(&worker, Some(&deployment_id), &diagnostic)
                .await?;
            match restore_result {
                Ok(restored_id) => {
                    restored += 1;
                    info!(
                        worker = %worker,
                        deployment_id = %restored_id,
                        "restored worker from control store"
                    );
                }
                Err(error) => {
                    return Err(PlatformError::internal(format!(
                        "failed to restore worker {worker} deployment {deployment_id}: {error}"
                    )));
                }
            }
        }

        if restored > 0 {
            info!(restored, "restored workers from control store");
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.close_deployment_admission();
        self.deployment_stopping.send_replace(true);
        // Cancel queued validations, but allow transactions that have already
        // started persistence to finish publication before stopping the runtime.
        // Validation threads retain their slots until V8 actually exits.
        while self.deployment_admission.available_permits() != MAX_ACCEPTED_DEPLOYMENTS
            || self.deployment_validator.slots.available_permits() != 1
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Use the fast control lane so shutdown is not queued behind a full
        // request/deploy channel once the bounded drain deadline has elapsed.
        self.shutdown.start(self.fast_sender.clone());
        self.shutdown.wait().await
    }

    pub fn close_deployment_admission(&self) {
        self.deployment_admission.close();
    }

    pub fn active_deployment_operations(&self) -> usize {
        MAX_ACCEPTED_DEPLOYMENTS - self.deployment_admission.available_permits()
    }
}
