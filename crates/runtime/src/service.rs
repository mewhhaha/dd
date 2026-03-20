use crate::cache::{CacheConfig, CacheStore};
use crate::engine::{
    abort_worker_request, build_bootstrap_snapshot, build_worker_snapshot, dispatch_worker_request,
    new_runtime_from_snapshot, pump_event_loop_once, validate_worker,
};
use crate::kv::KvStore;
use crate::ops::{IsolateEventPayload, IsolateEventSender};
use common::{DeployBinding, DeployConfig, PlatformError, Result, WorkerInvocation, WorkerOutput};
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc as std_mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub min_isolates: usize,
    pub max_isolates: usize,
    pub max_inflight_per_isolate: usize,
    pub idle_ttl: Duration,
    pub scale_tick: Duration,
    pub queue_warn_thresholds: Vec<usize>,
    pub cache_max_entries: usize,
    pub cache_max_bytes: usize,
    pub cache_default_ttl: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            min_isolates: 0,
            max_isolates: 8,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(30),
            scale_tick: Duration::from_secs(1),
            queue_warn_thresholds: vec![10, 100, 1000],
            cache_max_entries: 2048,
            cache_max_bytes: 64 * 1024 * 1024,
            cache_default_ttl: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub generation: u64,
    pub queued: usize,
    pub busy: usize,
    pub inflight_total: usize,
    pub wait_until_total: usize,
    pub isolates_total: usize,
    pub spawn_count: u64,
    pub reuse_count: u64,
    pub scale_down_count: u64,
}

#[derive(Debug)]
pub struct WorkerStreamOutput {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: mpsc::UnboundedReceiver<Result<Vec<u8>>>,
}

#[derive(Clone)]
pub struct RuntimeService {
    sender: mpsc::Sender<RuntimeCommand>,
    cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
}

enum RuntimeCommand {
    Deploy {
        worker_name: String,
        source: String,
        config: DeployConfig,
        reply: oneshot::Sender<Result<String>>,
    },
    Invoke {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    RegisterStream {
        worker_name: String,
        runtime_request_id: String,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
    },
    Cancel {
        worker_name: String,
        runtime_request_id: String,
    },
    Stats {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerStats>>,
    },
}

struct WorkerManager {
    config: RuntimeConfig,
    bootstrap_snapshot: &'static [u8],
    kv_store: KvStore,
    cache_store: CacheStore,
    workers: HashMap<String, WorkerEntry>,
    pre_canceled: HashMap<String, HashSet<String>>,
    stream_registrations: HashMap<String, StreamRegistration>,
    next_generation: u64,
    next_isolate_id: u64,
}

struct StreamRegistration {
    worker_name: String,
    completion_token: Option<String>,
    ready: Option<oneshot::Sender<Result<WorkerStreamOutput>>>,
    body_sender: mpsc::UnboundedSender<Result<Vec<u8>>>,
    body_receiver: Option<mpsc::UnboundedReceiver<Result<Vec<u8>>>>,
    started: bool,
}

struct WorkerEntry {
    current_generation: u64,
    pools: HashMap<u64, WorkerPool>,
}

struct WorkerPool {
    worker_name: String,
    generation: u64,
    deployment_id: String,
    snapshot: &'static [u8],
    kv_bindings: Vec<String>,
    queue: VecDeque<PendingInvoke>,
    isolates: Vec<IsolateHandle>,
    stats: PoolStats,
    queue_warn_level: usize,
}

#[derive(Default)]
struct PoolStats {
    spawn_count: u64,
    reuse_count: u64,
    scale_down_count: u64,
}

struct PendingInvoke {
    runtime_request_id: String,
    request: WorkerInvocation,
    reply: oneshot::Sender<Result<WorkerOutput>>,
}

struct PendingReply {
    completion_token: String,
    canceled: bool,
    reply: oneshot::Sender<Result<WorkerOutput>>,
}

struct IsolateHandle {
    id: u64,
    sender: mpsc::UnboundedSender<IsolateCommand>,
    inflight_count: usize,
    served_requests: u64,
    last_used_at: Instant,
    pending_replies: HashMap<String, PendingReply>,
    pending_wait_until: HashMap<String, String>,
}

enum IsolateCommand {
    Execute {
        runtime_request_id: String,
        completion_token: String,
        worker_name: String,
        kv_bindings: Vec<String>,
        request: WorkerInvocation,
    },
    Abort {
        runtime_request_id: String,
    },
    Shutdown,
}

#[derive(Clone)]
struct InvokeCancelGuard {
    cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
    worker_name: String,
    runtime_request_id: String,
    armed: bool,
}

impl InvokeCancelGuard {
    fn new(
        cancel_sender: mpsc::UnboundedSender<RuntimeCommand>,
        worker_name: String,
        runtime_request_id: String,
    ) -> Self {
        Self {
            cancel_sender,
            worker_name,
            runtime_request_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InvokeCancelGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let _ = self.cancel_sender.send(RuntimeCommand::Cancel {
            worker_name: self.worker_name.clone(),
            runtime_request_id: self.runtime_request_id.clone(),
        });
    }
}

enum RuntimeEvent {
    RequestFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        completion_token: String,
        wait_until_count: usize,
        result: Result<WorkerOutput>,
    },
    WaitUntilFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        completion_token: String,
    },
    ResponseStart {
        worker_name: String,
        request_id: String,
        completion_token: String,
        status: u16,
        headers: Vec<(String, String)>,
    },
    ResponseChunk {
        worker_name: String,
        request_id: String,
        completion_token: String,
        chunk: Vec<u8>,
    },
    IsolateFailed {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    },
}

#[derive(Deserialize)]
struct CompletionPayload {
    request_id: String,
    completion_token: String,
    #[serde(default)]
    wait_until_count: usize,
    ok: bool,
    result: Option<WorkerOutput>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct WaitUntilPayload {
    request_id: String,
    completion_token: String,
}

#[derive(Deserialize)]
struct ResponseStartPayload {
    request_id: String,
    completion_token: String,
    status: u16,
    headers: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct ResponseChunkPayload {
    request_id: String,
    completion_token: String,
    chunk: Vec<u8>,
}

impl RuntimeService {
    pub async fn start() -> Result<Self> {
        Self::start_with_config(RuntimeConfig::default()).await
    }

    pub async fn start_with_config(config: RuntimeConfig) -> Result<Self> {
        if config.max_isolates == 0 {
            return Err(PlatformError::internal(
                "max_isolates must be greater than 0",
            ));
        }
        if config.max_inflight_per_isolate == 0 {
            return Err(PlatformError::internal(
                "max_inflight_per_isolate must be greater than 0",
            ));
        }
        if config.min_isolates > config.max_isolates {
            return Err(PlatformError::internal(
                "min_isolates cannot exceed max_isolates",
            ));
        }
        if config.cache_max_entries == 0 {
            return Err(PlatformError::internal(
                "cache_max_entries must be greater than 0",
            ));
        }
        if config.cache_max_bytes == 0 {
            return Err(PlatformError::internal(
                "cache_max_bytes must be greater than 0",
            ));
        }
        if config.cache_default_ttl.is_zero() {
            return Err(PlatformError::internal(
                "cache_default_ttl must be greater than 0",
            ));
        }

        let bootstrap_snapshot = build_bootstrap_snapshot().await?;
        let kv_store = KvStore::from_env().await?;
        let cache_store = CacheStore::from_env(CacheConfig {
            max_entries: config.cache_max_entries,
            max_bytes: config.cache_max_bytes,
            default_ttl: config.cache_default_ttl,
            ..CacheConfig::default()
        })
        .await?;
        let (sender, receiver) = mpsc::channel(256);
        let (cancel_sender, cancel_receiver) = mpsc::unbounded_channel();
        spawn_runtime_thread(
            receiver,
            cancel_receiver,
            bootstrap_snapshot,
            kv_store,
            cache_store,
            config,
        )?;
        Ok(Self {
            sender,
            cancel_sender,
        })
    }

    pub async fn deploy(&self, worker_name: String, source: String) -> Result<String> {
        self.deploy_with_config(worker_name, source, DeployConfig::default())
            .await
    }

    pub async fn deploy_with_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
    ) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Deploy {
                worker_name,
                source,
                config,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime deploy channel closed"))?
    }

    pub async fn invoke(
        &self,
        worker_name: String,
        request: WorkerInvocation,
    ) -> Result<WorkerOutput> {
        let runtime_request_id = Uuid::new_v4().to_string();
        let mut cancel_guard = InvokeCancelGuard::new(
            self.cancel_sender.clone(),
            worker_name.clone(),
            runtime_request_id.clone(),
        );
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Invoke {
                worker_name,
                runtime_request_id,
                request,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let reply = reply_rx.await;
        cancel_guard.disarm();
        reply.map_err(|_| PlatformError::internal("runtime invoke channel closed"))?
    }

    pub async fn invoke_stream(
        &self,
        worker_name: String,
        request: WorkerInvocation,
    ) -> Result<WorkerStreamOutput> {
        let runtime_request_id = Uuid::new_v4().to_string();
        let mut cancel_guard = InvokeCancelGuard::new(
            self.cancel_sender.clone(),
            worker_name.clone(),
            runtime_request_id.clone(),
        );
        let (ready_tx, ready_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::RegisterStream {
                worker_name: worker_name.clone(),
                runtime_request_id: runtime_request_id.clone(),
                ready: ready_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Invoke {
                worker_name,
                runtime_request_id,
                request,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
        tokio::spawn(async move {
            let _ = reply_rx.await;
        });

        let ready = ready_rx
            .await
            .map_err(|_| PlatformError::internal("runtime stream channel closed"))?;
        cancel_guard.disarm();
        ready
    }

    pub async fn stats(&self, worker_name: String) -> Option<WorkerStats> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::Stats {
                worker_name,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return None;
        }
        reply_rx.await.ok().flatten()
    }
}

impl WorkerManager {
    fn new(
        bootstrap_snapshot: &'static [u8],
        kv_store: KvStore,
        cache_store: CacheStore,
        config: RuntimeConfig,
    ) -> Self {
        Self {
            config,
            bootstrap_snapshot,
            kv_store,
            cache_store,
            workers: HashMap::new(),
            pre_canceled: HashMap::new(),
            stream_registrations: HashMap::new(),
            next_generation: 1,
            next_isolate_id: 1,
        }
    }

    async fn handle_command(
        &mut self,
        command: RuntimeCommand,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        match command {
            RuntimeCommand::Deploy {
                worker_name,
                source,
                config,
                reply,
            } => {
                let result = self.deploy(worker_name, source, config).await;
                let _ = reply.send(result);
            }
            RuntimeCommand::Invoke {
                worker_name,
                runtime_request_id,
                request,
                reply,
            } => {
                self.enqueue_invoke(worker_name, runtime_request_id, request, reply, event_tx);
            }
            RuntimeCommand::RegisterStream {
                worker_name,
                runtime_request_id,
                ready,
            } => {
                self.register_stream(worker_name, runtime_request_id, ready);
            }
            RuntimeCommand::Cancel {
                worker_name,
                runtime_request_id,
            } => {
                self.cancel_invoke(worker_name, runtime_request_id, event_tx);
            }
            RuntimeCommand::Stats { worker_name, reply } => {
                let _ = reply.send(self.worker_stats(&worker_name));
            }
        }
    }

    fn handle_event(
        &mut self,
        event: RuntimeEvent,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        match event {
            RuntimeEvent::RequestFinished {
                worker_name,
                generation,
                isolate_id,
                request_id,
                completion_token,
                wait_until_count,
                result,
            } => {
                self.finish_request(
                    &worker_name,
                    generation,
                    isolate_id,
                    &request_id,
                    &completion_token,
                    wait_until_count,
                    result,
                );
                self.dispatch_pool(&worker_name, generation, event_tx);
                self.cleanup_drained_generations_for(&worker_name);
            }
            RuntimeEvent::WaitUntilFinished {
                worker_name,
                generation,
                isolate_id,
                request_id,
                completion_token,
            } => {
                self.finish_wait_until(
                    &worker_name,
                    generation,
                    isolate_id,
                    &request_id,
                    &completion_token,
                );
                self.cleanup_drained_generations_for(&worker_name);
            }
            RuntimeEvent::ResponseStart {
                worker_name,
                request_id,
                completion_token,
                status,
                headers,
            } => {
                self.handle_response_start(
                    &worker_name,
                    &request_id,
                    &completion_token,
                    status,
                    headers,
                );
            }
            RuntimeEvent::ResponseChunk {
                worker_name,
                request_id,
                completion_token,
                chunk,
            } => {
                self.handle_response_chunk(&worker_name, &request_id, &completion_token, chunk);
            }
            RuntimeEvent::IsolateFailed {
                worker_name,
                generation,
                isolate_id,
                error,
            } => {
                self.fail_isolate(&worker_name, generation, isolate_id, error);
                self.dispatch_pool(&worker_name, generation, event_tx);
                self.cleanup_drained_generations_for(&worker_name);
            }
        }
    }

    async fn deploy(
        &mut self,
        worker_name: String,
        source: String,
        config: DeployConfig,
    ) -> Result<String> {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return Err(PlatformError::bad_request("Worker name must not be empty"));
        }
        let kv_bindings = extract_kv_bindings(&config)?;

        validate_worker(self.bootstrap_snapshot, &source).await?;
        let worker_snapshot = build_worker_snapshot(self.bootstrap_snapshot, &source).await?;
        let generation = self.next_generation;
        self.next_generation += 1;
        let deployment_id = Uuid::new_v4().to_string();

        let pool = WorkerPool {
            worker_name: worker_name.clone(),
            generation,
            deployment_id: deployment_id.clone(),
            snapshot: worker_snapshot,
            kv_bindings,
            queue: VecDeque::new(),
            isolates: Vec::new(),
            stats: PoolStats::default(),
            queue_warn_level: 0,
        };

        let entry = self
            .workers
            .entry(worker_name.clone())
            .or_insert_with(|| WorkerEntry {
                current_generation: generation,
                pools: HashMap::new(),
            });
        entry.current_generation = generation;
        entry.pools.insert(generation, pool);
        self.cleanup_drained_generations_for(&worker_name);
        info!(worker = %worker_name, generation, deployment_id = %deployment_id, "deployed worker");
        Ok(deployment_id)
    }

    fn register_stream(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
    ) {
        let (body_sender, body_receiver) = mpsc::unbounded_channel();
        self.stream_registrations.insert(
            runtime_request_id,
            StreamRegistration {
                worker_name,
                completion_token: None,
                ready: Some(ready),
                body_sender,
                body_receiver: Some(body_receiver),
                started: false,
            },
        );
    }

    fn enqueue_invoke(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let worker_name = worker_name.trim().to_string();
        if self
            .pre_canceled
            .get_mut(&worker_name)
            .map(|request_ids| request_ids.remove(&runtime_request_id))
            .unwrap_or(false)
        {
            let _ = reply.send(Err(PlatformError::runtime("request was aborted")));
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
            return;
        }
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        let Some(generation) = self
            .workers
            .get(&worker_name)
            .map(|entry| entry.current_generation)
        else {
            let error = PlatformError::not_found("Worker not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        };

        if let Some(pool) = self.get_pool_mut(&worker_name, generation) {
            pool.queue.push_back(PendingInvoke {
                runtime_request_id,
                request,
                reply,
            });
            pool.update_queue_warning(&warn_thresholds);
        } else {
            let error = PlatformError::not_found("Worker not found");
            let _ = reply.send(Err(error.clone()));
            self.fail_stream_registration(&worker_name, &runtime_request_id, error);
            return;
        }

        self.dispatch_pool(&worker_name, generation, event_tx);
    }

    fn cancel_invoke(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return;
        }

        let mut touched_generations = Vec::new();
        let mut abort_commands = Vec::new();
        let mut matched = false;

        if let Some(entry) = self.workers.get_mut(&worker_name) {
            for (generation, pool) in &mut entry.pools {
                let mut generation_touched = false;

                if let Some(idx) = pool
                    .queue
                    .iter()
                    .position(|pending| pending.runtime_request_id == runtime_request_id)
                {
                    if let Some(pending) = pool.queue.remove(idx) {
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::runtime("request was aborted")));
                        generation_touched = true;
                        matched = true;
                    }
                }

                for isolate in &mut pool.isolates {
                    if let Some(pending_reply) =
                        isolate.pending_replies.get_mut(&runtime_request_id)
                    {
                        pending_reply.canceled = true;
                        abort_commands.push((*generation, isolate.id, isolate.sender.clone()));
                        generation_touched = true;
                        matched = true;
                    }
                }

                if generation_touched {
                    pool.log_stats("cancel");
                    touched_generations.push(*generation);
                }
            }
        }

        for (generation, isolate_id, sender) in abort_commands {
            if sender
                .send(IsolateCommand::Abort {
                    runtime_request_id: runtime_request_id.clone(),
                })
                .is_err()
            {
                let failed = self.remove_isolate_by_id(&worker_name, generation, isolate_id);
                for reply in failed {
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
            }
        }

        touched_generations.sort_unstable();
        touched_generations.dedup();
        for generation in touched_generations {
            self.dispatch_pool(&worker_name, generation, event_tx);
        }
        if !matched {
            self.pre_canceled
                .entry(worker_name.clone())
                .or_default()
                .insert(runtime_request_id.clone());
            self.fail_stream_registration(
                &worker_name,
                &runtime_request_id,
                PlatformError::runtime("request was aborted"),
            );
        }
        self.cleanup_drained_generations_for(&worker_name);
    }

    fn dispatch_pool(
        &mut self,
        worker_name: &str,
        generation: u64,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        loop {
            let mut selected_isolate_idx: Option<usize> = None;
            let mut pending: Option<PendingInvoke> = None;
            let max_inflight = self.config.max_inflight_per_isolate;

            let spawn_needed = {
                let Some(pool) = self.get_pool_mut(worker_name, generation) else {
                    return;
                };

                if pool.queue.is_empty() {
                    pool.log_stats("dispatch");
                    return;
                }

                if let Some((idx, _)) = pool
                    .isolates
                    .iter()
                    .enumerate()
                    .filter(|(_, isolate)| isolate.inflight_count < max_inflight)
                    .min_by_key(|(_, isolate)| isolate.inflight_count)
                {
                    selected_isolate_idx = Some(idx);
                    pending = pool.queue.pop_front();
                    false
                } else {
                    pool.isolates.len() < self.config.max_isolates
                }
            };

            if spawn_needed {
                if let Err(error) = self.spawn_isolate(worker_name, generation, event_tx.clone()) {
                    if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                        if let Some(pending) = pool.queue.pop_front() {
                            let _ = pending.reply.send(Err(error));
                        }
                    }
                    return;
                }
                continue;
            }

            let Some(isolate_idx) = selected_isolate_idx else {
                return;
            };
            let Some(pending_invoke) = pending else {
                return;
            };

            let runtime_request_id = pending_invoke.runtime_request_id.clone();
            let completion_token = Uuid::new_v4().to_string();
            if let Some(registration) = self.stream_registrations.get_mut(&runtime_request_id) {
                if registration.worker_name == worker_name {
                    registration.completion_token = Some(completion_token.clone());
                }
            }
            let mut send_failed = false;
            if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                if isolate_idx >= pool.isolates.len() {
                    continue;
                }

                let kv_bindings = pool.kv_bindings.clone();
                let should_count_reuse = pool.isolates[isolate_idx].served_requests > 0;
                if should_count_reuse {
                    pool.stats.reuse_count += 1;
                }
                let isolate = &mut pool.isolates[isolate_idx];
                isolate.served_requests += 1;
                let command = IsolateCommand::Execute {
                    runtime_request_id: runtime_request_id.clone(),
                    completion_token: completion_token.clone(),
                    worker_name: worker_name.to_string(),
                    kv_bindings,
                    request: pending_invoke.request,
                };

                if isolate.sender.send(command).is_err() {
                    send_failed = true;
                } else {
                    isolate.inflight_count += 1;
                    isolate.pending_replies.insert(
                        runtime_request_id.clone(),
                        PendingReply {
                            completion_token,
                            canceled: false,
                            reply: pending_invoke.reply,
                        },
                    );
                }
            }

            if send_failed {
                let failed = self.remove_isolate(worker_name, generation, isolate_idx);
                for reply in failed {
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
                self.fail_stream_registration(
                    worker_name,
                    &runtime_request_id,
                    PlatformError::internal("isolate is unavailable"),
                );
                continue;
            }
        }
    }

    fn spawn_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let snapshot = self
            .get_pool_mut(worker_name, generation)
            .ok_or_else(|| PlatformError::not_found("Worker not found"))?
            .snapshot;
        let isolate_id = self.next_isolate_id;
        self.next_isolate_id += 1;
        let kv_store = self.kv_store.clone();
        let cache_store = self.cache_store.clone();
        let isolate = spawn_isolate_thread(
            snapshot,
            kv_store,
            cache_store,
            worker_name.to_string(),
            generation,
            isolate_id,
            event_tx,
        )?;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.stats.spawn_count += 1;
            pool.isolates.push(isolate);
            pool.log_stats("spawn");
            Ok(())
        } else {
            Err(PlatformError::internal("worker pool missing"))
        }
    }

    fn finish_request(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        request_id: &str,
        completion_token: &str,
        wait_until_count: usize,
        result: Result<WorkerOutput>,
    ) {
        let stream_result = result.clone();
        let mut reply = None;
        let mut canceled = false;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                let Some(pending) = isolate.pending_replies.get(request_id) else {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion for unknown request id"
                    );
                    return;
                };
                if pending.completion_token != completion_token {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion with invalid token"
                    );
                    return;
                }

                isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                if isolate.inflight_count == 0 {
                    isolate.last_used_at = Instant::now();
                }
                if let Some(pending) = isolate.pending_replies.remove(request_id) {
                    canceled = pending.canceled;
                    if wait_until_count > 0 {
                        isolate
                            .pending_wait_until
                            .insert(request_id.to_string(), completion_token.to_string());
                    }
                    reply = Some(pending.reply);
                }
            }
            pool.log_stats("complete");
        }

        if !canceled {
            if let Some(reply) = reply {
                let _ = reply.send(result);
            }
        } else {
            info!(
                worker = %worker_name,
                generation,
                isolate_id,
                request_id,
                "dropped completion for canceled request"
            );
        }
        self.complete_stream_registration(worker_name, request_id, completion_token, stream_result);
    }

    fn fail_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    ) {
        let failed = self.remove_isolate_by_id(worker_name, generation, isolate_id);
        for reply in failed {
            let _ = reply.send(Err(error.clone()));
        }
        self.fail_all_streams_for_worker(worker_name, error);
    }

    fn finish_wait_until(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        request_id: &str,
        completion_token: &str,
    ) {
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                if let Some(token) = isolate.pending_wait_until.get(request_id) {
                    if token == completion_token {
                        isolate.pending_wait_until.remove(request_id);
                        if isolate.inflight_count == 0 && isolate.pending_wait_until.is_empty() {
                            isolate.last_used_at = Instant::now();
                        }
                    }
                }
            }
            pool.log_stats("wait_until_done");
        }
    }

    fn handle_response_start(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        status: u16,
        headers: Vec<(String, String)>,
    ) {
        let Some(registration) = self.stream_registrations.get_mut(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            return;
        }
        registration.started = true;
        if let Some(ready) = registration.ready.take() {
            if let Some(body) = registration.body_receiver.take() {
                let _ = ready.send(Ok(WorkerStreamOutput {
                    status,
                    headers,
                    body,
                }));
            } else {
                let _ = ready.send(Err(PlatformError::internal("stream body receiver missing")));
            }
        }
    }

    fn handle_response_chunk(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        chunk: Vec<u8>,
    ) {
        let Some(registration) = self.stream_registrations.get(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            return;
        }
        let _ = registration.body_sender.send(Ok(chunk));
    }

    fn fail_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        error: PlatformError,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if let Some(ready) = registration.ready.take() {
            let _ = ready.send(Err(error.clone()));
            return;
        }
        let _ = registration.body_sender.send(Err(error));
    }

    fn complete_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        result: Result<WorkerOutput>,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }

        match result {
            Ok(output) => {
                if !registration.started {
                    if let Some(ready) = registration.ready.take() {
                        if let Some(body) = registration.body_receiver.take() {
                            let _ = ready.send(Ok(WorkerStreamOutput {
                                status: output.status,
                                headers: output.headers.clone(),
                                body,
                            }));
                        } else {
                            let _ = ready
                                .send(Err(PlatformError::internal("stream body receiver missing")));
                        }
                    }
                    if !output.body.is_empty() {
                        let _ = registration.body_sender.send(Ok(output.body));
                    }
                }
            }
            Err(error) => {
                if let Some(ready) = registration.ready.take() {
                    let _ = ready.send(Err(error.clone()));
                } else {
                    let _ = registration.body_sender.send(Err(error));
                }
            }
        }
    }

    fn fail_all_streams_for_worker(&mut self, worker_name: &str, error: PlatformError) {
        let request_ids: Vec<String> = self
            .stream_registrations
            .iter()
            .filter(|(_, registration)| registration.worker_name == worker_name)
            .map(|(request_id, _)| request_id.clone())
            .collect();

        for request_id in request_ids {
            self.fail_stream_registration(worker_name, &request_id, error.clone());
        }
    }

    fn remove_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_idx: usize,
    ) -> Vec<oneshot::Sender<Result<WorkerOutput>>> {
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if isolate_idx < pool.isolates.len() {
                let isolate = pool.isolates.swap_remove(isolate_idx);
                let _ = isolate.sender.send(IsolateCommand::Shutdown);
                return isolate
                    .pending_replies
                    .into_values()
                    .map(|pending| pending.reply)
                    .collect();
            }
        }
        Vec::new()
    }

    fn remove_isolate_by_id(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) -> Vec<oneshot::Sender<Result<WorkerOutput>>> {
        let isolate_idx = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .position(|isolate| isolate.id == isolate_id)
            });
        if let Some(idx) = isolate_idx {
            return self.remove_isolate(worker_name, generation, idx);
        }
        Vec::new()
    }

    fn scale_down_idle(&mut self) {
        let now = Instant::now();
        let worker_names: Vec<String> = self.workers.keys().cloned().collect();
        for worker_name in worker_names {
            let generations: Vec<u64> = self
                .workers
                .get(&worker_name)
                .map(|entry| entry.pools.keys().copied().collect())
                .unwrap_or_default();
            for generation in generations {
                self.scale_down_pool(&worker_name, generation, now);
            }
            self.cleanup_drained_generations_for(&worker_name);
        }
    }

    fn scale_down_pool(&mut self, worker_name: &str, generation: u64, now: Instant) {
        let min_isolates = self.config.min_isolates;
        let idle_ttl = self.config.idle_ttl;
        let mut removed = Vec::new();
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            loop {
                if pool.isolates.len() <= min_isolates {
                    break;
                }

                let candidate = pool
                    .isolates
                    .iter()
                    .enumerate()
                    .filter(|(_, isolate)| isolate.inflight_count == 0)
                    .filter(|(_, isolate)| isolate.pending_wait_until.is_empty())
                    .filter(|(_, isolate)| now.duration_since(isolate.last_used_at) >= idle_ttl)
                    .min_by_key(|(_, isolate)| isolate.last_used_at);
                let Some((idx, _)) = candidate else {
                    break;
                };
                let isolate = pool.isolates.swap_remove(idx);
                pool.stats.scale_down_count += 1;
                removed.push(isolate);
            }

            if !removed.is_empty() {
                pool.log_stats("scale_down");
            }
        }

        for isolate in removed {
            let _ = isolate.sender.send(IsolateCommand::Shutdown);
            for pending in isolate.pending_replies.into_values() {
                let _ = pending
                    .reply
                    .send(Err(PlatformError::internal("isolate scaled down")));
            }
        }
    }

    fn cleanup_drained_generations_for(&mut self, worker_name: &str) {
        let Some(entry) = self.workers.get_mut(worker_name) else {
            return;
        };
        let current_generation = entry.current_generation;
        let drained: Vec<u64> = entry
            .pools
            .iter()
            .filter(|(generation, pool)| **generation != current_generation && pool.is_drained())
            .map(|(generation, _)| *generation)
            .collect();

        for generation in drained {
            if let Some(pool) = entry.pools.remove(&generation) {
                for isolate in pool.isolates {
                    let _ = isolate.sender.send(IsolateCommand::Shutdown);
                    for pending in isolate.pending_replies.into_values() {
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("worker generation retired")));
                    }
                }
                info!(worker = %pool.worker_name, generation, "retired worker generation");
            }
        }
    }

    fn worker_stats(&self, worker_name: &str) -> Option<WorkerStats> {
        let entry = self.workers.get(worker_name)?;
        let pool = entry.pools.get(&entry.current_generation)?;
        Some(pool.stats_snapshot())
    }

    fn get_pool_mut(&mut self, worker_name: &str, generation: u64) -> Option<&mut WorkerPool> {
        self.workers
            .get_mut(worker_name)
            .and_then(|entry| entry.pools.get_mut(&generation))
    }

    fn shutdown_all(&mut self) {
        for entry in self.workers.values_mut() {
            for pool in entry.pools.values_mut() {
                for isolate in pool.isolates.drain(..) {
                    let _ = isolate.sender.send(IsolateCommand::Shutdown);
                    for pending in isolate.pending_replies.into_values() {
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("runtime shutting down")));
                    }
                }
            }
        }
        for (_, mut registration) in std::mem::take(&mut self.stream_registrations) {
            let error = PlatformError::internal("runtime shutting down");
            if let Some(ready) = registration.ready.take() {
                let _ = ready.send(Err(error.clone()));
            } else {
                let _ = registration.body_sender.send(Err(error));
            }
        }
    }
}

impl WorkerPool {
    fn is_drained(&self) -> bool {
        self.queue.is_empty() && self.inflight_total() == 0 && self.wait_until_total() == 0
    }

    fn busy_count(&self) -> usize {
        self.isolates
            .iter()
            .filter(|isolate| isolate.inflight_count > 0 || !isolate.pending_wait_until.is_empty())
            .count()
    }

    fn inflight_total(&self) -> usize {
        self.isolates
            .iter()
            .map(|isolate| isolate.inflight_count)
            .sum()
    }

    fn wait_until_total(&self) -> usize {
        self.isolates
            .iter()
            .map(|isolate| isolate.pending_wait_until.len())
            .sum()
    }

    fn update_queue_warning(&mut self, thresholds: &[usize]) {
        let queue_len = self.queue.len();
        let level = thresholds
            .iter()
            .take_while(|threshold| queue_len >= **threshold)
            .count();
        if level > self.queue_warn_level {
            warn!(
                worker = %self.worker_name,
                generation = self.generation,
                queued = queue_len,
                "worker queue depth crossed warning threshold"
            );
        }
        self.queue_warn_level = level;
    }

    fn stats_snapshot(&self) -> WorkerStats {
        WorkerStats {
            generation: self.generation,
            queued: self.queue.len(),
            busy: self.busy_count(),
            inflight_total: self.inflight_total(),
            wait_until_total: self.wait_until_total(),
            isolates_total: self.isolates.len(),
            spawn_count: self.stats.spawn_count,
            reuse_count: self.stats.reuse_count,
            scale_down_count: self.stats.scale_down_count,
        }
    }

    fn log_stats(&self, event: &str) {
        let snapshot = self.stats_snapshot();
        info!(
            worker = %self.worker_name,
            generation = snapshot.generation,
            deployment_id = %self.deployment_id,
            queued = snapshot.queued,
            busy = snapshot.busy,
            inflight_total = snapshot.inflight_total,
            wait_until_total = snapshot.wait_until_total,
            isolates_total = snapshot.isolates_total,
            spawn_count = snapshot.spawn_count,
            reuse_count = snapshot.reuse_count,
            scale_down_count = snapshot.scale_down_count,
            event,
            "worker pool stats"
        );
    }
}

fn extract_kv_bindings(config: &DeployConfig) -> Result<Vec<String>> {
    let mut bindings = Vec::new();
    let mut seen = HashSet::new();
    for binding in &config.bindings {
        match binding {
            DeployBinding::Kv { binding } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                bindings.push(name.to_string());
            }
        }
    }
    Ok(bindings)
}

fn spawn_runtime_thread(
    mut receiver: mpsc::Receiver<RuntimeCommand>,
    mut cancel_receiver: mpsc::UnboundedReceiver<RuntimeCommand>,
    bootstrap_snapshot: &'static [u8],
    kv_store: KvStore,
    cache_store: CacheStore,
    config: RuntimeConfig,
) -> Result<()> {
    thread::Builder::new()
        .name("grugd-runtime".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime thread should build");

            runtime.block_on(async move {
                let (event_tx, mut event_rx) = mpsc::unbounded_channel();
                let mut manager =
                    WorkerManager::new(bootstrap_snapshot, kv_store, cache_store, config.clone());
                let mut ticker = tokio::time::interval(config.scale_tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        Some(command) = receiver.recv() => {
                            manager.handle_command(command, &event_tx).await;
                        }
                        Some(command) = cancel_receiver.recv() => {
                            manager.handle_command(command, &event_tx).await;
                        }
                        Some(event) = event_rx.recv() => {
                            manager.handle_event(event, &event_tx);
                        }
                        _ = ticker.tick() => {
                            manager.scale_down_idle();
                        }
                        else => {
                            break;
                        }
                    }
                }

                manager.shutdown_all();
            });
        })
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    Ok(())
}

fn spawn_isolate_thread(
    snapshot: &'static [u8],
    kv_store: KvStore,
    cache_store: CacheStore,
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
) -> Result<IsolateHandle> {
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
    let (init_tx, init_rx) = std_mpsc::channel::<Result<()>>();
    let thread_name = format!("grugd-isolate-{worker_name}-{generation}-{isolate_id}");

    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = init_tx.send(Err(PlatformError::internal(error.to_string())));
                    return;
                }
            };

            runtime.block_on(async move {
                let mut js_runtime = match new_runtime_from_snapshot(snapshot) {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = init_tx.send(Err(error));
                        return;
                    }
                };

                let (event_payload_tx, mut event_payload_rx) =
                    mpsc::unbounded_channel::<IsolateEventPayload>();
                {
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    op_state.put(IsolateEventSender(event_payload_tx));
                    op_state.put(kv_store.clone());
                    op_state.put(cache_store.clone());
                }
                let _ = init_tx.send(Ok(()));

                let mut ticker = tokio::time::interval(Duration::from_millis(1));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        Some(payload) = event_payload_rx.recv() => {
                            match payload {
                                IsolateEventPayload::Completion(payload) => {
                                    match decode_completion_payload(&payload) {
                                        Ok((request_id, completion_token, wait_until_count, result)) => {
                                            let _ = event_tx.send(RuntimeEvent::RequestFinished {
                                                worker_name: worker_name.clone(),
                                                generation,
                                                isolate_id,
                                                request_id,
                                                completion_token,
                                                wait_until_count,
                                                result,
                                            });
                                        }
                                        Err(error) => {
                                            warn!(
                                                worker = %worker_name,
                                                generation,
                                                isolate_id,
                                                error = %error,
                                                "ignoring invalid completion payload"
                                            );
                                        }
                                    }
                                }
                                IsolateEventPayload::WaitUntilDone(payload) => {
                                    match decode_wait_until_payload(&payload) {
                                        Ok((request_id, completion_token)) => {
                                            let _ = event_tx.send(RuntimeEvent::WaitUntilFinished {
                                                worker_name: worker_name.clone(),
                                                generation,
                                                isolate_id,
                                                request_id,
                                                completion_token,
                                            });
                                        }
                                        Err(error) => {
                                            warn!(
                                                worker = %worker_name,
                                                generation,
                                                isolate_id,
                                                error = %error,
                                                "ignoring invalid waitUntil payload"
                                            );
                                        }
                                    }
                                }
                                IsolateEventPayload::ResponseStart(payload) => {
                                    match decode_response_start_payload(&payload) {
                                        Ok((request_id, completion_token, status, headers)) => {
                                            let _ = event_tx.send(RuntimeEvent::ResponseStart {
                                                worker_name: worker_name.clone(),
                                                request_id,
                                                completion_token,
                                                status,
                                                headers,
                                            });
                                        }
                                        Err(error) => {
                                            warn!(
                                                worker = %worker_name,
                                                generation,
                                                isolate_id,
                                                error = %error,
                                                "ignoring invalid response start payload"
                                            );
                                        }
                                    }
                                }
                                IsolateEventPayload::ResponseChunk(payload) => {
                                    match decode_response_chunk_payload(&payload) {
                                        Ok((request_id, completion_token, chunk)) => {
                                            let _ = event_tx.send(RuntimeEvent::ResponseChunk {
                                                worker_name: worker_name.clone(),
                                                request_id,
                                                completion_token,
                                                chunk,
                                            });
                                        }
                                        Err(error) => {
                                            warn!(
                                                worker = %worker_name,
                                                generation,
                                                isolate_id,
                                                error = %error,
                                                "ignoring invalid response chunk payload"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Some(command) = command_rx.recv() => {
                            match command {
                                IsolateCommand::Execute {
                                    runtime_request_id,
                                    completion_token,
                                    worker_name: worker_name_for_env,
                                    kv_bindings,
                                    request,
                                } => {
                                    if let Err(error) = dispatch_worker_request(
                                        &mut js_runtime,
                                        &runtime_request_id,
                                        &completion_token,
                                        &worker_name_for_env,
                                        &kv_bindings,
                                        request,
                                    ) {
                                        let _ = event_tx.send(RuntimeEvent::RequestFinished {
                                            worker_name: worker_name.clone(),
                                            generation,
                                            isolate_id,
                                            request_id: runtime_request_id,
                                            completion_token,
                                            wait_until_count: 0,
                                            result: Err(error),
                                        });
                                    }
                                }
                                IsolateCommand::Abort { runtime_request_id } => {
                                    if let Err(error) =
                                        abort_worker_request(&mut js_runtime, &runtime_request_id)
                                    {
                                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                                            worker_name: worker_name.clone(),
                                            generation,
                                            isolate_id,
                                            error,
                                        });
                                        break;
                                    }
                                }
                                IsolateCommand::Shutdown => {
                                    break;
                                }
                            }
                        }
                        _ = ticker.tick() => {}
                        else => {
                            break;
                        }
                    }

                    if let Err(error) = pump_event_loop_once(&mut js_runtime) {
                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                            worker_name: worker_name.clone(),
                            generation,
                            isolate_id,
                            error,
                        });
                        break;
                    }
                }
            });
        })
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    match init_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => Ok(IsolateHandle {
            id: isolate_id,
            sender: command_tx,
            inflight_count: 0,
            served_requests: 0,
            last_used_at: Instant::now(),
            pending_replies: HashMap::new(),
            pending_wait_until: HashMap::new(),
        }),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(PlatformError::internal("isolate startup timed out")),
    }
}

fn decode_completion_payload(
    payload: &str,
) -> Result<(String, String, usize, Result<WorkerOutput>)> {
    let completion: CompletionPayload = serde_json::from_str(payload)
        .map_err(|error| PlatformError::runtime(format!("invalid completion payload: {error}")))?;
    if completion.ok {
        let output = completion
            .result
            .ok_or_else(|| PlatformError::runtime("completion is missing result"))?;
        Ok((
            completion.request_id,
            completion.completion_token,
            completion.wait_until_count,
            Ok(output),
        ))
    } else {
        let message = completion
            .error
            .unwrap_or_else(|| "worker execution failed".to_string());
        Ok((
            completion.request_id,
            completion.completion_token,
            completion.wait_until_count,
            Err(PlatformError::runtime(message)),
        ))
    }
}

fn decode_wait_until_payload(payload: &str) -> Result<(String, String)> {
    let done: WaitUntilPayload = serde_json::from_str(payload)
        .map_err(|error| PlatformError::runtime(format!("invalid waitUntil payload: {error}")))?;
    Ok((done.request_id, done.completion_token))
}

fn decode_response_start_payload(
    payload: &str,
) -> Result<(String, String, u16, Vec<(String, String)>)> {
    let start: ResponseStartPayload = serde_json::from_str(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid response start payload: {error}"))
    })?;
    Ok((
        start.request_id,
        start.completion_token,
        start.status,
        start.headers,
    ))
}

fn decode_response_chunk_payload(payload: &str) -> Result<(String, String, Vec<u8>)> {
    let chunk: ResponseChunkPayload = serde_json::from_str(payload).map_err(|error| {
        PlatformError::runtime(format!("invalid response chunk payload: {error}"))
    })?;
    Ok((chunk.request_id, chunk.completion_token, chunk.chunk))
}

#[cfg(test)]
mod tests {
    use super::{RuntimeConfig, RuntimeService};
    use common::WorkerInvocation;
    use serial_test::serial;
    use std::time::{Duration, Instant};
    use tokio::time::{sleep, timeout};
    use uuid::Uuid;

    fn test_invocation() -> WorkerInvocation {
        WorkerInvocation {
            method: "GET".to_string(),
            url: "http://worker/".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: "test-request".to_string(),
        }
    }

    fn test_invocation_with_path(path: &str, request_id: &str) -> WorkerInvocation {
        WorkerInvocation {
            method: "GET".to_string(),
            url: format!("http://worker{path}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: request_id.to_string(),
        }
    }

    fn counter_worker() -> String {
        r#"
let counter = 0;
export default {
  async fetch() {
    counter += 1;
    return new Response(String(counter));
  },
};
"#
        .to_string()
    }

    fn slow_worker() -> String {
        r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(40);
    return new Response("ok");
  },
};
"#
        .to_string()
    }

    fn versioned_worker(version: &str, delay_ms: u64) -> String {
        format!(
            r#"
export default {{
  async fetch() {{
    await Deno.core.ops.op_sleep({delay_ms});
    return new Response("{version}");
  }},
}};
"#
        )
    }

    fn io_wait_worker() -> String {
        r#"
export default {
  async fetch() {
    await Deno.core.ops.op_sleep(50);
    return new Response("ok");
  },
};
"#
        .to_string()
    }

    fn abort_aware_worker() -> String {
        r#"
let abortCount = 0;

export default {
  async fetch(_request, _env, ctx) {
    if (ctx.requestId === "block") {
      await new Promise((resolve) => {
        const done = () => {
          abortCount += 1;
          resolve();
        };
        if (ctx.signal?.aborted) {
          done();
          return;
        }
        ctx.signal?.addEventListener("abort", done);
      });
      return new Response("aborted");
    }

    return new Response(`abortCount=${abortCount}`);
  },
};
"#
        .to_string()
    }

    fn malicious_completion_worker() -> String {
        r#"
let counter = 0;

export default {
  async fetch(_request, _env, ctx) {
    counter += 1;

    Deno.core.ops.op_emit_completion("{");
    Deno.core.ops.op_emit_completion(
      JSON.stringify({
        request_id: ctx.requestId,
        completion_token: "forged-token",
        ok: true,
        result: { status: 200, headers: [], body: [102, 97, 107, 101] },
      }),
    );

    return new Response(String(counter));
  },
};
"#
        .to_string()
    }

    fn cache_worker(cache_name: &str, label: &str) -> String {
        format!(
            r#"
let count = 0;

export default {{
  async fetch() {{
    const cache = await caches.open("{cache_name}");
    const key = new Request("http://cache/item", {{ method: "GET" }});
    const hit = await cache.match(key);
    if (hit) {{
      return hit;
    }}

    count += 1;
    const response = new Response("{label}:" + String(count), {{
      headers: [["cache-control", "public, max-age=60"]],
    }});
    await cache.put(key, response.clone());
    return response;
  }},
}};
"#
        )
    }

    async fn test_service(config: RuntimeConfig) -> RuntimeService {
        let db_path = format!("/tmp/grugd-test-{}.db", Uuid::new_v4());
        std::env::set_var("TURSO_DATABASE_URL", format!("file:{db_path}"));
        RuntimeService::start_with_config(config)
            .await
            .expect("service should start")
    }

    #[tokio::test]
    #[serial]
    async fn reuse_preserves_state() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("counter".to_string(), counter_worker())
            .await
            .expect("deploy should succeed");

        let one = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("first invoke should succeed");
        let two = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("second invoke should succeed");

        assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");
        assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn scales_up_with_backlog() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 4,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("slow".to_string(), slow_worker())
            .await
            .expect("deploy should succeed");

        let mut tasks = Vec::new();
        for idx in 0..12 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = format!("req-{idx}");
                svc.invoke("slow".to_string(), req).await
            }));
        }

        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }

        let stats = service
            .stats("slow".to_string())
            .await
            .expect("stats should exist");
        assert!(stats.spawn_count > 1);
        assert!(stats.isolates_total <= 4);
    }

    #[tokio::test]
    #[serial]
    async fn scales_down_when_idle() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 3,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_millis(200),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("slow".to_string(), slow_worker())
            .await
            .expect("deploy should succeed");

        for idx in 0..6 {
            let mut req = test_invocation();
            req.request_id = format!("req-{idx}");
            service
                .invoke("slow".to_string(), req)
                .await
                .expect("invoke should succeed");
        }

        let before = service
            .stats("slow".to_string())
            .await
            .expect("stats should exist");
        assert!(before.isolates_total > 0);

        timeout(Duration::from_secs(3), async {
            loop {
                let stats = service.stats("slow".to_string()).await.expect("stats");
                if stats.isolates_total == 0 {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("isolates should scale down to zero");
    }

    #[tokio::test]
    #[serial]
    async fn invalid_redeploy_keeps_previous_generation() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("counter".to_string(), counter_worker())
            .await
            .expect("initial deploy should succeed");

        let one = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("first invoke should succeed");
        assert_eq!(String::from_utf8(one.body).expect("utf8"), "1");

        let bad_redeploy = service
            .deploy("counter".to_string(), "export default {};".to_string())
            .await;
        assert!(bad_redeploy.is_err());

        let two = service
            .invoke("counter".to_string(), test_invocation())
            .await
            .expect("invoke should still use old generation");
        assert_eq!(String::from_utf8(two.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn redeploy_switches_new_traffic_while_old_generation_drains() {
        let service = test_service(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("worker".to_string(), versioned_worker("v1", 120))
            .await
            .expect("deploy v1 should succeed");

        let svc_one = service.clone();
        let first = tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "first".to_string();
            svc_one.invoke("worker".to_string(), req).await
        });

        sleep(Duration::from_millis(10)).await;

        let svc_two = service.clone();
        let second = tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "second".to_string();
            svc_two.invoke("worker".to_string(), req).await
        });

        sleep(Duration::from_millis(10)).await;
        service
            .deploy("worker".to_string(), versioned_worker("v2", 0))
            .await
            .expect("deploy v2 should succeed");

        let mut third_req = test_invocation();
        third_req.request_id = "third".to_string();
        let third = service
            .invoke("worker".to_string(), third_req)
            .await
            .expect("third invoke should succeed");
        assert_eq!(String::from_utf8(third.body).expect("utf8"), "v2");

        let first_output = first.await.expect("join first").expect("first invoke");
        let second_output = second.await.expect("join second").expect("second invoke");
        assert_eq!(String::from_utf8(first_output.body).expect("utf8"), "v1");
        assert_eq!(String::from_utf8(second_output.body).expect("utf8"), "v1");
    }

    #[tokio::test]
    #[serial]
    async fn single_isolate_allows_multiple_inflight_requests() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("io".to_string(), io_wait_worker())
            .await
            .expect("deploy should succeed");

        let started = Instant::now();
        let mut tasks = Vec::new();
        for idx in 0..8 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = format!("io-{idx}");
                svc.invoke("io".to_string(), req).await
            }));
        }

        for task in tasks {
            task.await.expect("join").expect("invoke should succeed");
        }
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_millis(260),
            "expected multiplexed inflight execution, elapsed={elapsed:?}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn dropped_invoke_aborts_inflight_request() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("abortable".to_string(), abort_aware_worker())
            .await
            .expect("deploy should succeed");

        let service_for_blocked = service.clone();
        let blocked = tokio::spawn(async move {
            let mut req = test_invocation();
            req.request_id = "block".to_string();
            service_for_blocked
                .invoke("abortable".to_string(), req)
                .await
        });

        timeout(Duration::from_secs(1), async {
            loop {
                let stats = service.stats("abortable".to_string()).await.expect("stats");
                if stats.inflight_total == 1 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("request should become inflight");

        blocked.abort();
        assert!(blocked.await.is_err(), "aborted task should be canceled");

        timeout(Duration::from_secs(2), async {
            loop {
                let stats = service.stats("abortable".to_string()).await.expect("stats");
                if stats.inflight_total == 0 && stats.queued == 0 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("abort should clear inflight slot");

        let mut followup_req = test_invocation();
        followup_req.request_id = "after".to_string();
        let followup = service
            .invoke("abortable".to_string(), followup_req)
            .await
            .expect("followup invoke should succeed");

        assert_eq!(
            String::from_utf8(followup.body).expect("utf8"),
            "abortCount=1"
        );
    }

    #[tokio::test]
    #[serial]
    async fn duplicate_user_request_ids_do_not_collide() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("io".to_string(), io_wait_worker())
            .await
            .expect("deploy should succeed");

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let svc = service.clone();
            tasks.push(tokio::spawn(async move {
                let mut req = test_invocation();
                req.request_id = "same-user-request-id".to_string();
                svc.invoke("io".to_string(), req).await
            }));
        }

        for task in tasks {
            let output = task.await.expect("join").expect("invoke should succeed");
            assert_eq!(String::from_utf8(output.body).expect("utf8"), "ok");
        }
    }

    #[tokio::test]
    #[serial]
    async fn forged_and_invalid_completion_payloads_are_ignored() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 2,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("malicious".to_string(), malicious_completion_worker())
            .await
            .expect("deploy should succeed");

        let first = service
            .invoke("malicious".to_string(), test_invocation())
            .await
            .expect("first invoke should succeed");
        assert_eq!(String::from_utf8(first.body).expect("utf8"), "1");

        let second = service
            .invoke("malicious".to_string(), test_invocation())
            .await
            .expect("second invoke should succeed");
        assert_eq!(String::from_utf8(second.body).expect("utf8"), "2");
    }

    #[tokio::test]
    #[serial]
    async fn invoke_stream_delivers_chunked_response_body() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy(
                "streaming".to_string(),
                r#"
export default {
  async fetch() {
    return new Response(new ReadableStream({
      start(controller) {
        controller.enqueue("hel");
        controller.enqueue("lo");
        controller.close();
      }
    }), { status: 201, headers: [["x-mode", "stream"]] });
  },
};
"#
                .to_string(),
            )
            .await
            .expect("deploy should succeed");

        let mut output = service
            .invoke_stream("streaming".to_string(), test_invocation())
            .await
            .expect("invoke stream should succeed");
        assert_eq!(output.status, 201);
        assert!(output
            .headers
            .iter()
            .any(|(name, value)| name == "x-mode" && value == "stream"));

        let mut body = Vec::new();
        while let Some(chunk) = output.body.recv().await {
            body.extend(chunk.expect("chunk should be ok"));
        }
        assert_eq!(String::from_utf8(body).expect("utf8"), "hello");
    }

    #[tokio::test]
    #[serial]
    async fn cache_default_reuses_response() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("cache".to_string(), cache_worker("default", "cache"))
            .await
            .expect("deploy should succeed");

        let one = service
            .invoke(
                "cache".to_string(),
                test_invocation_with_path("/", "cache-one"),
            )
            .await
            .expect("first invoke should succeed");
        let two = service
            .invoke(
                "cache".to_string(),
                test_invocation_with_path("/", "cache-two"),
            )
            .await
            .expect("second invoke should succeed");

        assert_eq!(String::from_utf8(one.body).expect("utf8"), "cache:1");
        assert_eq!(String::from_utf8(two.body).expect("utf8"), "cache:1");
    }

    #[tokio::test]
    #[serial]
    async fn named_caches_share_global_capacity_budget() {
        let service = test_service(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 1,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
            cache_max_entries: 1,
            ..RuntimeConfig::default()
        })
        .await;

        service
            .deploy("worker-a".to_string(), cache_worker("cache-a", "A"))
            .await
            .expect("deploy a should succeed");
        service
            .deploy("worker-b".to_string(), cache_worker("cache-b", "B"))
            .await
            .expect("deploy b should succeed");

        let a1 = service
            .invoke(
                "worker-a".to_string(),
                test_invocation_with_path("/", "a-1"),
            )
            .await
            .expect("a1 should succeed");
        let b1 = service
            .invoke(
                "worker-b".to_string(),
                test_invocation_with_path("/", "b-1"),
            )
            .await
            .expect("b1 should succeed");
        let a2 = service
            .invoke(
                "worker-a".to_string(),
                test_invocation_with_path("/", "a-2"),
            )
            .await
            .expect("a2 should succeed");

        assert_eq!(String::from_utf8(a1.body).expect("utf8"), "A:1");
        assert_eq!(String::from_utf8(b1.body).expect("utf8"), "B:1");
        assert_eq!(String::from_utf8(a2.body).expect("utf8"), "A:2");
    }
}
