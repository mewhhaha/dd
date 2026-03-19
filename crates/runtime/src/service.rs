use crate::engine::{
    build_bootstrap_snapshot, build_worker_snapshot, dispatch_worker_request,
    new_runtime_from_snapshot, pump_event_loop_once, validate_worker,
};
use crate::ops::CompletionSender;
use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
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
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub generation: u64,
    pub queued: usize,
    pub busy: usize,
    pub inflight_total: usize,
    pub isolates_total: usize,
    pub spawn_count: u64,
    pub reuse_count: u64,
    pub scale_down_count: u64,
}

#[derive(Clone)]
pub struct RuntimeService {
    sender: mpsc::Sender<RuntimeCommand>,
}

enum RuntimeCommand {
    Deploy {
        worker_name: String,
        source: String,
        reply: oneshot::Sender<Result<String>>,
    },
    Invoke {
        worker_name: String,
        request: WorkerInvocation,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    #[cfg(test)]
    Stats {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerStats>>,
    },
}

struct WorkerManager {
    config: RuntimeConfig,
    bootstrap_snapshot: &'static [u8],
    workers: HashMap<String, WorkerEntry>,
    next_generation: u64,
    next_isolate_id: u64,
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
    request: WorkerInvocation,
    reply: oneshot::Sender<Result<WorkerOutput>>,
}

struct IsolateHandle {
    id: u64,
    sender: mpsc::UnboundedSender<IsolateCommand>,
    inflight_count: usize,
    served_requests: u64,
    last_used_at: Instant,
    pending_replies: HashMap<String, oneshot::Sender<Result<WorkerOutput>>>,
}

enum IsolateCommand {
    Execute {
        request_id: String,
        request: WorkerInvocation,
    },
    Shutdown,
}

enum RuntimeEvent {
    RequestFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        result: Result<WorkerOutput>,
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
    ok: bool,
    result: Option<WorkerOutput>,
    error: Option<String>,
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

        let bootstrap_snapshot = build_bootstrap_snapshot().await?;
        let (sender, receiver) = mpsc::channel(256);
        spawn_runtime_thread(receiver, bootstrap_snapshot, config)?;
        Ok(Self { sender })
    }

    pub async fn deploy(&self, worker_name: String, source: String) -> Result<String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Deploy {
                worker_name,
                source,
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::Invoke {
                worker_name,
                request,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx
            .await
            .map_err(|_| PlatformError::internal("runtime invoke channel closed"))?
    }

    #[cfg(test)]
    async fn stats(&self, worker_name: String) -> Option<WorkerStats> {
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
    fn new(bootstrap_snapshot: &'static [u8], config: RuntimeConfig) -> Self {
        Self {
            config,
            bootstrap_snapshot,
            workers: HashMap::new(),
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
                reply,
            } => {
                let result = self.deploy(worker_name, source).await;
                let _ = reply.send(result);
            }
            RuntimeCommand::Invoke {
                worker_name,
                request,
                reply,
            } => {
                self.enqueue_invoke(worker_name, request, reply, event_tx);
            }
            #[cfg(test)]
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
                result,
            } => {
                self.finish_request(&worker_name, generation, isolate_id, &request_id, result);
                self.dispatch_pool(&worker_name, generation, event_tx);
                self.cleanup_drained_generations_for(&worker_name);
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

    async fn deploy(&mut self, worker_name: String, source: String) -> Result<String> {
        let worker_name = worker_name.trim().to_string();
        if worker_name.is_empty() {
            return Err(PlatformError::bad_request("Worker name must not be empty"));
        }

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

    fn enqueue_invoke(
        &mut self,
        worker_name: String,
        request: WorkerInvocation,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let worker_name = worker_name.trim().to_string();
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        let Some(generation) = self
            .workers
            .get(&worker_name)
            .map(|entry| entry.current_generation)
        else {
            let _ = reply.send(Err(PlatformError::not_found("Worker not found")));
            return;
        };

        if let Some(pool) = self.get_pool_mut(&worker_name, generation) {
            pool.queue.push_back(PendingInvoke { request, reply });
            pool.update_queue_warning(&warn_thresholds);
        } else {
            let _ = reply.send(Err(PlatformError::not_found("Worker not found")));
            return;
        }

        self.dispatch_pool(&worker_name, generation, event_tx);
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

            let request_id = pending_invoke.request.request_id.clone();
            let command = IsolateCommand::Execute {
                request_id: request_id.clone(),
                request: pending_invoke.request,
            };

            let mut send_failed = false;
            if let Some(pool) = self.get_pool_mut(worker_name, generation) {
                if isolate_idx >= pool.isolates.len() {
                    continue;
                }

                let isolate = &mut pool.isolates[isolate_idx];
                if isolate.served_requests > 0 {
                    pool.stats.reuse_count += 1;
                }
                isolate.served_requests += 1;

                if isolate.sender.send(command).is_err() {
                    send_failed = true;
                } else {
                    isolate.inflight_count += 1;
                    isolate
                        .pending_replies
                        .insert(request_id, pending_invoke.reply);
                }
            }

            if send_failed {
                let failed = self.remove_isolate(worker_name, generation, isolate_idx);
                for reply in failed {
                    let _ = reply.send(Err(PlatformError::internal("isolate is unavailable")));
                }
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
        let isolate = spawn_isolate_thread(
            snapshot,
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
        result: Result<WorkerOutput>,
    ) {
        let mut reply = None;
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                if isolate.inflight_count == 0 {
                    isolate.last_used_at = Instant::now();
                }
                reply = isolate.pending_replies.remove(request_id);
            }
            pool.log_stats("complete");
        }

        if let Some(reply) = reply {
            let _ = reply.send(result);
        }
    }

    fn fail_isolate(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    ) {
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(idx) = pool
                .isolates
                .iter()
                .position(|isolate| isolate.id == isolate_id)
            {
                let failed = self.remove_isolate(worker_name, generation, idx);
                for reply in failed {
                    let _ = reply.send(Err(error.clone()));
                }
            }
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
                return isolate.pending_replies.into_values().collect();
            }
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
            for reply in isolate.pending_replies.into_values() {
                let _ = reply.send(Err(PlatformError::internal("isolate scaled down")));
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
                    for reply in isolate.pending_replies.into_values() {
                        let _ =
                            reply.send(Err(PlatformError::internal("worker generation retired")));
                    }
                }
                info!(worker = %pool.worker_name, generation, "retired worker generation");
            }
        }
    }

    #[cfg(test)]
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
                    for reply in isolate.pending_replies.into_values() {
                        let _ = reply.send(Err(PlatformError::internal("runtime shutting down")));
                    }
                }
            }
        }
    }
}

impl WorkerPool {
    fn is_drained(&self) -> bool {
        self.queue.is_empty() && self.inflight_total() == 0
    }

    fn busy_count(&self) -> usize {
        self.isolates
            .iter()
            .filter(|isolate| isolate.inflight_count > 0)
            .count()
    }

    fn inflight_total(&self) -> usize {
        self.isolates
            .iter()
            .map(|isolate| isolate.inflight_count)
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
            isolates_total = snapshot.isolates_total,
            spawn_count = snapshot.spawn_count,
            reuse_count = snapshot.reuse_count,
            scale_down_count = snapshot.scale_down_count,
            event,
            "worker pool stats"
        );
    }
}

fn spawn_runtime_thread(
    mut receiver: mpsc::Receiver<RuntimeCommand>,
    bootstrap_snapshot: &'static [u8],
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
                let mut manager = WorkerManager::new(bootstrap_snapshot, config.clone());
                let mut ticker = tokio::time::interval(config.scale_tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        Some(command) = receiver.recv() => {
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

                let (completion_tx, mut completion_rx) = mpsc::unbounded_channel::<String>();
                {
                    let op_state = js_runtime.op_state();
                    op_state.borrow_mut().put(CompletionSender(completion_tx));
                }
                let _ = init_tx.send(Ok(()));

                let mut ticker = tokio::time::interval(Duration::from_millis(1));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                loop {
                    tokio::select! {
                        Some(payload) = completion_rx.recv() => {
                            match decode_completion_payload(&payload) {
                                Ok((request_id, result)) => {
                                    let _ = event_tx.send(RuntimeEvent::RequestFinished {
                                        worker_name: worker_name.clone(),
                                        generation,
                                        isolate_id,
                                        request_id,
                                        result,
                                    });
                                }
                                Err(error) => {
                                    let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                                        worker_name: worker_name.clone(),
                                        generation,
                                        isolate_id,
                                        error,
                                    });
                                    break;
                                }
                            }
                        }
                        Some(command) = command_rx.recv() => {
                            match command {
                                IsolateCommand::Execute { request_id, request } => {
                                    if let Err(error) = dispatch_worker_request(&mut js_runtime, &request_id, request) {
                                        let _ = event_tx.send(RuntimeEvent::RequestFinished {
                                            worker_name: worker_name.clone(),
                                            generation,
                                            isolate_id,
                                            request_id,
                                            result: Err(error),
                                        });
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
        }),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(PlatformError::internal("isolate startup timed out")),
    }
}

fn decode_completion_payload(payload: &str) -> Result<(String, Result<WorkerOutput>)> {
    let completion: CompletionPayload = serde_json::from_str(payload)
        .map_err(|error| PlatformError::runtime(format!("invalid completion payload: {error}")))?;
    if completion.ok {
        let output = completion
            .result
            .ok_or_else(|| PlatformError::runtime("completion is missing result"))?;
        Ok((completion.request_id, Ok(output)))
    } else {
        let message = completion
            .error
            .unwrap_or_else(|| "worker execution failed".to_string());
        Ok((completion.request_id, Err(PlatformError::runtime(message))))
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeConfig, RuntimeService};
    use common::WorkerInvocation;
    use serial_test::serial;
    use std::time::{Duration, Instant};
    use tokio::time::{sleep, timeout};

    fn test_invocation() -> WorkerInvocation {
        WorkerInvocation {
            method: "GET".to_string(),
            url: "http://worker/".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: "test-request".to_string(),
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
    const started = Date.now();
    while (Date.now() - started < 40) {}
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
    const started = Date.now();
    while (Date.now() - started < {delay_ms}) {{}}
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

    #[tokio::test]
    #[serial]
    async fn reuse_preserves_state() {
        let service = RuntimeService::start_with_config(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
        })
        .await
        .expect("service should start");

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
        let service = RuntimeService::start_with_config(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 4,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
        })
        .await
        .expect("service should start");

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
        let service = RuntimeService::start_with_config(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 3,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_millis(200),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
        })
        .await
        .expect("service should start");

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
        let service = RuntimeService::start_with_config(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 2,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
        })
        .await
        .expect("service should start");

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
        let service = RuntimeService::start_with_config(RuntimeConfig {
            min_isolates: 0,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
        })
        .await
        .expect("service should start");

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
        let service = RuntimeService::start_with_config(RuntimeConfig {
            min_isolates: 1,
            max_isolates: 1,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(5),
            scale_tick: Duration::from_millis(50),
            queue_warn_thresholds: vec![10],
        })
        .await
        .expect("service should start");

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
}
