use super::*;

#[derive(Clone, Default)]
pub(super) struct WorkerRoutes {
    snapshot: Arc<ArcSwap<HashMap<String, WorkerRoute>>>,
}

#[derive(Clone)]
pub(super) struct WorkerRoute {
    pub(super) commands: mpsc::Sender<RuntimeCommand>,
    pub(super) internal: mpsc::Sender<RuntimeCommand>,
    pub(super) cancellation: mpsc::UnboundedSender<RuntimeCommand>,
    pub(super) events: RuntimeEventSender,
}

impl WorkerRoutes {
    pub(super) fn get(&self, worker: &str) -> Option<WorkerRoute> {
        self.snapshot.load().get(worker).cloned()
    }

    pub(super) fn insert(&self, worker: String, route: WorkerRoute) {
        self.snapshot.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.insert(worker.clone(), route.clone());
            next
        });
    }

    pub(super) fn remove(&self, worker: &str, commands: &mpsc::Sender<RuntimeCommand>) {
        self.snapshot.rcu(|snapshot| {
            if !snapshot
                .get(worker)
                .is_some_and(|route| route.commands.same_channel(commands))
            {
                return Arc::clone(snapshot);
            }
            let mut next = (**snapshot).clone();
            next.remove(worker);
            Arc::new(next)
        });
    }

    pub(super) fn clear(&self) {
        self.snapshot.store(Arc::new(HashMap::new()));
    }

    pub(super) fn all(&self) -> Arc<HashMap<String, WorkerRoute>> {
        self.snapshot.load_full()
    }
}

#[derive(Clone, Copy)]
pub(super) enum CommandLane {
    Request,
    Internal,
}

#[derive(Clone)]
pub(crate) struct RuntimeCommandSender {
    coordinator: mpsc::Sender<RuntimeCommand>,
    routes: WorkerRoutes,
    lane: CommandLane,
    admission: Arc<RuntimeAdmission>,
}

impl RuntimeCommandSender {
    pub(super) fn new(
        coordinator: mpsc::Sender<RuntimeCommand>,
        routes: WorkerRoutes,
        lane: CommandLane,
        admission: Arc<RuntimeAdmission>,
    ) -> Self {
        Self {
            coordinator,
            routes,
            lane,
            admission,
        }
    }

    fn admit(
        &self,
        command: RuntimeCommand,
    ) -> std::result::Result<RuntimeCommand, Box<(RuntimeCommand, PlatformError)>> {
        let (bytes, internal) = match &command {
            RuntimeCommand::Invoke {
                request,
                request_body,
                ..
            }
            | RuntimeCommand::InvokeStream {
                request,
                request_body,
                ..
            }
            | RuntimeCommand::OpenWebsocket {
                request,
                request_body,
                ..
            } => (
                super::dispatch::estimate_pending_invoke_bytes(request, request_body.is_some()),
                false,
            ),
            RuntimeCommand::InvokeInternal(invoke) if invoke.queue_admission.is_none() => (
                super::dispatch::estimate_pending_invoke_bytes(
                    &invoke.request,
                    invoke.request_body.is_some(),
                )
                .saturating_add(
                    invoke
                        .memory_call
                        .as_ref()
                        .map_or(0, MemoryExecutionCall::queued_bytes),
                ),
                true,
            ),
            RuntimeCommand::ServiceBindingFetchStart { request, .. } => (
                super::dispatch::estimate_pending_invoke_bytes(request, false),
                true,
            ),
            RuntimeCommand::SendWebsocketFrame { frame, .. } => {
                (frame.len().saturating_add(1024), true)
            }
            _ => return Ok(command),
        };
        match self.admission.reserve_queue(bytes, internal) {
            Ok(admission) => Ok(RuntimeCommand::Admitted {
                command: Box::new(command),
                admission,
            }),
            Err(error) => Err(Box::new((command, error))),
        }
    }

    fn destination(&self, command: &RuntimeCommand) -> mpsc::Sender<RuntimeCommand> {
        if self.coordinator.is_closed() || matches!(command, RuntimeCommand::Deploy { .. }) {
            return self.coordinator.clone();
        }
        let route = command
            .worker_name()
            .and_then(|worker| self.routes.get(worker));
        match (route, self.lane) {
            (Some(route), CommandLane::Request) => route.commands,
            (Some(route), CommandLane::Internal) => route.internal,
            (None, _) => self.coordinator.clone(),
        }
    }

    pub(crate) async fn send(
        &self,
        command: RuntimeCommand,
    ) -> std::result::Result<(), Box<mpsc::error::SendError<RuntimeCommand>>> {
        let command = match self.admit(command) {
            Ok(command) => command,
            Err(failure) => {
                let (command, error) = *failure;
                command.reject(error);
                return Ok(());
            }
        };
        self.destination(&command)
            .send(command)
            .await
            .map_err(Box::new)
    }

    pub(crate) fn try_send(
        &self,
        command: RuntimeCommand,
    ) -> std::result::Result<(), Box<mpsc::error::TrySendError<RuntimeCommand>>> {
        let command = self.admit(command).map_err(|failure| {
            let (command, _) = *failure;
            Box::new(mpsc::error::TrySendError::Full(command))
        })?;
        self.destination(&command)
            .try_send(command)
            .map_err(Box::new)
    }

    pub(super) fn blocking_send(
        &self,
        command: RuntimeCommand,
    ) -> std::result::Result<(), Box<mpsc::error::SendError<RuntimeCommand>>> {
        let command = match self.admit(command) {
            Ok(command) => command,
            Err(failure) => {
                let (command, error) = *failure;
                command.reject(error);
                return Ok(());
            }
        };
        self.destination(&command)
            .blocking_send(command)
            .map_err(Box::new)
    }

    pub(super) fn worker_schedulers(&self) -> usize {
        self.routes.all().len()
    }

    pub(super) fn is_closed(&self) -> bool {
        self.coordinator.is_closed()
    }
}

#[derive(Clone)]
pub(super) struct RuntimeCancellationSender {
    coordinator: mpsc::UnboundedSender<RuntimeCommand>,
    routes: WorkerRoutes,
}

impl RuntimeCancellationSender {
    pub(super) fn new(
        coordinator: mpsc::UnboundedSender<RuntimeCommand>,
        routes: WorkerRoutes,
    ) -> Self {
        Self {
            coordinator,
            routes,
        }
    }

    pub(super) fn send(
        &self,
        command: RuntimeCommand,
    ) -> std::result::Result<(), Box<mpsc::error::SendError<RuntimeCommand>>> {
        match command
            .worker_name()
            .and_then(|worker| self.routes.get(worker))
        {
            Some(route) => route.cancellation.send(command),
            None => self.coordinator.send(command),
        }
        .map_err(Box::new)
    }
}

impl RuntimeCommand {
    pub(super) fn worker_name(&self) -> Option<&str> {
        match self {
            Self::Admitted { command, .. } => command.worker_name(),
            Self::CheckDeployment { worker_name, .. }
            | Self::Undeploy { worker_name, .. }
            | Self::Invoke { worker_name, .. }
            | Self::InvokeStream { worker_name, .. }
            | Self::Cancel { worker_name, .. }
            | Self::Stats { worker_name, .. }
            | Self::DebugDump { worker_name, .. }
            | Self::OpenWebsocket { worker_name, .. }
            | Self::SendWebsocketFrame { worker_name, .. }
            | Self::WaitWebsocketFrame { worker_name, .. }
            | Self::DrainWebsocketFrame { worker_name, .. }
            | Self::CloseWebsocket { worker_name, .. } => Some(worker_name),
            Self::Deploy { prepared, .. } => Some(&prepared.worker_name),
            Self::ServiceBindingFetchStart { owner_worker, .. } => Some(owner_worker),
            Self::InvokeInternal(invoke) => Some(&invoke.worker_name),
            #[cfg(test)]
            Self::ForceFailIsolate { worker_name, .. } => Some(worker_name),
            Self::Shutdown { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct AdmissionSnapshot {
    pub(super) regular: usize,
    pub(super) rescue: usize,
    pub(super) starting: usize,
    pub(super) queue: RuntimeQueueCounters,
}

pub(super) struct RuntimeAdmission {
    counters: StdMutex<AdmissionSnapshot>,
    max_queued_requests: usize,
    max_queued_bytes: usize,
    reserved_internal_requests: usize,
    idle_candidates: StdMutex<HashMap<String, (Instant, String, u64, u64)>>,
    idle_retirement: AtomicBool,
    pub(super) response_bytes: Arc<tokio::sync::Semaphore>,
    pub(super) capacity_changed: tokio::sync::watch::Sender<()>,
    pub(super) reclaim_idle: tokio::sync::watch::Sender<Option<(String, u64)>>,
}

impl RuntimeAdmission {
    pub(super) fn new(config: &RuntimeConfig) -> Arc<Self> {
        Arc::new(Self {
            counters: StdMutex::new(AdmissionSnapshot::default()),
            max_queued_requests: config.max_global_queued_requests,
            max_queued_bytes: config.max_global_queued_bytes,
            reserved_internal_requests: config.reserved_internal_queued_requests_per_worker,
            idle_candidates: StdMutex::new(HashMap::new()),
            idle_retirement: AtomicBool::new(false),
            response_bytes: Arc::new(tokio::sync::Semaphore::new(
                config.max_buffered_response_bytes,
            )),
            capacity_changed: tokio::sync::watch::channel(()).0,
            reclaim_idle: tokio::sync::watch::channel(None).0,
        })
    }

    pub(super) fn publish_idle_isolate(
        &self,
        worker: &str,
        candidate: Option<(Instant, String, u64, u64)>,
    ) {
        let mut candidates = self
            .idle_candidates
            .lock()
            .expect("idle isolate lock poisoned");
        match candidate {
            Some(candidate) => {
                candidates.insert(worker.to_string(), candidate);
            }
            None => {
                candidates.remove(worker);
            }
        }
    }

    pub(super) fn claim_idle_isolate(&self, candidate: &(Instant, String, u64, u64)) -> bool {
        let mut candidates = self
            .idle_candidates
            .lock()
            .expect("idle isolate lock poisoned");
        if candidates.values().min() != Some(candidate)
            || self.idle_retirement.swap(true, Ordering::AcqRel)
        {
            return false;
        }
        candidates.remove(&candidate.1);
        true
    }

    pub(super) fn snapshot(&self) -> AdmissionSnapshot {
        *self
            .counters
            .lock()
            .expect("runtime admission lock poisoned")
    }

    pub(super) fn reserve_queue(
        self: &Arc<Self>,
        bytes: usize,
        internal: bool,
    ) -> Result<QueueAdmission> {
        let mut counters = self
            .counters
            .lock()
            .expect("runtime admission lock poisoned");
        let limit = self.max_queued_requests
            + if internal {
                self.reserved_internal_requests
            } else {
                0
            };
        if counters.queue.requests >= limit {
            return Err(PlatformError::overloaded(format!(
                "runtime queue is full (max {limit} queued requests)"
            )));
        }
        if counters.queue.bytes.saturating_add(bytes) > self.max_queued_bytes {
            return Err(PlatformError::overloaded(format!(
                "runtime queue byte budget is full (max {} bytes)",
                self.max_queued_bytes
            )));
        }
        counters.queue.requests += 1;
        counters.queue.bytes += bytes;
        Ok(QueueAdmission {
            admission: Arc::clone(self),
            bytes,
            held: AtomicBool::new(true),
        })
    }

    pub(super) fn reserve_isolate(
        self: &Arc<Self>,
        config: &RuntimeConfig,
        policy: IsolateSpawnPolicy,
    ) -> Option<IsolateAdmission> {
        let mut counters = self
            .counters
            .lock()
            .expect("runtime admission lock poisoned");
        let reservation = if counters.regular < config.max_global_isolates
            && !matches!(policy, IsolateSpawnPolicy::InternalRescueOnly)
        {
            counters.regular += 1;
            IsolateSlotReservation::Regular
        } else if !matches!(policy, IsolateSpawnPolicy::WithinGlobalBudget)
            && counters.rescue < config.max_global_isolates
        {
            counters.rescue += 1;
            IsolateSlotReservation::InternalRescue
        } else {
            return None;
        };
        counters.starting += 1;
        Some(IsolateAdmission {
            admission: Arc::clone(self),
            reservation,
            starting: Arc::new(AtomicBool::new(true)),
        })
    }

    pub(super) fn isolate_ready(&self, starting: &AtomicBool) {
        if starting.swap(false, Ordering::AcqRel) {
            self.counters
                .lock()
                .expect("runtime admission lock poisoned")
                .starting -= 1;
        }
    }
}

pub(crate) struct QueueAdmission {
    admission: Arc<RuntimeAdmission>,
    bytes: usize,
    held: AtomicBool,
}

impl QueueAdmission {
    pub(super) fn resize(&mut self, bytes: usize) -> Result<()> {
        let mut counters = self
            .admission
            .counters
            .lock()
            .expect("runtime admission lock poisoned");
        let retained = counters.queue.bytes - self.bytes;
        if retained.saturating_add(bytes) > self.admission.max_queued_bytes {
            return Err(PlatformError::overloaded(format!(
                "runtime queue byte budget is full (max {} bytes)",
                self.admission.max_queued_bytes
            )));
        }
        counters.queue.bytes = retained + bytes;
        self.bytes = bytes;
        Ok(())
    }

    pub(super) fn release(&self) {
        if self.held.swap(false, Ordering::AcqRel) {
            let mut counters = self
                .admission
                .counters
                .lock()
                .expect("runtime admission lock poisoned");
            counters.queue.requests -= 1;
            counters.queue.bytes -= self.bytes;
        }
    }
}

impl Drop for QueueAdmission {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) struct IsolateAdmission {
    admission: Arc<RuntimeAdmission>,
    pub(super) reservation: IsolateSlotReservation,
    pub(super) starting: Arc<AtomicBool>,
}

impl Drop for IsolateAdmission {
    fn drop(&mut self) {
        let mut counters = self
            .admission
            .counters
            .lock()
            .expect("runtime admission lock poisoned");
        match self.reservation {
            IsolateSlotReservation::Regular => counters.regular -= 1,
            IsolateSlotReservation::InternalRescue => counters.rescue -= 1,
        }
        if self.starting.swap(false, Ordering::AcqRel) {
            counters.starting -= 1;
        }
        drop(counters);
        self.admission
            .idle_retirement
            .store(false, Ordering::Release);
        self.admission.capacity_changed.send_replace(());
    }
}

impl RuntimeCommand {
    pub(super) fn reject(self, error: PlatformError) {
        match self {
            Self::Admitted { command, .. } => command.reject(error),
            Self::CheckDeployment { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Deploy { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Undeploy { reply, .. }
            | Self::WaitWebsocketFrame { reply, .. }
            | Self::CloseWebsocket { reply, .. }
            | Self::Shutdown { reply } => {
                let _ = reply.send(Err(error));
            }
            Self::Invoke { reply, .. } | Self::SendWebsocketFrame { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::InvokeInternal(invoke) => {
                let _ = invoke.reply.send(Err(error));
            }
            Self::InvokeStream { ready, reply, .. } => {
                let _ = ready.send(Err(error.clone()));
                let _ = reply.send(Err(error));
            }
            Self::OpenWebsocket { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::DrainWebsocketFrame { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Stats { reply, .. } => {
                let _ = reply.send(None);
            }
            Self::DebugDump { reply, .. } => {
                let _ = reply.send(None);
            }
            Self::ServiceBindingFetchStart {
                pending_replies,
                reply_id,
                reply_inbox,
                ..
            } => {
                pending_replies.finish_into(
                    reply_id,
                    crate::ops::PendingReplyPayload::Fetch {
                        result: Err(error),
                        boundary: None,
                    },
                    &reply_inbox,
                );
            }
            Self::Cancel { .. } => {}
            #[cfg(test)]
            Self::ForceFailIsolate { reply, .. } => {
                let _ = reply.send(false);
            }
        }
    }
}

pub(super) async fn run_runtime_coordinator(start: RuntimeThreadStart) {
    let RuntimeThreadStart {
        mut receiver,
        mut cancel_receiver,
        mut cancellation_receiver,
        runtime_fast_sender,
        routes,
        admission,
        asset_catalog,
        bootstrap_snapshot,
        kv_store,
        memory_store,
        cache_store,
        config,
        storage,
        control_store,
        module_registry,
    } = start;
    let (event_tx, mut event_rx) = mpsc::channel(4096);
    let (memory_outbox_drain_sender, memory_outbox_drain_receiver) = memory_outbox_worker_channel();
    let memory_outbox_worker = tokio::spawn(run_memory_outbox_worker(
        memory_store.clone(),
        event_tx,
        memory_outbox_drain_receiver,
        storage.memory_outbox_max_concurrent_shards,
        config.scale_tick,
    ));
    let init = WorkerManagerInit {
        bootstrap_snapshot,
        kv_store,
        memory_store,
        memory_outbox_drain_sender,
        cache_store,
        config,
        control_store,
        module_registry,
        runtime_fast_sender,
        asset_catalog,
        admission,
    };
    let mut workers = JoinSet::new();
    let mut outbox_deliveries = JoinSet::new();
    let shutdown_reply = loop {
        tokio::select! {
            command = async {
                tokio::select! {
                    biased;
                    Some(command) = cancellation_receiver.recv() => Some(command),
                    Some(command) = cancel_receiver.recv() => Some(command),
                    Some(command) = receiver.recv() => Some(command),
                    else => None,
                }
            } => {
                let Some(command) = command else { break None; };
                if let RuntimeCommand::Shutdown { reply } = command {
                    break Some(reply);
                }
                let worker = command.worker_name().expect("worker command has a destination").to_string();
                let route = match routes.get(&worker) {
                    Some(route) if !route.commands.is_closed() => route,
                    _ if matches!(command, RuntimeCommand::Deploy { .. }) => {
                        let (commands, command_rx) = mpsc::channel(256);
                        let (internal, internal_rx) = mpsc::channel(super::facade::RUNTIME_FAST_COMMAND_CHANNEL_CAPACITY);
                        let (cancellation, cancellation_rx) = mpsc::unbounded_channel();
                        let (events, events_rx) = mpsc::channel(4096);
                        let route = WorkerRoute { commands, internal, cancellation, events };
                        routes.insert(worker.clone(), route.clone());
                        let scheduler_worker = worker.clone();
                        let scheduler_init = init.clone();
                        let retired_commands = route.commands.clone();
                        let scheduler_events = route.events.clone();
                        workers.spawn(async move {
                            run_worker_scheduler(
                                scheduler_worker.clone(), scheduler_init, command_rx, internal_rx, cancellation_rx, events_rx, scheduler_events,
                            ).await;
                            (scheduler_worker, retired_commands)
                        });
                        route
                    }
                    _ => {
                        if let RuntimeCommand::CheckDeployment { reply, .. } = command {
                            let _ = reply.send(Ok(()));
                            continue;
                        }
                        command.reject(PlatformError::not_found(format!("worker {worker} is not deployed")));
                        continue;
                    }
                };
                if matches!(command, RuntimeCommand::Cancel { .. } | RuntimeCommand::Deploy { .. }) {
                    // Deployment preparation is bounded before this lane; publication must survive invocation backpressure.
                    if let Err(error) = route.cancellation.send(command) {
                        error.0.reject(PlatformError::internal(format!("worker {worker} control channel closed")));
                    }
                } else if let Err(error) = route.commands.try_send(command) {
                    error.into_inner().reject(PlatformError::overloaded(format!("worker {worker} command queue is full")));
                }
            }
            Some(event) = event_rx.recv() => {
                match event {
                    RuntimeEvent::MemoryOutboxDelivery { shard_index, claims, saturated, reply } => {
                        let snapshot = routes.all();
                        if claims.is_empty() {
                            for route in snapshot.values() {
                                let (reply, _) = oneshot::channel();
                                let _ = route.events.try_send(RuntimeEvent::MemoryOutboxDelivery {
                                    shard_index, claims: Vec::new(), saturated: false, reply,
                                });
                            }
                            let _ = reply.send(Vec::new());
                            continue;
                        }
                        outbox_deliveries.spawn(async move {
                            let mut grouped = HashMap::<String, Vec<MemoryOutboxClaim>>::new();
                            let mut outcomes = Vec::new();
                            for claim in claims {
                                let worker = claim.namespace.split_once(':').and_then(|(length, suffix)| {
                                    length.parse::<usize>().ok().and_then(|length| suffix.get(..length))
                                });
                                if let Some(worker) = worker.filter(|worker| snapshot.contains_key(*worker)) {
                                    grouped.entry(worker.to_string()).or_default().push(claim);
                                } else {
                                    let action = if super::sessions::deliver_memory_log_effect(&claim) {
                                        MemoryOutboxDeliveryAction::Delivered
                                    } else {
                                        MemoryOutboxDeliveryAction::DroppedTerminal
                                    };
                                    outcomes.push(MemoryOutboxDeliveryOutcome {
                                        namespace: claim.namespace, memory_key: claim.memory_key,
                                        effect_id: claim.record.effect_id,
                                        action,
                                    });
                                }
                            }
                            let deliveries = grouped.into_iter().map(|(worker, claims)| {
                                let destination = snapshot[&worker].events.clone();
                                async move {
                                    let (reply, result) = oneshot::channel();
                                    destination.send(RuntimeEvent::MemoryOutboxDelivery {
                                        shard_index, claims, saturated, reply,
                                    }).await.map_err(|_| PlatformError::internal(format!("worker {worker} outbox channel closed")))?;
                                    result.await.map_err(|_| PlatformError::internal(format!("worker {worker} outbox reply closed")))
                                }
                            });
                            for result in futures_util::future::join_all(deliveries).await {
                                match result {
                                    Ok(delivered) => outcomes.extend(delivered),
                                    Err(error) => warn!(error = %error, "memory outbox delivery interrupted"),
                                }
                            }
                            let _ = reply.send(outcomes);
                        });
                    }
                    RuntimeEvent::MemoryOutboxAckFailed { shard_index } => {
                        for route in routes.all().values() {
                            let _ = route.events.try_send(RuntimeEvent::MemoryOutboxAckFailed { shard_index });
                        }
                    }
                    RuntimeEvent::MemoryOutboxWorkerStats {
                        pending_shards, in_flight_shards, parallelism_limit, parallelism_peak,
                        duplicate_schedule_coalesced_count, task_failure_count, shard_requeue_count,
                    } => {
                        for route in routes.all().values() {
                            let _ = route.events.try_send(RuntimeEvent::MemoryOutboxWorkerStats {
                                pending_shards, in_flight_shards, parallelism_limit, parallelism_peak,
                                duplicate_schedule_coalesced_count, task_failure_count, shard_requeue_count,
                            });
                        }
                    }
                    _ => unreachable!("isolate events are delivered directly to their worker"),
                }
            }
            Some(result) = workers.join_next() => {
                match result {
                    Ok((worker, commands)) => routes.remove(&worker, &commands),
                    Err(error) => warn!(error = %error, "worker scheduler stopped unexpectedly"),
                }
            }
            Some(result) = outbox_deliveries.join_next() => {
                if let Err(error) = result { warn!(error = %error, "outbox delivery task failed"); }
            }
        }
    };
    receiver.close();
    cancel_receiver.close();
    cancellation_receiver.close();
    let mut shutdown_replies = Vec::new();
    for route in routes.all().values() {
        let (reply, result) = oneshot::channel();
        if route
            .cancellation
            .send(RuntimeCommand::Shutdown { reply })
            .is_ok()
        {
            shutdown_replies.push(result);
        }
    }
    routes.clear();
    let mut shutdown_error = None;
    while let Some(result) = workers.join_next().await {
        if let Err(error) = result {
            shutdown_error = Some(PlatformError::internal(format!(
                "worker scheduler shutdown failed: {error}"
            )));
        }
    }
    for result in shutdown_replies {
        match result.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => shutdown_error = Some(error),
            Err(error) => {
                shutdown_error = Some(PlatformError::internal(format!(
                    "worker shutdown reply closed: {error}"
                )))
            }
        }
    }
    drop(init);
    // Queued deliveries own reply senders that the outbox worker awaits.
    drop(event_rx);
    outbox_deliveries.abort_all();
    while outbox_deliveries.join_next().await.is_some() {}
    if let Err(error) = memory_outbox_worker.await {
        shutdown_error = Some(PlatformError::internal(format!(
            "memory outbox shutdown failed: {error}"
        )));
    }
    if let Some(reply) = shutdown_reply {
        let _ = reply.send(shutdown_error.map_or(Ok(()), Err));
    }
}

async fn run_worker_scheduler(
    worker_name: String,
    init: WorkerManagerInit,
    mut receiver: mpsc::Receiver<RuntimeCommand>,
    mut internal_receiver: mpsc::Receiver<RuntimeCommand>,
    mut cancellation_receiver: mpsc::UnboundedReceiver<RuntimeCommand>,
    mut event_rx: RuntimeEventReceiver,
    event_tx: RuntimeEventSender,
) {
    let mut capacity_changed = init.admission.capacity_changed.subscribe();
    let mut reclaim_idle = init.admission.reclaim_idle.subscribe();
    let mut ticker = tokio::time::interval(init.config.scale_tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut manager = WorkerManager::new(init);
    let mut shutdown_complete = false;
    loop {
        manager.begin_runtime_batch();
        let keep_running = tokio::select! {
            Some(command) = cancellation_receiver.recv() => {
                if matches!(command, RuntimeCommand::Shutdown { .. }) {
                    event_rx.close();
                    shutdown_complete = true;
                }
                manager.handle_command(command, &event_tx).await
            },
            Some(command) = internal_receiver.recv() => manager.handle_command(command, &event_tx).await,
            Some(event) = event_rx.recv() => { manager.handle_event(event, &event_tx).await; true },
            Some(command) = receiver.recv() => manager.handle_command(command, &event_tx).await,
            _ = capacity_changed.changed() => { manager.process_scale_up_requests(&event_tx); true },
            _ = reclaim_idle.changed() => {
                let request = reclaim_idle.borrow_and_update().clone();
                if let Some((worker, generation)) = request {
                    manager.retire_lru_idle_isolate_for_budget(&worker, generation);
                }
                true
            },
            _ = ticker.tick() => {
                manager.expire_temporary_workers();
                manager.expire_queued_requests();
                manager.expire_starting_isolates(&event_tx);
                manager.expire_inflight_requests(&event_tx);
                manager.retry_pending_memory_outbox_drains();
                manager.scale_down_idle();
                manager.process_scale_up_requests(&event_tx);
                for worker in manager.workers.keys().cloned().collect::<Vec<_>>() {
                    manager.cleanup_drained_generations_for(&worker);
                }
                true
            },
            else => false,
        };
        let keep_running = keep_running
            && drain_ready_runtime_work(
                &mut manager,
                &mut receiver,
                &mut internal_receiver,
                &mut event_rx,
                &event_tx,
            )
            .await;
        manager.finish_runtime_batch(&event_tx);
        manager
            .admission
            .publish_idle_isolate(&worker_name, manager.oldest_idle_isolate());
        if !keep_running {
            break;
        }
        if manager.workers.is_empty() && manager.exiting_isolate_slots.is_empty() {
            receiver.close();
            internal_receiver.close();
            cancellation_receiver.close();
            for commands in [&mut receiver, &mut internal_receiver] {
                while let Ok(command) = commands.try_recv() {
                    if let Err(error) = manager.runtime_fast_sender.coordinator.send(command).await
                    {
                        error.0.reject(PlatformError::internal(
                            "runtime stopped before worker command was rerouted",
                        ));
                    }
                }
            }
            while let Ok(command) = cancellation_receiver.try_recv() {
                if let Err(error) = manager.runtime_fast_sender.coordinator.send(command).await {
                    error.0.reject(PlatformError::internal(
                        "runtime stopped before cancellation was rerouted",
                    ));
                }
            }
            break;
        }
        tokio::task::yield_now().await;
    }
    manager.admission.publish_idle_isolate(&worker_name, None);
    event_rx.close();
    if !shutdown_complete && let Err(error) = manager.shutdown_all().await {
        warn!(error = %error, "worker scheduler failed to stop isolates");
    }
}
