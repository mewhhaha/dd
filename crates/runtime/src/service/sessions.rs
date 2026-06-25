use super::*;

const MEMORY_OUTBOX_DRAIN_LIMIT: usize = 64;
const MEMORY_OUTBOX_SCHEDULED_DRAIN_BATCHES: usize = 1;
const MEMORY_OUTBOX_LEASE: Duration = Duration::from_secs(30);
const MEMORY_OUTBOX_WORKER_CHANNEL_CAPACITY: usize = 1024;
const MEMORY_OUTBOX_EFFECT_KINDS: &[&str] = &[
    "audit.*",
    "socket.send",
    "socket.close",
    "trace.*",
    "transport.stream",
    "transport.datagram",
    "transport.close",
];

pub(super) type MemoryOutboxDrainSender = mpsc::Sender<MemoryOutboxWorkerCommand>;
pub(super) type MemoryOutboxDrainReceiver = mpsc::Receiver<MemoryOutboxWorkerCommand>;

#[derive(Debug)]
pub(super) enum MemoryOutboxWorkerCommand {
    DrainShard { shard_index: usize },
}

pub(super) fn memory_outbox_worker_channel() -> (MemoryOutboxDrainSender, MemoryOutboxDrainReceiver)
{
    mpsc::channel(MEMORY_OUTBOX_WORKER_CHANNEL_CAPACITY)
}

pub(super) async fn run_memory_outbox_worker(
    memory_store: MemoryStore,
    event_tx: RuntimeEventSender,
    mut receiver: MemoryOutboxDrainReceiver,
    max_concurrent_shards: usize,
) {
    let max_concurrent_shards = max_concurrent_shards.max(1);
    let mut coordinator = MemoryOutboxDrainCoordinator::new(max_concurrent_shards);
    let mut tasks = JoinSet::new();
    let mut accepting = true;

    loop {
        coordinator.start_ready_drains(&mut tasks, &memory_store, &event_tx);
        if !accepting && tasks.is_empty() {
            break;
        }

        tokio::select! {
            command = receiver.recv(), if accepting => {
                match command {
                    Some(MemoryOutboxWorkerCommand::DrainShard { shard_index }) => {
                        coordinator.schedule(shard_index);
                        coordinator.send_stats(&event_tx);
                    }
                    None => {
                        accepting = false;
                        coordinator.clear_pending();
                        coordinator.send_stats(&event_tx);
                    }
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                coordinator.finish_joined(joined);
                coordinator.send_stats(&event_tx);
            }
        }
    }
}

#[derive(Debug)]
struct MemoryOutboxDrainResult {
    shard_index: usize,
    saturated: bool,
    panicked: bool,
}

struct MemoryOutboxDrainCoordinator {
    max_concurrent_shards: usize,
    pending: VecDeque<usize>,
    pending_members: HashSet<usize>,
    in_flight: HashSet<usize>,
    follow_up: HashSet<usize>,
    duplicate_schedule_coalesced_count: u64,
    task_failure_count: u64,
    shard_requeue_count: u64,
    parallelism_peak: usize,
}

impl MemoryOutboxDrainCoordinator {
    fn new(max_concurrent_shards: usize) -> Self {
        Self {
            max_concurrent_shards: max_concurrent_shards.max(1),
            pending: VecDeque::new(),
            pending_members: HashSet::new(),
            in_flight: HashSet::new(),
            follow_up: HashSet::new(),
            duplicate_schedule_coalesced_count: 0,
            task_failure_count: 0,
            shard_requeue_count: 0,
            parallelism_peak: 0,
        }
    }

    fn schedule(&mut self, shard_index: usize) {
        if self.in_flight.contains(&shard_index) {
            if !self.follow_up.insert(shard_index) {
                self.duplicate_schedule_coalesced_count =
                    self.duplicate_schedule_coalesced_count.saturating_add(1);
            }
            return;
        }
        if !self.pending_members.insert(shard_index) {
            self.duplicate_schedule_coalesced_count =
                self.duplicate_schedule_coalesced_count.saturating_add(1);
            return;
        }
        self.pending.push_back(shard_index);
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
        self.pending_members.clear();
        self.follow_up.clear();
    }

    fn start_ready_drains(
        &mut self,
        tasks: &mut JoinSet<MemoryOutboxDrainResult>,
        memory_store: &MemoryStore,
        event_tx: &RuntimeEventSender,
    ) {
        while self.in_flight.len() < self.max_concurrent_shards {
            let Some(shard_index) = self.pending.pop_front() else {
                break;
            };
            self.pending_members.remove(&shard_index);
            if !self.in_flight.insert(shard_index) {
                self.follow_up.insert(shard_index);
                continue;
            }
            self.parallelism_peak = self.parallelism_peak.max(self.in_flight.len());
            let memory_store = memory_store.clone();
            let event_tx = event_tx.clone();
            tasks.spawn(async move {
                match AssertUnwindSafe(drain_memory_outbox_shard_in_background(
                    &memory_store,
                    &event_tx,
                    shard_index,
                    MEMORY_OUTBOX_SCHEDULED_DRAIN_BATCHES,
                ))
                .catch_unwind()
                .await
                {
                    Ok(result) => result,
                    Err(_) => MemoryOutboxDrainResult {
                        shard_index,
                        saturated: false,
                        panicked: true,
                    },
                }
            });
        }
    }

    fn finish_joined(
        &mut self,
        joined: Option<std::result::Result<MemoryOutboxDrainResult, tokio::task::JoinError>>,
    ) {
        match joined {
            Some(Ok(result)) => {
                self.in_flight.remove(&result.shard_index);
                if result.panicked {
                    self.task_failure_count = self.task_failure_count.saturating_add(1);
                    warn!(
                        shard_index = result.shard_index,
                        "memory outbox drain task panicked"
                    );
                }
                let follow_up = self.follow_up.remove(&result.shard_index);
                if result.saturated || follow_up {
                    self.requeue(result.shard_index);
                }
            }
            Some(Err(error)) => {
                self.task_failure_count = self.task_failure_count.saturating_add(1);
                warn!(error = %error, "memory outbox drain task failed");
            }
            None => {}
        }
    }

    fn requeue(&mut self, shard_index: usize) {
        if self.pending_members.insert(shard_index) {
            self.pending.push_back(shard_index);
            self.shard_requeue_count = self.shard_requeue_count.saturating_add(1);
        }
    }

    fn send_stats(&self, event_tx: &RuntimeEventSender) {
        let _ = event_tx.try_send(RuntimeEvent::MemoryOutboxWorkerStats {
            pending_shards: self.pending_members.len(),
            in_flight_shards: self.in_flight.len(),
            parallelism_limit: self.max_concurrent_shards,
            parallelism_peak: self.parallelism_peak,
            duplicate_schedule_coalesced_count: self.duplicate_schedule_coalesced_count,
            task_failure_count: self.task_failure_count,
            shard_requeue_count: self.shard_requeue_count,
        });
    }
}

async fn drain_memory_outbox_shard_in_background(
    memory_store: &MemoryStore,
    event_tx: &RuntimeEventSender,
    shard_index: usize,
    max_batches: usize,
) -> MemoryOutboxDrainResult {
    let mut saturated_any = false;
    for _ in 0..max_batches {
        let started_at = Instant::now();
        let claims = match memory_store
            .claim_due_outbox_records_for_shard_index(
                shard_index,
                MEMORY_OUTBOX_DRAIN_LIMIT,
                MEMORY_OUTBOX_LEASE,
                MEMORY_OUTBOX_EFFECT_KINDS,
            )
            .await
        {
            Ok(claims) => claims,
            Err(error) => {
                warn!(shard_index, error = %error, "memory outbox shard claim failed");
                return MemoryOutboxDrainResult {
                    shard_index,
                    saturated: false,
                    panicked: false,
                };
            }
        };
        let claimed = claims.len();
        let saturated = claimed == MEMORY_OUTBOX_DRAIN_LIMIT;
        saturated_any |= saturated;
        memory_store.record_profile(
            MemoryProfileMetricKind::RuntimeAtomicOutboxDrain,
            duration_us(started_at.elapsed()),
            claimed as u64,
        );

        if claimed == 0 {
            let (reply, _reply_rx) = oneshot::channel();
            let _ = event_tx
                .send(RuntimeEvent::MemoryOutboxDelivery {
                    shard_index,
                    claims,
                    saturated: false,
                    reply,
                })
                .await;
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }

        let (reply, reply_rx) = oneshot::channel();
        if let Err(error) = event_tx
            .send(RuntimeEvent::MemoryOutboxDelivery {
                shard_index,
                claims,
                saturated,
                reply,
            })
            .await
        {
            warn!(
                shard_index,
                error = %error,
                "memory outbox delivery event could not be sent"
            );
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }

        let outcomes = match reply_rx.await {
            Ok(outcomes) => outcomes,
            Err(error) => {
                warn!(
                    shard_index,
                    error = %error,
                    "memory outbox delivery response dropped"
                );
                return MemoryOutboxDrainResult {
                    shard_index,
                    saturated: false,
                    panicked: false,
                };
            }
        };
        let ack_started_at = Instant::now();
        if let Err(error) = memory_store.apply_outbox_delivery_outcomes(&outcomes).await {
            warn!(
                shard_index,
                error = %error,
                "memory outbox state update failed"
            );
            let _ = event_tx
                .send(RuntimeEvent::MemoryOutboxAckFailed { shard_index })
                .await;
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }
        memory_store.record_profile(
            MemoryProfileMetricKind::RuntimeAtomicOutboxDrain,
            duration_us(ack_started_at.elapsed()),
            outcomes.len() as u64,
        );

        if !saturated {
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }
    }
    MemoryOutboxDrainResult {
        shard_index,
        saturated: saturated_any,
        panicked: false,
    }
}

impl WorkerManager {
    pub(super) fn schedule_memory_outbox_drain_shard(
        &mut self,
        shard_index: usize,
        event_tx: &RuntimeEventSender,
    ) {
        let shard_count = self.memory_store.namespace_shards().max(1);
        let shard_index = shard_index % shard_count;
        self.pending_memory_outbox_shards.insert(shard_index);
        if let Err(error) = self
            .memory_outbox_drain_sender
            .try_send(MemoryOutboxWorkerCommand::DrainShard { shard_index })
        {
            self.stats.memory_outbox_channel_full_count = self
                .stats
                .memory_outbox_channel_full_count
                .saturating_add(1);
            warn!(
                shard_index,
                error = %error,
                "memory outbox worker channel full; drain request queued for later retry"
            );
        }
        let _ = event_tx;
    }

    pub(super) fn schedule_all_memory_outbox_shards(&mut self, event_tx: &RuntimeEventSender) {
        for shard_index in 0..self.memory_store.namespace_shards().max(1) {
            self.schedule_memory_outbox_drain_shard(shard_index, event_tx);
        }
    }

    pub(super) fn retry_pending_memory_outbox_drains(&mut self) {
        let pending = self
            .pending_memory_outbox_shards
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for shard_index in pending {
            if let Err(error) = self
                .memory_outbox_drain_sender
                .try_send(MemoryOutboxWorkerCommand::DrainShard { shard_index })
            {
                self.stats.memory_outbox_channel_full_count = self
                    .stats
                    .memory_outbox_channel_full_count
                    .saturating_add(1);
                warn!(
                    shard_index,
                    error = %error,
                    "memory outbox pending drain retry deferred"
                );
            } else {
                self.stats.memory_outbox_reschedule_count =
                    self.stats.memory_outbox_reschedule_count.saturating_add(1);
            }
        }
    }

    pub(super) fn handle_memory_outbox_ack_failed(
        &mut self,
        shard_index: usize,
        event_tx: &RuntimeEventSender,
    ) {
        self.stats.memory_outbox_ack_failure_count =
            self.stats.memory_outbox_ack_failure_count.saturating_add(1);
        self.schedule_memory_outbox_drain_shard(shard_index, event_tx);
    }

    fn increment_websocket_session_count(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) -> Result<()> {
        let Some(pool) = self.get_pool_mut(worker_name, generation) else {
            return Err(PlatformError::not_found("worker pool missing"));
        };
        let Some(isolate) = pool
            .isolates
            .iter_mut()
            .find(|isolate| isolate.id == isolate_id)
        else {
            return Err(PlatformError::internal(
                "websocket session owner isolate is unavailable",
            ));
        };
        isolate.active_websocket_sessions += 1;
        Ok(())
    }

    fn decrement_websocket_session_count(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) {
        let Some(pool) = self.get_pool_mut(worker_name, generation) else {
            return;
        };
        let Some(isolate) = pool
            .isolates
            .iter_mut()
            .find(|isolate| isolate.id == isolate_id)
        else {
            return;
        };
        isolate.active_websocket_sessions = isolate.active_websocket_sessions.saturating_sub(1);
    }

    fn increment_transport_session_count(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) -> Result<()> {
        let Some(pool) = self.get_pool_mut(worker_name, generation) else {
            return Err(PlatformError::not_found("worker pool missing"));
        };
        let Some(isolate) = pool
            .isolates
            .iter_mut()
            .find(|isolate| isolate.id == isolate_id)
        else {
            return Err(PlatformError::internal(
                "transport session owner isolate is unavailable",
            ));
        };
        isolate.active_transport_sessions += 1;
        Ok(())
    }

    fn decrement_transport_session_count(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
    ) {
        let Some(pool) = self.get_pool_mut(worker_name, generation) else {
            return;
        };
        let Some(isolate) = pool
            .isolates
            .iter_mut()
            .find(|isolate| isolate.id == isolate_id)
        else {
            return;
        };
        isolate.active_transport_sessions = isolate.active_transport_sessions.saturating_sub(1);
    }

    pub(super) fn reap_owned_sessions(
        &mut self,
        worker_name: &str,
        generation: Option<u64>,
        isolate_id: Option<u64>,
    ) {
        let websocket_session_ids = self
            .websocket_sessions
            .iter()
            .filter(|(_, session)| {
                session.worker_name == worker_name
                    && generation
                        .map(|value| session.generation == value)
                        .unwrap_or(true)
                    && isolate_id
                        .map(|value| session.owner_isolate_id == value)
                        .unwrap_or(true)
            })
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        for session_id in websocket_session_ids {
            let _ = self.unregister_websocket_session(&session_id);
        }

        let transport_session_ids = self
            .transport_sessions
            .iter()
            .filter(|(_, session)| {
                session.worker_name == worker_name
                    && generation
                        .map(|value| session.generation == value)
                        .unwrap_or(true)
                    && isolate_id
                        .map(|value| session.owner_isolate_id == value)
                        .unwrap_or(true)
            })
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        for session_id in transport_session_ids {
            let _ = self.unregister_transport_session(&session_id);
        }
    }

    pub(super) fn complete_websocket_open(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        session_id: String,
        result: Result<WorkerOutput>,
    ) {
        let Some(waiter) = self.websocket_open_waiters.remove(&session_id) else {
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                session_id,
                "missing websocket open waiter"
            );
            return;
        };
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        let (handle, binding, key) = match parse_websocket_open_metadata(&output, &session_id) {
            Ok(values) => values,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        if let Err(error) = self.register_websocket_session(WebSocketSessionRegistration {
            worker_name: worker_name.to_string(),
            generation,
            isolate_id,
            session_id: session_id.clone(),
            binding,
            key,
            handle,
        }) {
            let _ = waiter.send(Err(error));
            return;
        }
        let mut output = output;
        output.headers = strip_websocket_open_internal_headers(&output.headers);
        let _ = waiter.send(Ok(WebSocketOpen {
            session_id,
            worker_name: worker_name.to_string(),
            output,
        }));
    }

    pub(super) fn register_websocket_session(
        &mut self,
        registration: WebSocketSessionRegistration,
    ) -> Result<()> {
        let WebSocketSessionRegistration {
            worker_name,
            generation,
            isolate_id,
            session_id,
            binding,
            key,
            handle,
        } = registration;
        if self.websocket_sessions.contains_key(&session_id) {
            let _ = self.unregister_websocket_session(&session_id);
        }
        self.increment_websocket_session_count(&worker_name, generation, isolate_id)?;

        let owner_key = memory_owner_key(&binding, &key);
        let handle_key = memory_handle_key(&binding, &key, &handle);
        let session = WorkerWebSocketSession {
            worker_name,
            generation,
            owner_isolate_id: isolate_id,
            binding,
            key,
            handle,
        };
        self.websocket_handle_index
            .insert(handle_key, session_id.clone());
        self.websocket_open_handles
            .entry(owner_key)
            .or_default()
            .insert(session.handle.clone());
        self.open_handle_registry.add_socket_handle(
            &session.binding,
            &session.key,
            &session.handle,
        );
        self.websocket_sessions.insert(session_id, session);
        Ok(())
    }

    pub(super) fn unregister_websocket_session(
        &mut self,
        session_id: &str,
    ) -> Option<WorkerWebSocketSession> {
        let session = self.websocket_sessions.remove(session_id)?;
        self.decrement_websocket_session_count(
            &session.worker_name,
            session.generation,
            session.owner_isolate_id,
        );
        self.fail_websocket_frame_waiters(
            session_id,
            PlatformError::not_found("websocket session not found"),
        );
        self.websocket_handle_index.remove(&memory_handle_key(
            &session.binding,
            &session.key,
            &session.handle,
        ));
        self.open_handle_registry.remove_socket_handle(
            &session.binding,
            &session.key,
            &session.handle,
        );
        self.websocket_outbound_frames.remove(session_id);
        self.websocket_close_signals.remove(session_id);
        if let Some(replies) = self.websocket_pending_frame_replies.remove(session_id) {
            for WebSocketFrameReply { reply, .. } in replies {
                let _ = reply.send(Err(PlatformError::not_found("websocket session not found")));
            }
        }

        let owner_key = memory_owner_key(&session.binding, &session.key);
        let remove_owner_key =
            if let Some(handles) = self.websocket_open_handles.get_mut(&owner_key) {
                handles.remove(&session.handle);
                handles.is_empty()
            } else {
                false
            };
        if remove_owner_key {
            self.websocket_open_handles.remove(&owner_key);
        }
        let remove_pending_owner_key =
            if let Some(by_handle) = self.websocket_pending_closes.get_mut(&owner_key) {
                by_handle.remove(&session.handle);
                by_handle.is_empty()
            } else {
                false
            };
        if remove_pending_owner_key {
            self.websocket_pending_closes.remove(&owner_key);
        }

        Some(session)
    }

    pub(super) fn register_transport_session(
        &mut self,
        registration: TransportSessionRegistration,
    ) -> Result<()> {
        let TransportSessionRegistration {
            worker_name,
            generation,
            isolate_id,
            session_id,
            binding,
            key,
            handle,
            stream_sender,
            datagram_sender,
        } = registration;
        if self.transport_sessions.contains_key(&session_id) {
            let _ = self.unregister_transport_session(&session_id);
        }
        self.increment_transport_session_count(&worker_name, generation, isolate_id)?;

        let owner_key = memory_owner_key(&binding, &key);
        let handle_key = memory_handle_key(&binding, &key, &handle);
        let session = WorkerTransportSession {
            worker_name,
            generation,
            owner_isolate_id: isolate_id,
            binding,
            key,
            handle,
            stream_sender,
            datagram_sender,
        };
        self.transport_handle_index
            .insert(handle_key, session_id.clone());
        self.transport_open_handles
            .entry(owner_key)
            .or_default()
            .insert(session.handle.clone());
        self.open_handle_registry.add_transport_handle(
            &session.binding,
            &session.key,
            &session.handle,
        );
        self.transport_sessions.insert(session_id, session);
        Ok(())
    }

    pub(super) fn unregister_transport_session(
        &mut self,
        session_id: &str,
    ) -> Option<WorkerTransportSession> {
        let session = self.transport_sessions.remove(session_id)?;
        self.decrement_transport_session_count(
            &session.worker_name,
            session.generation,
            session.owner_isolate_id,
        );
        self.transport_handle_index.remove(&memory_handle_key(
            &session.binding,
            &session.key,
            &session.handle,
        ));
        self.open_handle_registry.remove_transport_handle(
            &session.binding,
            &session.key,
            &session.handle,
        );
        self.transport_open_channels.remove(session_id);
        self.transport_open_waiters.remove(session_id);

        let owner_key = memory_owner_key(&session.binding, &session.key);
        let remove_owner_key =
            if let Some(handles) = self.transport_open_handles.get_mut(&owner_key) {
                handles.remove(&session.handle);
                handles.is_empty()
            } else {
                false
            };
        if remove_owner_key {
            self.transport_open_handles.remove(&owner_key);
        }
        let remove_pending_owner_key =
            if let Some(by_handle) = self.transport_pending_closes.get_mut(&owner_key) {
                by_handle.remove(&session.handle);
                by_handle.is_empty()
            } else {
                false
            };
        if remove_pending_owner_key {
            self.transport_pending_closes.remove(&owner_key);
        }

        Some(session)
    }

    pub(super) fn queue_transport_close_replay(
        &mut self,
        session: &WorkerTransportSession,
        close_code: u16,
        close_reason: String,
    ) {
        let owner_key = memory_owner_key(&session.binding, &session.key);
        self.transport_pending_closes
            .entry(owner_key)
            .or_default()
            .entry(session.handle.clone())
            .or_default()
            .push(TransportCloseEvent {
                code: close_code,
                reason: close_reason,
            });
    }

    pub(super) fn queue_websocket_close_replay(
        &mut self,
        session: &WorkerWebSocketSession,
        close_code: u16,
        close_reason: String,
    ) {
        let owner_key = memory_owner_key(&session.binding, &session.key);
        self.websocket_pending_closes
            .entry(owner_key)
            .or_default()
            .entry(session.handle.clone())
            .or_default()
            .push(SocketCloseEvent {
                code: close_code,
                reason: close_reason,
            });
    }

    pub(super) fn complete_transport_open(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        session_id: String,
        result: Result<WorkerOutput>,
    ) {
        let Some(waiter) = self.transport_open_waiters.remove(&session_id) else {
            warn!(
                worker = %worker_name,
                generation,
                isolate_id,
                session_id,
                "missing transport open waiter"
            );
            self.transport_open_channels.remove(&session_id);
            return;
        };
        let channels = self.transport_open_channels.remove(&session_id);
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        let (handle, binding, key) = match parse_transport_open_metadata(&output, &session_id) {
            Ok(values) => values,
            Err(error) => {
                let _ = waiter.send(Err(error));
                return;
            }
        };
        let Some(channels) = channels else {
            let _ = waiter.send(Err(PlatformError::internal(
                "missing transport open channels",
            )));
            return;
        };
        if let Err(error) = self.register_transport_session(TransportSessionRegistration {
            worker_name: worker_name.to_string(),
            generation,
            isolate_id,
            session_id: session_id.clone(),
            binding,
            key,
            handle,
            stream_sender: channels.stream_sender,
            datagram_sender: channels.datagram_sender,
        }) {
            let _ = waiter.send(Err(error));
            return;
        }
        let mut output = output;
        output.headers = strip_transport_open_internal_headers(&output.headers);
        let _ = waiter.send(Ok(TransportOpen {
            session_id,
            worker_name: worker_name.to_string(),
            output,
        }));
    }

    pub(super) fn handle_memory_socket_send(
        &mut self,
        payload: crate::ops::MemorySocketSendEvent,
        _event_tx: &RuntimeEventSender,
    ) {
        let crate::ops::MemorySocketSendEvent {
            reply,
            handle,
            binding,
            key,
            is_text,
            message,
        } = payload;
        let result = self.send_memory_socket(&binding, &key, &handle, is_text, message);
        let _ = reply.send(result);
    }

    pub(super) fn handle_memory_socket_close(
        &mut self,
        payload: crate::ops::MemorySocketCloseEvent,
        _event_tx: &RuntimeEventSender,
    ) {
        let crate::ops::MemorySocketCloseEvent {
            reply,
            handle,
            binding,
            key,
            code,
            reason,
        } = payload;
        let result = self.close_memory_socket(&binding, &key, &handle, code, reason);
        let _ = reply.send(result);
    }

    fn send_memory_socket(
        &mut self,
        binding: &str,
        key: &str,
        handle: &str,
        is_text: bool,
        message: Vec<u8>,
    ) -> Result<()> {
        let index_key = memory_handle_key(binding, key, handle);
        let session_id = self
            .websocket_handle_index
            .get(&index_key)
            .cloned()
            .ok_or_else(|| PlatformError::not_found("websocket session not found"))?;
        self.websocket_outbound_frames
            .entry(session_id.clone())
            .or_default()
            .push_back(WebSocketOutboundFrame {
                is_binary: !is_text,
                payload: message,
            });
        self.flush_pending_websocket_frame_replies(&session_id);
        self.notify_websocket_frame_waiters(&session_id);
        Ok(())
    }

    fn close_memory_socket(
        &mut self,
        binding: &str,
        key: &str,
        handle: &str,
        code: u16,
        reason: String,
    ) -> Result<()> {
        let index_key = memory_handle_key(binding, key, handle);
        let session_id = self
            .websocket_handle_index
            .get(&index_key)
            .cloned()
            .ok_or_else(|| PlatformError::not_found("websocket session not found"))?;
        self.websocket_close_signals
            .insert(session_id.clone(), SocketCloseEvent { code, reason });
        self.flush_pending_websocket_frame_replies(&session_id);
        self.notify_websocket_frame_waiters(&session_id);
        Ok(())
    }

    pub(super) fn websocket_handles_snapshot(
        &self,
        binding: &str,
        key: &str,
        include_handle: Option<&str>,
    ) -> Vec<String> {
        let owner_key = memory_owner_key(binding, key);
        let mut handles: Vec<String> = self
            .websocket_open_handles
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default();
        if let Some(handle) = include_handle {
            let normalized = handle.trim();
            if !normalized.is_empty() {
                handles.push(normalized.to_string());
            }
        }
        handles.sort();
        handles.dedup();
        handles
    }

    pub(super) fn handle_memory_socket_consume_close(
        &mut self,
        payload: crate::ops::MemorySocketConsumeCloseEvent,
        _event_tx: &RuntimeEventSender,
    ) {
        let owner_key = memory_owner_key(&payload.binding, &payload.key);
        let events = self
            .websocket_pending_closes
            .get_mut(&owner_key)
            .and_then(|by_handle| by_handle.remove(&payload.handle))
            .unwrap_or_default();
        let remove_owner_key = self
            .websocket_pending_closes
            .get(&owner_key)
            .map(|by_handle| by_handle.is_empty())
            .unwrap_or(false);
        if remove_owner_key {
            self.websocket_pending_closes.remove(&owner_key);
        }
        let replay: Vec<crate::ops::MemorySocketCloseReplayEvent> = events
            .into_iter()
            .map(|event| crate::ops::MemorySocketCloseReplayEvent {
                code: event.code,
                reason: event.reason,
            })
            .collect();
        let _ = payload.reply.send(Ok(replay));
    }

    pub(super) fn push_transport_stream(
        &mut self,
        worker_name: &str,
        session_id: &str,
        chunk: Vec<u8>,
        done: bool,
        event_tx: &RuntimeEventSender,
    ) -> Result<()> {
        let _ = done;
        let Some((session_worker_name, generation, binding, key, handle)) =
            self.transport_sessions.get(session_id).map(|session| {
                (
                    session.worker_name.clone(),
                    session.generation,
                    session.binding.clone(),
                    session.key.clone(),
                    session.handle.clone(),
                )
            })
        else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if session_worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = MemoryRoute::new(binding.clone(), key.clone());
        let socket_handles = self.websocket_handles_snapshot(&binding, &key, None);
        let transport_handles = self.transport_handles_snapshot(&binding, &key, Some(&handle));
        let memory_call = MemoryExecutionCall::TransportStream {
            binding,
            key,
            handle,
            data: chunk,
            socket_handles,
            transport_handles,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-STREAM".to_string(),
            url: format!("http://memory/__dd_transport_stream/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-stream-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            EnqueueInvokeRequest {
                worker_name: session_worker_name,
                runtime_request_id,
                request: invoke,
                request_body: None,
                memory_route: Some(route),
                memory_call: Some(memory_call),
                host_rpc_call: None,
                target_isolate_id: None,
                target_generation: Some(generation),
                internal_origin: true,
                reply,
                reply_kind: PendingReplyKind::Normal,
            },
            event_tx,
        );
        let session_id = session_id.to_string();
        tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(1), receiver).await {
                Ok(Ok(Err(error))) => {
                    warn!(session_id, error = %error, "transport stream wake dispatch failed");
                }
                Ok(Ok(Ok(_))) | Ok(Err(_)) => {}
                Err(_) => {
                    warn!(session_id, "transport stream wake dispatch timed out");
                }
            }
        });
        Ok(())
    }

    pub(super) fn push_transport_datagram(
        &mut self,
        worker_name: &str,
        session_id: &str,
        datagram: Vec<u8>,
        event_tx: &RuntimeEventSender,
    ) -> Result<()> {
        let Some((session_worker_name, generation, binding, key, handle)) =
            self.transport_sessions.get(session_id).map(|session| {
                (
                    session.worker_name.clone(),
                    session.generation,
                    session.binding.clone(),
                    session.key.clone(),
                    session.handle.clone(),
                )
            })
        else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if session_worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }
        if datagram.is_empty() {
            return Ok(());
        }
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = MemoryRoute::new(binding.clone(), key.clone());
        let socket_handles = self.websocket_handles_snapshot(&binding, &key, None);
        let transport_handles = self.transport_handles_snapshot(&binding, &key, Some(&handle));
        let memory_call = MemoryExecutionCall::TransportDatagram {
            binding,
            key,
            handle,
            data: datagram,
            socket_handles,
            transport_handles,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-DATAGRAM".to_string(),
            url: format!("http://memory/__dd_transport_datagram/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-datagram-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            EnqueueInvokeRequest {
                worker_name: session_worker_name,
                runtime_request_id,
                request: invoke,
                request_body: None,
                memory_route: Some(route),
                memory_call: Some(memory_call),
                host_rpc_call: None,
                target_isolate_id: None,
                target_generation: Some(generation),
                internal_origin: true,
                reply,
                reply_kind: PendingReplyKind::Normal,
            },
            event_tx,
        );
        let session_id = session_id.to_string();
        tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(1), receiver).await {
                Ok(Ok(Err(error))) => {
                    warn!(session_id, error = %error, "transport datagram wake dispatch failed");
                }
                Ok(Ok(Ok(_))) | Ok(Err(_)) => {}
                Err(_) => {
                    warn!(session_id, "transport datagram wake dispatch timed out");
                }
            }
        });
        Ok(())
    }

    pub(super) fn close_transport(
        &mut self,
        worker_name: &str,
        session_id: &str,
        close_code: u16,
        close_reason: String,
        event_tx: &RuntimeEventSender,
    ) -> Result<()> {
        let Some(existing) = self.transport_sessions.get(session_id) else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if existing.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }

        let session = self
            .unregister_transport_session(session_id)
            .ok_or_else(|| PlatformError::not_found("transport session not found"))?;
        self.queue_transport_close_replay(&session, close_code, close_reason.clone());

        let runtime_request_id = Uuid::new_v4().to_string();
        let route = MemoryRoute::new(session.binding.clone(), session.key.clone());
        let socket_handles = self.websocket_handles_snapshot(&session.binding, &session.key, None);
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let memory_call = MemoryExecutionCall::TransportClose {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            code: close_code,
            reason: close_reason,
            socket_handles,
            transport_handles,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-CLOSE".to_string(),
            url: format!("http://memory/__dd_transport_close/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-close-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            EnqueueInvokeRequest {
                worker_name: session.worker_name,
                runtime_request_id,
                request: invoke,
                request_body: None,
                memory_route: Some(route),
                memory_call: Some(memory_call),
                host_rpc_call: None,
                target_isolate_id: None,
                target_generation: Some(session.generation),
                internal_origin: true,
                reply,
                reply_kind: PendingReplyKind::Normal,
            },
            event_tx,
        );
        let session_id = session_id.to_string();
        tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(1), receiver).await {
                Ok(Ok(Err(error))) => {
                    warn!(session_id, error = %error, "transport close wake dispatch failed");
                }
                Ok(Ok(Ok(_))) | Ok(Err(_)) => {}
                Err(_) => {
                    warn!(session_id, "transport close wake dispatch timed out");
                }
            }
        });
        Ok(())
    }

    pub(super) fn handle_memory_transport_send_stream(
        &mut self,
        payload: crate::ops::MemoryTransportSendStreamEvent,
        _event_tx: &RuntimeEventSender,
    ) {
        let crate::ops::MemoryTransportSendStreamEvent {
            reply,
            handle,
            binding,
            key,
            chunk,
        } = payload;
        let result = self.send_memory_transport_stream(&binding, &key, &handle, chunk);
        let _ = reply.send(result);
    }

    pub(super) fn handle_memory_transport_send_datagram(
        &mut self,
        payload: crate::ops::MemoryTransportSendDatagramEvent,
        _event_tx: &RuntimeEventSender,
    ) {
        let crate::ops::MemoryTransportSendDatagramEvent {
            reply,
            handle,
            binding,
            key,
            datagram,
        } = payload;
        let result = self.send_memory_transport_datagram(&binding, &key, &handle, datagram);
        let _ = reply.send(result);
    }

    pub(super) fn handle_memory_transport_close(
        &mut self,
        payload: crate::ops::MemoryTransportCloseEvent,
        _event_tx: &RuntimeEventSender,
    ) {
        let crate::ops::MemoryTransportCloseEvent {
            reply,
            handle,
            binding,
            key,
            code,
            reason,
        } = payload;
        let result = self.close_memory_transport(&binding, &key, &handle, code, reason);
        let _ = reply.send(result);
    }

    fn send_memory_transport_stream(
        &mut self,
        binding: &str,
        key: &str,
        handle: &str,
        chunk: Vec<u8>,
    ) -> Result<()> {
        let index_key = memory_handle_key(binding, key, handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get(session_id) {
                Some(session) => {
                    session
                        .stream_sender
                        .try_send(chunk)
                        .map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                PlatformError::overloaded("transport stream channel is full")
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                PlatformError::internal("transport stream channel closed")
                            }
                        })
                }
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        }
    }

    fn send_memory_transport_datagram(
        &mut self,
        binding: &str,
        key: &str,
        handle: &str,
        datagram: Vec<u8>,
    ) -> Result<()> {
        let index_key = memory_handle_key(binding, key, handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get(session_id) {
                Some(session) => {
                    session
                        .datagram_sender
                        .try_send(datagram)
                        .map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                PlatformError::overloaded("transport datagram channel is full")
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                PlatformError::internal("transport datagram channel closed")
                            }
                        })
                }
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        }
    }

    fn close_memory_transport(
        &mut self,
        binding: &str,
        key: &str,
        handle: &str,
        code: u16,
        reason: String,
    ) -> Result<()> {
        let index_key = memory_handle_key(binding, key, handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        match session_id.as_deref() {
            Some(session_id) => {
                if let Some(session) = self.unregister_transport_session(session_id) {
                    self.queue_transport_close_replay(&session, code, reason);
                    Ok(())
                } else {
                    Err(PlatformError::not_found("transport session not found"))
                }
            }
            None => Err(PlatformError::not_found("transport session not found")),
        }
    }

    pub(super) fn transport_handles_snapshot(
        &self,
        binding: &str,
        key: &str,
        include_handle: Option<&str>,
    ) -> Vec<String> {
        let owner_key = memory_owner_key(binding, key);
        let mut handles: Vec<String> = self
            .transport_open_handles
            .get(&owner_key)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default();
        if let Some(handle) = include_handle {
            let normalized = handle.trim();
            if !normalized.is_empty() {
                handles.push(normalized.to_string());
            }
        }
        handles.sort();
        handles.dedup();
        handles
    }

    pub(super) fn handle_memory_transport_consume_close(
        &mut self,
        payload: crate::ops::MemoryTransportConsumeCloseEvent,
        _event_tx: &RuntimeEventSender,
    ) {
        let owner_key = memory_owner_key(&payload.binding, &payload.key);
        let events = self
            .transport_pending_closes
            .get_mut(&owner_key)
            .and_then(|by_handle| by_handle.remove(&payload.handle))
            .unwrap_or_default();
        let remove_owner_key = self
            .transport_pending_closes
            .get(&owner_key)
            .map(|by_handle| by_handle.is_empty())
            .unwrap_or(false);
        if remove_owner_key {
            self.transport_pending_closes.remove(&owner_key);
        }
        let replay: Vec<crate::ops::MemoryTransportCloseReplayEvent> = events
            .into_iter()
            .map(|event| crate::ops::MemoryTransportCloseReplayEvent {
                code: event.code,
                reason: event.reason,
            })
            .collect();
        let _ = payload.reply.send(Ok(replay));
    }

    pub(super) fn handle_memory_outbox_delivery(
        &mut self,
        shard_index: usize,
        claims: Vec<MemoryOutboxClaim>,
        saturated: bool,
        reply: oneshot::Sender<Vec<MemoryOutboxDeliveryOutcome>>,
        event_tx: &RuntimeEventSender,
    ) {
        self.pending_memory_outbox_shards.remove(&shard_index);
        self.stats.memory_outbox_claim_batch_count =
            self.stats.memory_outbox_claim_batch_count.saturating_add(1);
        self.stats.memory_outbox_claim_row_count = self
            .stats
            .memory_outbox_claim_row_count
            .saturating_add(claims.len() as u64);
        if saturated {
            self.stats.memory_outbox_saturated_batch_count = self
                .stats
                .memory_outbox_saturated_batch_count
                .saturating_add(1);
        }
        let outcomes = self.deliver_memory_outbox_claims(claims);
        for outcome in &outcomes {
            match outcome.action {
                MemoryOutboxDeliveryAction::Delivered => {
                    self.stats.memory_outbox_delivery_success_count = self
                        .stats
                        .memory_outbox_delivery_success_count
                        .saturating_add(1);
                }
                MemoryOutboxDeliveryAction::DroppedTerminal => {
                    self.stats.memory_outbox_terminal_drop_count = self
                        .stats
                        .memory_outbox_terminal_drop_count
                        .saturating_add(1);
                }
                MemoryOutboxDeliveryAction::Retry { .. } => {
                    self.stats.memory_outbox_delivery_retry_count = self
                        .stats
                        .memory_outbox_delivery_retry_count
                        .saturating_add(1);
                }
            }
        }
        let _ = reply.send(outcomes);
        if saturated {
            self.schedule_memory_outbox_drain_shard(shard_index, event_tx);
        }
    }

    fn deliver_memory_outbox_claims(
        &mut self,
        claims: Vec<MemoryOutboxClaim>,
    ) -> Vec<MemoryOutboxDeliveryOutcome> {
        let mut outcomes = Vec::with_capacity(claims.len());
        for claim in claims {
            let effect_id = claim.record.effect_id.clone();
            let namespace = claim.namespace.clone();
            let memory_key = claim.memory_key.clone();
            let delivery = self.deliver_memory_outbox_claim(&claim);
            let action = match delivery {
                Ok(()) => MemoryOutboxDeliveryAction::Delivered,
                Err(error) if is_terminal_memory_outbox_delivery_error(&error) => {
                    warn!(
                        namespace = %namespace,
                        memory_key = %memory_key,
                        effect_id = %effect_id,
                        kind = %claim.record.kind,
                        error = %error,
                        "memory outbox effect dropped"
                    );
                    MemoryOutboxDeliveryAction::DroppedTerminal
                }
                Err(error) => {
                    let retry_after = memory_outbox_retry_after(claim.record.attempt_count);
                    warn!(
                        namespace = %namespace,
                        memory_key = %memory_key,
                        effect_id = %effect_id,
                        kind = %claim.record.kind,
                        error = %error,
                        "memory outbox delivery failed"
                    );
                    MemoryOutboxDeliveryAction::Retry { retry_after }
                }
            };
            outcomes.push(MemoryOutboxDeliveryOutcome {
                namespace,
                memory_key,
                effect_id,
                action,
            });
        }
        outcomes
    }

    fn deliver_memory_outbox_claim(&mut self, claim: &MemoryOutboxClaim) -> Result<()> {
        if claim.record.kind.starts_with("audit.") {
            info!(
                namespace = %claim.namespace,
                memory_key = %claim.memory_key,
                effect_id = %claim.record.effect_id,
                revision = claim.record.revision,
                kind = %claim.record.kind,
                payload_bytes = claim.record.payload.len(),
                payload_utf8 = std::str::from_utf8(&claim.record.payload).ok(),
                "memory audit outbox effect delivered"
            );
            return Ok(());
        }
        if claim.record.kind.starts_with("trace.") {
            info!(
                namespace = %claim.namespace,
                memory_key = %claim.memory_key,
                effect_id = %claim.record.effect_id,
                revision = claim.record.revision,
                kind = %claim.record.kind,
                payload_bytes = claim.record.payload.len(),
                payload_utf8 = std::str::from_utf8(&claim.record.payload).ok(),
                "memory trace outbox effect delivered"
            );
            return Ok(());
        }
        match claim.record.kind.as_str() {
            "socket.send" => {
                let (handle, is_text, message) =
                    parse_memory_socket_send_effect(&claim.record.payload)?;
                self.send_memory_socket(
                    &claim.namespace,
                    &claim.memory_key,
                    &handle,
                    is_text,
                    message,
                )
            }
            "socket.close" => {
                let (handle, code, reason) = parse_memory_close_effect(&claim.record.payload)?;
                self.close_memory_socket(&claim.namespace, &claim.memory_key, &handle, code, reason)
            }
            "transport.stream" => {
                let (handle, chunk) = parse_memory_transport_data_effect(&claim.record.payload)?;
                self.send_memory_transport_stream(
                    &claim.namespace,
                    &claim.memory_key,
                    &handle,
                    chunk,
                )
            }
            "transport.datagram" => {
                let (handle, datagram) = parse_memory_transport_data_effect(&claim.record.payload)?;
                self.send_memory_transport_datagram(
                    &claim.namespace,
                    &claim.memory_key,
                    &handle,
                    datagram,
                )
            }
            "transport.close" => {
                let (handle, code, reason) = parse_memory_close_effect(&claim.record.payload)?;
                self.close_memory_transport(
                    &claim.namespace,
                    &claim.memory_key,
                    &handle,
                    code,
                    reason,
                )
            }
            kind => Err(PlatformError::bad_request(format!(
                "unsupported memory outbox effect: {kind}"
            ))),
        }
    }
}

const MEMORY_RUNTIME_EFFECT_VERSION: u8 = 1;

fn parse_memory_socket_send_effect(payload: &[u8]) -> Result<(String, bool, Vec<u8>)> {
    let mut offset = 0;
    read_memory_effect_version(payload, &mut offset)?;
    let is_text = match read_memory_effect_u8(payload, &mut offset, "message kind")? {
        0 => false,
        1 => true,
        _ => {
            return Err(PlatformError::bad_request(
                "memory outbox socket message kind is invalid",
            ));
        }
    };
    let handle = read_memory_effect_string(payload, &mut offset, "handle")?;
    let message = read_memory_effect_bytes(payload, &mut offset, "message")?;
    require_memory_effect_end(payload, offset)?;
    Ok((handle, is_text, message))
}

fn parse_memory_transport_data_effect(payload: &[u8]) -> Result<(String, Vec<u8>)> {
    let mut offset = 0;
    read_memory_effect_version(payload, &mut offset)?;
    let handle = read_memory_effect_string(payload, &mut offset, "handle")?;
    let body = read_memory_effect_bytes(payload, &mut offset, "payload")?;
    require_memory_effect_end(payload, offset)?;
    Ok((handle, body))
}

fn parse_memory_close_effect(payload: &[u8]) -> Result<(String, u16, String)> {
    let mut offset = 0;
    read_memory_effect_version(payload, &mut offset)?;
    let code = read_memory_effect_u16(payload, &mut offset, "code")?;
    let handle = read_memory_effect_string(payload, &mut offset, "handle")?;
    let reason = read_memory_effect_string(payload, &mut offset, "reason")?;
    require_memory_effect_end(payload, offset)?;
    Ok((handle, code, reason))
}

fn read_memory_effect_version(payload: &[u8], offset: &mut usize) -> Result<()> {
    let version = read_memory_effect_u8(payload, offset, "version")?;
    if version != MEMORY_RUNTIME_EFFECT_VERSION {
        return Err(PlatformError::bad_request(format!(
            "memory outbox runtime effect version is unsupported: {version}"
        )));
    }
    Ok(())
}

fn read_memory_effect_u8(payload: &[u8], offset: &mut usize, field: &str) -> Result<u8> {
    let Some(value) = payload.get(*offset).copied() else {
        return Err(PlatformError::bad_request(format!(
            "memory outbox runtime effect {field} is missing"
        )));
    };
    *offset += 1;
    Ok(value)
}

fn read_memory_effect_u16(payload: &[u8], offset: &mut usize, field: &str) -> Result<u16> {
    if payload.len().saturating_sub(*offset) < 2 {
        return Err(PlatformError::bad_request(format!(
            "memory outbox runtime effect {field} is truncated"
        )));
    }
    let value = u16::from_be_bytes([payload[*offset], payload[*offset + 1]]);
    *offset += 2;
    Ok(value)
}

fn read_memory_effect_u32(payload: &[u8], offset: &mut usize, field: &str) -> Result<usize> {
    if payload.len().saturating_sub(*offset) < 4 {
        return Err(PlatformError::bad_request(format!(
            "memory outbox runtime effect {field} length is truncated"
        )));
    }
    let value = u32::from_be_bytes([
        payload[*offset],
        payload[*offset + 1],
        payload[*offset + 2],
        payload[*offset + 3],
    ]);
    *offset += 4;
    Ok(value as usize)
}

fn read_memory_effect_bytes(payload: &[u8], offset: &mut usize, field: &str) -> Result<Vec<u8>> {
    let len = read_memory_effect_u32(payload, offset, field)?;
    if payload.len().saturating_sub(*offset) < len {
        return Err(PlatformError::bad_request(format!(
            "memory outbox runtime effect {field} is truncated"
        )));
    }
    let bytes = payload[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(bytes)
}

fn read_memory_effect_string(payload: &[u8], offset: &mut usize, field: &str) -> Result<String> {
    let bytes = read_memory_effect_bytes(payload, offset, field)?;
    let value = String::from_utf8(bytes).map_err(|error| {
        PlatformError::bad_request(format!(
            "memory outbox runtime effect {field} is invalid utf8: {error}"
        ))
    })?;
    if field == "handle" && value.trim().is_empty() {
        return Err(PlatformError::bad_request(
            "memory outbox runtime effect handle is required",
        ));
    }
    Ok(if field == "handle" {
        value.trim().to_string()
    } else {
        value
    })
}

fn require_memory_effect_end(payload: &[u8], offset: usize) -> Result<()> {
    if offset == payload.len() {
        return Ok(());
    }
    Err(PlatformError::bad_request(
        "memory outbox runtime effect has trailing bytes",
    ))
}

fn is_terminal_memory_outbox_delivery_error(error: &PlatformError) -> bool {
    matches!(error.kind(), ErrorKind::BadRequest | ErrorKind::NotFound)
}

fn memory_outbox_retry_after(attempt_count: i64) -> Duration {
    let shift = attempt_count.clamp(0, 6) as u32;
    Duration::from_millis(250u64.saturating_mul(1u64 << shift))
}

#[cfg(test)]
mod outbox_coordinator_tests {
    use super::*;

    #[test]
    fn coordinator_deduplicates_pending_shards() {
        let mut coordinator = MemoryOutboxDrainCoordinator::new(2);
        coordinator.schedule(7);
        coordinator.schedule(7);

        assert_eq!(coordinator.pending.len(), 1);
        assert_eq!(coordinator.pending_members.len(), 1);
        assert_eq!(coordinator.duplicate_schedule_coalesced_count, 1);
    }

    #[test]
    fn coordinator_records_follow_up_for_in_flight_shard_once() {
        let mut coordinator = MemoryOutboxDrainCoordinator::new(2);
        coordinator.in_flight.insert(3);

        coordinator.schedule(3);
        coordinator.schedule(3);

        assert!(coordinator.follow_up.contains(&3));
        assert_eq!(coordinator.pending.len(), 0);
        assert_eq!(coordinator.duplicate_schedule_coalesced_count, 1);

        coordinator.finish_joined(Some(Ok(MemoryOutboxDrainResult {
            shard_index: 3,
            saturated: false,
            panicked: false,
        })));

        assert!(!coordinator.in_flight.contains(&3));
        assert!(!coordinator.follow_up.contains(&3));
        assert_eq!(coordinator.pending.pop_front(), Some(3));
        assert_eq!(coordinator.shard_requeue_count, 1);
    }

    #[test]
    fn coordinator_requeues_saturated_shard_behind_pending_work() {
        let mut coordinator = MemoryOutboxDrainCoordinator::new(2);
        coordinator.in_flight.insert(1);
        coordinator.schedule(2);

        coordinator.finish_joined(Some(Ok(MemoryOutboxDrainResult {
            shard_index: 1,
            saturated: true,
            panicked: false,
        })));

        assert_eq!(coordinator.pending.pop_front(), Some(2));
        assert_eq!(coordinator.pending.pop_front(), Some(1));
        assert_eq!(coordinator.shard_requeue_count, 1);
    }

    #[test]
    fn coordinator_clears_in_flight_after_task_panic_result() {
        let mut coordinator = MemoryOutboxDrainCoordinator::new(2);
        coordinator.in_flight.insert(9);

        coordinator.finish_joined(Some(Ok(MemoryOutboxDrainResult {
            shard_index: 9,
            saturated: false,
            panicked: true,
        })));

        assert!(!coordinator.in_flight.contains(&9));
        assert_eq!(coordinator.task_failure_count, 1);
        assert!(!coordinator.pending_members.contains(&9));
    }

    #[test]
    fn coordinator_limit_is_clamped_to_one() {
        let coordinator = MemoryOutboxDrainCoordinator::new(0);
        assert_eq!(coordinator.max_concurrent_shards, 1);
    }
}
