use super::*;
use std::{cell::RefCell, rc::Rc};

const RUNTIME_READY_WORK_BUDGET: usize = 256;
const RUNTIME_EVENT_CHANNEL_CAPACITY: usize = 4096;
const ISOLATE_COMMAND_DRAIN_BUDGET: usize = 256;
const ISOLATE_COMMAND_CHANNEL_CAPACITY: usize = 1024;
const ISOLATE_EVENT_QUEUE_CAPACITY: usize = 8192;

pub(super) fn select_dispatch_candidate(
    pool: &WorkerPool,
    max_inflight: usize,
    require_wait_until_idle: bool,
    allow_memory_atomic_overflow: bool,
) -> Option<DispatchSelection> {
    if let Some(selection) =
        pool.queue
            .find_oldest_map([PendingQueueLane::TargetedNested], |queue_key, pending| {
                let target_isolate_id = pending
                    .target_isolate_id
                    .expect("targeted nested queue entries must have a target isolate");
                if memory_atomic_route_is_active(pool, pending) {
                    return None;
                }
                if let Some(isolate_idx) = target_isolate_idx(pool, target_isolate_id) {
                    if pool.isolates[isolate_idx].startup.is_ready() {
                        Some(DispatchSelection::Dispatch(DispatchCandidate {
                            queue_key,
                            isolate_idx,
                        }))
                    } else {
                        None
                    }
                } else {
                    Some(DispatchSelection::DropStaleTarget { queue_key })
                }
            })
    {
        return Some(selection);
    }

    pool.queue.find_oldest_map(
        [
            PendingQueueLane::Targeted,
            PendingQueueLane::Memory,
            PendingQueueLane::General,
        ],
        |queue_key, pending| {
            let Some(target_isolate_id) = pending.target_isolate_id else {
                if pending.memory_route.is_none() {
                    return least_loaded_isolate_idx(
                        &pool.isolates,
                        max_inflight,
                        require_wait_until_idle,
                    )
                    .map(|isolate_idx| {
                        DispatchSelection::Dispatch(DispatchCandidate {
                            queue_key,
                            isolate_idx,
                        })
                    });
                }

                if memory_atomic_route_is_active(pool, pending) {
                    return None;
                }
                let isolate_idx = memory_route_is_atomic(pending)
                    .then(|| {
                        memory_shard_affinity_isolate_idx(
                            pool,
                            pending,
                            max_inflight,
                            require_wait_until_idle,
                        )
                    })
                    .flatten()
                    .or_else(|| {
                        least_loaded_isolate_idx(
                            &pool.isolates,
                            max_inflight,
                            require_wait_until_idle,
                        )
                    })
                    .or_else(|| {
                        (allow_memory_atomic_overflow && memory_route_is_atomic(pending))
                            .then(|| least_loaded_isolate_any_idx(&pool.isolates))
                            .flatten()
                    });
                return isolate_idx.map(|isolate_idx| {
                    DispatchSelection::Dispatch(DispatchCandidate {
                        queue_key,
                        isolate_idx,
                    })
                });
            };

            if let Some(isolate_idx) = target_isolate_idx(pool, target_isolate_id) {
                let isolate = &pool.isolates[isolate_idx];
                debug_assert!(pending.host_rpc_call.is_none());
                debug_assert!(pending.memory_route.is_none());
                if isolate.startup.is_ready()
                    && isolate.inflight_count < max_inflight
                    && (!require_wait_until_idle || isolate.pending_wait_until.is_empty())
                {
                    Some(DispatchSelection::Dispatch(DispatchCandidate {
                        queue_key,
                        isolate_idx,
                    }))
                } else {
                    None
                }
            } else {
                Some(DispatchSelection::DropStaleTarget { queue_key })
            }
        },
    )
}

fn memory_atomic_route_is_active(pool: &WorkerPool, pending: &PendingInvoke) -> bool {
    let Some(memory_route) = pending.memory_route.as_ref() else {
        return false;
    };
    if !memory_route_is_atomic(pending) {
        return false;
    }
    pool.memory_entity_is_leased(&memory_route.owner_key)
}

fn memory_route_is_atomic(pending: &PendingInvoke) -> bool {
    matches!(
        pending.memory_call.as_ref(),
        Some(MemoryExecutionCall::Method { name, .. }) if name == MEMORY_ATOMIC_METHOD
    )
}

fn memory_shard_affinity_isolate_idx(
    pool: &WorkerPool,
    pending: &PendingInvoke,
    max_inflight: usize,
    require_wait_until_idle: bool,
) -> Option<usize> {
    let shard_index = pending.memory_route.as_ref()?.shard_index?;
    let isolate_id = *pool.memory_shard_affinity.get(&shard_index)?;
    let isolate_idx = pool.isolate_idx(isolate_id)?;
    let isolate = &pool.isolates[isolate_idx];
    if isolate.startup.is_ready()
        && isolate.inflight_count < max_inflight
        && (!require_wait_until_idle || isolate.pending_wait_until.is_empty())
    {
        Some(isolate_idx)
    } else {
        None
    }
}

fn target_isolate_idx(pool: &WorkerPool, target_isolate_id: u64) -> Option<usize> {
    pool.isolate_idx(target_isolate_id)
}

pub(super) fn host_rpc_method_blocked(method: &str) -> bool {
    let method = method.trim();
    method.is_empty()
        || method == "constructor"
        || method == "then"
        || method == "fetch"
        || method.starts_with("__dd_")
}

pub(super) fn least_loaded_isolate_idx(
    isolates: &[IsolateHandle],
    max_inflight: usize,
    require_wait_until_idle: bool,
) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| isolate.startup.is_ready())
        .filter(|(_, isolate)| isolate.inflight_count < max_inflight)
        .filter(|(_, isolate)| !require_wait_until_idle || isolate.pending_wait_until.is_empty())
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

pub(super) fn least_loaded_isolate_any_idx(isolates: &[IsolateHandle]) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| isolate.startup.is_ready())
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

impl WorkerPool {
    pub(super) fn build_execute_command(&self, command: BuildExecuteCommand) -> IsolateCommand {
        let BuildExecuteCommand {
            runtime_request_id,
            completion_token,
            request,
            request_body,
            stream_response,
            memory_call,
            host_rpc_call,
            memory_route,
            dispatched_at,
            profile_memory_atomic,
        } = command;
        IsolateCommand::Execute {
            runtime_request_id,
            completion_token,
            request_context: Box::new(self.request_context.clone()),
            request: Box::new(request),
            request_body,
            stream_response,
            memory_call: Box::new(memory_call),
            host_rpc_call,
            memory_route: Box::new(memory_route),
            dispatched_at,
            profile_memory_atomic,
        }
    }

    pub(super) fn has_dispatch_capacity(&self, max_inflight: usize) -> bool {
        self.isolates.iter().any(|isolate| {
            isolate.startup.is_ready()
                && isolate.inflight_count < max_inflight
                && (!self.strict_request_isolation || isolate.pending_wait_until.is_empty())
        })
    }

    pub(super) fn has_starting_isolate(&self) -> bool {
        self.isolates
            .iter()
            .any(|isolate| isolate.startup.is_starting())
    }

    pub(super) fn is_drained(&self) -> bool {
        self.queue.is_empty() && self.activity_snapshot().is_idle()
    }

    pub(super) fn update_queue_warning(&mut self, thresholds: &[usize]) {
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

    pub(super) fn stats_snapshot(&self) -> WorkerStats {
        let activity = self.activity_snapshot();
        WorkerStats {
            generation: self.generation,
            public: self.is_public,
            temporary: self.expires_at_ms.is_some(),
            expires_at_ms: self.expires_at_ms,
            queued: self.queue.len(),
            busy: activity.busy,
            inflight_total: activity.inflight_total,
            wait_until_total: activity.wait_until_total,
            isolates_total: self.isolates.len(),
            spawn_count: self.stats.spawn_count,
            reuse_count: self.stats.reuse_count,
            scale_down_count: self.stats.scale_down_count,
        }
    }

    fn activity_snapshot(&self) -> PoolActivity {
        let mut activity = PoolActivity::default();
        for isolate in &self.isolates {
            let wait_until = isolate.pending_wait_until.len();
            if isolate.inflight_count > 0
                || wait_until > 0
                || isolate.active_websocket_sessions > 0
                || isolate.active_transport_sessions > 0
            {
                activity.busy += 1;
            }
            activity.inflight_total += isolate.inflight_count;
            activity.wait_until_total += wait_until;
            activity.active_websocket_total += isolate.active_websocket_sessions;
            activity.active_transport_total += isolate.active_transport_sessions;
        }
        activity
    }
}

#[derive(Default)]
struct PoolActivity {
    busy: usize,
    inflight_total: usize,
    wait_until_total: usize,
    active_websocket_total: usize,
    active_transport_total: usize,
}

impl PoolActivity {
    fn is_idle(&self) -> bool {
        self.inflight_total == 0
            && self.wait_until_total == 0
            && self.active_websocket_total == 0
            && self.active_transport_total == 0
    }
}

pub(super) fn spawn_runtime_thread(start: RuntimeThreadStart) -> Result<()> {
    let RuntimeThreadStart {
        mut receiver,
        mut cancel_receiver,
        runtime_fast_sender,
        asset_catalog,
        bootstrap_snapshot,
        kv_store,
        memory_store,
        cache_store,
        config,
        storage,
    } = start;
    thread::Builder::new()
        .name("dd-runtime".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime thread should build");

            runtime.block_on(async move {
                let (event_tx, mut event_rx) = mpsc::channel(RUNTIME_EVENT_CHANNEL_CAPACITY);
                let mut manager = WorkerManager::new(WorkerManagerInit {
                    bootstrap_snapshot,
                    kv_store,
                    memory_store,
                    cache_store,
                    config: config.clone(),
                    storage,
                    runtime_fast_sender,
                    asset_catalog,
                });
                let mut ticker = tokio::time::interval(config.scale_tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                manager.drain_memory_outbox().await;

                loop {
                    tokio::select! {
                        Some(command) = receiver.recv() => {
                            manager.begin_runtime_batch();
                            let keep_running = manager.handle_command(command, &event_tx).await
                                && drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                                .await;
                            manager.finish_runtime_batch(&event_tx);
                            if !keep_running {
                                break;
                            }
                        }
                        Some(command) = cancel_receiver.recv() => {
                            manager.begin_runtime_batch();
                            let keep_running = manager.handle_command(command, &event_tx).await;
                            let keep_running = keep_running
                                && drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                                .await;
                            manager.finish_runtime_batch(&event_tx);
                            if !keep_running {
                                break;
                            }
                        }
                        Some(event) = event_rx.recv() => {
                            manager.begin_runtime_batch();
                            manager.handle_event(event, &event_tx).await;
                            let keep_running = drain_ready_runtime_work(
                                &mut manager,
                                &mut receiver,
                                &mut cancel_receiver,
                                &mut event_rx,
                                &event_tx,
                            )
                            .await;
                            manager.finish_runtime_batch(&event_tx);
                            if !keep_running {
                                break;
                            }
                        }
                        _ = ticker.tick() => {
                            manager.expire_temporary_workers().await;
                            manager.expire_queued_requests();
                            manager.expire_starting_isolates(&event_tx);
                            manager.expire_inflight_requests(&event_tx);
                            manager.drain_memory_outbox().await;
                            manager.pending_memory_outbox_shards.clear();
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

pub(super) async fn drain_ready_runtime_work(
    manager: &mut WorkerManager,
    receiver: &mut mpsc::Receiver<RuntimeCommand>,
    cancel_receiver: &mut mpsc::Receiver<RuntimeCommand>,
    event_rx: &mut RuntimeEventReceiver,
    event_tx: &RuntimeEventSender,
) -> bool {
    let mut processed = 0usize;
    loop {
        let mut made_progress = false;
        let mut keep_running = true;

        match receiver.try_recv() {
            Ok(command) => {
                keep_running = manager.handle_command(command, event_tx).await;
                made_progress = true;
                processed = processed.saturating_add(1);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
        if !keep_running {
            return false;
        }

        match cancel_receiver.try_recv() {
            Ok(command) => {
                keep_running = manager.handle_command(command, event_tx).await;
                made_progress = true;
                processed = processed.saturating_add(1);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
        if !keep_running {
            return false;
        }

        match event_rx.try_recv() {
            Ok(event) => {
                manager.handle_event(event, event_tx).await;
                made_progress = true;
                processed = processed.saturating_add(1);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }

        if processed >= RUNTIME_READY_WORK_BUDGET {
            return true;
        }

        if !made_progress {
            return true;
        }
    }
}

fn enqueue_pending_isolate_event(
    pending_events: &Rc<RefCell<VecDeque<RuntimeEvent>>>,
    event: RuntimeEvent,
) -> bool {
    let mut pending_events = pending_events.borrow_mut();
    if pending_events.len() >= ISOLATE_EVENT_QUEUE_CAPACITY {
        return false;
    }
    pending_events.push_back(event);
    true
}

async fn flush_pending_isolate_events(
    pending_events: &Rc<RefCell<VecDeque<RuntimeEvent>>>,
    event_tx: &RuntimeEventSender,
) -> bool {
    loop {
        let event = pending_events.borrow_mut().pop_front();
        let Some(event) = event else {
            return true;
        };
        if event_tx.send(event).await.is_err() {
            return false;
        }
    }
}

pub(super) fn runtime_event_from_isolate_payload(
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
    payload: IsolateEventPayload,
) -> RuntimeEvent {
    match payload {
        IsolateEventPayload::Completion {
            request_id,
            completion_token,
            wait_until_count,
            result,
        } => RuntimeEvent::RequestFinished {
            worker_name: worker_name.to_string(),
            generation,
            isolate_id,
            request_id,
            completion_token,
            finished_at: Instant::now(),
            wait_until_count,
            result,
        },
        IsolateEventPayload::WaitUntilDone {
            request_id,
            completion_token,
        } => RuntimeEvent::WaitUntilFinished {
            worker_name: worker_name.to_string(),
            generation,
            isolate_id,
            request_id,
            completion_token,
        },
        IsolateEventPayload::ResponseStart {
            request_id,
            completion_token,
            status,
            headers,
        } => RuntimeEvent::ResponseStart {
            worker_name: worker_name.to_string(),
            request_id,
            completion_token,
            status,
            headers,
        },
        IsolateEventPayload::ResponseChunk {
            request_id,
            completion_token,
            chunk,
            reply,
        } => RuntimeEvent::ResponseChunk {
            worker_name: worker_name.to_string(),
            request_id,
            completion_token,
            chunk,
            reply,
        },
        IsolateEventPayload::CacheRevalidate(payload) => RuntimeEvent::CacheRevalidate {
            worker_name: worker_name.to_string(),
            generation,
            payload,
        },
        IsolateEventPayload::MemoryInvoke(payload) => RuntimeEvent::MemoryInvoke(payload),
        IsolateEventPayload::MemorySocketSend(payload) => RuntimeEvent::MemorySocketSend(payload),
        IsolateEventPayload::MemorySocketClose(payload) => RuntimeEvent::MemorySocketClose(payload),
        IsolateEventPayload::MemorySocketConsumeClose(payload) => {
            RuntimeEvent::MemorySocketConsumeClose {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            }
        }
        IsolateEventPayload::MemoryTransportSendStream(payload) => {
            RuntimeEvent::MemoryTransportSendStream(payload)
        }
        IsolateEventPayload::MemoryTransportSendDatagram(payload) => {
            RuntimeEvent::MemoryTransportSendDatagram(payload)
        }
        IsolateEventPayload::MemoryTransportClose(payload) => {
            RuntimeEvent::MemoryTransportClose(payload)
        }
        IsolateEventPayload::MemoryTransportConsumeClose(payload) => {
            RuntimeEvent::MemoryTransportConsumeClose {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            }
        }
        IsolateEventPayload::DynamicWorkerCreate(payload) => {
            RuntimeEvent::DynamicWorkerCreate(payload)
        }
        IsolateEventPayload::DynamicWorkerLookup(payload) => {
            RuntimeEvent::DynamicWorkerLookup(payload)
        }
        IsolateEventPayload::DynamicWorkerList(payload) => RuntimeEvent::DynamicWorkerList(payload),
        IsolateEventPayload::DynamicWorkerDelete(payload) => {
            RuntimeEvent::DynamicWorkerDelete(payload)
        }
        IsolateEventPayload::DynamicHostRpcInvoke(payload) => {
            RuntimeEvent::DynamicHostRpcInvoke(payload)
        }
        IsolateEventPayload::TestAsyncReply(payload) => RuntimeEvent::TestAsyncReply(payload),
        IsolateEventPayload::TestNestedTargetedInvoke(payload) => {
            RuntimeEvent::TestNestedTargetedInvoke(payload)
        }
    }
}

pub(super) fn spawn_isolate_thread(start: IsolateThreadStart) -> Result<IsolateHandle> {
    let IsolateThreadStart {
        snapshot,
        snapshot_preloaded,
        source,
        deployment_config,
        allow_code_generation,
        kv_store,
        memory_store,
        cache_store,
        open_handle_registry,
        dynamic_profile,
        execution_limits,
        runtime_fast_sender,
        worker_name,
        generation,
        isolate_id,
        event_tx,
    } = start;
    let (command_tx, mut command_rx) = mpsc::channel(ISOLATE_COMMAND_CHANNEL_CAPACITY);
    let dynamic_control_inbox = crate::ops::DynamicControlInbox::default();
    let thread_dynamic_control_inbox = dynamic_control_inbox.clone();
    let v8_handle = Arc::new(StdMutex::new(None));
    let thread_v8_handle = Arc::clone(&v8_handle);
    let event_loop_notify = Arc::new(Notify::new());
    let event_loop_waker = Waker::from(Arc::new(IsolateEventLoopWaker {
        notify: Arc::clone(&event_loop_notify),
    }));
    let thread_name = format!("dd-isolate-{worker_name}-{generation}-{isolate_id}");
    let thread_event_tx = event_tx.clone();

    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = thread_event_tx.blocking_send(RuntimeEvent::IsolateFailed {
                        worker_name: worker_name.clone(),
                        generation,
                        isolate_id,
                        error: PlatformError::internal(error.to_string()),
                    });
                    return;
                }
            };

            runtime.block_on(async move {
                let pending_isolate_events = Rc::new(RefCell::new(VecDeque::<RuntimeEvent>::new()));
                let mut js_runtime = match new_runtime_from_snapshot_with_heap_limit(
                    snapshot,
                    allow_code_generation,
                    execution_limits.max_isolate_heap_bytes,
                ) {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                            worker_name: worker_name.clone(),
                            generation,
                            isolate_id,
                            error,
                        }).await;
                        return;
                    }
                };

                {
                    let handle = js_runtime.v8_isolate().thread_safe_handle();
                    *thread_v8_handle.lock().expect("v8 handle mutex poisoned") = Some(handle);
                }

                {
                    let event_sender = {
                        let pending_isolate_events = Rc::clone(&pending_isolate_events);
                        let worker_name = worker_name.clone();
                        IsolateEventSender(Rc::new(move |payload| {
                            let event = runtime_event_from_isolate_payload(
                                &worker_name,
                                generation,
                                isolate_id,
                                payload,
                            );
                            enqueue_pending_isolate_event(&pending_isolate_events, event)
                        }))
                    };
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    op_state.put(event_sender);
                    op_state.put(kv_store.clone());
                    op_state.put(memory_store.clone());
                    op_state.put(cache_store.clone());
                    op_state.put(open_handle_registry.clone());
                    op_state.put(crate::ops::HttpPreparedBodies::default());
                    op_state.put(crate::ops::HttpPreparedHeaders::default());
                    op_state.put(crate::ops::RequestInvocationHandles::default());
                    op_state.put(crate::ops::WorkerDeploymentHandles::default());
                    op_state.put(RequestBodyStreams::default());
                    op_state.put(crate::ops::MemoryCommandHandles::default());
                    op_state.put(crate::ops::MemoryByteHandles::default());
                    op_state.put(crate::ops::MemoryBatchHandles::default());
                    op_state.put(crate::ops::MemoryRequestScopes::default());
                    op_state.put(crate::ops::ActiveRequestContextHandles::default());
                    op_state.put(crate::ops::RequestSecretContexts::default());
                    op_state.put(execution_limits.clone());
                    op_state.put(crate::ops::DynamicPendingReplies::default());
                    op_state.put(crate::ops::TestAsyncReplies::default());
                    op_state.put(thread_dynamic_control_inbox.clone());
                    op_state.put(RuntimeFastCommandSender(runtime_fast_sender.clone()));
                    op_state.put(dynamic_profile.clone());
                }
                if !snapshot_preloaded {
                    if let Err(error) =
                        crate::engine::load_worker_source(&mut js_runtime, &source).await
                    {
                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                            worker_name: worker_name.clone(),
                            generation,
                            isolate_id,
                            error,
                        }).await;
                        return;
                    }
                }
                if let Err(error) = cache_runtime_entrypoints(&mut js_runtime) {
                    let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                        worker_name: worker_name.clone(),
                        generation,
                        isolate_id,
                        error,
                    }).await;
                    return;
                }
                if let Err(error) =
                    install_worker_deployment_config(&mut js_runtime, (*deployment_config).clone())
                {
                    let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                        worker_name: worker_name.clone(),
                        generation,
                        isolate_id,
                        error,
                    }).await;
                    return;
                }
                let _ = event_tx.send(RuntimeEvent::IsolateReady {
                    worker_name: worker_name.clone(),
                    generation,
                    isolate_id,
                }).await;

                loop {
                    let mut made_progress = false;
                    let mut drained_commands = 0usize;

                    while drained_commands < ISOLATE_COMMAND_DRAIN_BUDGET {
                        match command_rx.try_recv() {
                            Ok(command) => {
                                made_progress = true;
                                drained_commands = drained_commands.saturating_add(1);
                                if !handle_isolate_command_or_fail(
                                    &mut js_runtime,
                                    &event_tx,
                                    &worker_name,
                                    generation,
                                    isolate_id,
                                    command,
                                )
                                .await
                                {
                                    return;
                                }
                                if !flush_pending_isolate_events(
                                    &pending_isolate_events,
                                    &event_tx,
                                )
                                .await
                                {
                                    return;
                                }
                            }
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => return,
                        }
                    }

                    if let Err(error) = pump_event_loop_once(&mut js_runtime, &event_loop_waker) {
                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                            worker_name: worker_name.clone(),
                            generation,
                            isolate_id,
                            error,
                        }).await;
                        break;
                    }
                    if !flush_pending_isolate_events(&pending_isolate_events, &event_tx).await {
                        return;
                    }

                    if made_progress {
                        continue;
                    }

                    tokio::select! {
                        command = command_rx.recv() => {
                            let Some(command) = command else {
                                return;
                            };
                            if !handle_isolate_command_or_fail(
                                &mut js_runtime,
                                &event_tx,
                                &worker_name,
                                generation,
                                isolate_id,
                                command,
                            )
                            .await
                            {
                                return;
                            }
                            if !flush_pending_isolate_events(&pending_isolate_events, &event_tx).await {
                                return;
                            }
                        }
                        _ = event_loop_notify.notified() => {}
                    }
                }
            });
        })
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    Ok(IsolateHandle {
        id: isolate_id,
        sender: command_tx,
        v8_handle,
        dynamic_control_inbox,
        startup: IsolateStartup::Starting {
            started_at: Instant::now(),
        },
        inflight_count: 0,
        active_websocket_sessions: 0,
        active_transport_sessions: 0,
        served_requests: 0,
        last_used_at: Instant::now(),
        pending_replies: HashMap::new(),
        pending_wait_until: HashMap::new(),
    })
}

pub(super) async fn handle_isolate_command(
    js_runtime: &mut deno_core::JsRuntime,
    event_tx: &RuntimeEventSender,
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
    command: IsolateCommand,
) -> Result<bool> {
    match command {
        IsolateCommand::Execute {
            runtime_request_id,
            completion_token,
            request_context,
            request,
            request_body,
            stream_response,
            memory_call,
            host_rpc_call,
            memory_route,
            dispatched_at,
            profile_memory_atomic,
        } => {
            if profile_memory_atomic {
                let store = js_runtime
                    .op_state()
                    .borrow()
                    .borrow::<MemoryStore>()
                    .clone();
                store.record_profile(
                    MemoryProfileMetricKind::RuntimeAtomicDispatchWait,
                    duration_us(dispatched_at.elapsed()),
                    1,
                );
            }
            let request_context = *request_context;
            let request = *request;
            let memory_call = *memory_call;
            let memory_route = *memory_route;
            let mut request_body_stream_handle = 0;
            let request_context_handle;
            let completion_handle;
            let mut memory_request_scope_handle = 0;
            {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                if let Some(request_body) = request_body {
                    let max_request_body_bytes = op_state
                        .borrow::<crate::ops::RuntimeExecutionLimits>()
                        .max_request_body_bytes;
                    request_body_stream_handle = register_request_body_stream(
                        &mut op_state,
                        request_body,
                        max_request_body_bytes,
                    );
                }
                if let Some(route) = memory_route {
                    memory_request_scope_handle = register_memory_request_scope(
                        &mut op_state,
                        route.binding,
                        route.key,
                        route.owner_epoch,
                    );
                }
                request_context_handle =
                    register_request_secret_context(&mut op_state, isolate_id, request_context);
                completion_handle = crate::ops::register_active_request_context(
                    &mut op_state,
                    runtime_request_id.clone(),
                    completion_token.clone(),
                    request_context_handle,
                );
            }
            let execute_span = if tracing::enabled!(Level::INFO) {
                let span = tracing::info_span!(
                    "runtime.isolate.execute",
                    worker.name = %worker_name,
                    worker.generation = generation,
                    isolate.id = isolate_id,
                    runtime.request_id = %runtime_request_id,
                    request.id = %request.request_id
                );
                set_span_parent_from_traceparent(&span, traceparent_from_headers(&request.headers));
                Some(span)
            } else {
                None
            };
            let _execute_guard = execute_span.as_ref().map(|span| span.enter());
            let started_at = Instant::now();
            if let Err(error) = dispatch_worker_request(
                js_runtime,
                WorkerDispatchRequest {
                    request_id: &runtime_request_id,
                    request_context_handle,
                    completion_handle,
                    memory_request_scope_handle,
                    request_body_stream_handle,
                    stream_response,
                    memory_call: memory_call.as_ref(),
                    host_rpc_call: host_rpc_call.as_ref(),
                    request,
                },
            ) {
                {
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    clear_request_body_stream(&mut op_state, request_body_stream_handle);
                    crate::ops::clear_memory_request_scope(
                        &mut op_state,
                        memory_request_scope_handle,
                    );
                    crate::ops::clear_memory_command_handles(&mut op_state, request_context_handle);
                    crate::ops::clear_memory_byte_handles(&mut op_state, request_context_handle);
                    crate::ops::clear_memory_batch_handles(&mut op_state, request_context_handle);
                    clear_request_secret_context(&mut op_state, request_context_handle);
                }
                tracing::warn!(
                    dispatch_ms = started_at.elapsed().as_millis() as u64,
                    error = %error,
                    "failed to dispatch request into isolate"
                );
                let _ = event_tx
                    .send(RuntimeEvent::RequestFinished {
                        worker_name: worker_name.to_string(),
                        generation,
                        isolate_id,
                        request_id: runtime_request_id,
                        completion_token,
                        finished_at: Instant::now(),
                        wait_until_count: 0,
                        result: Err(error),
                    })
                    .await;
            } else {
                tracing::info!(
                    dispatch_ms = started_at.elapsed().as_millis() as u64,
                    "request dispatched into isolate event loop"
                );
            }
            Ok(true)
        }
        IsolateCommand::Abort { runtime_request_id } => {
            let request_context_handle = {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                let request_context_handle = crate::ops::active_request_context_handle_for_request(
                    &op_state,
                    &runtime_request_id,
                );
                if let Some(request_context_handle) = request_context_handle {
                    crate::ops::clear_memory_command_handles(&mut op_state, request_context_handle);
                    crate::ops::clear_memory_byte_handles(&mut op_state, request_context_handle);
                    crate::ops::clear_memory_batch_handles(&mut op_state, request_context_handle);
                }
                request_context_handle
            };
            if let Some(request_context_handle) = request_context_handle {
                abort_worker_request_handle(js_runtime, request_context_handle)?;
            }
            Ok(true)
        }
        IsolateCommand::DrainDynamicControl => {
            drain_dynamic_control_queue(js_runtime)?;
            {
                let op_state = js_runtime.op_state();
                op_state.borrow().waker.wake();
            }
            Ok(true)
        }
        IsolateCommand::Shutdown => Ok(false),
    }
}

async fn handle_isolate_command_or_fail(
    js_runtime: &mut deno_core::JsRuntime,
    event_tx: &RuntimeEventSender,
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
    command: IsolateCommand,
) -> bool {
    match handle_isolate_command(
        js_runtime,
        event_tx,
        worker_name,
        generation,
        isolate_id,
        command,
    )
    .await
    {
        Ok(continue_running) => continue_running,
        Err(error) => {
            let _ = event_tx
                .send(RuntimeEvent::IsolateFailed {
                    worker_name: worker_name.to_string(),
                    generation,
                    isolate_id,
                    error,
                })
                .await;
            false
        }
    }
}
