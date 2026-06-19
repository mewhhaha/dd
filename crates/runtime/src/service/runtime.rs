use super::*;

pub(super) fn select_dispatch_candidate(
    pool: &WorkerPool,
    max_inflight: usize,
    require_wait_until_idle: bool,
) -> Option<DispatchSelection> {
    for (queue_idx, pending) in pool.queue.iter().enumerate() {
        let Some(target_isolate_id) = pending.target_isolate_id else {
            continue;
        };
        let targeted_nested_call =
            pending.host_rpc_call.is_some() || pending.memory_route.is_some();
        if !targeted_nested_call {
            continue;
        }
        if let Some(isolate_idx) = target_isolate_idx(pool, target_isolate_id) {
            return Some(DispatchSelection::Dispatch(DispatchCandidate {
                queue_idx,
                isolate_idx,
            }));
        } else {
            return Some(DispatchSelection::DropStaleTarget { queue_idx });
        }
    }

    for (queue_idx, pending) in pool.queue.iter().enumerate() {
        if let Some(target_isolate_id) = pending.target_isolate_id {
            if let Some(isolate_idx) = target_isolate_idx(pool, target_isolate_id) {
                let isolate = &pool.isolates[isolate_idx];
                debug_assert!(pending.host_rpc_call.is_none());
                debug_assert!(pending.memory_route.is_none());
                if isolate.inflight_count < max_inflight
                    && (!require_wait_until_idle || isolate.pending_wait_until.is_empty())
                {
                    return Some(DispatchSelection::Dispatch(DispatchCandidate {
                        queue_idx,
                        isolate_idx,
                    }));
                }
            } else {
                return Some(DispatchSelection::DropStaleTarget { queue_idx });
            }
            continue;
        }

        if pending.memory_route.is_none() {
            return least_loaded_isolate_idx(&pool.isolates, max_inflight, require_wait_until_idle)
                .map(|isolate_idx| {
                    DispatchSelection::Dispatch(DispatchCandidate {
                        queue_idx,
                        isolate_idx,
                    })
                });
        }

        if let Some(isolate_idx) =
            least_loaded_isolate_any_idx(&pool.isolates, require_wait_until_idle)
        {
            return Some(DispatchSelection::Dispatch(DispatchCandidate {
                queue_idx,
                isolate_idx,
            }));
        }
    }
    None
}

fn target_isolate_idx(pool: &WorkerPool, target_isolate_id: u64) -> Option<usize> {
    pool.isolates
        .iter()
        .position(|isolate| isolate.id == target_isolate_id)
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
        .filter(|(_, isolate)| isolate.inflight_count < max_inflight)
        .filter(|(_, isolate)| !require_wait_until_idle || isolate.pending_wait_until.is_empty())
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

pub(super) fn least_loaded_isolate_any_idx(
    isolates: &[IsolateHandle],
    require_wait_until_idle: bool,
) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| !require_wait_until_idle || isolate.pending_wait_until.is_empty())
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

impl WorkerPool {
    pub(super) fn build_execute_command(
        &self,
        runtime_request_id: String,
        completion_token: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        stream_response: bool,
        memory_call: Option<MemoryExecutionCall>,
        host_rpc_call: Option<HostRpcExecutionCall>,
        memory_route: Option<MemoryRoute>,
    ) -> IsolateCommand {
        IsolateCommand::Execute {
            runtime_request_id,
            completion_token,
            worker_name_json: Arc::clone(&self.worker_name_json),
            kv_bindings_json: Arc::clone(&self.kv_bindings_json),
            kv_read_cache_config_json: Arc::clone(&self.kv_read_cache_config_json),
            memory_bindings_json: Arc::clone(&self.memory_bindings_json),
            dynamic_bindings_json: Arc::clone(&self.dynamic_bindings_json),
            dynamic_rpc_bindings_json: Arc::clone(&self.dynamic_rpc_bindings_json),
            dynamic_env_json: Arc::clone(&self.dynamic_env_json),
            request_context: self.request_context.clone(),
            request,
            request_body,
            stream_response,
            memory_call,
            host_rpc_call,
            memory_route,
        }
    }

    pub(super) fn has_dispatch_capacity(&self, max_inflight: usize) -> bool {
        self.isolates.iter().any(|isolate| {
            isolate.inflight_count < max_inflight
                && (!self.strict_request_isolation || isolate.pending_wait_until.is_empty())
        })
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

pub(super) fn spawn_runtime_thread(
    mut receiver: mpsc::Receiver<RuntimeCommand>,
    mut cancel_receiver: mpsc::UnboundedReceiver<RuntimeCommand>,
    runtime_fast_sender: mpsc::UnboundedSender<RuntimeCommand>,
    bootstrap_snapshot: &'static [u8],
    kv_store: KvStore,
    memory_store: MemoryStore,
    cache_store: CacheStore,
    config: RuntimeConfig,
    storage: RuntimeStorageConfig,
) -> Result<()> {
    thread::Builder::new()
        .name("dd-runtime".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime thread should build");

            runtime.block_on(async move {
                let (event_tx, mut event_rx) = mpsc::unbounded_channel();
                let mut manager = WorkerManager::new(
                    bootstrap_snapshot,
                    kv_store,
                    memory_store,
                    cache_store,
                    config.clone(),
                    storage,
                    runtime_fast_sender,
                );
                let mut ticker = tokio::time::interval(config.scale_tick);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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
                            manager.expire_queued_requests();
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
    cancel_receiver: &mut mpsc::UnboundedReceiver<RuntimeCommand>,
    event_rx: &mut mpsc::UnboundedReceiver<RuntimeEvent>,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
) -> bool {
    loop {
        let mut made_progress = false;
        let mut keep_running = true;

        match receiver.try_recv() {
            Ok(command) => {
                keep_running = manager.handle_command(command, event_tx).await;
                made_progress = true;
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
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }

        if !made_progress {
            return true;
        }
    }
}

pub(super) fn handle_isolate_event_payload(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
    payload: IsolateEventPayload,
) {
    match payload {
        IsolateEventPayload::Completion(payload) => match decode_completion_payload(payload) {
            Ok((request_id, completion_token, wait_until_count, result)) => {
                let _ = event_tx.send(RuntimeEvent::RequestFinished {
                    worker_name: worker_name.to_string(),
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
        },
        IsolateEventPayload::WaitUntilDone(payload) => match decode_wait_until_payload(payload) {
            Ok((request_id, completion_token)) => {
                let _ = event_tx.send(RuntimeEvent::WaitUntilFinished {
                    worker_name: worker_name.to_string(),
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
        },
        IsolateEventPayload::ResponseStart(payload) => match decode_response_start_payload(payload)
        {
            Ok((request_id, completion_token, status, headers)) => {
                let _ = event_tx.send(RuntimeEvent::ResponseStart {
                    worker_name: worker_name.to_string(),
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
        },
        IsolateEventPayload::ResponseChunk(payload) => match decode_response_chunk_payload(payload)
        {
            Ok((request_id, completion_token, chunk)) => {
                let _ = event_tx.send(RuntimeEvent::ResponseChunk {
                    worker_name: worker_name.to_string(),
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
        },
        IsolateEventPayload::CacheRevalidate(payload) => {
            let _ = event_tx.send(RuntimeEvent::CacheRevalidate {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
        }
        IsolateEventPayload::MemoryInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemoryInvoke(payload));
        }
        IsolateEventPayload::MemorySocketSend(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemorySocketSend(payload));
        }
        IsolateEventPayload::MemorySocketClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemorySocketClose(payload));
        }
        IsolateEventPayload::MemorySocketConsumeClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemorySocketConsumeClose {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
        }
        IsolateEventPayload::MemoryTransportSendStream(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemoryTransportSendStream(payload));
        }
        IsolateEventPayload::MemoryTransportSendDatagram(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemoryTransportSendDatagram(payload));
        }
        IsolateEventPayload::MemoryTransportRecvStream(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemoryTransportRecvStream(payload));
        }
        IsolateEventPayload::MemoryTransportRecvDatagram(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemoryTransportRecvDatagram(payload));
        }
        IsolateEventPayload::MemoryTransportClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemoryTransportClose(payload));
        }
        IsolateEventPayload::MemoryTransportConsumeClose(payload) => {
            let _ = event_tx.send(RuntimeEvent::MemoryTransportConsumeClose {
                worker_name: worker_name.to_string(),
                generation,
                payload,
            });
        }
        IsolateEventPayload::DynamicWorkerCreate(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerCreate(payload));
        }
        IsolateEventPayload::DynamicWorkerLookup(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerLookup(payload));
        }
        IsolateEventPayload::DynamicWorkerList(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerList(payload));
        }
        IsolateEventPayload::DynamicWorkerDelete(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerDelete(payload));
        }
        IsolateEventPayload::DynamicWorkerInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicWorkerInvoke(payload));
        }
        IsolateEventPayload::DynamicHostRpcInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::DynamicHostRpcInvoke(payload));
        }
        IsolateEventPayload::TestAsyncReply(payload) => {
            let _ = event_tx.send(RuntimeEvent::TestAsyncReply(payload));
        }
        IsolateEventPayload::TestNestedTargetedInvoke(payload) => {
            let _ = event_tx.send(RuntimeEvent::TestNestedTargetedInvoke(payload));
        }
    }
}

pub(super) fn spawn_isolate_thread(
    snapshot: &'static [u8],
    snapshot_preloaded: bool,
    source: Arc<str>,
    kv_store: KvStore,
    memory_store: MemoryStore,
    cache_store: CacheStore,
    open_handle_registry: crate::ops::MemoryOpenHandleRegistry,
    dynamic_profile: crate::ops::DynamicProfile,
    runtime_fast_sender: mpsc::UnboundedSender<RuntimeCommand>,
    worker_name: String,
    generation: u64,
    isolate_id: u64,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
) -> Result<IsolateHandle> {
    let (command_tx, command_rx) = std_mpsc::channel();
    let (init_tx, init_rx) = std_mpsc::channel::<Result<()>>();
    let dynamic_control_inbox = crate::ops::DynamicControlInbox::default();
    let thread_dynamic_control_inbox = dynamic_control_inbox.clone();
    let event_loop_waker = Waker::from(Arc::new(IsolateEventLoopWaker {
        sender: command_tx.clone(),
    }));
    let thread_name = format!("dd-isolate-{worker_name}-{generation}-{isolate_id}");

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

                let (event_payload_tx, event_payload_rx) =
                    std_mpsc::channel::<IsolateEventPayload>();
                {
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    op_state.put(IsolateEventSender(event_payload_tx));
                    op_state.put(kv_store.clone());
                    op_state.put(memory_store.clone());
                    op_state.put(cache_store.clone());
                    op_state.put(open_handle_registry.clone());
                    op_state.put(RequestBodyStreams::default());
                    op_state.put(crate::ops::MemoryRequestScopes::default());
                    op_state.put(crate::ops::RequestSecretContexts::default());
                    op_state.put(crate::ops::DynamicPendingReplies::default());
                    op_state.put(crate::ops::TestAsyncReplies::default());
                    op_state.put(thread_dynamic_control_inbox.clone());
                    op_state.put(RuntimeFastCommandSender(runtime_fast_sender.clone()));
                    op_state.put(dynamic_profile.clone());
                }
                {
                    let event_tx = event_tx.clone();
                    let worker_name = worker_name.clone();
                    thread::Builder::new()
                        .name(format!("dd-isolate-events-{isolate_id}"))
                        .spawn(move || {
                            while let Ok(payload) = event_payload_rx.recv() {
                                handle_isolate_event_payload(
                                    &event_tx,
                                    &worker_name,
                                    generation,
                                    isolate_id,
                                    payload,
                                );
                            }
                        })
                        .expect("isolate event forwarder should spawn");
                }
                if !snapshot_preloaded {
                    if let Err(error) = load_worker(&mut js_runtime, &source).await {
                        let _ = init_tx.send(Err(error));
                        return;
                    }
                }
                let _ = init_tx.send(Ok(()));

                loop {
                    let mut made_progress = false;

                    loop {
                        match command_rx.try_recv() {
                            Ok(command) => {
                                made_progress = true;
                                match handle_isolate_command(
                                    &mut js_runtime,
                                    &event_tx,
                                    &worker_name,
                                    generation,
                                    isolate_id,
                                    command,
                                ) {
                                    Ok(true) => {}
                                    Ok(false) => return,
                                    Err(error) => {
                                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                                            worker_name: worker_name.clone(),
                                            generation,
                                            isolate_id,
                                            error,
                                        });
                                        return;
                                    }
                                }
                            }
                            Err(std_mpsc::TryRecvError::Empty) => break,
                            Err(std_mpsc::TryRecvError::Disconnected) => return,
                        }
                    }

                    if let Err(error) = pump_event_loop_once(&mut js_runtime, &event_loop_waker) {
                        let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                            worker_name: worker_name.clone(),
                            generation,
                            isolate_id,
                            error,
                        });
                        break;
                    }

                    if made_progress {
                        continue;
                    }

                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            });
        })
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    match init_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => Ok(IsolateHandle {
            id: isolate_id,
            sender: command_tx,
            dynamic_control_inbox,
            inflight_count: 0,
            active_websocket_sessions: 0,
            active_transport_sessions: 0,
            served_requests: 0,
            last_used_at: Instant::now(),
            pending_replies: HashMap::new(),
            pending_wait_until: HashMap::new(),
        }),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(PlatformError::internal("isolate startup timed out")),
    }
}

pub(super) fn handle_isolate_command(
    js_runtime: &mut deno_core::JsRuntime,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    worker_name: &str,
    generation: u64,
    isolate_id: u64,
    command: IsolateCommand,
) -> Result<bool> {
    match command {
        IsolateCommand::Execute {
            runtime_request_id,
            completion_token,
            worker_name_json,
            kv_bindings_json,
            kv_read_cache_config_json,
            memory_bindings_json,
            dynamic_bindings_json,
            dynamic_rpc_bindings_json,
            dynamic_env_json,
            request_context,
            request,
            request_body,
            stream_response,
            memory_call,
            host_rpc_call,
            memory_route,
        } => {
            let has_request_body_stream = request_body.is_some();
            {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                if let Some(request_body) = request_body {
                    register_request_body_stream(
                        &mut op_state,
                        runtime_request_id.clone(),
                        request_body,
                    );
                }
                if let Some(route) = memory_route {
                    register_memory_request_scope(
                        &mut op_state,
                        runtime_request_id.clone(),
                        route.binding,
                        route.key,
                    );
                }
                register_request_secret_context(
                    &mut op_state,
                    runtime_request_id.clone(),
                    isolate_id,
                    request_context,
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
                &runtime_request_id,
                &completion_token,
                &worker_name_json,
                &kv_bindings_json,
                &kv_read_cache_config_json,
                &memory_bindings_json,
                &dynamic_bindings_json,
                &dynamic_rpc_bindings_json,
                &dynamic_env_json,
                has_request_body_stream,
                stream_response,
                memory_call.as_ref(),
                host_rpc_call.as_ref(),
                request,
            ) {
                {
                    let op_state = js_runtime.op_state();
                    let mut op_state = op_state.borrow_mut();
                    clear_request_body_stream(&mut op_state, &runtime_request_id);
                    crate::ops::clear_memory_request_scope(&mut op_state, &runtime_request_id);
                    clear_request_secret_context(&mut op_state, &runtime_request_id);
                }
                tracing::warn!(
                    dispatch_ms = started_at.elapsed().as_millis() as u64,
                    error = %error,
                    "failed to dispatch request into isolate"
                );
                let _ = event_tx.send(RuntimeEvent::RequestFinished {
                    worker_name: worker_name.to_string(),
                    generation,
                    isolate_id,
                    request_id: runtime_request_id,
                    completion_token,
                    wait_until_count: 0,
                    result: Err(error),
                });
            } else {
                tracing::info!(
                    dispatch_ms = started_at.elapsed().as_millis() as u64,
                    "request dispatched into isolate event loop"
                );
            }
            Ok(true)
        }
        IsolateCommand::Abort { runtime_request_id } => {
            {
                let op_state = js_runtime.op_state();
                let mut op_state = op_state.borrow_mut();
                cancel_request_body_stream(&mut op_state, &runtime_request_id);
                clear_request_body_stream(&mut op_state, &runtime_request_id);
                crate::ops::clear_memory_request_scope(&mut op_state, &runtime_request_id);
                clear_request_secret_context(&mut op_state, &runtime_request_id);
            }
            abort_worker_request(js_runtime, &runtime_request_id)?;
            Ok(true)
        }
        IsolateCommand::DrainDynamicControl => {
            js_runtime
                .execute_script(
                    "<dd:dynamic-control-drain>",
                    "(() => {
                        const drain = globalThis.__dd_drain_dynamic_control_queue;
                        if (typeof drain !== \"function\") {
                          throw new Error(\"dynamic control drain helper missing\");
                        }
                        void Promise.resolve(drain()).catch(() => undefined);
                      })()",
                )
                .map_err(|error| PlatformError::internal(error.to_string()))?;
            {
                let op_state = js_runtime.op_state();
                op_state.borrow().waker.wake();
            }
            Ok(true)
        }
        IsolateCommand::PollEventLoop => Ok(true),
        IsolateCommand::Shutdown => Ok(false),
    }
}
