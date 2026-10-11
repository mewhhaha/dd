//! Runtime and isolate threads, events, and dispatch helpers.

use super::*;

mod pool;
mod selection;

pub(super) use selection::select_dispatch_candidate;
use std::{cell::RefCell, rc::Rc};

const RUNTIME_READY_WORK_BUDGET: usize = 256;
const ISOLATE_COMMAND_DRAIN_BUDGET: usize = 256;
const ISOLATE_COMMAND_CHANNEL_CAPACITY: usize = 1024;
const ISOLATE_EVENT_QUEUE_CAPACITY: usize = 8192;

#[derive(Clone, Copy)]
enum IsolateEligibility {
    RegularOnly,
    IncludeInternalRescue,
}

impl IsolateEligibility {
    fn for_pending(pending: &PendingInvoke) -> Self {
        if pending.internal_origin {
            Self::IncludeInternalRescue
        } else {
            Self::RegularOnly
        }
    }

    fn accepts(self, isolate: &IsolateHandle) -> bool {
        matches!(self, Self::IncludeInternalRescue) || !isolate.internal_rescue
    }
}

enum DispatchReadiness {
    Ready(DispatchSelection),
    OwnerBlocked(PendingQueueKey),
}

fn memory_atomic_route_is_active(
    memory_entity_leases: &HashMap<String, MemoryEntityLease>,
    pending: &PendingInvoke,
) -> bool {
    let Some(memory_route) = pending.memory_route.as_ref() else {
        return false;
    };
    if !memory_route_needs_ordering(pending) {
        return false;
    }
    memory_entity_leases.contains_key(&memory_route.owner_key)
}

fn memory_route_needs_ordering(pending: &PendingInvoke) -> bool {
    pending.memory_call.is_some()
}

enum MemoryAffinityOutcome {
    Hit(usize),
    MissNoMapping,
    MissStale,
    MissSaturated,
}

fn memory_shard_affinity_outcome(
    isolates: &[IsolateHandle],
    isolate_indices: &HashMap<u64, usize>,
    memory_shard_affinity: &HashMap<usize, u64>,
    pending: &PendingInvoke,
    max_inflight: usize,
    require_wait_until_idle: bool,
    eligibility: IsolateEligibility,
) -> MemoryAffinityOutcome {
    let Some(shard_index) = pending
        .memory_route
        .as_ref()
        .and_then(|route| route.shard_index)
    else {
        return MemoryAffinityOutcome::MissNoMapping;
    };
    let Some(isolate_id) = memory_shard_affinity.get(&shard_index).copied() else {
        return MemoryAffinityOutcome::MissNoMapping;
    };
    let Some(isolate_idx) = target_isolate_idx(isolate_indices, isolate_id) else {
        return MemoryAffinityOutcome::MissStale;
    };
    let isolate = &isolates[isolate_idx];
    if isolate.startup.is_ready()
        && eligibility.accepts(isolate)
        && isolate.inflight_count < max_inflight
        && (!require_wait_until_idle || isolate.pending_wait_until.is_empty())
    {
        MemoryAffinityOutcome::Hit(isolate_idx)
    } else {
        MemoryAffinityOutcome::MissSaturated
    }
}

fn target_isolate_idx(
    isolate_indices: &HashMap<u64, usize>,
    target_isolate_id: u64,
) -> Option<usize> {
    isolate_indices.get(&target_isolate_id).copied()
}

fn least_loaded_isolate_idx(
    isolates: &[IsolateHandle],
    max_inflight: usize,
    require_wait_until_idle: bool,
    eligibility: IsolateEligibility,
) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| isolate.startup.is_ready())
        .filter(|(_, isolate)| eligibility.accepts(isolate))
        .filter(|(_, isolate)| isolate.inflight_count < max_inflight)
        .filter(|(_, isolate)| !require_wait_until_idle || isolate.pending_wait_until.is_empty())
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

fn least_loaded_isolate_any_idx(
    isolates: &[IsolateHandle],
    eligibility: IsolateEligibility,
) -> Option<usize> {
    isolates
        .iter()
        .enumerate()
        .filter(|(_, isolate)| isolate.startup.is_ready())
        .filter(|(_, isolate)| eligibility.accepts(isolate))
        .min_by_key(|(_, isolate)| isolate.inflight_count)
        .map(|(idx, _)| idx)
}

#[derive(Default)]
struct PoolActivity {
    busy: usize,
    inflight_total: usize,
    wait_until_total: usize,
    active_websocket_total: usize,
}

impl PoolActivity {
    fn is_idle(&self) -> bool {
        self.inflight_total == 0 && self.wait_until_total == 0 && self.active_websocket_total == 0
    }
}

pub(super) fn spawn_runtime_thread(start: RuntimeThreadStart) -> Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("dd-runtime".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime thread should build");
            runtime.block_on(run_runtime_coordinator(start));
        })
        .map_err(|error| PlatformError::internal(error.to_string()))
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
            manager.stats.ready_work_budget_exhausted_count = manager
                .stats
                .ready_work_budget_exhausted_count
                .saturating_add(1);
            manager.stats.max_ready_work_batch_size =
                manager.stats.max_ready_work_batch_size.max(processed);
            return true;
        }

        if !made_progress {
            manager.stats.max_ready_work_batch_size =
                manager.stats.max_ready_work_batch_size.max(processed);
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

fn runtime_event_from_isolate_payload(
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
        IsolateEventPayload::MemorySocketClose(payload) => RuntimeEvent::MemorySocketClose(payload),
        #[cfg(test)]
        IsolateEventPayload::TestAsyncReply(payload) => RuntimeEvent::TestAsyncReply(payload),
    }
}

pub(super) fn spawn_isolate_thread(start: IsolateThreadStart) -> Result<IsolateHandle> {
    let IsolateThreadStart {
        admission,
        snapshot,
        snapshot_preloaded,
        source,
        module_registry,
        deployment_config,
        policy,
        console,
        inspector,
        kv_store,
        memory_store,
        cache_store,
        open_handle_registry,
        execution_limits,
        runtime_fast_sender,
        worker_name,
        generation,
        isolate_id,
        event_tx,
        thread_tracker,
    } = start;
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(());
    let slot_starting = Arc::clone(&admission.starting);
    let (command_tx, mut command_rx) = mpsc::channel(ISOLATE_COMMAND_CHANNEL_CAPACITY);
    let request_control_inbox = crate::ops::RequestControlInbox::default();
    let thread_request_control_inbox = request_control_inbox.clone();
    let v8_handle = Arc::new(StdMutex::new(None));
    let thread_v8_handle = Arc::clone(&v8_handle);
    let event_loop_notify = Arc::new(Notify::new());
    let event_loop_waker = Waker::from(Arc::new(IsolateEventLoopWaker {
        notify: Arc::clone(&event_loop_notify),
    }));
    let thread_name = format!("dd-isolate-{worker_name}-{generation}-{isolate_id}");
    let thread_event_tx = event_tx.clone();
    let exit_event_tx = event_tx.clone();
    let exit_worker_name = worker_name.clone();
    let thread_guard = thread_tracker.register();

    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    drop(thread_guard);
                    let _ = thread_event_tx.blocking_send(RuntimeEvent::IsolateFailed {
                        worker_name: worker_name.clone(),
                        generation,
                        isolate_id,
                        error: PlatformError::internal(error.to_string()),
                    });
                    let _ = exit_event_tx.blocking_send(RuntimeEvent::IsolateExited {
                        worker_name: exit_worker_name.clone(),
                        generation,
                        isolate_id,
                    });
                    return;
                }
            };

            runtime.block_on(async move {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {},
                    _ = async move {
                        let pending_isolate_events = Rc::new(RefCell::new(VecDeque::<RuntimeEvent>::new()));
                        let mut js_runtime = match new_runtime_from_snapshot_with_heap_limit(
                            snapshot,
                            policy,
                            execution_limits.max_isolate_heap_bytes,
                            module_registry,
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

                        // The inspector comes before any worker code runs,
                        // so breakpoints in top-level code hold.
                        let _inspector_target = match &inspector {
                            Some(registry) => match js_runtime
                                .enable_inspector(&format!("{worker_name} (isolate {isolate_id})"))
                            {
                                Ok(handle) => Some(registry.register(&worker_name, generation, isolate_id, handle)),
                                Err(error) => {
                                    let _ = event_tx.send(RuntimeEvent::IsolateFailed {
                                        worker_name: worker_name.clone(),
                                        generation,
                                        isolate_id,
                                        error: PlatformError::runtime(error.to_string()),
                                    }).await;
                                    return;
                                }
                            },
                            None => None,
                        };
                        *thread_v8_handle.lock().expect("v8 handle mutex poisoned") = Some(js_runtime.handle());

                        {
                            let event_sender = {
                                let pending_isolate_events = Rc::clone(&pending_isolate_events);
                                let worker_name = worker_name.clone();
                                let event_loop_notify = Arc::clone(&event_loop_notify);
                                IsolateEventSender(Rc::new(move |payload| {
                                    let event = runtime_event_from_isolate_payload(
                                        &worker_name,
                                        generation,
                                        isolate_id,
                                        payload,
                                    );
                                    let accepted = enqueue_pending_isolate_event(&pending_isolate_events, event);
                                    if accepted {
                                        // Async ops can emit after the main loop has finished polling V8.
                                        event_loop_notify.notify_one();
                                    }
                                    accepted
                                }))
                            };
                            let op_state = js_runtime.op_state();
                            let mut op_state = op_state.borrow_mut();
                            op_state.put(event_sender);
                            op_state.put(kv_store.clone());
                            op_state.put(memory_store.clone());
                            op_state.put(cache_store.clone());
                            op_state.put(crate::ops::WorkerCacheNamespace(worker_name.clone()));
                            op_state.put(console.clone());
                            op_state.put(open_handle_registry.clone());
                            op_state.put(crate::ops::HttpPreparedBodies::default());
                            op_state.put(crate::ops::HttpPreparedHeaders::default());
                            op_state.put(crate::ops::RequestInvocationHandles::default());
                            op_state.put(crate::ops::WorkerDeploymentHandles::default());
                            op_state.put(RequestBodyStreams::default());
                            op_state.put(crate::ops::MemoryCommandHandles::default());
                            op_state.put(crate::ops::MemoryByteHandles::default());
                            op_state.put(crate::ops::MemoryBatchHandles::default());
                            op_state.put(crate::ops::MemoryReadHandles::default());
                            op_state.put(crate::ops::MemoryRequestScopes::default());
                            op_state.put(crate::ops::ActiveRequestContextHandles::default());
                            op_state.put(crate::ops::RequestSecretContexts::default());
                            op_state.put(execution_limits.clone());
                            op_state.put(crate::ops::PendingReplies::default());
                            #[cfg(test)]
                            op_state.put(crate::ops::TestAsyncReplies::default());
                            op_state.put(thread_request_control_inbox.clone());
                            op_state.put(RuntimeFastCommandSender(runtime_fast_sender.clone()));
                        }
                        if inspector.as_ref().is_some_and(InspectorRegistry::wait_for_debugger) {
                            js_runtime.wait_for_debugger();
                        }
                        if !snapshot_preloaded
                            && let Err(error) =
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
                            if !thread_request_control_inbox.is_empty() {
                                if !handle_isolate_command_or_fail(
                                    &mut js_runtime, &event_tx, &worker_name, generation,
                                    isolate_id, IsolateCommand::DrainRequestControl,
                                ).await { return; }
                                made_progress = true;
                            }


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
                                _ = thread_request_control_inbox.ready.notified() => {}
                            }
                        }
                    } => {},
                }
            });
            drop(thread_guard);
            drop(admission);
            let _ = exit_event_tx.blocking_send(RuntimeEvent::IsolateExited {
                worker_name: exit_worker_name,
                generation,
                isolate_id,
            });
        })
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    Ok(IsolateHandle {
        shutdown,
        slot_starting,
        id: isolate_id,
        sender: command_tx,
        v8_handle,
        request_control_inbox,
        startup: IsolateStartup::Starting {
            started_at: Instant::now(),
        },
        internal_rescue: false,
        inflight_count: 0,
        active_websocket_sessions: 0,
        served_requests: 0,
        last_used_at: Instant::now(),
        pending_replies: HashMap::new(),
        pending_wait_until: HashMap::new(),
    })
}

async fn handle_isolate_command(
    js_runtime: &mut dd_v8::JsRuntime,
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
                    MemoryProfileMetricKind::RuntimeSocketDispatchWait,
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
            let execute_span = if tracing::enabled!(Level::DEBUG) {
                let span = tracing::debug_span!(
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
            async move {
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
                        crate::ops::clear_memory_handles(&mut op_state, request_context_handle);
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
                    tracing::debug!(
                        dispatch_ms = started_at.elapsed().as_millis() as u64,
                        "request dispatched into isolate event loop"
                    );
                }
                Ok(true)
            }
            .instrument(execute_span.unwrap_or_else(tracing::Span::none))
            .await
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
                    crate::ops::clear_memory_handles(&mut op_state, request_context_handle);
                }
                request_context_handle
            };
            if let Some(request_context_handle) = request_context_handle {
                abort_worker_request_handle(js_runtime, request_context_handle)?;
            }
            Ok(true)
        }
        IsolateCommand::DrainRequestControl => {
            drain_request_control_queue(js_runtime)?;
            {
                let op_state = js_runtime.op_state();
                op_state.borrow().waker.wake();
            }
            Ok(true)
        }
    }
}

async fn handle_isolate_command_or_fail(
    js_runtime: &mut dd_v8::JsRuntime,
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
