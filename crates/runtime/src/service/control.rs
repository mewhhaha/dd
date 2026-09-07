use super::*;
use tracing::warn;
#[derive(Clone)]
pub(crate) struct RuntimeFastCommandSender(pub RuntimeCommandSender);

pub(crate) enum RuntimeCommand {
    Admitted {
        command: Box<RuntimeCommand>,
        admission: QueueAdmission,
    },
    InvokeInternal(EnqueueInvokeRequest),
    CheckDeployment {
        worker_name: String,
        temporary: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    Deploy {
        prepared: PreparedWorkerDeployment,
        deployment_id: String,
        expires_at_ms: Option<i64>,
        reply: oneshot::Sender<Result<String>>,
    },
    Undeploy {
        worker_name: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Invoke {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    InvokeStream {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    ServiceBindingFetchStart {
        reply_inbox: crate::ops::RequestControlInbox,
        owner_worker: String,
        owner_generation: u64,
        binding: String,
        target_worker: String,
        request: WorkerInvocation,
        reply_id: String,
        pending_replies: crate::ops::PendingReplies,
    },
    Cancel {
        worker_name: String,
        runtime_request_id: String,
    },
    Stats {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerStats>>,
    },
    DebugDump {
        worker_name: String,
        reply: oneshot::Sender<Option<WorkerDebugDump>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<()>>,
    },
    #[cfg(test)]
    ForceFailIsolate {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        reply: oneshot::Sender<bool>,
    },
    OpenWebsocket {
        worker_name: String,
        runtime_request_id: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
        session_id: String,
        reply: oneshot::Sender<Result<WebSocketOpen>>,
    },
    SendWebsocketFrame {
        worker_name: String,
        session_id: String,
        frame: Vec<u8>,
        is_binary: bool,
        reply: oneshot::Sender<Result<WorkerOutput>>,
    },
    WaitWebsocketFrame {
        worker_name: String,
        session_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    DrainWebsocketFrame {
        worker_name: String,
        session_id: String,
        reply: oneshot::Sender<Result<Option<WorkerOutput>>>,
    },
    CloseWebsocket {
        worker_name: String,
        session_id: String,
        close_code: u16,
        close_reason: String,
        reply: oneshot::Sender<Result<()>>,
    },
}
pub(super) enum RuntimeEvent {
    RequestFinished {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        request_id: String,
        completion_token: String,
        finished_at: Instant,
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
        chunk: Bytes,
        reply: oneshot::Sender<Result<()>>,
    },
    CacheRevalidate {
        worker_name: String,
        generation: u64,
        payload: CacheRevalidatePayload,
    },
    MemorySocketSend(crate::ops::MemorySocketSendEvent),
    MemorySocketClose(crate::ops::MemorySocketCloseEvent),
    TestAsyncReply(crate::ops::TestAsyncReplyEvent),
    TestAsyncReplyComplete {
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        result: Result<String>,
    },
    IsolateReady {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
    },
    IsolateFailed {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
        error: PlatformError,
    },
    IsolateExited {
        worker_name: String,
        generation: u64,
        isolate_id: u64,
    },
    MemoryOutboxDelivery {
        shard_index: usize,
        claims: Vec<MemoryOutboxClaim>,
        saturated: bool,
        reply: oneshot::Sender<Vec<MemoryOutboxDeliveryOutcome>>,
    },
    MemoryOutboxAckFailed {
        shard_index: usize,
    },
    MemoryOutboxWorkerStats {
        pending_shards: usize,
        in_flight_shards: usize,
        parallelism_limit: usize,
        parallelism_peak: usize,
        duplicate_schedule_coalesced_count: u64,
        task_failure_count: u64,
        shard_requeue_count: u64,
    },
}
impl WorkerManager {
    pub(super) fn new(init: WorkerManagerInit) -> Self {
        let WorkerManagerInit {
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
        } = init;
        let response_byte_budget = Arc::clone(&admission.response_bytes);
        let next_memory_entity_epoch = memory_store.owner_epoch_floor();
        Self {
            config,
            control_store,
            module_registry,
            bootstrap_snapshot,
            runtime_fast_sender,
            kv_store,
            memory_store,
            memory_outbox_drain_sender,
            cache_store,
            asset_catalog,
            workers: HashMap::new(),
            queue_counters: RuntimeQueueCounters::default(),
            next_queue_expiry_at: None,
            pre_canceled: HashMap::new(),
            stream_registrations: HashMap::new(),
            response_byte_budget,
            admission,
            revalidation_keys: HashSet::new(),
            revalidation_requests: HashMap::new(),
            websocket_sessions: HashMap::new(),
            websocket_handle_index: HashMap::new(),
            websocket_open_handles: HashMap::new(),
            open_handle_registry: crate::ops::MemoryOpenHandleRegistry::default(),
            websocket_outbound_frames: HashMap::new(),
            websocket_close_signals: HashMap::new(),
            websocket_frame_waiters: HashMap::new(),
            websocket_pending_frame_replies: HashMap::new(),
            websocket_open_waiters: HashMap::new(),
            runtime_batch_depth: 0,
            pending_dispatches: HashSet::new(),
            pending_cleanup_workers: HashSet::new(),
            pending_memory_outbox_shards: HashSet::new(),
            scale_up_requests: VecDeque::new(),
            scale_up_request_members: HashSet::new(),
            internal_rescue_isolate_slots: HashSet::new(),
            exiting_isolate_slots: HashMap::new(),
            isolate_thread_tracker: IsolateThreadTracker::default(),
            stats: RuntimeManagerStats::default(),
            next_generation: 1,
            next_isolate_id: 1,
            next_memory_entity_epoch,
        }
    }

    pub(super) fn begin_runtime_batch(&mut self) {
        self.runtime_batch_depth += 1;
    }

    pub(super) fn finish_runtime_batch(&mut self, event_tx: &RuntimeEventSender) {
        debug_assert!(self.runtime_batch_depth > 0);
        if self.runtime_batch_depth == 0 {
            return;
        }
        self.runtime_batch_depth -= 1;
        if self.runtime_batch_depth > 0 {
            return;
        }

        loop {
            let pending_dispatches = mem::take(&mut self.pending_dispatches);
            let pending_cleanup_workers = mem::take(&mut self.pending_cleanup_workers);
            if pending_dispatches.is_empty() && pending_cleanup_workers.is_empty() {
                break;
            }

            for (worker_name, generation) in pending_dispatches {
                self.dispatch_pool(&worker_name, generation, event_tx);
            }
            self.process_scale_up_requests(event_tx);
            for worker_name in pending_cleanup_workers {
                self.cleanup_drained_generations_for(&worker_name);
            }
        }
    }

    pub(super) async fn handle_command(
        &mut self,
        command: RuntimeCommand,
        event_tx: &RuntimeEventSender,
    ) -> bool {
        let (command, queue_admission) = match command {
            RuntimeCommand::Admitted { command, admission } => (*command, Some(admission)),
            command => (command, None),
        };
        match command {
            RuntimeCommand::Admitted { .. } => {
                unreachable!("commands are admitted once at their boundary")
            }
            RuntimeCommand::InvokeInternal(mut invoke) => {
                invoke.queue_admission = queue_admission.or(invoke.queue_admission);
                self.enqueue_invoke(invoke, event_tx);
                true
            }
            RuntimeCommand::CheckDeployment {
                worker_name,
                temporary,
                reply,
            } => {
                let result = if temporary && self.current_worker_is_permanent(&worker_name) {
                    Err(PlatformError::conflict(format!(
                        "cannot deploy permanent worker {worker_name} as temporary"
                    )))
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::Deploy {
                prepared,
                deployment_id,
                expires_at_ms,
                reply,
            } => {
                let result = self.deploy(prepared, deployment_id, expires_at_ms);
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::Undeploy { worker_name, reply } => {
                let result = if self.workers.contains_key(&worker_name) {
                    self.retire_worker_completely_with_error(
                        &worker_name,
                        PlatformError::not_found("worker was undeployed"),
                    );
                    Ok(())
                } else {
                    Err(PlatformError::not_found("worker not found"))
                };
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::Invoke {
                worker_name,
                runtime_request_id,
                request,
                request_body,
                reply,
            } => {
                self.enqueue_invoke(
                    EnqueueInvokeRequest {
                        queue_admission,
                        worker_name,
                        runtime_request_id,
                        request,
                        request_body,
                        memory_route: None,
                        memory_call: None,
                        target_isolate_id: None,
                        target_generation: None,
                        internal_origin: false,
                        reply,
                        reply_kind: PendingReplyKind::Normal,
                    },
                    event_tx,
                );
                true
            }
            RuntimeCommand::InvokeStream {
                worker_name,
                runtime_request_id,
                request,
                request_body,
                ready,
                reply,
            } => {
                self.register_stream(worker_name.clone(), runtime_request_id.clone(), ready);
                self.enqueue_invoke(
                    EnqueueInvokeRequest {
                        queue_admission,
                        worker_name,
                        runtime_request_id,
                        request,
                        request_body,
                        memory_route: None,
                        memory_call: None,
                        target_isolate_id: None,
                        target_generation: None,
                        internal_origin: false,
                        reply,
                        reply_kind: PendingReplyKind::Stream,
                    },
                    event_tx,
                );
                true
            }
            RuntimeCommand::ServiceBindingFetchStart {
                reply_inbox,
                owner_worker,
                owner_generation,
                binding,
                target_worker,
                request,
                reply_id,
                pending_replies,
            } => {
                self.start_service_binding_fetch(ServiceBindingFetchStart {
                    reply_inbox,
                    queue_admission,
                    owner_worker,
                    owner_generation,
                    binding,
                    target_worker,
                    request,
                    reply_id,
                    pending_replies,
                });
                true
            }
            RuntimeCommand::OpenWebsocket {
                worker_name,
                runtime_request_id,
                mut request,
                request_body,
                session_id,
                reply,
            } => {
                if !self.workers.contains_key(worker_name.trim()) {
                    let _ = reply.send(Err(PlatformError::not_found("Worker not found")));
                    return true;
                }
                let (inner_tx, _inner_rx) = oneshot::channel();
                append_or_update_header(
                    &mut request.headers,
                    INTERNAL_WS_SESSION_HEADER,
                    &session_id,
                );
                self.websocket_open_waiters
                    .insert(session_id.clone(), reply);

                self.enqueue_invoke(
                    EnqueueInvokeRequest {
                        queue_admission,
                        worker_name,
                        runtime_request_id,
                        request,
                        request_body,
                        memory_route: None,
                        memory_call: None,
                        target_isolate_id: None,
                        target_generation: None,
                        internal_origin: false,
                        reply: inner_tx,
                        reply_kind: PendingReplyKind::WebsocketOpen { session_id },
                    },
                    event_tx,
                );
                true
            }
            RuntimeCommand::SendWebsocketFrame {
                worker_name,
                session_id,
                frame,
                is_binary,
                reply,
            } => {
                self.enqueue_websocket_frame(
                    WebSocketFrameRequest {
                        worker_name,
                        session_id,
                        frame,
                        is_binary,
                        queue_admission,
                        reply,
                    },
                    event_tx,
                );
                true
            }
            RuntimeCommand::WaitWebsocketFrame {
                worker_name,
                session_id,
                reply,
            } => {
                self.wait_websocket_frame(&worker_name, &session_id, reply);
                true
            }
            RuntimeCommand::DrainWebsocketFrame {
                worker_name,
                session_id,
                reply,
            } => {
                let result = self.drain_websocket_frame(&worker_name, &session_id);
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::CloseWebsocket {
                worker_name,
                session_id,
                close_code,
                close_reason,
                reply,
            } => {
                let result = self.close_websocket(
                    &worker_name,
                    &session_id,
                    close_code,
                    close_reason,
                    event_tx,
                );
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::Cancel {
                worker_name,
                runtime_request_id,
            } => {
                self.cancel_invoke(worker_name, runtime_request_id, event_tx);
                true
            }
            RuntimeCommand::Stats { worker_name, reply } => {
                let _ = reply.send(self.worker_stats(&worker_name));
                true
            }
            RuntimeCommand::DebugDump { worker_name, reply } => {
                let _ = reply.send(self.worker_debug_dump(&worker_name));
                true
            }
            RuntimeCommand::Shutdown { reply } => {
                let _ = reply.send(self.shutdown_all().await);
                false
            }
            #[cfg(test)]
            RuntimeCommand::ForceFailIsolate {
                worker_name,
                generation,
                isolate_id,
                reply,
            } => {
                let exists = self
                    .workers
                    .get(&worker_name)
                    .and_then(|entry| entry.pools.get(&generation))
                    .map(|pool| pool.isolates.iter().any(|isolate| isolate.id == isolate_id))
                    .unwrap_or(false);
                if exists {
                    self.fail_isolate(
                        &worker_name,
                        generation,
                        isolate_id,
                        PlatformError::internal("isolate removed for test"),
                    );
                }
                let _ = reply.send(exists);
                true
            }
        }
    }

    pub(super) async fn handle_event(
        &mut self,
        event: RuntimeEvent,
        event_tx: &RuntimeEventSender,
    ) {
        match event {
            RuntimeEvent::RequestFinished {
                worker_name,
                generation,
                isolate_id,
                request_id,
                completion_token,
                finished_at,
                wait_until_count,
                result,
            } => {
                self.finish_request(
                    FinishRequest {
                        worker_name: worker_name.clone(),
                        generation,
                        isolate_id,
                        request_id,
                        completion_token,
                        finished_at,
                        wait_until_count,
                        result,
                    },
                    event_tx,
                )
                .await;
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
                reply,
            } => {
                self.handle_response_chunk(
                    &worker_name,
                    &request_id,
                    &completion_token,
                    chunk,
                    event_tx,
                    reply,
                );
            }
            RuntimeEvent::CacheRevalidate {
                worker_name,
                generation,
                payload,
            } => {
                self.schedule_cache_revalidate(&worker_name, generation, payload, event_tx);
            }
            RuntimeEvent::MemorySocketSend(payload) => {
                self.handle_memory_socket_send(payload, event_tx);
            }
            RuntimeEvent::MemorySocketClose(payload) => {
                self.handle_memory_socket_close(payload, event_tx);
            }
            RuntimeEvent::TestAsyncReply(payload) => {
                self.handle_test_async_reply(payload, event_tx);
            }
            RuntimeEvent::TestAsyncReplyComplete {
                reply_id,
                replies,
                result,
            } => {
                self.complete_test_async_reply(reply_id, replies, result);
            }
            RuntimeEvent::IsolateReady {
                worker_name,
                generation,
                isolate_id,
            } => {
                self.mark_isolate_ready(&worker_name, generation, isolate_id);
                self.dispatch_pool(&worker_name, generation, event_tx);
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
            RuntimeEvent::IsolateExited {
                worker_name,
                generation,
                isolate_id,
            } => {
                self.handle_isolate_exited(&worker_name, generation, isolate_id);
                self.dispatch_pool(&worker_name, generation, event_tx);
                self.cleanup_drained_generations_for(&worker_name);
            }
            RuntimeEvent::MemoryOutboxDelivery {
                shard_index,
                claims,
                saturated,
                reply,
            } => {
                self.handle_memory_outbox_delivery(shard_index, claims, saturated, reply, event_tx);
            }
            RuntimeEvent::MemoryOutboxAckFailed { shard_index } => {
                self.handle_memory_outbox_ack_failed(shard_index, event_tx);
            }
            RuntimeEvent::MemoryOutboxWorkerStats {
                pending_shards,
                in_flight_shards,
                parallelism_limit,
                parallelism_peak,
                duplicate_schedule_coalesced_count,
                task_failure_count,
                shard_requeue_count,
            } => {
                self.stats.memory_outbox_worker_pending_shards = pending_shards;
                self.stats.memory_outbox_worker_in_flight_shards = in_flight_shards;
                self.stats.memory_outbox_worker_parallelism_limit = parallelism_limit;
                self.stats.memory_outbox_worker_parallelism_peak = self
                    .stats
                    .memory_outbox_worker_parallelism_peak
                    .max(parallelism_peak);
                self.stats.memory_outbox_duplicate_schedule_coalesced_count =
                    duplicate_schedule_coalesced_count;
                self.stats.memory_outbox_task_failure_count = task_failure_count;
                self.stats.memory_outbox_shard_requeue_count = shard_requeue_count;
            }
        }
    }

    fn enqueue_websocket_frame(
        &mut self,
        request: WebSocketFrameRequest,
        event_tx: &RuntimeEventSender,
    ) {
        let WebSocketFrameRequest {
            worker_name,
            session_id,
            frame,
            is_binary,
            queue_admission,
            reply,
        } = request;
        let worker_name = worker_name.as_str();
        let session_id = session_id.as_str();
        let Some((session_worker_name, generation, binding, key, handle)) =
            self.websocket_sessions.get(session_id).map(|session| {
                (
                    session.worker_name.clone(),
                    session.generation,
                    session.binding.clone(),
                    session.key.clone(),
                    session.handle.clone(),
                )
            })
        else {
            let _ = reply.send(Err(PlatformError::not_found("websocket session not found")));
            return;
        };
        if session_worker_name != worker_name {
            let _ = reply.send(Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            )));
            return;
        }

        let runtime_request_id = Uuid::new_v4().to_string();
        let route = MemoryRoute::new(binding.clone(), key.clone());
        let socket_handles = self.websocket_handles_snapshot(
            &crate::memory::worker_namespace(worker_name, &binding),
            &key,
            Some(&handle),
        );
        let memory_call = MemoryExecutionCall::Message {
            binding,
            key,
            handle,
            is_text: !is_binary,
            data: frame,
            socket_handles,
        };
        let invoke = WorkerInvocation {
            method: "WS-MESSAGE".to_string(),
            url: format!("http://memory/__dd_socket/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("ws-message-{runtime_request_id}"),
        };
        self.enqueue_invoke(
            EnqueueInvokeRequest {
                queue_admission,
                worker_name: session_worker_name,
                runtime_request_id,
                request: invoke,
                request_body: None,
                memory_route: Some(route),
                memory_call: Some(memory_call),
                target_isolate_id: None,
                target_generation: Some(generation),
                internal_origin: true,
                reply,
                reply_kind: PendingReplyKind::WebsocketFrame {
                    session_id: session_id.to_string(),
                },
            },
            event_tx,
        );
    }

    fn close_websocket(
        &mut self,
        worker_name: &str,
        session_id: &str,
        close_code: u16,
        close_reason: String,
        event_tx: &RuntimeEventSender,
    ) -> Result<()> {
        let Some(existing) = self.websocket_sessions.get(session_id) else {
            return Err(PlatformError::not_found("websocket session not found"));
        };
        if existing.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            ));
        }

        let session = self
            .unregister_websocket_session(session_id)
            .ok_or_else(|| PlatformError::not_found("websocket session not found"))?;

        let runtime_request_id = Uuid::new_v4().to_string();
        let route = MemoryRoute::new(session.binding.clone(), session.key.clone());
        let socket_handles = self.websocket_handles_snapshot(
            &session.namespace,
            &session.key,
            Some(&session.handle),
        );
        let memory_call = MemoryExecutionCall::Close {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            code: close_code,
            reason: close_reason,
            socket_handles,
        };
        let invoke = WorkerInvocation {
            method: "WS-CLOSE".to_string(),
            url: format!("http://memory/__dd_socket_close/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("ws-close-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            EnqueueInvokeRequest {
                queue_admission: None,
                worker_name: session.worker_name,
                runtime_request_id,
                request: invoke,
                request_body: None,
                memory_route: Some(route),
                memory_call: Some(memory_call),
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
            if let Ok(Err(error)) = receiver.await {
                warn!(session_id, error = %error, "websocket close wake dispatch failed");
            }
        });
        Ok(())
    }

    fn wait_websocket_frame(
        &mut self,
        worker_name: &str,
        session_id: &str,
        reply: oneshot::Sender<Result<()>>,
    ) {
        let Some(session) = self.websocket_sessions.get(session_id) else {
            let _ = reply.send(Err(PlatformError::not_found("websocket session not found")));
            return;
        };
        if session.worker_name != worker_name {
            let _ = reply.send(Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            )));
            return;
        }
        let has_frame = self
            .websocket_outbound_frames
            .get(session_id)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false);
        let has_close = self.websocket_close_signals.contains_key(session_id);
        if has_frame || has_close {
            let _ = reply.send(Ok(()));
            return;
        }
        self.websocket_frame_waiters
            .entry(session_id.to_string())
            .or_default()
            .push(reply);
    }

    pub(super) fn notify_websocket_frame_waiters(&mut self, session_id: &str) {
        if let Some(waiters) = self.websocket_frame_waiters.remove(session_id) {
            for waiter in waiters {
                let _ = waiter.send(Ok(()));
            }
        }
    }

    pub(super) fn fail_websocket_frame_waiters(&mut self, session_id: &str, error: PlatformError) {
        if let Some(waiters) = self.websocket_frame_waiters.remove(session_id) {
            for waiter in waiters {
                let _ = waiter.send(Err(error.clone()));
            }
        }
    }

    fn drain_websocket_frame(
        &mut self,
        worker_name: &str,
        session_id: &str,
    ) -> Result<Option<WorkerOutput>> {
        let Some(session) = self.websocket_sessions.get(session_id) else {
            return Err(PlatformError::not_found("websocket session not found"));
        };
        if session.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            ));
        }

        let mut output = WorkerOutput {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
        };
        let mut has_output = false;

        if let Some(frame) = self
            .websocket_outbound_frames
            .get_mut(session_id)
            .and_then(|queue| queue.pop_front())
        {
            has_output = true;
            output.body = frame.payload;
            if frame.is_binary {
                append_or_update_header(&mut output.headers, INTERNAL_WS_BINARY_HEADER, "1");
            }
        }

        if let Some(close) = self.websocket_close_signals.remove(session_id) {
            has_output = true;
            append_or_update_header(
                &mut output.headers,
                INTERNAL_WS_CLOSE_CODE_HEADER,
                close.code.to_string().as_str(),
            );
            append_or_update_header(
                &mut output.headers,
                INTERNAL_WS_CLOSE_REASON_HEADER,
                &close.reason,
            );
        }

        if has_output {
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }

    pub(super) fn complete_websocket_frame(
        &mut self,
        session_id: String,
        reply: Option<oneshot::Sender<Result<WorkerOutput>>>,
        result: Result<WorkerOutput>,
        wait_for_outbox_frame: bool,
    ) {
        let Some(reply) = reply else {
            return;
        };
        match result {
            Ok(mut output) => {
                output.headers = strip_websocket_frame_internal_headers(&output.headers);
                let mut has_outbox_output = false;
                if let Some(frame) = self
                    .websocket_outbound_frames
                    .get_mut(&session_id)
                    .and_then(|queue| queue.pop_front())
                {
                    has_outbox_output = true;
                    output.body = frame.payload;
                    if frame.is_binary {
                        append_or_update_header(
                            &mut output.headers,
                            INTERNAL_WS_BINARY_HEADER,
                            "1",
                        );
                    } else {
                        output.headers.retain(|(name, _)| {
                            !name.eq_ignore_ascii_case(INTERNAL_WS_BINARY_HEADER)
                        });
                    }
                }
                if let Some(close) = self.websocket_close_signals.remove(&session_id) {
                    has_outbox_output = true;
                    append_or_update_header(
                        &mut output.headers,
                        INTERNAL_WS_CLOSE_CODE_HEADER,
                        close.code.to_string().as_str(),
                    );
                    append_or_update_header(
                        &mut output.headers,
                        INTERNAL_WS_CLOSE_REASON_HEADER,
                        &close.reason,
                    );
                }
                if wait_for_outbox_frame && !has_outbox_output {
                    self.websocket_pending_frame_replies
                        .entry(session_id)
                        .or_default()
                        .push(WebSocketFrameReply { output, reply });
                    return;
                }
                let _ = reply.send(Ok(output));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    pub(super) fn flush_pending_websocket_frame_replies(&mut self, session_id: &str) {
        let Some(replies) = self.websocket_pending_frame_replies.remove(session_id) else {
            return;
        };
        for WebSocketFrameReply { mut output, reply } in replies {
            let mut has_output = false;
            if let Some(frame) = self
                .websocket_outbound_frames
                .get_mut(session_id)
                .and_then(|queue| queue.pop_front())
            {
                has_output = true;
                output.body = frame.payload;
                if frame.is_binary {
                    append_or_update_header(&mut output.headers, INTERNAL_WS_BINARY_HEADER, "1");
                } else {
                    output
                        .headers
                        .retain(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_BINARY_HEADER));
                }
            }
            if let Some(close) = self.websocket_close_signals.remove(session_id) {
                has_output = true;
                append_or_update_header(
                    &mut output.headers,
                    INTERNAL_WS_CLOSE_CODE_HEADER,
                    close.code.to_string().as_str(),
                );
                append_or_update_header(
                    &mut output.headers,
                    INTERNAL_WS_CLOSE_REASON_HEADER,
                    &close.reason,
                );
            }
            if has_output {
                let _ = reply.send(Ok(output));
            } else {
                self.websocket_pending_frame_replies
                    .entry(session_id.to_string())
                    .or_default()
                    .push(WebSocketFrameReply { output, reply });
            }
        }
    }

    fn handle_test_async_reply(
        &mut self,
        payload: crate::ops::TestAsyncReplyEvent,
        event_tx: &RuntimeEventSender,
    ) {
        let result = if payload.ok {
            Ok(payload.value)
        } else {
            Err(PlatformError::runtime(if payload.error.trim().is_empty() {
                "test async reply failed".to_string()
            } else {
                payload.error
            }))
        };
        if payload.delay_ms == 0 {
            self.complete_test_async_reply(payload.reply_id, payload.replies, result);
            return;
        }
        let event_tx = event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(payload.delay_ms)).await;
            let _ = event_tx
                .send(RuntimeEvent::TestAsyncReplyComplete {
                    reply_id: payload.reply_id,
                    replies: payload.replies,
                    result,
                })
                .await;
        });
    }

    fn complete_test_async_reply(
        &mut self,
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        result: Result<String>,
    ) {
        let Some(delivery) = replies.finish(reply_id, result) else {
            return;
        };
        self.enqueue_isolate_reply(
            &delivery.owner.worker_name,
            delivery.owner.generation,
            delivery.owner.isolate_id,
            crate::ops::PushedReplyPayload::TestAsync(delivery.payload),
        );
    }

    fn enqueue_isolate_reply(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        payload: crate::ops::PushedReplyPayload,
    ) {
        let Some(inbox) = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == isolate_id)
                    .map(|isolate| isolate.request_control_inbox.clone())
            })
        else {
            return;
        };
        inbox.push_reply(payload);
    }

    pub(super) fn get_pool_mut(
        &mut self,
        worker_name: &str,
        generation: u64,
    ) -> Option<&mut WorkerPool> {
        self.workers
            .get_mut(worker_name)
            .and_then(|entry| entry.pools.get_mut(&generation))
    }

    pub(super) async fn shutdown_all(&mut self) -> Result<()> {
        let worker_names = self.workers.keys().cloned().collect::<Vec<_>>();
        for worker_name in worker_names {
            self.reap_owned_sessions(&worker_name, None, None);
        }
        let mut clear_request_ids = Vec::new();
        let mut queued_pending = Vec::new();
        let mut dequeued_count = 0usize;
        let mut dequeued_bytes = 0usize;
        let mut exiting_slots = Vec::new();
        for (worker_name, entry) in &mut self.workers {
            for (generation, pool) in &mut entry.pools {
                while let Some(pending) = pool.queue.pop_front() {
                    dequeued_count = dequeued_count.saturating_add(1);
                    dequeued_bytes = dequeued_bytes.saturating_add(pending.queued_bytes);
                    queued_pending.push((worker_name.clone(), pending));
                }
                pool.clear_isolate_indices();
                for isolate in pool.isolates.drain(..) {
                    isolate.request_shutdown();
                    exiting_slots.push((
                        worker_name.clone(),
                        *generation,
                        isolate.id,
                        isolate.startup,
                    ));
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("runtime shutting down")));
                    }
                }
            }
        }
        for (worker_name, generation, isolate_id, startup) in exiting_slots {
            self.track_exiting_isolate_slot(&worker_name, generation, isolate_id, startup);
        }
        self.account_dequeued_many(dequeued_count, dequeued_bytes);
        for (worker_name, pending) in queued_pending {
            self.reject_pending_invoke(
                &worker_name,
                pending,
                PlatformError::internal("runtime shutting down"),
            );
        }
        for request_id in clear_request_ids {
            self.clear_revalidation_for_request(&request_id);
        }
        for (_, mut registration) in std::mem::take(&mut self.stream_registrations) {
            let error = PlatformError::internal("runtime shutting down");
            if let Some(ready) = registration.ready.take() {
                let _ = ready.send(Err(error.clone()));
            } else if let Some(completion) = registration.completion.take() {
                let _ = completion.send(Err(error));
            }
        }
        for (_, waiter) in std::mem::take(&mut self.websocket_open_waiters) {
            let _ = waiter.send(Err(PlatformError::internal("runtime shutting down")));
        }
        for (_, waiters) in std::mem::take(&mut self.websocket_frame_waiters) {
            for waiter in waiters {
                let _ = waiter.send(Err(PlatformError::internal("runtime shutting down")));
            }
        }
        self.websocket_sessions.clear();
        self.websocket_handle_index.clear();
        self.websocket_open_handles.clear();
        self.open_handle_registry.clear();
        self.websocket_outbound_frames.clear();
        self.websocket_close_signals.clear();

        let thread_tracker = self.isolate_thread_tracker.clone();
        tokio::time::timeout(Duration::from_secs(10), async move {
            thread_tracker.wait_for_empty().await;
            Ok(())
        })
        .await
        .map_err(|_| PlatformError::internal("timed out waiting for isolate shutdown"))?
    }
}
