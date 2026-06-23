use super::*;
use tracing::warn;
#[derive(Clone)]
pub(crate) struct RuntimeFastCommandSender(pub mpsc::Sender<RuntimeCommand>);

pub(crate) enum RuntimeCommand {
    Deploy {
        prepared: PreparedWorkerDeployment,
        persist: bool,
        temporary: bool,
        expires_at_ms: Option<i64>,
        enforce_temporary_transition: bool,
        reply: oneshot::Sender<Result<String>>,
    },
    DeployDynamic {
        source: String,
        env: HashMap<String, String>,
        egress_allow_hosts: Vec<String>,
        reply: oneshot::Sender<Result<DynamicDeployResult>>,
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
    DynamicWorkerFetchStart {
        owner_worker: String,
        owner_generation: u64,
        binding: String,
        handle: String,
        request: WorkerInvocation,
        reply_id: String,
        pending_replies: crate::ops::DynamicPendingReplies,
    },
    RetireDynamicWorkerHandle {
        handle: String,
        reason: String,
    },
    RetireDynamicWorker {
        worker_name: String,
        reason: String,
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
    DynamicDebugDump {
        reply: oneshot::Sender<DynamicRuntimeDebugDump>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
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
    OpenTransport {
        worker_name: String,
        request: WorkerInvocation,
        session_id: String,
        stream_sender: mpsc::Sender<Vec<u8>>,
        datagram_sender: mpsc::Sender<Vec<u8>>,
        reply: oneshot::Sender<Result<TransportOpen>>,
    },
    PushTransportStream {
        worker_name: String,
        session_id: String,
        chunk: Vec<u8>,
        done: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    PushTransportDatagram {
        worker_name: String,
        session_id: String,
        datagram: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    CloseTransport {
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
    MemoryInvoke(MemoryInvokeEvent),
    MemorySocketSend(crate::ops::MemorySocketSendEvent),
    MemorySocketClose(crate::ops::MemorySocketCloseEvent),
    MemorySocketConsumeClose {
        worker_name: String,
        generation: u64,
        payload: crate::ops::MemorySocketConsumeCloseEvent,
    },
    MemoryTransportSendStream(crate::ops::MemoryTransportSendStreamEvent),
    MemoryTransportSendDatagram(crate::ops::MemoryTransportSendDatagramEvent),
    MemoryTransportClose(crate::ops::MemoryTransportCloseEvent),
    MemoryTransportConsumeClose {
        worker_name: String,
        generation: u64,
        payload: crate::ops::MemoryTransportConsumeCloseEvent,
    },
    DynamicWorkerCreate(crate::ops::DynamicWorkerCreateEvent),
    DynamicWorkerLookup(crate::ops::DynamicWorkerLookupEvent),
    DynamicWorkerList(crate::ops::DynamicWorkerListEvent),
    DynamicWorkerDelete(crate::ops::DynamicWorkerDeleteEvent),
    DynamicHostRpcInvoke(crate::ops::DynamicHostRpcInvokeEvent),
    DynamicReplyReady(crate::ops::DynamicPendingReplyDelivery),
    DynamicFetchReplyReady(crate::ops::DynamicPendingReplyDelivery),
    TestAsyncReply(crate::ops::TestAsyncReplyEvent),
    TestNestedTargetedInvoke(crate::ops::TestNestedTargetedInvokeEvent),
    TestAsyncReplyComplete {
        reply_id: String,
        replies: crate::ops::TestAsyncReplies,
        result: Result<String>,
    },
    DynamicTimeoutDiagnostic(DynamicTimeoutDiagnostic),
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
}
impl WorkerManager {
    pub(super) fn new(init: WorkerManagerInit) -> Self {
        let WorkerManagerInit {
            bootstrap_snapshot,
            kv_store,
            memory_store,
            cache_store,
            config,
            storage,
            runtime_fast_sender,
            asset_catalog,
        } = init;
        let next_memory_entity_epoch = memory_store.owner_epoch_floor();
        Self {
            config,
            storage,
            bootstrap_snapshot,
            runtime_fast_sender,
            kv_store,
            memory_store,
            cache_store,
            asset_catalog,
            workers: HashMap::new(),
            queue_counters: RuntimeQueueCounters::default(),
            next_queue_expiry_at: None,
            pre_canceled: HashMap::new(),
            stream_registrations: HashMap::new(),
            revalidation_keys: HashSet::new(),
            revalidation_requests: HashMap::new(),
            websocket_sessions: HashMap::new(),
            websocket_handle_index: HashMap::new(),
            websocket_open_handles: HashMap::new(),
            open_handle_registry: crate::ops::MemoryOpenHandleRegistry::default(),
            websocket_pending_closes: HashMap::new(),
            websocket_outbound_frames: HashMap::new(),
            websocket_close_signals: HashMap::new(),
            websocket_frame_waiters: HashMap::new(),
            websocket_open_waiters: HashMap::new(),
            transport_sessions: HashMap::new(),
            transport_handle_index: HashMap::new(),
            transport_open_handles: HashMap::new(),
            transport_pending_closes: HashMap::new(),
            transport_open_channels: HashMap::new(),
            transport_open_waiters: HashMap::new(),
            dynamic_worker_handles: HashMap::new(),
            dynamic_worker_ids: HashMap::new(),
            host_rpc_providers: HashMap::new(),
            dynamic_profile: crate::ops::DynamicProfile::default(),
            validated_worker_sources: HashSet::new(),
            runtime_batch_depth: 0,
            pending_dispatches: HashSet::new(),
            pending_cleanup_workers: HashSet::new(),
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
        match command {
            RuntimeCommand::Deploy {
                prepared,
                persist,
                temporary,
                expires_at_ms,
                enforce_temporary_transition,
                reply,
            } => {
                let result = self
                    .deploy(
                        prepared,
                        persist,
                        temporary,
                        expires_at_ms,
                        enforce_temporary_transition,
                    )
                    .await;
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::DeployDynamic {
                source,
                env,
                egress_allow_hosts,
                reply,
            } => {
                let result = self
                    .deploy_dynamic(source, env, egress_allow_hosts, Vec::new())
                    .await;
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
                        worker_name,
                        runtime_request_id,
                        request,
                        request_body,
                        memory_route: None,
                        memory_call: None,
                        host_rpc_call: None,
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
                        worker_name,
                        runtime_request_id,
                        request,
                        request_body,
                        memory_route: None,
                        memory_call: None,
                        host_rpc_call: None,
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
            RuntimeCommand::DynamicWorkerFetchStart {
                owner_worker,
                owner_generation,
                binding,
                handle,
                request,
                reply_id,
                pending_replies,
            } => {
                let runtime_fast_sender = self.runtime_fast_sender.clone();
                self.start_dynamic_worker_fetch(
                    DynamicWorkerFetchStart {
                        owner_worker,
                        owner_generation,
                        binding,
                        handle,
                        request,
                        reply_id,
                        pending_replies,
                        command_tx: runtime_fast_sender,
                    },
                    event_tx,
                );
                true
            }
            RuntimeCommand::RetireDynamicWorkerHandle { handle, reason } => {
                self.retire_dynamic_worker_handle(&handle, &reason, true);
                true
            }
            RuntimeCommand::RetireDynamicWorker {
                worker_name,
                reason,
            } => {
                self.retire_dynamic_worker_by_worker_name(&worker_name, &reason, true);
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
                        worker_name,
                        runtime_request_id,
                        request,
                        request_body,
                        memory_route: None,
                        memory_call: None,
                        host_rpc_call: None,
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
                    &worker_name,
                    &session_id,
                    frame,
                    is_binary,
                    reply,
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
            RuntimeCommand::OpenTransport {
                worker_name,
                mut request,
                session_id,
                stream_sender,
                datagram_sender,
                reply,
            } => {
                if !self.workers.contains_key(worker_name.trim()) {
                    let _ = reply.send(Err(PlatformError::not_found("Worker not found")));
                    return true;
                }
                let (inner_tx, _inner_rx) = oneshot::channel();
                append_or_update_header(
                    &mut request.headers,
                    INTERNAL_TRANSPORT_SESSION_HEADER,
                    &session_id,
                );
                self.transport_open_waiters
                    .insert(session_id.clone(), reply);
                self.transport_open_channels.insert(
                    session_id.clone(),
                    TransportOpenChannels {
                        stream_sender,
                        datagram_sender,
                    },
                );

                let runtime_request_id = Uuid::new_v4().to_string();
                self.enqueue_invoke(
                    EnqueueInvokeRequest {
                        worker_name,
                        runtime_request_id,
                        request,
                        request_body: None,
                        memory_route: None,
                        memory_call: None,
                        host_rpc_call: None,
                        target_isolate_id: None,
                        target_generation: None,
                        internal_origin: false,
                        reply: inner_tx,
                        reply_kind: PendingReplyKind::TransportOpen { session_id },
                    },
                    event_tx,
                );
                true
            }
            RuntimeCommand::PushTransportStream {
                worker_name,
                session_id,
                chunk,
                done,
                reply,
            } => {
                let result =
                    self.push_transport_stream(&worker_name, &session_id, chunk, done, event_tx);
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::PushTransportDatagram {
                worker_name,
                session_id,
                datagram,
                reply,
            } => {
                let result =
                    self.push_transport_datagram(&worker_name, &session_id, datagram, event_tx);
                let _ = reply.send(result);
                true
            }
            RuntimeCommand::CloseTransport {
                worker_name,
                session_id,
                close_code,
                close_reason,
                reply,
            } => {
                let result = self.close_transport(
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
            RuntimeCommand::DynamicDebugDump { reply } => {
                let _ = reply.send(self.dynamic_debug_dump());
                true
            }
            RuntimeCommand::Shutdown { reply } => {
                self.shutdown_all();
                let _ = reply.send(());
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
                )
                .await;
            }
            RuntimeEvent::CacheRevalidate {
                worker_name,
                generation,
                payload,
            } => {
                self.schedule_cache_revalidate(&worker_name, generation, payload, event_tx);
            }
            RuntimeEvent::MemoryInvoke(payload) => {
                self.enqueue_memory_invoke(payload, event_tx);
            }
            RuntimeEvent::MemorySocketSend(payload) => {
                self.handle_memory_socket_send(payload, event_tx);
            }
            RuntimeEvent::MemorySocketClose(payload) => {
                self.handle_memory_socket_close(payload, event_tx);
            }
            RuntimeEvent::MemorySocketConsumeClose {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_memory_socket_consume_close(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportSendStream(payload) => {
                self.handle_memory_transport_send_stream(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportSendDatagram(payload) => {
                self.handle_memory_transport_send_datagram(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportClose(payload) => {
                self.handle_memory_transport_close(payload, event_tx);
            }
            RuntimeEvent::MemoryTransportConsumeClose {
                worker_name: _worker_name,
                generation: _generation,
                payload,
            } => {
                self.handle_memory_transport_consume_close(payload, event_tx);
            }
            RuntimeEvent::DynamicWorkerCreate(payload) => {
                self.handle_dynamic_worker_create(payload).await;
            }
            RuntimeEvent::DynamicWorkerLookup(payload) => {
                self.handle_dynamic_worker_lookup(payload);
            }
            RuntimeEvent::DynamicWorkerList(payload) => {
                self.handle_dynamic_worker_list(payload);
            }
            RuntimeEvent::DynamicWorkerDelete(payload) => {
                self.handle_dynamic_worker_delete(payload);
            }
            RuntimeEvent::DynamicHostRpcInvoke(payload) => {
                self.handle_dynamic_host_rpc_invoke(payload, event_tx);
            }
            RuntimeEvent::DynamicReplyReady(delivery) => {
                self.enqueue_isolate_reply(
                    &delivery.owner.worker_name,
                    delivery.owner.generation,
                    delivery.owner.isolate_id,
                    delivery.payload,
                );
            }
            RuntimeEvent::DynamicFetchReplyReady(delivery) => {
                self.enqueue_isolate_reply(
                    &delivery.owner.worker_name,
                    delivery.owner.generation,
                    delivery.owner.isolate_id,
                    delivery.payload,
                );
            }
            RuntimeEvent::TestAsyncReply(payload) => {
                self.handle_test_async_reply(payload, event_tx);
            }
            RuntimeEvent::TestNestedTargetedInvoke(payload) => {
                self.handle_test_nested_targeted_invoke(payload, event_tx);
            }
            RuntimeEvent::TestAsyncReplyComplete {
                reply_id,
                replies,
                result,
            } => {
                self.complete_test_async_reply(reply_id, replies, result);
            }
            RuntimeEvent::DynamicTimeoutDiagnostic(payload) => {
                self.log_dynamic_timeout_diagnostic(payload);
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
        }
    }

    fn enqueue_websocket_frame(
        &mut self,
        worker_name: &str,
        session_id: &str,
        frame: Vec<u8>,
        is_binary: bool,
        reply: oneshot::Sender<Result<WorkerOutput>>,
        event_tx: &RuntimeEventSender,
    ) {
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
        let socket_handles = self.websocket_handles_snapshot(&binding, &key, Some(&handle));
        let transport_handles = self.transport_handles_snapshot(&binding, &key, None);
        let memory_call = MemoryExecutionCall::Message {
            binding,
            key,
            handle,
            is_text: !is_binary,
            data: frame,
            socket_handles,
            transport_handles,
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
        self.queue_websocket_close_replay(&session, close_code, close_reason.clone());

        let runtime_request_id = Uuid::new_v4().to_string();
        let route = MemoryRoute::new(session.binding.clone(), session.key.clone());
        let socket_handles =
            self.websocket_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, None);
        let memory_call = MemoryExecutionCall::Close {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            code: close_code,
            reason: close_reason,
            socket_handles,
            transport_handles,
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
    ) {
        let Some(reply) = reply else {
            return;
        };
        match result {
            Ok(mut output) => {
                output.headers = strip_websocket_frame_internal_headers(&output.headers);
                if let Some(frame) = self
                    .websocket_outbound_frames
                    .get_mut(&session_id)
                    .and_then(|queue| queue.pop_front())
                {
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
                let _ = reply.send(Ok(output));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
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

    fn handle_test_nested_targeted_invoke(
        &mut self,
        payload: crate::ops::TestNestedTargetedInvokeEvent,
        event_tx: &RuntimeEventSender,
    ) {
        let Some(pool) = self.get_pool_mut(&payload.worker_name, payload.generation) else {
            self.complete_test_async_reply(
                payload.reply_id,
                payload.replies,
                Err(PlatformError::runtime(
                    "test nested invoke worker pool is unavailable",
                )),
            );
            return;
        };

        let target_isolate_id = match payload.target_mode.trim() {
            "same" | "" => Some(payload.caller_isolate_id),
            "other" => pool
                .isolates
                .iter()
                .find(|isolate| isolate.id != payload.caller_isolate_id)
                .map(|isolate| isolate.id),
            mode => {
                self.complete_test_async_reply(
                    payload.reply_id,
                    payload.replies,
                    Err(PlatformError::runtime(format!(
                        "unknown test nested target mode: {mode}"
                    ))),
                );
                return;
            }
        };
        let Some(target_isolate_id) = target_isolate_id else {
            self.complete_test_async_reply(
                payload.reply_id,
                payload.replies,
                Err(PlatformError::runtime(
                    "test nested invoke target isolate is unavailable",
                )),
            );
            return;
        };

        let Some(target_isolate_id) = self
            .workers
            .get(&payload.worker_name)
            .and_then(|entry| entry.pools.get(&payload.generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == target_isolate_id)
                    .map(|isolate| isolate.id)
            })
        else {
            self.complete_test_async_reply(
                payload.reply_id,
                payload.replies,
                Err(PlatformError::runtime(
                    "test nested invoke target isolate sender is unavailable",
                )),
            );
            return;
        };

        let reply_id = payload.reply_id;
        let replies = payload.replies;
        if let Err(error) = self.start_targeted_host_rpc_invoke(
            TargetedHostRpcInvoke {
                worker_name: payload.worker_name,
                generation: payload.generation,
                isolate_id: target_isolate_id,
                target_id: payload.target_id,
                method_name: payload.method_name,
                args: payload.args,
                reply: TargetedHostRpcReply::Test {
                    reply_id: reply_id.clone(),
                    replies: replies.clone(),
                    success_value: format!("ok:{target_isolate_id}"),
                },
            },
            event_tx,
        ) {
            self.complete_test_async_reply(reply_id, replies, Err(error));
        }
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
            crate::ops::DynamicPushedReplyPayload::TestAsync(delivery.payload),
        );
    }

    pub(super) fn finish_dynamic_reply(
        &mut self,
        pending_replies: crate::ops::DynamicPendingReplies,
        reply_id: String,
        payload: crate::ops::DynamicPendingReplyPayload,
    ) {
        let Some(delivery) = pending_replies.finish(reply_id, payload) else {
            return;
        };
        self.enqueue_isolate_reply(
            &delivery.owner.worker_name,
            delivery.owner.generation,
            delivery.owner.isolate_id,
            delivery.payload,
        );
    }

    fn enqueue_isolate_reply(
        &mut self,
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        payload: crate::ops::DynamicPushedReplyPayload,
    ) {
        let Some((sender, inbox)) = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| {
                pool.isolates
                    .iter()
                    .find(|isolate| isolate.id == isolate_id)
                    .map(|isolate| {
                        (
                            isolate.sender.clone(),
                            isolate.dynamic_control_inbox.clone(),
                        )
                    })
            })
        else {
            return;
        };
        let schedule = inbox.push_reply(payload);
        if schedule {
            let _ = sender.try_send(IsolateCommand::DrainDynamicControl);
        }
    }

    pub(super) fn start_targeted_host_rpc_invoke(
        &mut self,
        invoke: TargetedHostRpcInvoke,
        event_tx: &RuntimeEventSender,
    ) -> Result<()> {
        let TargetedHostRpcInvoke {
            worker_name,
            generation,
            isolate_id,
            target_id,
            method_name,
            args,
            reply,
        } = invoke;
        let provider_available = self
            .workers
            .get(&worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .map(|pool| pool.isolates.iter().any(|isolate| isolate.id == isolate_id))
            .unwrap_or(false);
        if !provider_available {
            return Err(PlatformError::runtime(
                "dynamic host rpc provider isolate is unavailable",
            ));
        }
        let runtime_request_id = next_runtime_token("dhrpc");
        let request = WorkerInvocation {
            method: "POST".to_string(),
            url: "http://worker/__dd_internal_host_rpc".to_string(),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: runtime_request_id.clone(),
        };
        let (inner_reply_tx, inner_reply_rx) = oneshot::channel();
        self.enqueue_invoke(
            EnqueueInvokeRequest {
                worker_name,
                runtime_request_id,
                request,
                request_body: None,
                memory_route: None,
                memory_call: None,
                host_rpc_call: Some(HostRpcExecutionCall {
                    target_id,
                    method: method_name,
                    args,
                }),
                target_isolate_id: Some(isolate_id),
                target_generation: Some(generation),
                internal_origin: true,
                reply: inner_reply_tx,
                reply_kind: PendingReplyKind::Normal,
            },
            event_tx,
        );
        let event_tx = event_tx.clone();
        let profile = self.dynamic_profile.clone();
        tokio::spawn(async move {
            let result = match inner_reply_rx.await {
                Ok(Ok(output)) if output.body.len() > MAX_DYNAMIC_HOST_RPC_REPLY_BYTES => {
                    profile.record_rpc_deny();
                    Err(PlatformError::runtime(format!(
                        "dynamic host rpc reply exceeds limit ({MAX_DYNAMIC_HOST_RPC_REPLY_BYTES} bytes)"
                    )))
                }
                Ok(Ok(output)) => Ok(output.body),
                Ok(Err(error)) => Err(error),
                Err(_) => Err(PlatformError::internal(
                    "dynamic host rpc response channel closed",
                )),
            };
            profile.record_provider_task_callback();
            match reply {
                TargetedHostRpcReply::Dynamic {
                    reply_id,
                    pending_replies,
                } => {
                    if let Some(delivery) = pending_replies.finish(
                        reply_id,
                        crate::ops::DynamicPendingReplyPayload::HostRpc(result),
                    ) {
                        let _ = event_tx
                            .send(RuntimeEvent::DynamicReplyReady(delivery))
                            .await;
                    }
                }
                TargetedHostRpcReply::Test {
                    reply_id,
                    replies,
                    success_value,
                } => {
                    let string_result = result.map(|_| success_value);
                    let _ = event_tx
                        .send(RuntimeEvent::TestAsyncReplyComplete {
                            reply_id,
                            replies,
                            result: string_result,
                        })
                        .await;
                }
            }
        });
        Ok(())
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

    pub(super) fn shutdown_all(&mut self) {
        let worker_names = self.workers.keys().cloned().collect::<Vec<_>>();
        for worker_name in worker_names {
            self.reap_owned_sessions(&worker_name, None, None);
        }
        let mut clear_request_ids = Vec::new();
        let mut queued_pending = Vec::new();
        let mut dequeued_count = 0usize;
        let mut dequeued_bytes = 0usize;
        for (worker_name, entry) in &mut self.workers {
            for pool in entry.pools.values_mut() {
                while let Some(pending) = pool.queue.pop_front() {
                    dequeued_count = dequeued_count.saturating_add(1);
                    dequeued_bytes = dequeued_bytes.saturating_add(pending.queued_bytes);
                    queued_pending.push((worker_name.clone(), pending));
                }
                pool.clear_isolate_indices();
                for isolate in pool.isolates.drain(..) {
                    let _ = isolate.sender.try_send(IsolateCommand::Shutdown);
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("runtime shutting down")));
                    }
                }
            }
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
            } else {
                let _ = registration.body_sender.try_send(Err(error));
            }
        }
        for (_, waiter) in std::mem::take(&mut self.websocket_open_waiters) {
            let _ = waiter.send(Err(PlatformError::internal("runtime shutting down")));
        }
        for (_, waiter) in std::mem::take(&mut self.transport_open_waiters) {
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
        self.websocket_pending_closes.clear();
        self.websocket_outbound_frames.clear();
        self.websocket_close_signals.clear();
        self.transport_sessions.clear();
        self.transport_handle_index.clear();
        self.transport_open_handles.clear();
        self.transport_pending_closes.clear();
        self.transport_open_channels.clear();
        self.dynamic_worker_handles.clear();
        self.dynamic_worker_ids.clear();
        self.host_rpc_providers.clear();
    }
}
