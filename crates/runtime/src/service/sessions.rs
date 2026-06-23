use super::*;

const MEMORY_OUTBOX_DRAIN_LIMIT: usize = 64;
const MEMORY_OUTBOX_MAX_DRAIN_BATCHES: usize = 64;
const MEMORY_OUTBOX_LEASE: Duration = Duration::from_secs(30);
const MEMORY_OUTBOX_EFFECT_KINDS: &[&str] = &[
    "audit.*",
    "socket.send",
    "socket.close",
    "trace.*",
    "transport.stream",
    "transport.datagram",
    "transport.close",
];

impl WorkerManager {
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

    pub(super) async fn drain_memory_outbox(&mut self) -> usize {
        let mut processed = 0usize;
        for _ in 0..MEMORY_OUTBOX_MAX_DRAIN_BATCHES {
            let claims = match self
                .memory_store
                .claim_due_outbox_records(
                    MEMORY_OUTBOX_DRAIN_LIMIT,
                    MEMORY_OUTBOX_LEASE,
                    MEMORY_OUTBOX_EFFECT_KINDS,
                )
                .await
            {
                Ok(claims) => claims,
                Err(error) => {
                    warn!(error = %error, "memory outbox claim failed");
                    return processed;
                }
            };
            let claimed = claims.len();
            if claimed == 0 {
                break;
            }
            processed = processed.saturating_add(claimed);
            for claim in claims {
                let effect_id = claim.record.effect_id.clone();
                let namespace = claim.namespace.clone();
                let memory_key = claim.memory_key.clone();
                let delivery = self.deliver_memory_outbox_claim(&claim);
                let store_result = match delivery {
                    Ok(()) => {
                        self.memory_store
                            .mark_outbox_delivered(&namespace, &memory_key, &effect_id)
                            .await
                    }
                    Err(error) if is_terminal_memory_outbox_delivery_error(&error) => {
                        warn!(
                            namespace = %namespace,
                            memory_key = %memory_key,
                            effect_id = %effect_id,
                            kind = %claim.record.kind,
                            error = %error,
                            "memory outbox effect dropped"
                        );
                        self.memory_store
                            .mark_outbox_delivered(&namespace, &memory_key, &effect_id)
                            .await
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
                        self.memory_store
                            .retry_outbox_record(&namespace, &memory_key, &effect_id, retry_after)
                            .await
                    }
                };
                if let Err(error) = store_result {
                    warn!(
                        namespace = %namespace,
                        memory_key = %memory_key,
                        effect_id = %effect_id,
                        error = %error,
                        "memory outbox state update failed"
                    );
                }
            }
            if claimed < MEMORY_OUTBOX_DRAIN_LIMIT {
                break;
            }
        }
        processed
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
