use super::*;

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
        if let Err(error) = self.register_websocket_session(
            worker_name,
            generation,
            isolate_id,
            &session_id,
            &binding,
            &key,
            &handle,
        ) {
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
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        session_id: &str,
        binding: &str,
        key: &str,
        handle: &str,
    ) -> Result<()> {
        if self.websocket_sessions.contains_key(session_id) {
            let _ = self.unregister_websocket_session(session_id);
        }
        self.increment_websocket_session_count(worker_name, generation, isolate_id)?;

        let session = WorkerWebSocketSession {
            worker_name: worker_name.to_string(),
            generation,
            owner_isolate_id: isolate_id,
            binding: binding.to_string(),
            key: key.to_string(),
            handle: handle.to_string(),
        };
        self.websocket_sessions
            .insert(session_id.to_string(), session.clone());
        self.websocket_handle_index.insert(
            actor_handle_key(&session.binding, &session.key, &session.handle),
            session_id.to_string(),
        );
        self.websocket_open_handles
            .entry(actor_owner_key(binding, key))
            .or_default()
            .insert(handle.to_string());
        self.open_handle_registry
            .add_socket_handle(binding, key, handle);
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
        self.websocket_handle_index.remove(&actor_handle_key(
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

        let owner_key = actor_owner_key(&session.binding, &session.key);
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
        worker_name: &str,
        generation: u64,
        isolate_id: u64,
        session_id: &str,
        binding: &str,
        key: &str,
        handle: &str,
        stream_sender: mpsc::UnboundedSender<Vec<u8>>,
        datagram_sender: mpsc::UnboundedSender<Vec<u8>>,
    ) -> Result<()> {
        if self.transport_sessions.contains_key(session_id) {
            let _ = self.unregister_transport_session(session_id);
        }
        self.increment_transport_session_count(worker_name, generation, isolate_id)?;

        let session = WorkerTransportSession {
            worker_name: worker_name.to_string(),
            generation,
            owner_isolate_id: isolate_id,
            binding: binding.to_string(),
            key: key.to_string(),
            handle: handle.to_string(),
            stream_sender,
            datagram_sender,
            inbound_streams: VecDeque::new(),
            inbound_stream_closed: false,
            inbound_datagrams: VecDeque::new(),
        };
        self.transport_sessions
            .insert(session_id.to_string(), session.clone());
        self.transport_handle_index.insert(
            actor_handle_key(&session.binding, &session.key, &session.handle),
            session_id.to_string(),
        );
        self.transport_open_handles
            .entry(actor_owner_key(binding, key))
            .or_default()
            .insert(handle.to_string());
        self.open_handle_registry
            .add_transport_handle(binding, key, handle);
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
        self.transport_handle_index.remove(&actor_handle_key(
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

        let owner_key = actor_owner_key(&session.binding, &session.key);
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
        let owner_key = actor_owner_key(&session.binding, &session.key);
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
        let owner_key = actor_owner_key(&session.binding, &session.key);
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
        if let Err(error) = self.register_transport_session(
            worker_name,
            generation,
            isolate_id,
            &session_id,
            &binding,
            &key,
            &handle,
            channels.stream_sender,
            channels.datagram_sender,
        ) {
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

    pub(super) fn handle_actor_socket_send(
        &mut self,
        payload: crate::ops::ActorSocketSendEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorSocketSendEvent {
            reply,
            handle,
            binding,
            key,
            is_text,
            message,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let result = match self.websocket_handle_index.get(&index_key).cloned() {
            Some(session_id) => {
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
            None => Err(PlatformError::not_found("websocket session not found")),
        };
        let _ = reply.send(result);
    }

    pub(super) fn handle_actor_socket_close(
        &mut self,
        payload: crate::ops::ActorSocketCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorSocketCloseEvent {
            reply,
            handle,
            binding,
            key,
            code,
            reason,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let result = match self.websocket_handle_index.get(&index_key).cloned() {
            Some(session_id) => {
                self.websocket_close_signals
                    .insert(session_id.clone(), SocketCloseEvent { code, reason });
                self.notify_websocket_frame_waiters(&session_id);
                Ok(())
            }
            None => Err(PlatformError::not_found("websocket session not found")),
        };
        let _ = reply.send(result);
    }

    pub(super) fn websocket_handles_snapshot(
        &self,
        binding: &str,
        key: &str,
        include_handle: Option<&str>,
    ) -> Vec<String> {
        let owner_key = actor_owner_key(binding, key);
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

    pub(super) fn handle_actor_socket_consume_close(
        &mut self,
        payload: crate::ops::ActorSocketConsumeCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let owner_key = actor_owner_key(&payload.binding, &payload.key);
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
        let replay: Vec<crate::ops::ActorSocketCloseReplayEvent> = events
            .into_iter()
            .map(|event| crate::ops::ActorSocketCloseReplayEvent {
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
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let _ = done;
        let Some(session) = self.transport_sessions.get(session_id).cloned() else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if session.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }
        if chunk.is_empty() {
            return Ok(());
        }
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let socket_handles = self.websocket_handles_snapshot(&session.binding, &session.key, None);
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let actor_call = ActorExecutionCall::TransportStream {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            data: chunk,
            socket_handles,
            transport_handles,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-STREAM".to_string(),
            url: format!("http://actor/__dd_transport_stream/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-stream-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            session.worker_name,
            runtime_request_id,
            invoke,
            None,
            Some(route),
            Some(actor_call),
            None,
            None,
            Some(session.generation),
            true,
            reply,
            PendingReplyKind::Normal,
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
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) -> Result<()> {
        let Some(session) = self.transport_sessions.get(session_id).cloned() else {
            return Err(PlatformError::not_found("transport session not found"));
        };
        if session.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "transport session worker mismatch",
            ));
        }
        if datagram.is_empty() {
            return Ok(());
        }
        let runtime_request_id = Uuid::new_v4().to_string();
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let socket_handles = self.websocket_handles_snapshot(&session.binding, &session.key, None);
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let actor_call = ActorExecutionCall::TransportDatagram {
            binding: session.binding.clone(),
            key: session.key.clone(),
            handle: session.handle.clone(),
            data: datagram,
            socket_handles,
            transport_handles,
        };
        let invoke = WorkerInvocation {
            method: "TRANSPORT-DATAGRAM".to_string(),
            url: format!("http://actor/__dd_transport_datagram/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-datagram-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            session.worker_name,
            runtime_request_id,
            invoke,
            None,
            Some(route),
            Some(actor_call),
            None,
            None,
            Some(session.generation),
            true,
            reply,
            PendingReplyKind::Normal,
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
        event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
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
        let route = ActorRoute {
            binding: session.binding.clone(),
            key: session.key.clone(),
        };
        let socket_handles = self.websocket_handles_snapshot(&session.binding, &session.key, None);
        let transport_handles =
            self.transport_handles_snapshot(&session.binding, &session.key, Some(&session.handle));
        let actor_call = ActorExecutionCall::TransportClose {
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
            url: format!("http://actor/__dd_transport_close/{session_id}"),
            headers: Vec::new(),
            body: Vec::new(),
            request_id: format!("transport-close-{runtime_request_id}"),
        };
        let (reply, receiver) = oneshot::channel();
        self.enqueue_invoke(
            session.worker_name,
            runtime_request_id,
            invoke,
            None,
            Some(route),
            Some(actor_call),
            None,
            None,
            Some(session.generation),
            true,
            reply,
            PendingReplyKind::Normal,
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

    pub(super) fn handle_actor_transport_send_stream(
        &mut self,
        payload: crate::ops::ActorTransportSendStreamEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportSendStreamEvent {
            reply,
            handle,
            binding,
            key,
            chunk,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get(session_id) {
                Some(session) => session
                    .stream_sender
                    .send(chunk)
                    .map_err(|_| PlatformError::internal("transport stream channel closed")),
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    pub(super) fn handle_actor_transport_send_datagram(
        &mut self,
        payload: crate::ops::ActorTransportSendDatagramEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportSendDatagramEvent {
            reply,
            handle,
            binding,
            key,
            datagram,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get(session_id) {
                Some(session) => session
                    .datagram_sender
                    .send(datagram)
                    .map_err(|_| PlatformError::internal("transport datagram channel closed")),
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    pub(super) fn handle_actor_transport_recv_stream(
        &mut self,
        payload: crate::ops::ActorTransportRecvStreamEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportRecvStreamEvent {
            reply,
            handle,
            binding,
            key,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get_mut(session_id) {
                Some(session) => {
                    let chunk = session.inbound_streams.pop_front().unwrap_or_default();
                    let done = chunk.is_empty() && session.inbound_stream_closed;
                    Ok(crate::ops::TransportRecvEvent { done, chunk })
                }
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    pub(super) fn handle_actor_transport_recv_datagram(
        &mut self,
        payload: crate::ops::ActorTransportRecvDatagramEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportRecvDatagramEvent {
            reply,
            handle,
            binding,
            key,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => match self.transport_sessions.get_mut(session_id) {
                Some(session) => {
                    let chunk = session.inbound_datagrams.pop_front().unwrap_or_default();
                    Ok(crate::ops::TransportRecvEvent { done: false, chunk })
                }
                None => Err(PlatformError::not_found("transport session not found")),
            },
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    pub(super) fn handle_actor_transport_close(
        &mut self,
        payload: crate::ops::ActorTransportCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let crate::ops::ActorTransportCloseEvent {
            reply,
            handle,
            binding,
            key,
            code,
            reason,
        } = payload;
        let index_key = actor_handle_key(&binding, &key, &handle);
        let session_id = self.transport_handle_index.get(&index_key).cloned();
        let result = match session_id.as_deref() {
            Some(session_id) => {
                if let Some(session) = self.unregister_transport_session(session_id) {
                    self.queue_transport_close_replay(&session, code, reason);
                    Ok(())
                } else {
                    Err(PlatformError::not_found("transport session not found"))
                }
            }
            None => Err(PlatformError::not_found("transport session not found")),
        };
        let _ = reply.send(result);
    }

    pub(super) fn transport_handles_snapshot(
        &self,
        binding: &str,
        key: &str,
        include_handle: Option<&str>,
    ) -> Vec<String> {
        let owner_key = actor_owner_key(binding, key);
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

    pub(super) fn handle_actor_transport_consume_close(
        &mut self,
        payload: crate::ops::ActorTransportConsumeCloseEvent,
        _event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    ) {
        let owner_key = actor_owner_key(&payload.binding, &payload.key);
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
        let replay: Vec<crate::ops::ActorTransportCloseReplayEvent> = events
            .into_iter()
            .map(|event| crate::ops::ActorTransportCloseReplayEvent {
                code: event.code,
                reason: event.reason,
            })
            .collect();
        let _ = payload.reply.send(Ok(replay));
    }
}
