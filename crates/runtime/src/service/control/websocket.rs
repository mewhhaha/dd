//! Websocket frame delivery and waiters.

use super::*;
use tracing::warn;

impl WorkerManager {
    pub(super) fn enqueue_websocket_frame(
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
                reply: reply.into(),
                reply_kind: PendingReplyKind::WebsocketFrame {
                    session_id: session_id.to_string(),
                },
            },
            event_tx,
        );
    }

    pub(super) fn close_websocket(
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
        let (reply, receiver) = oneshot::channel::<Result<WorkerOutput>>();
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
                reply: reply.into(),
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

    pub(super) fn wait_websocket_frame(
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

    pub(in crate::service) fn notify_websocket_frame_waiters(&mut self, session_id: &str) {
        if let Some(waiters) = self.websocket_frame_waiters.remove(session_id) {
            for waiter in waiters {
                let _ = waiter.send(Ok(()));
            }
        }
    }

    pub(in crate::service) fn fail_websocket_frame_waiters(
        &mut self,
        session_id: &str,
        error: PlatformError,
    ) {
        if let Some(waiters) = self.websocket_frame_waiters.remove(session_id) {
            for waiter in waiters {
                let _ = waiter.send(Err(error.clone()));
            }
        }
    }

    pub(super) fn drain_websocket_frame(
        &mut self,
        worker_name: &str,
        session_id: &str,
    ) -> Result<Option<WebSocketFrameOutput>> {
        let Some(session) = self.websocket_sessions.get(session_id) else {
            return Err(PlatformError::not_found("websocket session not found"));
        };
        if session.worker_name != worker_name {
            return Err(PlatformError::bad_request(
                "websocket session worker mismatch",
            ));
        }

        let mut output = WebSocketFrameOutput::from(WorkerOutput {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
        });
        let mut has_output = false;

        if let Some(frame) = self
            .websocket_outbound_frames
            .get_mut(session_id)
            .and_then(|queue| queue.pop_front())
        {
            has_output = true;
            output.output.body = frame.payload;
            output.lease = frame.lease;
            if frame.is_binary {
                append_or_update_header(&mut output.output.headers, INTERNAL_WS_BINARY_HEADER, "1");
            }
        }

        if !has_output && let Some(close) = self.websocket_close_signals.remove(session_id) {
            has_output = true;
            append_or_update_header(
                &mut output.output.headers,
                INTERNAL_WS_CLOSE_CODE_HEADER,
                close.code.to_string().as_str(),
            );
            append_or_update_header(
                &mut output.output.headers,
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

    pub(in crate::service) fn complete_websocket_frame(
        &mut self,
        session_id: String,
        reply: Option<InvocationReply>,
        result: Result<WorkerOutput>,
        wait_for_outbox_frame: bool,
    ) {
        let Some(reply) = reply else {
            return;
        };
        match result {
            Ok(output) => {
                let mut output = WebSocketFrameOutput::from(output);
                output.output.headers =
                    strip_websocket_frame_internal_headers(&output.output.headers);
                let mut has_outbox_output = false;
                if let Some(frame) = self
                    .websocket_outbound_frames
                    .get_mut(&session_id)
                    .and_then(|queue| queue.pop_front())
                {
                    has_outbox_output = true;
                    output.output.body = frame.payload;
                    output.lease = frame.lease;
                    if frame.is_binary {
                        append_or_update_header(
                            &mut output.output.headers,
                            INTERNAL_WS_BINARY_HEADER,
                            "1",
                        );
                    } else {
                        output.output.headers.retain(|(name, _)| {
                            !name.eq_ignore_ascii_case(INTERNAL_WS_BINARY_HEADER)
                        });
                    }
                }
                if !has_outbox_output
                    && let Some(close) = self.websocket_close_signals.remove(&session_id)
                {
                    has_outbox_output = true;
                    append_or_update_header(
                        &mut output.output.headers,
                        INTERNAL_WS_CLOSE_CODE_HEADER,
                        close.code.to_string().as_str(),
                    );
                    append_or_update_header(
                        &mut output.output.headers,
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
                let _ = reply.send_websocket(Ok(output));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    pub(in crate::service) fn flush_pending_websocket_frame_replies(&mut self, session_id: &str) {
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
                output.output.body = frame.payload;
                output.lease = frame.lease;
                if frame.is_binary {
                    append_or_update_header(
                        &mut output.output.headers,
                        INTERNAL_WS_BINARY_HEADER,
                        "1",
                    );
                } else {
                    output
                        .output
                        .headers
                        .retain(|(name, _)| !name.eq_ignore_ascii_case(INTERNAL_WS_BINARY_HEADER));
                }
            }
            if !has_output && let Some(close) = self.websocket_close_signals.remove(session_id) {
                has_output = true;
                append_or_update_header(
                    &mut output.output.headers,
                    INTERNAL_WS_CLOSE_CODE_HEADER,
                    close.code.to_string().as_str(),
                );
                append_or_update_header(
                    &mut output.output.headers,
                    INTERNAL_WS_CLOSE_REASON_HEADER,
                    &close.reason,
                );
            }
            if has_output {
                let _ = reply.send_websocket(Ok(output));
            } else {
                self.websocket_pending_frame_replies
                    .entry(session_id.to_string())
                    .or_default()
                    .push(WebSocketFrameReply { output, reply });
            }
        }
    }
}
