//! Worker invocation, response streams and WebSocket operations.

use super::*;

impl RuntimeService {
    pub async fn invoke(
        &self,
        worker_name: String,
        request: WorkerInvocation,
    ) -> Result<WorkerOutput> {
        self.invoke_with_request_body(worker_name, request, None)
            .await
    }

    pub async fn invoke_with_request_body(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
    ) -> Result<WorkerOutput> {
        let runtime_request_id = next_runtime_token("req");
        let invoke_span = if tracing::enabled!(Level::DEBUG) {
            let span = tracing::debug_span!(
                "runtime.invoke",
                worker.name = %worker_name,
                runtime.request_id = %runtime_request_id,
                request.id = %request.request_id
            );
            set_span_parent_from_traceparent(&span, traceparent_from_headers(&request.headers));
            Some(span)
        } else {
            None
        };
        async move {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.sender
                .send(RuntimeCommand::Invoke {
                    worker_name: worker_name.clone(),
                    runtime_request_id: runtime_request_id.clone(),
                    request,
                    request_body,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

            let mut cancel_guard =
                InvokeCancelGuard::new(self.cancel_sender.clone(), worker_name, runtime_request_id);
            let reply = reply_rx.await;
            cancel_guard.disarm();
            reply.map_err(|_| PlatformError::internal("runtime invoke channel closed"))?
        }
        .instrument(invoke_span.unwrap_or_else(tracing::Span::none))
        .await
    }

    pub async fn invoke_stream(
        &self,
        worker_name: String,
        request: WorkerInvocation,
    ) -> Result<WorkerStreamOutput> {
        self.invoke_stream_with_request_body(worker_name, request, None)
            .await
    }

    pub async fn invoke_stream_with_request_body(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
    ) -> Result<WorkerStreamOutput> {
        let runtime_request_id = next_runtime_token("req");
        let stream_span = if tracing::enabled!(Level::DEBUG) {
            let span = tracing::debug_span!(
                "runtime.invoke_stream",
                worker.name = %worker_name,
                runtime.request_id = %runtime_request_id,
                request.id = %request.request_id
            );
            set_span_parent_from_traceparent(&span, traceparent_from_headers(&request.headers));
            Some(span)
        } else {
            None
        };
        async move {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (reply_tx, _reply_rx) = oneshot::channel();
            self.sender
                .send(RuntimeCommand::InvokeStream {
                    worker_name: worker_name.clone(),
                    runtime_request_id: runtime_request_id.clone(),
                    request,
                    request_body,
                    ready: ready_tx,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
            let mut cancel_guard =
                InvokeCancelGuard::new(self.cancel_sender.clone(), worker_name, runtime_request_id);

            let ready = ready_rx
                .await
                .map_err(|_| PlatformError::internal("runtime stream channel closed"))?;
            match ready {
                Ok(mut output) => {
                    output.body.attach_cancel_guard(cancel_guard);
                    Ok(output)
                }
                Err(error) => {
                    cancel_guard.disarm();
                    Err(error)
                }
            }
        }
        .instrument(stream_span.unwrap_or_else(tracing::Span::none))
        .await
    }

    pub async fn open_websocket(
        &self,
        worker_name: String,
        request: WorkerInvocation,
        request_body: Option<InvokeRequestBodyReceiver>,
    ) -> Result<WebSocketOpen> {
        let runtime_request_id = next_runtime_token("ws");
        let session_id = Uuid::new_v4().to_string();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::OpenWebsocket {
                worker_name: worker_name.clone(),
                runtime_request_id: runtime_request_id.clone(),
                request,
                request_body,
                session_id: session_id.clone(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        let mut cancel_guard =
            InvokeCancelGuard::new(self.cancel_sender.clone(), worker_name, runtime_request_id);
        let reply = reply_rx.await;
        cancel_guard.disarm();
        let mut opened = reply
            .map_err(|_| PlatformError::internal("runtime open websocket channel closed"))??;
        opened.session_id = session_id;
        Ok(opened)
    }

    pub async fn websocket_send_frame(
        &self,
        worker_name: String,
        session_id: String,
        frame: Vec<u8>,
        is_binary: bool,
    ) -> Result<WebSocketFrameOutput> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::SendWebsocketFrame {
                worker_name,
                session_id,
                frame,
                is_binary,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket send channel closed",
            ))
        })
    }

    pub async fn websocket_wait_frame(
        &self,
        worker_name: String,
        session_id: String,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::WaitWebsocketFrame {
                worker_name,
                session_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket wait channel closed",
            ))
        })
    }

    pub async fn websocket_drain_frame(
        &self,
        worker_name: String,
        session_id: String,
    ) -> Result<Option<WebSocketFrameOutput>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::DrainWebsocketFrame {
                worker_name,
                session_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket drain channel closed",
            ))
        })
    }

    pub async fn websocket_close(
        &self,
        worker_name: String,
        session_id: String,
        close_code: u16,
        close_reason: String,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(RuntimeCommand::CloseWebsocket {
                worker_name,
                session_id,
                close_code,
                close_reason,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PlatformError::internal("runtime thread is not available"))?;

        reply_rx.await.unwrap_or_else(|_| {
            Err(PlatformError::internal(
                "runtime websocket close channel closed",
            ))
        })
    }
}
