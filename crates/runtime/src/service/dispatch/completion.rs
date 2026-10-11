//! Stream registration, request completion, and trace forwarding.

use super::*;

impl WorkerManager {
    pub(crate) fn register_stream(
        &mut self,
        worker_name: String,
        runtime_request_id: String,
        ready: oneshot::Sender<Result<WorkerStreamOutput>>,
    ) {
        let (body_sender, body_receiver) = mpsc::channel(RESPONSE_STREAM_CHANNEL_CAPACITY);
        let (completion, completed) = oneshot::channel();
        self.stream_registrations.insert(
            runtime_request_id,
            StreamRegistration {
                worker_name,
                completion_token: None,
                ready: Some(ready),
                body_sender,
                body_receiver: Some((body_receiver, completed)),
                completion: Some(completion),
                pending_send: None,
                started: false,
                bytes_sent: 0,
                max_bytes: self.config.max_response_body_bytes,
            },
        );
    }

    pub(crate) async fn finish_request(
        &mut self,
        finish: FinishRequest,
        event_tx: &RuntimeEventSender,
    ) {
        let FinishRequest {
            worker_name,
            generation,
            isolate_id,
            request_id,
            completion_token,
            finished_at,
            wait_until_count,
            mut result,
        } = finish;
        let completion_started_at = Instant::now();
        let worker_name = worker_name.as_str();
        let request_id = request_id.as_str();
        let completion_token = completion_token.as_str();
        let mut reply = None;
        let mut canceled = false;
        let mut clear_revalidation = false;
        let mut completion_traceparent: Option<String> = None;
        let mut user_request_id = String::new();
        let mut request_method = String::new();
        let mut request_url = String::new();
        let mut execution_ms: Option<u64> = None;
        let mut internal_origin = false;
        let mut pending_kind = PendingReplyKind::Normal;
        let mut memory_outbox_shard = None;
        let mut atomic_execution_duration = None;
        let mut atomic_completion_wait = None;
        let trace_destination = self
            .get_pool_mut(worker_name, generation)
            .and_then(|pool| pool.internal_trace.clone());
        let debug_tracing_enabled = tracing::enabled!(Level::DEBUG);
        let stream_registered = self.stream_registrations.contains_key(request_id);
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            if let Some(isolate) = pool
                .isolates
                .iter_mut()
                .find(|isolate| isolate.id == isolate_id)
            {
                let Some(pending) = isolate.pending_replies.get(request_id) else {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion for unknown request id"
                    );
                    return;
                };
                let token_mismatch = pending.completion_token != completion_token;
                if token_mismatch {
                    warn!(
                        worker = %worker_name,
                        generation,
                        isolate_id,
                        request_id,
                        "dropping completion with invalid token"
                    );
                }

                isolate.inflight_count = isolate.inflight_count.saturating_sub(1);
                if isolate.inflight_count == 0 {
                    isolate.last_used_at = Instant::now();
                }
                let mut active_memory_lease = None;
                if let Some(pending) = isolate.pending_replies.remove(request_id) {
                    canceled = pending.canceled;
                    internal_origin = pending.internal_origin;
                    active_memory_lease = pending.active_memory_lease.clone();
                    memory_outbox_shard = pending.memory_outbox_shard;
                    if let Some(meta) = pending.completion_meta {
                        request_method = meta.method;
                        request_url = meta.url;
                        completion_traceparent = meta.traceparent;
                        user_request_id = meta.user_request_id;
                    }
                    if token_mismatch {
                        result = Err(PlatformError::internal(format!(
                            "completion token mismatch for runtime request {request_id}"
                        )));
                    } else if wait_until_count > 0 {
                        isolate.pending_wait_until.insert(
                            request_id.to_string(),
                            PendingWaitUntil {
                                completion_token: completion_token.to_string(),
                                completed_at: finished_at,
                            },
                        );
                    }
                    clear_revalidation = true;
                    execution_ms = Some(pending.dispatched_at.elapsed().as_millis() as u64);
                    if pending.active_memory_lease.is_some() {
                        atomic_execution_duration =
                            finished_at.checked_duration_since(pending.dispatched_at);
                        atomic_completion_wait =
                            completion_started_at.checked_duration_since(finished_at);
                    }
                    pending_kind = pending.kind;
                    reply = Some(pending.reply);
                }
                if let Some(active_memory_lease) = active_memory_lease {
                    pool.release_memory_entity_lease(&active_memory_lease);
                }
            }
            pool.log_stats("complete");
        }
        if let Some(execution_duration) = atomic_execution_duration {
            self.memory_store.record_profile(
                MemoryProfileMetricKind::RuntimeSocketExecution,
                duration_us(execution_duration),
                1,
            );
        }
        if let Some(completion_wait) = atomic_completion_wait {
            self.memory_store.record_profile(
                MemoryProfileMetricKind::RuntimeSocketCompletionWait,
                duration_us(completion_wait),
                1,
            );
        }
        if clear_revalidation {
            self.clear_revalidation_for_request(request_id);
        }
        if !stream_registered
            && let Ok(output) = &result
            && output.body.len() > self.config.max_response_body_bytes
        {
            result = Err(PlatformError::runtime(format!(
                "response body exceeded max_response_body_bytes ({} bytes)",
                self.config.max_response_body_bytes
            )));
        }
        let complete_span = if debug_tracing_enabled {
            let result_status = match &result {
                Ok(output) => output.status as i64,
                Err(_) => -1,
            };
            let result_ok = result.is_ok();
            let span = tracing::debug_span!(
                "runtime.complete",
                worker.name = %worker_name,
                worker.generation = generation,
                isolate.id = isolate_id,
                runtime.request_id = %request_id,
                request.id = %user_request_id,
                request.ok = result_ok,
                response.status = result_status,
                request.execution_ms = execution_ms.unwrap_or_default(),
                request.wait_until_count = wait_until_count as u64
            );
            set_span_parent_from_traceparent(&span, completion_traceparent.as_deref());
            Some(span)
        } else {
            None
        };
        async move {
            let stream_consumes_result =
                stream_registered && matches!(&pending_kind, PendingReplyKind::Stream);
            let mut stream_result = if stream_registered && !stream_consumes_result {
                Some(result.clone())
            } else {
                None
            };
            let trace_result = if trace_destination.is_some() {
                Some(match &result {
                    Ok(output) => TraceResultMeta {
                        status: Some(output.status),
                        error: None,
                    },
                    Err(error) => TraceResultMeta {
                        status: None,
                        error: Some(error.to_string()),
                    },
                })
            } else {
                None
            };

            if stream_consumes_result {
                stream_result = Some(result);
                if canceled {
                    if debug_tracing_enabled {
                        tracing::debug!(
                            worker = %worker_name,
                            generation,
                            isolate_id,
                            request_id,
                            "dropped completion for canceled request"
                        );
                    }
                } else if debug_tracing_enabled {
                    tracing::debug!("request completion delivered");
                }
            } else if !canceled {
                match pending_kind {
                    PendingReplyKind::Normal | PendingReplyKind::Stream => {
                        if let Some(reply) = reply {
                            let _ = reply.send(result);
                        }
                    }
                    PendingReplyKind::WebsocketOpen { session_id } => {
                        self.complete_websocket_open(
                            worker_name,
                            generation,
                            isolate_id,
                            session_id,
                            result,
                        );
                    }
                    PendingReplyKind::WebsocketFrame { session_id } => {
                        self.complete_websocket_frame(
                            session_id,
                            reply,
                            result,
                            memory_outbox_shard.is_some(),
                        );
                    }
                }
                if debug_tracing_enabled {
                    tracing::debug!("request completion delivered");
                }
            } else if debug_tracing_enabled {
                tracing::debug!(
                    worker = %worker_name,
                    generation,
                    isolate_id,
                    request_id,
                    "dropped completion for canceled request"
                );
            }
            if let Some(shard_index) = memory_outbox_shard {
                self.schedule_memory_outbox_drain_shard(shard_index, event_tx);
            }
            if let (Some(trace_destination), Some(trace_result)) = (trace_destination, trace_result)
            {
                self.enqueue_trace_forward(TraceForwardRequest {
                    worker_name: worker_name.to_string(),
                    generation,
                    request_method,
                    request_url,
                    runtime_request_id: request_id.to_string(),
                    user_request_id,
                    result: trace_result,
                    execution_ms: execution_ms.unwrap_or_default(),
                    wait_until_count,
                    internal_origin,
                    trace_destination: Some(trace_destination),
                });
            }
            if let Some(stream_result) = stream_result {
                self.complete_stream_registration(
                    worker_name,
                    request_id,
                    completion_token,
                    stream_result,
                );
            }
        }
        .instrument(complete_span.unwrap_or_else(tracing::Span::none))
        .await;
    }

    fn enqueue_trace_forward(&mut self, forward: TraceForwardRequest) {
        let TraceForwardRequest {
            worker_name,
            generation,
            request_method,
            request_url,
            runtime_request_id,
            user_request_id,
            result,
            execution_ms,
            wait_until_count,
            internal_origin,
            trace_destination,
        } = forward;
        let worker_name = worker_name.as_str();
        let request_method = request_method.as_str();
        let request_url = request_url.as_str();
        let runtime_request_id = runtime_request_id.as_str();
        let user_request_id = user_request_id.as_str();
        let Some(trace_destination) = trace_destination else {
            return;
        };
        if internal_origin {
            return;
        }

        let payload = TraceEventPayload {
            ts_ms: match epoch_ms_i64() {
                Ok(ts_ms) => ts_ms,
                Err(error) => {
                    warn!(
                        worker = %worker_name,
                        generation,
                        runtime_request_id,
                        error = %error,
                        "skipping trace forward due invalid clock"
                    );
                    return;
                }
            },
            worker: worker_name.to_string(),
            generation,
            request_id: user_request_id.to_string(),
            runtime_request_id: runtime_request_id.to_string(),
            method: request_method.to_string(),
            url: request_url.to_string(),
            status: result.status,
            ok: result.status.is_some(),
            error: result.error,
            execution_ms,
            wait_until_count,
        };

        // Use serde_json for trace forwarding payloads to avoid simd-json's
        // mutable-buffer serialization path on this hot async boundary.
        let body = match serde_json::to_vec(&payload) {
            Ok(body) => body,
            Err(error) => {
                warn!(
                    worker = %worker_name,
                    generation,
                    runtime_request_id,
                    error = %error,
                    "skipping trace forward due payload serialization failure"
                );
                return;
            }
        };
        let mut headers = vec![(
            CONTENT_TYPE_HEADER.to_string(),
            JSON_CONTENT_TYPE.to_string(),
        )];
        append_internal_trace_headers(&mut headers, worker_name, generation);
        let trace_request = WorkerInvocation {
            method: "POST".to_string(),
            url: format!(
                "http://{}{}",
                trace_destination.worker,
                normalize_trace_path(&trace_destination.path)
            ),
            headers,
            body,
            request_id: Uuid::new_v4().to_string(),
        };
        let (reply, reply_rx) = oneshot::channel::<Result<WorkerOutput>>();
        if let Err(error) = self
            .runtime_fast_sender
            .try_send(RuntimeCommand::InvokeInternal(EnqueueInvokeRequest {
                queue_admission: None,
                worker_name: trace_destination.worker,
                runtime_request_id: Uuid::new_v4().to_string(),
                request: trace_request,
                request_body: None,
                memory_route: None,
                memory_call: None,
                target_isolate_id: None,
                target_generation: None,
                internal_origin: true,
                reply: reply.into(),
                reply_kind: PendingReplyKind::Normal,
            }))
        {
            error.into_inner().reject(PlatformError::overloaded(
                "trace worker command queue is full",
            ));
        }
        let request_id_for_warning = runtime_request_id.to_string();
        tokio::spawn(async move {
            match reply_rx.await {
                Ok(Ok(output)) => {
                    if !matches!(output.status, 200..=299) {
                        warn!(
                            request_id = %request_id_for_warning,
                            status = output.status,
                            "internal trace forward responded non-2xx"
                        );
                    }
                }
                Ok(Err(error)) => {
                    warn!(error = %error, "internal trace forward failed");
                }
                Err(error) => {
                    warn!(error = %error, "internal trace forward receiver dropped");
                }
            }
        });
    }
}
