//! Runtime command and event handlers.

use super::*;

impl WorkerManager {
    pub(in crate::service) async fn handle_command(
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
                        reply: reply.into(),
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
                        reply: reply.into(),
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
                cancellation,
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
                    cancellation,
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
                let (inner_tx, _inner_rx) = oneshot::channel::<Result<WorkerOutput>>();
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
                        reply: inner_tx.into(),
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

    pub(in crate::service) async fn handle_event(
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
            RuntimeEvent::MemorySocketClose(payload) => {
                self.handle_memory_socket_close(payload, event_tx);
            }
            #[cfg(test)]
            RuntimeEvent::TestAsyncReply(payload) => {
                self.handle_test_async_reply(payload, event_tx);
            }
            #[cfg(test)]
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
}
