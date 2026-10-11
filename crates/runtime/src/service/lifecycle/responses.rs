//! Response streams and cache revalidation.

use super::*;
use crate::service::dispatch::estimate_pending_invoke_bytes;

impl WorkerManager {
    pub(crate) fn handle_response_start(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        status: u16,
        headers: Vec<(String, String)>,
    ) {
        let Some(registration) = self.stream_registrations.get_mut(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            return;
        }
        registration.started = true;
        if let Some(ready) = registration.ready.take() {
            if let Some((body, completion)) = registration.body_receiver.take() {
                let _ = ready.send(Ok(WorkerStreamOutput {
                    status,
                    headers,
                    body: WorkerStreamBody::new(body, completion),
                }));
            } else {
                let _ = ready.send(Err(PlatformError::internal("stream body receiver missing")));
            }
        }
    }

    pub(crate) fn handle_response_chunk(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        chunk: Bytes,
        event_tx: &RuntimeEventSender,
        reply: oneshot::Sender<Result<()>>,
    ) {
        let send = {
            let Some(registration) = self.stream_registrations.get_mut(request_id) else {
                let _ = reply.send(Err(PlatformError::internal(
                    "stream registration is unavailable",
                )));
                return;
            };
            if registration.worker_name != worker_name {
                let _ = reply.send(Err(PlatformError::internal(
                    "stream registration worker mismatch",
                )));
                return;
            }
            if registration.completion_token.as_deref() != Some(completion_token) {
                let _ = reply.send(Err(PlatformError::internal(
                    "stream registration completion token mismatch",
                )));
                return;
            }
            let next_bytes = registration.bytes_sent.saturating_add(chunk.len());
            if next_bytes > registration.max_bytes {
                Err(PlatformError::runtime(format!(
                    "response body exceeded max_response_body_bytes ({} bytes)",
                    registration.max_bytes
                )))
            } else {
                registration.bytes_sent = next_bytes;
                Ok(registration.body_sender.clone())
            }
        };

        let sender = match send {
            Ok(sender) => sender,
            Err(error) => {
                self.fail_stream_registration(worker_name, request_id, error.clone());
                self.cancel_invoke(worker_name.to_string(), request_id.to_string(), event_tx);
                let _ = reply.send(Err(error));
                return;
            }
        };

        match sender.try_send(chunk) {
            Ok(()) => {
                let _ = reply.send(Ok(()));
            }
            Err(mpsc::error::TrySendError::Full(chunk)) => {
                // The producer waits for this acknowledgment before emitting another chunk.
                let task = tokio::spawn(async move {
                    let result = sender
                        .send(chunk)
                        .await
                        .map_err(|_| PlatformError::internal("stream response receiver closed"));
                    let _ = reply.send(result);
                });
                self.stream_registrations
                    .get_mut(request_id)
                    .expect("validated stream registration must exist")
                    .pending_send = Some(task);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let error = PlatformError::internal("stream response receiver closed");
                self.fail_stream_registration(worker_name, request_id, error.clone());
                self.cancel_invoke(worker_name.to_string(), request_id.to_string(), event_tx);
                let _ = reply.send(Err(error));
            }
        }
    }

    pub(crate) fn schedule_cache_revalidate(
        &mut self,
        worker_name: &str,
        generation: u64,
        request: CacheRevalidatePayload,
        event_tx: &RuntimeEventSender,
    ) {
        let Some(entry) = self.workers.get(worker_name) else {
            return;
        };
        if entry.current_generation != generation {
            return;
        }

        if request.cache_name.trim().is_empty()
            || request.method.trim().is_empty()
            || request.url.trim().is_empty()
        {
            warn!(
                worker = %worker_name,
                generation,
                "ignoring invalid cache revalidate payload"
            );
            return;
        }

        let method = request.method.trim().to_ascii_uppercase();
        if method != "GET" {
            return;
        }
        let revalidate_span = tracing::debug_span!(
            "runtime.cache.revalidate_schedule",
            worker.name = %worker_name,
            worker.generation = generation,
            cache.name = %request.cache_name,
            http.method = %method,
            http.url = %request.url
        );
        set_span_parent_from_traceparent(
            &revalidate_span,
            traceparent_from_headers(&request.headers),
        );
        let _revalidate_guard = revalidate_span.enter();

        let key = cache_revalidation_key(worker_name, generation, &request);
        if !self.revalidation_keys.insert(key.clone()) {
            tracing::debug!("skipping duplicate cache revalidation");
            return;
        }

        let runtime_request_id = Uuid::new_v4().to_string();
        let request_id = format!("cache-revalidate-{runtime_request_id}");
        let mut headers = request.headers.clone();
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-dd-cache-bypass-stale"))
        {
            headers.push(("x-dd-cache-bypass-stale".to_string(), "1".to_string()));
        }

        let invocation = WorkerInvocation {
            method,
            url: request.url,
            headers,
            body: Vec::new(),
            request_id,
        };
        let (reply, _receiver) = oneshot::channel::<Result<WorkerOutput>>();
        let warn_thresholds = self.config.queue_warn_thresholds.clone();
        let queued_bytes = estimate_pending_invoke_bytes(&invocation, false);
        let admission_error = self
            .workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&generation))
            .and_then(|pool| self.queue_admission_error(pool, false));
        if let Some(error) = admission_error {
            self.revalidation_keys.remove(&key);
            warn!(
                worker = %worker_name,
                generation,
                error = %error,
                "skipping cache revalidation because worker queue is overloaded"
            );
            return;
        }
        let queue_admission = match self.admission.reserve_queue(queued_bytes, false) {
            Ok(admission) => Some(admission),
            Err(error) => {
                self.revalidation_keys.remove(&key);
                warn!(error = %error, "cache revalidation queue is overloaded");
                return;
            }
        };
        if let Some(pool) = self.get_pool_mut(worker_name, generation) {
            pool.queue.push_back(PendingInvoke {
                queue_admission,
                runtime_request_id: runtime_request_id.clone(),
                request: invocation,
                request_body: None,
                memory_route: None,
                memory_call: None,
                target_isolate_id: None,
                internal_origin: false,
                reply: reply.into(),
                reply_kind: PendingReplyKind::Normal,
                enqueued_at: Instant::now(),
                queued_bytes,
            });
            pool.update_queue_warning(&warn_thresholds);
            self.account_queued_pending(queued_bytes);
            self.revalidation_requests.insert(runtime_request_id, key);
            self.dispatch_pool(worker_name, generation, event_tx);
            tracing::debug!("scheduled background cache revalidation");
        } else {
            self.revalidation_keys.remove(&key);
        }
    }

    pub(crate) fn clear_revalidation_for_request(&mut self, request_id: &str) {
        if let Some(key) = self.revalidation_requests.remove(request_id) {
            self.revalidation_keys.remove(&key);
        }
    }

    pub(crate) fn fail_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        error: PlatformError,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if let Some(ready) = registration.ready.take() {
            let _ = ready.send(Err(error));
            return;
        }
        if let Some(completion) = registration.completion.take() {
            let _ = completion.send(Err(error));
        }
    }

    pub(crate) fn complete_stream_registration(
        &mut self,
        worker_name: &str,
        request_id: &str,
        completion_token: &str,
        result: Result<WorkerOutput>,
    ) {
        let Some(mut registration) = self.stream_registrations.remove(request_id) else {
            return;
        };
        if registration.worker_name != worker_name {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }
        if registration.completion_token.as_deref() != Some(completion_token) {
            self.stream_registrations
                .insert(request_id.to_string(), registration);
            return;
        }

        let completion_result = match result {
            Ok(_) if registration.started => Ok(()),
            Ok(_) => Err(PlatformError::internal(format!(
                "worker {worker_name} completed streamed request {request_id} without response headers"
            ))),
            Err(error) => Err(error),
        };
        if let Some(ready) = registration.ready.take() {
            let error = completion_result
                .as_ref()
                .expect_err("unstarted stream must fail")
                .clone();
            let _ = ready.send(Err(error));
        }
        if let Some(completion) = registration.completion.take() {
            let _ = completion.send(completion_result);
        }
    }

    pub(super) fn fail_all_streams_for_worker(&mut self, worker_name: &str, error: PlatformError) {
        let request_ids: Vec<String> = self
            .stream_registrations
            .iter()
            .filter(|(_, registration)| registration.worker_name == worker_name)
            .map(|(request_id, _)| request_id.clone())
            .collect();

        for request_id in request_ids {
            self.fail_stream_registration(worker_name, &request_id, error.clone());
        }
    }
}
