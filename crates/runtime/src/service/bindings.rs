use super::*;

impl WorkerManager {
    pub(super) fn start_service_binding_fetch(&mut self, fetch: ServiceBindingFetchStart) {
        let ServiceBindingFetchStart {
            reply_inbox,
            queue_admission,
            owner_worker,
            owner_generation,
            binding,
            target_worker,
            request,
            reply_id,
            pending_replies,
        } = fetch;
        let allowed_target = self
            .workers
            .get(&owner_worker)
            .and_then(|entry| entry.pools.get(&owner_generation))
            .and_then(|pool| pool.request_context.service_bindings.get(binding.trim()))
            .cloned();
        match allowed_target {
            Some(allowed_target) if allowed_target == target_worker => {}
            Some(_) => {
                pending_replies.finish_into(
                    reply_id,
                    crate::ops::PendingReplyPayload::Fetch {
                        result: Err(PlatformError::runtime(format!(
                            "service binding target mismatch: {binding}"
                        ))),
                        boundary: None,
                    },
                    &reply_inbox,
                );
                return;
            }
            None => {
                pending_replies.finish_into(
                    reply_id,
                    crate::ops::PendingReplyPayload::Fetch {
                        result: Err(PlatformError::runtime(format!(
                            "service binding is not allowed: {binding}"
                        ))),
                        boundary: None,
                    },
                    &reply_inbox,
                );
                return;
            }
        }

        let runtime_request_id = next_runtime_token("svc");
        let (inner_reply_tx, inner_reply_rx) = oneshot::channel();
        if let Err(error) = self
            .runtime_fast_sender
            .try_send(RuntimeCommand::InvokeInternal(EnqueueInvokeRequest {
                queue_admission,
                worker_name: target_worker,
                runtime_request_id,
                request,
                request_body: None,
                memory_route: None,
                memory_call: None,
                target_isolate_id: None,
                target_generation: None,
                internal_origin: true,
                reply: inner_reply_tx,
                reply_kind: PendingReplyKind::Normal,
            }))
        {
            error.into_inner().reject(PlatformError::overloaded(
                "service worker command queue is full",
            ));
        }

        let wall_timeout = self.config.request_wall_timeout;
        tokio::spawn(async move {
            let result = match tokio::time::timeout(wall_timeout, inner_reply_rx).await {
                Ok(Ok(output)) => output,
                Ok(Err(_)) => Err(PlatformError::internal(
                    "service binding invoke response channel closed",
                )),
                Err(_) => Err(PlatformError::runtime(format!(
                    "service binding invoke timed out after {}ms",
                    wall_timeout.as_millis()
                ))),
            };
            pending_replies.finish_into(
                reply_id,
                crate::ops::PendingReplyPayload::Fetch {
                    result,
                    boundary: Some(crate::ops::current_time_boundary()),
                },
                &reply_inbox,
            );
        });
    }
}
