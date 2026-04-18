use super::*;

impl WorkerPool {
    pub(super) fn debug_dump(&self) -> WorkerDebugDump {
        let mut memory_owners = self
            .memory_owners
            .iter()
            .map(|(key, isolate_id)| (key.clone(), *isolate_id))
            .collect::<Vec<_>>();
        memory_owners.sort_by(|left, right| left.0.cmp(&right.0));

        let mut memory_inflight = self
            .memory_inflight
            .iter()
            .map(|(key, count)| (key.clone(), *count))
            .collect::<Vec<_>>();
        memory_inflight.sort_by(|left, right| left.0.cmp(&right.0));

        let queued_requests = self
            .queue
            .iter()
            .map(|pending| WorkerDebugRequest {
                runtime_request_id: pending.runtime_request_id.clone(),
                user_request_id: pending.request.request_id.clone(),
                method: pending.request.method.clone(),
                url: pending.request.url.clone(),
                memory_key: pending.memory_route.as_ref().map(MemoryRoute::owner_key),
                target_isolate_id: pending.target_isolate_id,
                internal_origin: pending.internal_origin,
                reply_kind: pending.reply_kind.label().to_string(),
                host_rpc_target_id: pending
                    .host_rpc_call
                    .as_ref()
                    .map(|call| call.target_id.clone()),
                host_rpc_method: pending
                    .host_rpc_call
                    .as_ref()
                    .map(|call| call.method.clone()),
            })
            .collect::<Vec<_>>();

        let isolates = self
            .isolates
            .iter()
            .map(|isolate| {
                let mut pending_requests = isolate
                    .pending_replies
                    .iter()
                    .map(|(runtime_request_id, pending)| {
                        let meta = pending.completion_meta.as_ref();
                        WorkerDebugRequest {
                            runtime_request_id: runtime_request_id.clone(),
                            user_request_id: meta
                                .map(|meta| meta.user_request_id.clone())
                                .unwrap_or_default(),
                            method: meta.map(|meta| meta.method.clone()).unwrap_or_default(),
                            url: meta.map(|meta| meta.url.clone()).unwrap_or_default(),
                            memory_key: pending.memory_key.clone(),
                            target_isolate_id: Some(isolate.id),
                            internal_origin: pending.internal_origin,
                            reply_kind: pending.kind.label().to_string(),
                            host_rpc_target_id: None,
                            host_rpc_method: None,
                        }
                    })
                    .collect::<Vec<_>>();
                pending_requests
                    .sort_by(|left, right| left.runtime_request_id.cmp(&right.runtime_request_id));
                WorkerDebugIsolate {
                    id: isolate.id,
                    inflight_count: isolate.inflight_count,
                    pending_wait_until: isolate.pending_wait_until.len(),
                    active_websocket_sessions: isolate.active_websocket_sessions,
                    active_transport_sessions: isolate.active_transport_sessions,
                    pending_requests,
                }
            })
            .collect::<Vec<_>>();

        WorkerDebugDump {
            generation: self.generation,
            queued: self.queue.len(),
            memory_owners,
            memory_inflight,
            isolates,
            queued_requests,
        }
    }

    pub(super) fn log_stats(&self, event: &str) {
        if !tracing::enabled!(Level::INFO) {
            return;
        }
        let snapshot = self.stats_snapshot();
        info!(
            worker = %self.worker_name,
            generation = snapshot.generation,
            public = snapshot.public,
            deployment_id = %self.deployment_id,
            queued = snapshot.queued,
            busy = snapshot.busy,
            inflight_total = snapshot.inflight_total,
            wait_until_total = snapshot.wait_until_total,
            isolates_total = snapshot.isolates_total,
            spawn_count = snapshot.spawn_count,
            reuse_count = snapshot.reuse_count,
            scale_down_count = snapshot.scale_down_count,
            event,
            "worker pool stats"
        );
    }
}
