//! Worker manager setup, batching, and shutdown.

use super::*;

mod handling;
mod messages;
#[cfg(test)]
mod test_hooks;
mod websocket;

pub(super) use messages::RuntimeEvent;
pub(crate) use messages::{RuntimeCommand, RuntimeFastCommandSender};

impl WorkerManager {
    pub(super) fn new(init: WorkerManagerInit) -> Self {
        let WorkerManagerInit {
            bootstrap_snapshot,
            console,
            inspector,
            kv_store,
            memory_store,
            memory_outbox_drain_sender,
            cache_store,
            config,
            control_store,
            module_registry,
            runtime_fast_sender,
            asset_catalog,
            admission,
        } = init;
        let response_byte_budget = Arc::clone(&admission.response_bytes);
        let next_memory_entity_epoch = memory_store.owner_epoch_floor();
        Self {
            config,
            control_store,
            module_registry,
            bootstrap_snapshot,
            runtime_fast_sender,
            kv_store,
            memory_store,
            memory_outbox_drain_sender,
            cache_store,
            console,
            inspector,
            asset_catalog,
            workers: HashMap::new(),
            queue_counters: RuntimeQueueCounters::default(),
            next_queue_expiry_at: None,
            pre_canceled: HashMap::new(),
            stream_registrations: HashMap::new(),
            response_byte_budget,
            admission,
            revalidation_keys: HashSet::new(),
            revalidation_requests: HashMap::new(),
            websocket_sessions: HashMap::new(),
            websocket_handle_index: HashMap::new(),
            websocket_open_handles: HashMap::new(),
            open_handle_registry: crate::ops::MemoryOpenHandleRegistry::default(),
            websocket_outbound_frames: HashMap::new(),
            websocket_close_signals: HashMap::new(),
            websocket_frame_waiters: HashMap::new(),
            websocket_pending_frame_replies: HashMap::new(),
            websocket_open_waiters: HashMap::new(),
            runtime_batch_depth: 0,
            pending_dispatches: HashSet::new(),
            pending_cleanup_workers: HashSet::new(),
            pending_memory_outbox_shards: HashSet::new(),
            scale_up_requests: VecDeque::new(),
            scale_up_request_members: HashSet::new(),
            internal_rescue_isolate_slots: HashSet::new(),
            exiting_isolate_slots: HashMap::new(),
            isolate_thread_tracker: IsolateThreadTracker::default(),
            stats: RuntimeManagerStats::default(),
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
            self.process_scale_up_requests(event_tx);
            for worker_name in pending_cleanup_workers {
                self.cleanup_drained_generations_for(&worker_name);
            }
        }
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

    pub(super) async fn shutdown_all(&mut self) -> Result<()> {
        let worker_names = self.workers.keys().cloned().collect::<Vec<_>>();
        for worker_name in worker_names {
            self.reap_owned_sessions(&worker_name, None, None);
        }
        let mut clear_request_ids = Vec::new();
        let mut queued_pending = Vec::new();
        let mut dequeued_count = 0usize;
        let mut dequeued_bytes = 0usize;
        let mut exiting_slots = Vec::new();
        for (worker_name, entry) in &mut self.workers {
            for (generation, pool) in &mut entry.pools {
                while let Some(pending) = pool.queue.pop_front() {
                    dequeued_count = dequeued_count.saturating_add(1);
                    dequeued_bytes = dequeued_bytes.saturating_add(pending.queued_bytes);
                    queued_pending.push((worker_name.clone(), pending));
                }
                pool.clear_isolate_indices();
                for isolate in pool.isolates.drain(..) {
                    isolate.request_shutdown();
                    exiting_slots.push((
                        worker_name.clone(),
                        *generation,
                        isolate.id,
                        isolate.startup,
                    ));
                    for (request_id, pending) in isolate.pending_replies {
                        clear_request_ids.push(request_id);
                        let _ = pending
                            .reply
                            .send(Err(PlatformError::internal("runtime shutting down")));
                    }
                }
            }
        }
        for (worker_name, generation, isolate_id, startup) in exiting_slots {
            self.track_exiting_isolate_slot(&worker_name, generation, isolate_id, startup);
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
            } else if let Some(completion) = registration.completion.take() {
                let _ = completion.send(Err(error));
            }
        }
        for (_, waiter) in std::mem::take(&mut self.websocket_open_waiters) {
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
        self.websocket_outbound_frames.clear();
        self.websocket_close_signals.clear();

        let thread_tracker = self.isolate_thread_tracker.clone();
        tokio::time::timeout(Duration::from_secs(10), async move {
            thread_tracker.wait_for_empty().await;
            Ok(())
        })
        .await
        .map_err(|_| PlatformError::internal("timed out waiting for isolate shutdown"))?
    }
}
