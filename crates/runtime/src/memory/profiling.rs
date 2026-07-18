impl MemoryProfileMetric {
    fn record(&self, duration_us: u64, items: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(duration_us, Ordering::Relaxed);
        self.total_items.fetch_add(items, Ordering::Relaxed);
        let mut current = self.max_us.load(Ordering::Relaxed);
        while duration_us > current {
            match self.max_us.compare_exchange(
                current,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn snapshot(&self) -> MemoryProfileMetricSnapshot {
        MemoryProfileMetricSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            total_us: self.total_us.load(Ordering::Relaxed),
            total_items: self.total_items.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.calls.store(0, Ordering::Relaxed);
        self.total_us.store(0, Ordering::Relaxed);
        self.total_items.store(0, Ordering::Relaxed);
        self.max_us.store(0, Ordering::Relaxed);
    }
}

impl MemoryProfile {
    fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    fn record(&self, metric: MemoryProfileMetricKind, duration_us: u64, items: u64) {
        match metric {
            MemoryProfileMetricKind::StoreSnapshotCacheHit => {
                self.snapshot_cache_hits_total
                    .fetch_add(items.max(1), Ordering::Relaxed);
            }
            MemoryProfileMetricKind::StoreSnapshotCacheMiss => {
                self.snapshot_cache_misses_total
                    .fetch_add(items.max(1), Ordering::Relaxed);
            }
            MemoryProfileMetricKind::StoreSnapshotCacheEviction => {
                self.snapshot_cache_evictions_total
                    .fetch_add(items.max(1), Ordering::Relaxed);
            }
            _ => {}
        }
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        let target = match metric {
            MemoryProfileMetricKind::JsReadOnlyTotal => &self.js_read_only_total,
            MemoryProfileMetricKind::JsHydrateFull => &self.js_hydrate_full,
            MemoryProfileMetricKind::JsHydrateKeys => &self.js_hydrate_keys,
            MemoryProfileMetricKind::JsTxnCommit => &self.js_txn_commit,
            MemoryProfileMetricKind::JsCacheHit => &self.js_cache_hit,
            MemoryProfileMetricKind::JsCacheMiss => &self.js_cache_miss,
            MemoryProfileMetricKind::JsCacheStale => &self.js_cache_stale,
            MemoryProfileMetricKind::OpRead => &self.op_read,
            MemoryProfileMetricKind::OpSnapshot => &self.op_snapshot,
            MemoryProfileMetricKind::OpVersionIfNewer => &self.op_version_if_newer,
            MemoryProfileMetricKind::OpApplyBatch => &self.op_apply_batch,
            MemoryProfileMetricKind::StoreRead => &self.store_read,
            MemoryProfileMetricKind::StoreSnapshot => &self.store_snapshot,
            MemoryProfileMetricKind::StoreSnapshotKeys => &self.store_snapshot_keys,
            MemoryProfileMetricKind::StoreVersionIfNewer => &self.store_version_if_newer,
            MemoryProfileMetricKind::StoreApplyBatch => &self.store_apply_batch,
            MemoryProfileMetricKind::StoreApplyBatchValidate => &self.store_apply_batch_validate,
            MemoryProfileMetricKind::StoreApplyBatchWrite => &self.store_apply_batch_write,
            MemoryProfileMetricKind::StoreDatabaseCacheHit => &self.store_database_cache_hit,
            MemoryProfileMetricKind::StoreDatabaseCacheMiss => &self.store_database_cache_miss,
            MemoryProfileMetricKind::StoreDatabaseCacheEviction => {
                &self.store_database_cache_eviction
            }
            MemoryProfileMetricKind::StoreConnectionCreate => &self.store_connection_create,
            MemoryProfileMetricKind::StoreConnectionReuse => &self.store_connection_reuse,
            MemoryProfileMetricKind::StoreReaderPoolWait => &self.store_reader_pool_wait,
            MemoryProfileMetricKind::StoreWriterLaneWait => &self.store_writer_lane_wait,
            MemoryProfileMetricKind::StoreWriterBusyRetry => &self.store_writer_busy_retry,
            MemoryProfileMetricKind::StoreConnectionDiscard => &self.store_connection_discard,
            MemoryProfileMetricKind::StoreConnectionsLive => &self.store_connections_live,
            MemoryProfileMetricKind::StoreConnectionsPeak => &self.store_connections_peak,
            MemoryProfileMetricKind::StoreSnapshotCacheHit => &self.store_snapshot_cache_hit,
            MemoryProfileMetricKind::StoreSnapshotCacheMiss => &self.store_snapshot_cache_miss,
            MemoryProfileMetricKind::StoreSnapshotCacheEviction => {
                &self.store_snapshot_cache_eviction
            }
            MemoryProfileMetricKind::EntityCacheLockWait => &self.entity_cache_lock_wait,
            MemoryProfileMetricKind::EntityCacheStripeCount => &self.entity_cache_stripe_count,
            MemoryProfileMetricKind::EntityCachePeakEntriesPerStripe => {
                &self.entity_cache_peak_entries_per_stripe
            }
            MemoryProfileMetricKind::EntityCacheMaxEntriesPerStripe => {
                &self.entity_cache_max_entries_per_stripe
            }
            MemoryProfileMetricKind::RuntimeAtomicInvokeEventWait => {
                &self.runtime_atomic_invoke_event_wait
            }
            MemoryProfileMetricKind::RuntimeAtomicQueueWait => &self.runtime_atomic_queue_wait,
            MemoryProfileMetricKind::RuntimeAtomicDispatchWait => {
                &self.runtime_atomic_dispatch_wait
            }
            MemoryProfileMetricKind::RuntimeAtomicExecution => &self.runtime_atomic_execution,
            MemoryProfileMetricKind::RuntimeAtomicCompletionWait => {
                &self.runtime_atomic_completion_wait
            }
            MemoryProfileMetricKind::RuntimeAtomicOutboxDrain => &self.runtime_atomic_outbox_drain,
        };
        target.record(duration_us, items.max(1));
    }

    fn cache_performance_snapshot(&self) -> MemoryCachePerformanceSnapshot {
        MemoryCachePerformanceSnapshot {
            snapshot_hits: self.snapshot_cache_hits_total.load(Ordering::Relaxed),
            snapshot_misses: self.snapshot_cache_misses_total.load(Ordering::Relaxed),
            snapshot_evictions: self.snapshot_cache_evictions_total.load(Ordering::Relaxed),
        }
    }

    fn take_snapshot_and_reset(&self) -> MemoryProfileSnapshot {
        let snapshot = MemoryProfileSnapshot {
            enabled: self.enabled.load(Ordering::Relaxed),
            js_read_only_total: self.js_read_only_total.snapshot(),
            js_hydrate_full: self.js_hydrate_full.snapshot(),
            js_hydrate_keys: self.js_hydrate_keys.snapshot(),
            js_txn_commit: self.js_txn_commit.snapshot(),
            js_cache_hit: self.js_cache_hit.snapshot(),
            js_cache_miss: self.js_cache_miss.snapshot(),
            js_cache_stale: self.js_cache_stale.snapshot(),
            op_read: self.op_read.snapshot(),
            op_snapshot: self.op_snapshot.snapshot(),
            op_version_if_newer: self.op_version_if_newer.snapshot(),
            op_apply_batch: self.op_apply_batch.snapshot(),
            store_read: self.store_read.snapshot(),
            store_snapshot: self.store_snapshot.snapshot(),
            store_snapshot_keys: self.store_snapshot_keys.snapshot(),
            store_version_if_newer: self.store_version_if_newer.snapshot(),
            store_apply_batch: self.store_apply_batch.snapshot(),
            store_apply_batch_validate: self.store_apply_batch_validate.snapshot(),
            store_apply_batch_write: self.store_apply_batch_write.snapshot(),
            store_database_cache_hit: self.store_database_cache_hit.snapshot(),
            store_database_cache_miss: self.store_database_cache_miss.snapshot(),
            store_database_cache_eviction: self.store_database_cache_eviction.snapshot(),
            store_connection_create: self.store_connection_create.snapshot(),
            store_connection_reuse: self.store_connection_reuse.snapshot(),
            store_reader_pool_wait: self.store_reader_pool_wait.snapshot(),
            store_writer_lane_wait: self.store_writer_lane_wait.snapshot(),
            store_writer_busy_retry: self.store_writer_busy_retry.snapshot(),
            store_connection_discard: self.store_connection_discard.snapshot(),
            store_connections_live: self.store_connections_live.snapshot(),
            store_connections_peak: self.store_connections_peak.snapshot(),
            store_snapshot_cache_hit: self.store_snapshot_cache_hit.snapshot(),
            store_snapshot_cache_miss: self.store_snapshot_cache_miss.snapshot(),
            store_snapshot_cache_eviction: self.store_snapshot_cache_eviction.snapshot(),
            entity_cache_lock_wait: self.entity_cache_lock_wait.snapshot(),
            entity_cache_stripe_count: self.entity_cache_stripe_count.snapshot(),
            entity_cache_peak_entries_per_stripe: self
                .entity_cache_peak_entries_per_stripe
                .snapshot(),
            entity_cache_max_entries_per_stripe: self
                .entity_cache_max_entries_per_stripe
                .snapshot(),
            runtime_atomic_invoke_event_wait: self.runtime_atomic_invoke_event_wait.snapshot(),
            runtime_atomic_queue_wait: self.runtime_atomic_queue_wait.snapshot(),
            runtime_atomic_dispatch_wait: self.runtime_atomic_dispatch_wait.snapshot(),
            runtime_atomic_execution: self.runtime_atomic_execution.snapshot(),
            runtime_atomic_completion_wait: self.runtime_atomic_completion_wait.snapshot(),
            runtime_atomic_outbox_drain: self.runtime_atomic_outbox_drain.snapshot(),
        };
        self.reset();
        snapshot
    }

    fn reset(&self) {
        self.js_read_only_total.reset();
        self.js_hydrate_full.reset();
        self.js_hydrate_keys.reset();
        self.js_txn_commit.reset();
        self.js_cache_hit.reset();
        self.js_cache_miss.reset();
        self.js_cache_stale.reset();
        self.op_read.reset();
        self.op_snapshot.reset();
        self.op_version_if_newer.reset();
        self.op_apply_batch.reset();
        self.store_read.reset();
        self.store_snapshot.reset();
        self.store_snapshot_keys.reset();
        self.store_version_if_newer.reset();
        self.store_apply_batch.reset();
        self.store_apply_batch_validate.reset();
        self.store_apply_batch_write.reset();
        self.store_database_cache_hit.reset();
        self.store_database_cache_miss.reset();
        self.store_database_cache_eviction.reset();
        self.store_connection_create.reset();
        self.store_connection_reuse.reset();
        self.store_reader_pool_wait.reset();
        self.store_writer_lane_wait.reset();
        self.store_writer_busy_retry.reset();
        self.store_connection_discard.reset();
        self.store_connections_live.reset();
        self.store_connections_peak.reset();
        self.store_snapshot_cache_hit.reset();
        self.store_snapshot_cache_miss.reset();
        self.store_snapshot_cache_eviction.reset();
        self.entity_cache_lock_wait.reset();
        self.entity_cache_stripe_count.reset();
        self.entity_cache_peak_entries_per_stripe.reset();
        self.entity_cache_max_entries_per_stripe.reset();
        self.runtime_atomic_invoke_event_wait.reset();
        self.runtime_atomic_queue_wait.reset();
        self.runtime_atomic_dispatch_wait.reset();
        self.runtime_atomic_execution.reset();
        self.runtime_atomic_completion_wait.reset();
        self.runtime_atomic_outbox_drain.reset();
    }
}
