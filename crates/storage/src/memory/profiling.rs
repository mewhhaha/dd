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
            MemoryProfileMetricKind::JsReadOnlyCommit => &self.js_read_only_commit,
            MemoryProfileMetricKind::JsTxnBegin => &self.js_txn_begin,
            MemoryProfileMetricKind::JsTxnCommit => &self.js_txn_commit,
            MemoryProfileMetricKind::OpSnapshot => &self.op_snapshot,
            MemoryProfileMetricKind::OpApplyBatch => &self.op_apply_batch,
            MemoryProfileMetricKind::StoreLease => &self.store_lease,
            MemoryProfileMetricKind::StoreSnapshotCacheHit => &self.store_snapshot_cache_hit,
            MemoryProfileMetricKind::StoreSnapshotCacheMiss => &self.store_snapshot_cache_miss,
            MemoryProfileMetricKind::StoreSnapshotCacheEviction => {
                &self.store_snapshot_cache_eviction
            }
            MemoryProfileMetricKind::RuntimeSocketQueueWait => &self.runtime_socket_queue_wait,
            MemoryProfileMetricKind::RuntimeSocketDispatchWait => {
                &self.runtime_socket_dispatch_wait
            }
            MemoryProfileMetricKind::RuntimeSocketExecution => &self.runtime_socket_execution,
            MemoryProfileMetricKind::RuntimeSocketCompletionWait => {
                &self.runtime_socket_completion_wait
            }
            MemoryProfileMetricKind::RuntimeOutboxDrain => &self.runtime_outbox_drain,
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
            js_read_only_commit: self.js_read_only_commit.snapshot(),
            js_txn_begin: self.js_txn_begin.snapshot(),
            js_txn_commit: self.js_txn_commit.snapshot(),
            op_snapshot: self.op_snapshot.snapshot(),
            op_apply_batch: self.op_apply_batch.snapshot(),
            store_lease: self.store_lease.snapshot(),
            store_snapshot_cache_hit: self.store_snapshot_cache_hit.snapshot(),
            store_snapshot_cache_miss: self.store_snapshot_cache_miss.snapshot(),
            store_snapshot_cache_eviction: self.store_snapshot_cache_eviction.snapshot(),
            runtime_socket_queue_wait: self.runtime_socket_queue_wait.snapshot(),
            runtime_socket_dispatch_wait: self.runtime_socket_dispatch_wait.snapshot(),
            runtime_socket_execution: self.runtime_socket_execution.snapshot(),
            runtime_socket_completion_wait: self.runtime_socket_completion_wait.snapshot(),
            runtime_outbox_drain: self.runtime_outbox_drain.snapshot(),
        };
        self.reset();
        snapshot
    }

    fn reset(&self) {
        self.js_read_only_commit.reset();
        self.js_txn_begin.reset();
        self.js_txn_commit.reset();
        self.op_snapshot.reset();
        self.op_apply_batch.reset();
        self.store_lease.reset();
        self.store_snapshot_cache_hit.reset();
        self.store_snapshot_cache_miss.reset();
        self.store_snapshot_cache_eviction.reset();
        self.runtime_socket_queue_wait.reset();
        self.runtime_socket_dispatch_wait.reset();
        self.runtime_socket_execution.reset();
        self.runtime_socket_completion_wait.reset();
        self.runtime_outbox_drain.reset();
    }
}
