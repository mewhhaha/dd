// Generated from serialized Rust structs. Run node scripts/generate-runtime-types.mjs.

export interface DdRuntimeWorkerStats {
  generation: number;
  public: boolean;
  temporary: boolean;
  expires_at_ms: number | null;
  queued: number;
  busy: number;
  inflight_total: number;
  wait_until_total: number;
  isolates_total: number;
  spawn_count: number;
  reuse_count: number;
  scale_down_count: number;
  targeted_lane_queued: number;
  memory_lane_queued: number;
  general_lane_queued: number;
  memory_active_shards: number;
  memory_max_shard_depth: number;
  memory_median_shard_depth: number;
  memory_owner_queues: number;
  memory_blocked_owner_queues: number;
  active_memory_leases: number;
  oldest_queue_ms: number;
  queued_bytes: number;
  max_queued_requests_per_worker: number;
  max_global_queued_bytes: number;
  memory_affinity_entries: number;
  stale_memory_affinity_entries: number;
  pending_memory_outbox_shards: number;
  memory_affinity_hit_count: number;
  memory_affinity_miss_no_mapping_count: number;
  memory_affinity_miss_stale_count: number;
  memory_affinity_miss_saturated_count: number;
  memory_least_loaded_fallback_count: number;
  memory_atomic_overflow_dispatch_count: number;
  memory_candidate_rejected_owner_lease_count: number;
  memory_candidate_rejected_isolate_state_count: number;
  memory_candidate_heads_inspected_count: number;
  memory_dispatch_no_ready_candidate_count: number;
  runtime_ready_work_budget_exhausted_count: number;
  runtime_max_ready_work_batch_size: number;
  global_isolate_budget: number;
  global_isolates_total: number;
  global_isolates_starting: number;
  global_internal_rescue_isolates: number;
  global_isolate_slots_available: number;
  scale_up_waiting_pools: number;
  scale_up_budget_denied_count: number;
  memory_outbox_claim_batch_count: number;
  memory_outbox_claim_row_count: number;
  memory_outbox_saturated_batch_count: number;
  memory_outbox_delivery_success_count: number;
  memory_outbox_delivery_retry_count: number;
  memory_outbox_terminal_drop_count: number;
  memory_outbox_ack_failure_count: number;
  memory_outbox_channel_full_count: number;
  memory_outbox_reschedule_count: number;
  memory_outbox_worker_pending_shards: number;
  memory_outbox_worker_in_flight_shards: number;
  memory_outbox_worker_parallelism_limit: number;
  memory_outbox_worker_parallelism_peak: number;
  memory_outbox_duplicate_schedule_coalesced_count: number;
  memory_outbox_task_failure_count: number;
  memory_outbox_shard_requeue_count: number;
}

export interface DdRuntimeWorkerStatus extends DdRuntimeWorkerStats {
  name: string;
  outbox_lag_shards: number;
}

export interface DdRuntimeRestoreFailure {
  worker: string | null;
  source: string;
  error: string;
}

export interface DdRuntimeReadiness {
  ready: boolean;
  runtime_ready: boolean;
  migrations_ready: boolean;
  storage_ready: boolean;
  worker_restoration_ready: boolean;
  restore_failure_count: number;
  failed_components: Array<string>;
}

export interface DdStateStorageTimings {
  queued_commands: number;
  sql_attempts: number;
  queue_wait_us: number;
  sql_us: number;
  commit_us: number;
  snapshot_publish_us: number;
}

export interface DdStateStorageStats {
  committed_groups: number;
  committed_commands: number;
  rollbacks: number;
  discarded_connections: number;
  busy_retries: number;
  pending_commands: number;
  pending_bytes: number;
  timings?: DdStateStorageTimings;
}

export interface DdRuntimeAdminSnapshot {
  worker_schedulers: number;
  active_deployments: number;
  workers: Array<DdRuntimeWorkerStatus>;
  restore_failures: Array<DdRuntimeRestoreFailure>;
  readiness: DdRuntimeReadiness;
  storage_retry_count: number;
  state_storage: DdStateStorageStats;
  memory_snapshot_cache_hits: number;
  memory_snapshot_cache_misses: number;
  memory_snapshot_cache_evictions: number;
  memory_outbox_claimed_bytes: number;
  memory_outbox_max_claimed_bytes: number;
}

export interface DdRuntimeCheckpointResult {
  state_shards: number;
  cache: boolean;
  control: boolean;
}
