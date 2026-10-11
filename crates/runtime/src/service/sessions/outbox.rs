//! Background outbox shard scheduling and drains.

use super::*;

pub(super) const MEMORY_OUTBOX_DRAIN_LIMIT: usize = 64;
pub(super) const MEMORY_OUTBOX_SCHEDULED_DRAIN_BATCHES: usize = 1;
pub(super) const MEMORY_OUTBOX_LEASE: Duration = Duration::from_secs(30);
pub(super) const MEMORY_OUTBOX_WORKER_CHANNEL_CAPACITY: usize = 1024;
pub(super) const MEMORY_OUTBOX_EFFECT_KINDS: &[&str] =
    &["audit.*", "socket.send", "socket.close", "trace.*"];

pub(in crate::service) type MemoryOutboxDrainSender = mpsc::Sender<MemoryOutboxWorkerCommand>;
pub(in crate::service) type MemoryOutboxDrainReceiver = mpsc::Receiver<MemoryOutboxWorkerCommand>;

#[derive(Debug)]
pub(in crate::service) enum MemoryOutboxWorkerCommand {
    DrainShard { shard_index: usize },
}

pub(in crate::service) fn memory_outbox_worker_channel()
-> (MemoryOutboxDrainSender, MemoryOutboxDrainReceiver) {
    mpsc::channel(MEMORY_OUTBOX_WORKER_CHANNEL_CAPACITY)
}

/// The shortest interval between periodic outbox scans.
pub(super) const MIN_MEMORY_OUTBOX_SCAN_INTERVAL: Duration = Duration::from_millis(250);

/// How often the outbox scans every state shard for due records. It follows
/// the scheduler's tick (1 s by default) but never runs faster than every
/// 250 ms: each scan claims on all 32 shards from the coordinator's thread,
/// and at a 10 ms tick the scans kept that thread busy enough to delay
/// cancellations and other control traffic by up to a second.
pub(in crate::service) fn memory_outbox_scan_interval(scale_tick: Duration) -> Duration {
    scale_tick.max(MIN_MEMORY_OUTBOX_SCAN_INTERVAL)
}

pub(in crate::service) async fn run_memory_outbox_worker(
    memory_store: MemoryStore,
    event_tx: RuntimeEventSender,
    mut receiver: MemoryOutboxDrainReceiver,
    max_concurrent_shards: usize,
    scan_interval: Duration,
) {
    let max_concurrent_shards = max_concurrent_shards.max(1);
    let mut coordinator = MemoryOutboxDrainCoordinator::new(max_concurrent_shards);
    let mut tasks = JoinSet::new();
    let mut accepting = true;
    let mut ticker = tokio::time::interval(scan_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        coordinator.start_ready_drains(&mut tasks, &memory_store, &event_tx);
        if !accepting && tasks.is_empty() {
            break;
        }

        tokio::select! {
            _ = ticker.tick(), if accepting => {
                coordinator.schedule_periodic_scan(memory_store.shard_count());
            }
            command = receiver.recv(), if accepting => {
                match command {
                    Some(MemoryOutboxWorkerCommand::DrainShard { shard_index }) => {
                        coordinator.schedule(shard_index);
                        coordinator.send_stats(&event_tx);
                    }
                    None => {
                        accepting = false;
                        coordinator.clear_pending();
                        coordinator.send_stats(&event_tx);
                    }
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                coordinator.finish_joined(joined);
                coordinator.send_stats(&event_tx);
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct MemoryOutboxDrainResult {
    pub(super) shard_index: usize,
    pub(super) saturated: bool,
    pub(super) panicked: bool,
}

pub(super) struct MemoryOutboxDrainCoordinator {
    pub(super) max_concurrent_shards: usize,
    pub(super) next_scan_start: usize,
    pub(super) pending: VecDeque<usize>,
    pub(super) pending_members: HashSet<usize>,
    pub(super) in_flight: HashSet<usize>,
    pub(super) follow_up: HashSet<usize>,
    pub(super) duplicate_schedule_coalesced_count: u64,
    pub(super) task_failure_count: u64,
    pub(super) shard_requeue_count: u64,
    pub(super) parallelism_peak: usize,
}

impl MemoryOutboxDrainCoordinator {
    pub(super) fn new(max_concurrent_shards: usize) -> Self {
        Self {
            max_concurrent_shards: max_concurrent_shards.max(1),
            next_scan_start: 0,
            pending: VecDeque::new(),
            pending_members: HashSet::new(),
            in_flight: HashSet::new(),
            follow_up: HashSet::new(),
            duplicate_schedule_coalesced_count: 0,
            task_failure_count: 0,
            shard_requeue_count: 0,
            parallelism_peak: 0,
        }
    }

    pub(super) fn schedule_periodic_scan(&mut self, shard_count: usize) {
        let shard_count = shard_count.max(1);
        let start = self.next_scan_start % shard_count;
        // A claim can exhaust the shared byte budget without reaching the
        // record limit. Rotate the first opportunity rather than repeatedly
        // letting an early shard take the budget before every other shard.
        for shard in (start..shard_count).chain(0..start) {
            self.schedule(shard);
        }
        self.next_scan_start = (start + 1) % shard_count;
    }

    pub(super) fn schedule(&mut self, shard_index: usize) {
        if self.in_flight.contains(&shard_index) {
            if !self.follow_up.insert(shard_index) {
                self.duplicate_schedule_coalesced_count =
                    self.duplicate_schedule_coalesced_count.saturating_add(1);
            }
            return;
        }
        if !self.pending_members.insert(shard_index) {
            self.duplicate_schedule_coalesced_count =
                self.duplicate_schedule_coalesced_count.saturating_add(1);
            return;
        }
        self.pending.push_back(shard_index);
    }

    pub(super) fn clear_pending(&mut self) {
        self.pending.clear();
        self.pending_members.clear();
        self.follow_up.clear();
    }

    pub(super) fn start_ready_drains(
        &mut self,
        tasks: &mut JoinSet<MemoryOutboxDrainResult>,
        memory_store: &MemoryStore,
        event_tx: &RuntimeEventSender,
    ) {
        while self.in_flight.len() < self.max_concurrent_shards {
            let Some(shard_index) = self.pending.pop_front() else {
                break;
            };
            self.pending_members.remove(&shard_index);
            if !self.in_flight.insert(shard_index) {
                self.follow_up.insert(shard_index);
                continue;
            }
            self.parallelism_peak = self.parallelism_peak.max(self.in_flight.len());
            let memory_store = memory_store.clone();
            let event_tx = event_tx.clone();
            tasks.spawn(async move {
                match AssertUnwindSafe(drain_memory_outbox_shard_in_background(
                    &memory_store,
                    &event_tx,
                    shard_index,
                    MEMORY_OUTBOX_SCHEDULED_DRAIN_BATCHES,
                ))
                .catch_unwind()
                .await
                {
                    Ok(result) => result,
                    Err(_) => MemoryOutboxDrainResult {
                        shard_index,
                        saturated: false,
                        panicked: true,
                    },
                }
            });
        }
    }

    pub(super) fn finish_joined(
        &mut self,
        joined: Option<std::result::Result<MemoryOutboxDrainResult, tokio::task::JoinError>>,
    ) {
        match joined {
            Some(Ok(result)) => {
                self.in_flight.remove(&result.shard_index);
                if result.panicked {
                    self.task_failure_count = self.task_failure_count.saturating_add(1);
                    warn!(
                        shard_index = result.shard_index,
                        "memory outbox drain task panicked"
                    );
                }
                let follow_up = self.follow_up.remove(&result.shard_index);
                if result.saturated || follow_up {
                    self.requeue(result.shard_index);
                }
            }
            Some(Err(error)) => {
                self.task_failure_count = self.task_failure_count.saturating_add(1);
                warn!(error = %error, "memory outbox drain task failed");
            }
            None => {}
        }
    }

    pub(super) fn requeue(&mut self, shard_index: usize) {
        if self.pending_members.insert(shard_index) {
            self.pending.push_back(shard_index);
            self.shard_requeue_count = self.shard_requeue_count.saturating_add(1);
        }
    }

    pub(super) fn send_stats(&self, event_tx: &RuntimeEventSender) {
        let _ = event_tx.try_send(RuntimeEvent::MemoryOutboxWorkerStats {
            pending_shards: self.pending_members.len(),
            in_flight_shards: self.in_flight.len(),
            parallelism_limit: self.max_concurrent_shards,
            parallelism_peak: self.parallelism_peak,
            duplicate_schedule_coalesced_count: self.duplicate_schedule_coalesced_count,
            task_failure_count: self.task_failure_count,
            shard_requeue_count: self.shard_requeue_count,
        });
    }
}

pub(super) async fn drain_memory_outbox_shard_in_background(
    memory_store: &MemoryStore,
    event_tx: &RuntimeEventSender,
    shard_index: usize,
    max_batches: usize,
) -> MemoryOutboxDrainResult {
    let mut saturated_any = false;
    for _ in 0..max_batches {
        let started_at = Instant::now();
        let claims = match memory_store
            .claim_due_outbox_records_for_shard_index(
                shard_index,
                MEMORY_OUTBOX_DRAIN_LIMIT,
                MEMORY_OUTBOX_LEASE,
                MEMORY_OUTBOX_EFFECT_KINDS,
            )
            .await
        {
            Ok(claims) => claims,
            Err(error) => {
                warn!(shard_index, error = %error, "memory outbox shard claim failed");
                return MemoryOutboxDrainResult {
                    shard_index,
                    saturated: false,
                    panicked: false,
                };
            }
        };
        let claimed = claims.len();
        let _payload_leases = claims
            .iter()
            .filter_map(MemoryOutboxClaim::payload_lease)
            .collect::<Vec<_>>();
        let saturated = claimed == MEMORY_OUTBOX_DRAIN_LIMIT;
        saturated_any |= saturated;
        memory_store.record_profile(
            MemoryProfileMetricKind::RuntimeOutboxDrain,
            duration_us(started_at.elapsed()),
            claimed as u64,
        );

        if claimed == 0 {
            let (reply, _reply_rx) = oneshot::channel();
            let _ = event_tx
                .send(RuntimeEvent::MemoryOutboxDelivery {
                    shard_index,
                    claims,
                    saturated: false,
                    reply,
                })
                .await;
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }

        let (reply, reply_rx) = oneshot::channel();
        if let Err(error) = event_tx
            .send(RuntimeEvent::MemoryOutboxDelivery {
                shard_index,
                claims,
                saturated,
                reply,
            })
            .await
        {
            warn!(
                shard_index,
                error = %error,
                "memory outbox delivery event could not be sent"
            );
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }

        let outcomes = match reply_rx.await {
            Ok(outcomes) => outcomes,
            Err(error) => {
                warn!(
                    shard_index,
                    error = %error,
                    "memory outbox delivery response dropped"
                );
                return MemoryOutboxDrainResult {
                    shard_index,
                    saturated: false,
                    panicked: false,
                };
            }
        };
        let ack_started_at = Instant::now();
        if let Err(error) = memory_store.apply_outbox_delivery_outcomes(&outcomes).await {
            warn!(
                shard_index,
                error = %error,
                "memory outbox state update failed"
            );
            let _ = event_tx
                .send(RuntimeEvent::MemoryOutboxAckFailed { shard_index })
                .await;
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }
        memory_store.record_profile(
            MemoryProfileMetricKind::RuntimeOutboxDrain,
            duration_us(ack_started_at.elapsed()),
            outcomes.len() as u64,
        );

        if !saturated {
            return MemoryOutboxDrainResult {
                shard_index,
                saturated: false,
                panicked: false,
            };
        }
    }
    MemoryOutboxDrainResult {
        shard_index,
        saturated: saturated_any,
        panicked: false,
    }
}
