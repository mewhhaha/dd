use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum BenchPhase {
    Deploy = 0,
    Seed = 1,
    Invoke = 2,
    Verify = 3,
    Profile = 4,
    Shutdown = 5,
    Done = 6,
    Failed = 7,
}

impl BenchPhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Deploy => "deploy",
            Self::Seed => "seed",
            Self::Invoke => "invoke",
            Self::Verify => "verify",
            Self::Profile => "profile",
            Self::Shutdown => "shutdown",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Deploy,
            1 => Self::Seed,
            2 => Self::Invoke,
            3 => Self::Verify,
            4 => Self::Profile,
            5 => Self::Shutdown,
            6 => Self::Done,
            7 => Self::Failed,
            _ => Self::Failed,
        }
    }
}

pub(crate) struct BenchWatchdogState {
    pub(crate) phase: AtomicU8,
    pub(crate) completed_requests: AtomicUsize,
    pub(crate) last_completed_request: AtomicUsize,
    pub(crate) last_progress_ms: AtomicU64,
    pub(crate) stop: AtomicBool,
}

impl BenchWatchdogState {
    pub(crate) fn new() -> Self {
        Self {
            phase: AtomicU8::new(BenchPhase::Deploy as u8),
            completed_requests: AtomicUsize::new(0),
            last_completed_request: AtomicUsize::new(0),
            last_progress_ms: AtomicU64::new(0),
            stop: AtomicBool::new(false),
        }
    }
}

pub(crate) fn set_watchdog_phase(
    state: &BenchWatchdogState,
    started_at: Instant,
    phase: BenchPhase,
) {
    state.phase.store(phase as u8, Ordering::Relaxed);
    state
        .last_progress_ms
        .store(started_at.elapsed().as_millis() as u64, Ordering::Relaxed);
}

pub(crate) fn record_watchdog_completion(
    state: &BenchWatchdogState,
    started_at: Instant,
    request_idx: usize,
) {
    state.completed_requests.fetch_add(1, Ordering::Relaxed);
    state
        .last_completed_request
        .store(request_idx, Ordering::Relaxed);
    state
        .last_progress_ms
        .store(started_at.elapsed().as_millis() as u64, Ordering::Relaxed);
}

pub(crate) fn spawn_watchdog(
    label: String,
    total_requests: usize,
    started_at: Instant,
    options: BenchOptions,
    state: Arc<BenchWatchdogState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            sleep(options.watchdog_interval).await;
            if state.stop.load(Ordering::Relaxed) {
                break;
            }
            let phase = BenchPhase::from_u8(state.phase.load(Ordering::Relaxed));
            if matches!(phase, BenchPhase::Done | BenchPhase::Failed) {
                break;
            }
            let elapsed_ms = started_at.elapsed().as_millis() as u64;
            let last_progress_ms = state.last_progress_ms.load(Ordering::Relaxed);
            let stalled_for_ms = elapsed_ms.saturating_sub(last_progress_ms);
            let completed = state.completed_requests.load(Ordering::Relaxed);
            let last_completed_request = state.last_completed_request.load(Ordering::Relaxed);
            let health =
                if Duration::from_millis(stalled_for_ms) >= options.watchdog_silence_timeout {
                    "stalled"
                } else {
                    "progressing"
                };
            println!(
                "bench-status label={} phase={} health={} completed={}/{} last_completed_request={} elapsed_ms={} stalled_for_ms={}",
                label,
                phase.as_str(),
                health,
                completed,
                total_requests,
                last_completed_request,
                elapsed_ms,
                stalled_for_ms,
            );
        }
    })
}
