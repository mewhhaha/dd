use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

const EXPORT_UNKNOWN: u8 = 0;
const EXPORT_HEALTHY: u8 = 1;
const EXPORT_FAILED: u8 = 2;

static ENDPOINT_CONFIGURED: AtomicBool = AtomicBool::new(false);
static EXPORTER_ENABLED: AtomicBool = AtomicBool::new(false);
static EXPORT_STATE: AtomicU8 = AtomicU8::new(EXPORT_UNKNOWN);
static EXPORT_SUCCESSES: AtomicU64 = AtomicU64::new(0);
static EXPORT_FAILURES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Serialize)]
pub struct TraceExporterHealth {
    pub compiled: bool,
    pub configured: bool,
    pub enabled: bool,
    pub verified: bool,
    pub state: &'static str,
    pub export_successes: u64,
    pub export_failures: u64,
}

pub fn configure(endpoint_configured: bool, exporter_enabled: bool) {
    ENDPOINT_CONFIGURED.store(endpoint_configured, Ordering::Release);
    EXPORTER_ENABLED.store(exporter_enabled, Ordering::Release);
    EXPORT_STATE.store(EXPORT_UNKNOWN, Ordering::Release);
}

pub fn record_export(success: bool) {
    if success {
        EXPORT_SUCCESSES.fetch_add(1, Ordering::Relaxed);
        EXPORT_STATE.store(EXPORT_HEALTHY, Ordering::Release);
    } else {
        EXPORT_FAILURES.fetch_add(1, Ordering::Relaxed);
        EXPORT_STATE.store(EXPORT_FAILED, Ordering::Release);
    }
}

pub fn snapshot() -> TraceExporterHealth {
    let configured = ENDPOINT_CONFIGURED.load(Ordering::Acquire);
    let enabled = EXPORTER_ENABLED.load(Ordering::Acquire);
    let export_state = EXPORT_STATE.load(Ordering::Acquire);
    let state = if !configured {
        "disabled"
    } else if !enabled {
        "unverified"
    } else {
        match export_state {
            EXPORT_HEALTHY => "healthy",
            EXPORT_FAILED => "error",
            _ => "pending",
        }
    };
    TraceExporterHealth {
        compiled: cfg!(feature = "otel"),
        configured,
        enabled,
        verified: enabled && export_state == EXPORT_HEALTHY,
        state,
        export_successes: EXPORT_SUCCESSES.load(Ordering::Relaxed),
        export_failures: EXPORT_FAILURES.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unverified_endpoint_stays_disabled_until_operator_enables_it() {
        configure(true, false);
        let health = snapshot();
        assert_eq!(health.state, "unverified");
        assert!(!health.enabled);
        assert!(!health.verified);
    }
}
