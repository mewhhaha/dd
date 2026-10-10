// High-resolution time for `performance`, after deno_web's timers.rs
// (Copyright 2018-2026 the Deno authors, MIT license).

use dd_v8::OpState;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

struct StartTime(Instant);

fn start_time(state: &mut OpState) -> Instant {
    if !state.has::<StartTime>() {
        state.put(StartTime(Instant::now()));
    }
    state.borrow::<StartTime>().0
}

/// Milliseconds since this runtime's time origin.
pub(super) fn op_now(state: &mut OpState) -> f64 {
    start_time(state).elapsed().as_secs_f64() * 1000.0
}

/// The time origin in milliseconds since the Unix epoch.
/// <https://w3c.github.io/hr-time/#dfn-estimated-monotonic-time-of-the-unix-epoch>
pub(super) fn op_time_origin(state: &mut OpState) -> f64 {
    let monotonic = start_time(state).elapsed();
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    wall.saturating_sub(monotonic).as_secs_f64() * 1000.0
}
