// High-resolution time for `performance`, after deno_web's timers.rs
// (Copyright 2018-2026 the Deno authors, MIT license).

use dd_v8::OpState;
use std::time::Instant;

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
