//! Per-instance host state carried in the wasmtime `Store`.

use crate::heap::Heap;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use wasmtime::{Memory, Table};

/// Headers of the request currently being dispatched, served to the guest
/// through `ffi.dd_header`.
pub struct CurrentRequest {
    pub headers: Vec<(String, String)>,
}

pub struct Timer {
    pub id: u32,
    pub due: Instant,
    /// `Some` for intervals; the timer is rescheduled after firing.
    pub every: Option<std::time::Duration>,
    pub callback_bits: u64,
}

/// A queued promise reaction. `callback_bits == TAG_UNDEFINED` means
/// pass-through: the value resolves `downstream` directly (used for chaining
/// a promise returned from a `.then` callback).
pub struct Microtask {
    pub callback_bits: u64,
    pub value_bits: u64,
    pub downstream: u32,
}

#[derive(Default)]
pub struct HostState {
    pub heap: Heap,
    pub class_methods: HashMap<String, HashMap<String, u32>>,
    pub class_parents: HashMap<String, String>,
    pub class_statics: HashMap<String, HashMap<String, u64>>,
    pub registered_handler: Option<u64>,
    pub current_request: Option<CurrentRequest>,
    pub pending_exception: Option<u64>,
    pub try_depth: u32,
    pub timers: Vec<Timer>,
    pub microtasks: VecDeque<Microtask>,
    pub next_timer_id: u32,
    pub table: Option<Table>,
    pub memory: Option<Memory>,
}
