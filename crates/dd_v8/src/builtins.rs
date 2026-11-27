//! Builtin ops shared by every JavaScript layer on top of `dd_v8`.
//!
//! Host-side ops (`op_print`, `op_timer_sleep`, `op_timer_cancel`) run in Rust;
//! JS-level inspection, text coding, structured cloning, async-context,
//! error-builder, and microtask ops are bound directly inside `kernel.mjs`.

use crate::{OpDecl, OpState, RawBuffer, op_async, op_sync};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::oneshot;

/// Copies the bytes of a JavaScript `ArrayBuffer` or `ArrayBufferView`
/// handed to an [`crate::op_raw!`] handler; returns `None` for non-buffers.
pub fn buffer_bytes(buf: &RawBuffer) -> Option<Vec<u8>> {
    buf.bytes().map(|slice| slice.to_vec())
}

/// Runs `f` on the mutable byte slice of an `ArrayBuffer` or `ArrayBufferView`
/// handed to an [`crate::op_raw!`] handler; writes are committed strictly inside
/// the view's range. Returns `None` for non-buffers.
pub fn with_buffer_mut<R>(buf: &mut RawBuffer, f: impl FnOnce(&mut [u8]) -> R) -> Option<R> {
    buf.bytes_mut().map(f)
}

/// Writes as many native-endian `u32` words from `values` as fit wholly inside
/// the buffer view; leaves trailing partial bytes (< 4 bytes) and all bytes
/// outside the view untouched. Returns `false` for non-buffers.
pub fn write_u32s(buf: &mut RawBuffer, values: &[u32]) -> bool {
    let Some(bytes) = buf.bytes_mut() else {
        return false;
    };
    let count = values.len().min(bytes.len() / 4);
    for (i, &word) in values.iter().take(count).enumerate() {
        bytes[i * 4..i * 4 + 4].copy_from_slice(&word.to_ne_bytes());
    }
    true
}

/// The symbol objects use to describe how to clone themselves.
pub const HOST_OBJECT_BRAND: &str = "dd.hostObject";

pub fn ops() -> Vec<OpDecl> {
    vec![
        OpDecl::builtin_js("op_encode"),
        OpDecl::builtin_js("op_decode"),
        OpDecl::builtin_js("op_serialize"),
        OpDecl::builtin_js("op_deserialize"),
        OpDecl::builtin_js("op_structured_clone"),
        OpDecl::builtin_js("op_get_async_context"),
        OpDecl::builtin_js("op_set_async_context"),
        OpDecl::builtin_js("op_queue_microtask"),
        OpDecl::builtin_js("op_promise_state"),
        OpDecl::builtin_js("op_proxy_details"),
        OpDecl::builtin_js("op_own_non_index_keys"),
        OpDecl::builtin_js("op_is_any_array_buffer"),
        OpDecl::builtin_js("op_is_array_buffer"),
        OpDecl::builtin_js("op_is_array_buffer_view"),
        OpDecl::builtin_js("op_is_data_view"),
        OpDecl::builtin_js("op_is_date"),
        OpDecl::builtin_js("op_is_map"),
        OpDecl::builtin_js("op_is_native_error"),
        OpDecl::builtin_js("op_is_promise"),
        OpDecl::builtin_js("op_is_proxy"),
        OpDecl::builtin_js("op_is_reg_exp"),
        OpDecl::builtin_js("op_is_set"),
        OpDecl::builtin_js("op_is_shared_array_buffer"),
        OpDecl::builtin_js("op_is_string_object"),
        OpDecl::builtin_js("op_is_typed_array"),
        OpDecl::builtin_js("op_is_boxed_primitive"),
        OpDecl::builtin_js("op_register_error_builder"),
        OpDecl::builtin_js("op_set_wasm_streaming_handler"),
        OpDecl::builtin_js("op_wasm_streaming_feed"),
        OpDecl::builtin_js("op_wasm_streaming_set_url"),
        OpDecl::builtin_js("op_wasm_streaming_finish"),
        OpDecl::builtin_js("op_wasm_streaming_abort"),
        OpDecl::builtin_js("op_call_console"),
        op_sync!(op_print),
        op_async!(op_timer_sleep),
        op_sync!(op_timer_cancel),
    ]
}

fn op_print(_state: &mut OpState, message: String, is_err: bool) {
    use std::io::Write;
    if is_err {
        let _ = std::io::stderr().write_all(message.as_bytes());
    } else {
        let _ = std::io::stdout().write_all(message.as_bytes());
    }
}

#[derive(Default)]
struct Timers {
    cancels: HashMap<u32, oneshot::Sender<()>>,
}

/// Sleeps for `millis`; resolves `true` when the time passed and `false` when
/// the timer was cancelled first.
async fn op_timer_sleep(state: Rc<RefCell<OpState>>, id: u32, millis: f64) -> bool {
    let (cancel, cancelled) = oneshot::channel();
    {
        let mut state = state.borrow_mut();
        if !state.has::<Timers>() {
            state.put(Timers::default());
        }
        state.borrow_mut::<Timers>().cancels.insert(id, cancel);
    }
    let millis = if millis.is_finite() && millis > 0.0 {
        millis
    } else {
        0.0
    };
    let delay = Duration::try_from_secs_f64(millis / 1000.0).unwrap_or(Duration::MAX);
    let fired = tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        _ = cancelled => false,
    };
    if let Some(timers) = state.borrow_mut().try_borrow_mut::<Timers>() {
        timers.cancels.remove(&id);
    }
    fired
}

fn op_timer_cancel(state: &mut OpState, id: u32) {
    if let Some(timers) = state.try_borrow_mut::<Timers>()
        && let Some(cancel) = timers.cancels.remove(&id)
    {
        let _ = cancel.send(());
    }
}
