//! A small byte grammar (`program` in src/driver.js) that builds JavaScript
//! values — buffers of every kind, views that get detached or pushed out of
//! bounds, proxies, throwing and buffer-mutating getters, host objects,
//! cycles — and calls the builtin and typed ops with them, checking copies
//! and in-place writes against a JavaScript reference.

#![no_main]

use dd_v8_fuzz::{Entry, with};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    with(|harness| harness.call(Entry::Program, input));
});
