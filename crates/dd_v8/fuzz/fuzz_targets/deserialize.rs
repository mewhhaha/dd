//! Arbitrary bytes into op_deserialize, in storage and message mode, with
//! and without host objects and revivers. Half the inputs (flag 32) first
//! build and serialize a real value and then corrupt it, so mutations start
//! from well-formed data. See `deserialize` in src/driver.js for the flags.

#![no_main]

use dd_v8_fuzz::{Entry, with};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    with(|harness| harness.call(Entry::Deserialize, input));
});
