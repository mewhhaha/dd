//! Shared harness for the dd_v8 fuzz targets: one runtime per process, the
//! builtin ops plus typed and raw-buffer ops, and a JavaScript driver that
//! turns fuzz input into values and op calls.
//!
//! The driver catches everything an op throws and checks it is an `Error`;
//! copies and in-place writes are checked against a reference computed in
//! JavaScript. A broken check comes back as a string starting with `BUG:`,
//! and [`Harness::call`] panics on it so libFuzzer records the input.

use dd_v8::builtins::{buffer_bytes, with_buffer_mut, write_u32s};
use dd_v8::{
    JsBuffer, JsRuntime, JsValue, OpDecl, OpError, OpState, RawBuffer, RuntimeOptions, ToJsBuffer,
};
use dd_v8::{op_raw, op_sync};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

fn op_echo_bytes(_state: &mut OpState, bytes: JsBuffer) -> ToJsBuffer {
    bytes.into_vec().into()
}

fn op_echo_vec(_state: &mut OpState, bytes: Vec<u8>) -> ToJsBuffer {
    bytes.into()
}

fn op_json(_state: &mut OpState, value: serde_json::Value) -> serde_json::Value {
    value
}

fn op_u64(_state: &mut OpState, value: u64) -> u64 {
    value
}

fn op_f64(_state: &mut OpState, value: f64) -> f64 {
    value
}

fn op_strings(_state: &mut OpState, values: Vec<String>) -> Vec<String> {
    values
}

#[derive(Deserialize, Serialize)]
struct Shape {
    name: String,
    #[serde(default)]
    tags: Vec<String>,
    size: Option<f64>,
    #[serde(default, with = "serde_bytes")]
    data: Vec<u8>,
}

fn op_shape(_state: &mut OpState, shape: Shape) -> Shape {
    shape
}

#[derive(Deserialize, Serialize)]
enum Command {
    Stop,
    Move { x: i32 },
    Say(String),
    Pair(u8, u8),
}

fn op_command(_state: &mut OpState, command: Command) -> Command {
    command
}

fn op_raw_copy(_state: &mut OpState, buf: &mut RawBuffer) -> Result<ToJsBuffer, OpError> {
    buffer_bytes(buf)
        .map(ToJsBuffer::from)
        .ok_or_else(|| OpError::type_error("not a buffer"))
}

fn op_raw_fill(_state: &mut OpState, buf: &mut RawBuffer) -> Result<f64, OpError> {
    with_buffer_mut(buf, |bytes| {
        bytes.fill(0xab);
        bytes.len() as f64
    })
    .ok_or_else(|| OpError::type_error("not a buffer"))
}

fn op_raw_write_u32s(_state: &mut OpState, buf: &mut RawBuffer) -> Result<bool, OpError> {
    if write_u32s(buf, &[0x0403_0201; 1024]) {
        Ok(true)
    } else {
        Err(OpError::type_error("not a buffer"))
    }
}

fn fuzz_ops() -> Vec<OpDecl> {
    let mut ops = dd_v8::builtins::ops();
    ops.extend([
        op_sync!(op_echo_bytes),
        op_sync!(op_echo_vec),
        op_sync!(op_json),
        op_sync!(op_u64),
        op_sync!(op_f64),
        op_sync!(op_strings),
        op_sync!(op_shape),
        op_sync!(op_command),
        op_raw!(op_raw_copy),
        op_raw!(op_raw_fill),
        op_raw!(op_raw_write_u32s),
    ]);
    ops
}

const DRIVER: &str = include_str!("driver.js");

/// Which driver entry point to run.
#[derive(Clone, Copy)]
pub enum Entry {
    /// Bytes into op_deserialize (see `deserialize` in driver.js).
    Deserialize,
    /// A byte program that builds values and calls ops with them.
    Program,
    /// Round-trips one value through op_serialize/op_deserialize and
    /// op_structured_clone, returning the op_deserialize copy.
    Clone,
}

pub struct Harness {
    runtime: JsRuntime,
    deserialize: JsValue,
    program: JsValue,
    clone: JsValue,
}

thread_local! {
    static HARNESS: RefCell<Option<Harness>> = const { RefCell::new(None) };
}

/// Runs `f` with this thread's harness, creating it on first use.
pub fn with<R>(f: impl FnOnce(&mut Harness) -> R) -> R {
    HARNESS.with(|harness| {
        let mut harness = harness.borrow_mut();
        f(harness.get_or_insert_with(Harness::new))
    })
}

impl Harness {
    fn new() -> Self {
        let mut runtime = JsRuntime::new(RuntimeOptions {
            ops: fuzz_ops(),
            ..Default::default()
        })
        .expect("runtime");
        let entries = runtime
            .execute_with_ops("<fuzz driver>", DRIVER)
            .unwrap_or_else(|error| panic!("driver failed to load: {error}"));
        let deserialize = runtime
            .get_property(&entries, "deserialize")
            .expect("driver has no deserialize");
        let program = runtime
            .get_property(&entries, "program")
            .expect("driver has no program");
        let clone = runtime
            .get_property(&entries, "clone")
            .expect("driver has no clone");
        Self {
            runtime,
            deserialize,
            program,
            clone,
        }
    }

    pub fn runtime(&mut self) -> &mut JsRuntime {
        &mut self.runtime
    }

    /// Calls a driver entry point with `input` as a `Uint8Array`, panicking
    /// on a `BUG:` report or on anything escaping the driver.
    pub fn call(&mut self, entry: Entry, input: &[u8]) {
        let argument = self.runtime.uint8_array(input).expect("uint8_array");
        let report = self.call_value(entry, argument);
        let report = self
            .runtime
            .value_to_string(&report)
            .expect("report to string");
        assert!(!report.starts_with("BUG"), "{report}");
    }

    /// Calls a driver entry point with any value and returns its result.
    pub fn call_value(&mut self, entry: Entry, argument: JsValue) -> JsValue {
        let function = match entry {
            Entry::Deserialize => self.deserialize.clone(),
            Entry::Program => self.program.clone(),
            Entry::Clone => self.clone.clone(),
        };
        self.runtime
            .call_function(&function, &[argument])
            .unwrap_or_else(|error| panic!("BUG: escaped the driver: {error}"))
    }
}
