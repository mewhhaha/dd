//! Shared harness for the dd_v8 fuzz targets: one runtime per process (V8
//! initializes once per process and isolates are expensive), the builtin
//! ops plus typed ops that read buffers and serde values, and a JavaScript
//! driver that turns fuzz input into values and op calls.
//!
//! The driver catches everything an op throws and checks it is an `Error`;
//! copies and in-place writes are checked against a reference computed in
//! JavaScript. A broken check comes back as a string starting with `BUG:`,
//! and [`Harness::call`] panics on it so libFuzzer records the input. Any
//! crash, abort or sanitizer report is a finding too.

use dd_v8::builtins::{buffer_bytes, with_buffer_mut, write_u32s};
use dd_v8::{JsBuffer, JsRuntime, OpDecl, OpState, RuntimeOptions, ToJsBuffer, v8};
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

fn throw_type_error(scope: &mut v8::PinScope<'_, '_>, message: &str) {
    let message = v8::String::new(scope, message).expect("short message");
    let exception = v8::Exception::type_error(scope, message);
    scope.throw_exception(exception);
}

fn op_raw_copy<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    match buffer_bytes(args.get(0)) {
        Some(bytes) => rv.set(dd_v8::serde_v8::uint8_array(scope, bytes).into()),
        None => throw_type_error(scope, "not a buffer"),
    }
}

fn op_raw_fill<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let filled = with_buffer_mut(args.get(0), |bytes| {
        bytes.fill(0xab);
        bytes.len()
    });
    match filled {
        Some(length) => rv.set_double(length as f64),
        None => throw_type_error(scope, "not a buffer"),
    }
}

fn op_raw_write_u32s<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    if write_u32s(args.get(0), &[0x0403_0201; 1024]) {
        rv.set_bool(true);
    } else {
        throw_type_error(scope, "not a buffer");
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
    deserialize: v8::Global<v8::Function>,
    program: v8::Global<v8::Function>,
    clone: v8::Global<v8::Function>,
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
        let [deserialize, program, clone] = {
            dd_v8::scope!(scope, runtime);
            let entries = v8::Local::new(scope, entries)
                .to_object(scope)
                .expect("driver returns an object");
            ["deserialize", "program", "clone"].map(|name| {
                let key = v8::String::new(scope, name).expect("short key");
                let function = entries
                    .get(scope, key.into())
                    .and_then(|value| v8::Local::<v8::Function>::try_from(value).ok())
                    .unwrap_or_else(|| panic!("driver has no {name}"));
                v8::Global::new(scope, function)
            })
        };
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
        let argument = {
            dd_v8::scope!(scope, self.runtime);
            let array = dd_v8::serde_v8::uint8_array(scope, input.to_vec());
            v8::Global::new(scope, v8::Local::<v8::Value>::from(array))
        };
        let report = self.call_value(entry, argument);
        let report = self.string(report);
        assert!(!report.starts_with("BUG"), "{report}");
    }

    /// Calls a driver entry point with any value and returns its result.
    pub fn call_value(
        &mut self,
        entry: Entry,
        argument: v8::Global<v8::Value>,
    ) -> v8::Global<v8::Value> {
        let function = match entry {
            Entry::Deserialize => self.deserialize.clone(),
            Entry::Program => self.program.clone(),
            Entry::Clone => self.clone.clone(),
        };
        self.runtime
            .call_function(&function, &[argument])
            .unwrap_or_else(|error| panic!("BUG: escaped the driver: {error}"))
    }

    fn string(&mut self, value: v8::Global<v8::Value>) -> String {
        dd_v8::scope!(scope, self.runtime);
        let value = v8::Local::new(scope, value);
        value.to_rust_string_lossy(scope)
    }
}
