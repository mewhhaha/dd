//! Setup shared by the edge-case suites: a runtime with the builtin ops plus
//! test ops that take buffers and values every way an op can, and a small
//! JavaScript prelude for collecting failed checks.

#![allow(dead_code)]

use dd_v8::builtins::{buffer_bytes, with_buffer_mut, write_u32s};
use dd_v8::{JsBuffer, JsRuntime, OpDecl, OpState, RuntimeOptions, ToJsBuffer, v8};
use dd_v8::{op_async, op_raw, op_sync};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;

fn op_echo_bytes(_state: &mut OpState, bytes: JsBuffer) -> ToJsBuffer {
    bytes.into_vec().into()
}

fn op_echo_vec(_state: &mut OpState, bytes: Vec<u8>) -> ToJsBuffer {
    bytes.into()
}

fn op_echo_optional(_state: &mut OpState, bytes: Option<JsBuffer>) -> Option<ToJsBuffer> {
    bytes.map(|bytes| bytes.into_vec().into())
}

#[derive(Deserialize)]
struct Payload {
    data: JsBuffer,
}

fn op_echo_field(_state: &mut OpState, payload: Payload) -> ToJsBuffer {
    payload.data.into_vec().into()
}

async fn op_echo_async(_state: Rc<RefCell<OpState>>, bytes: JsBuffer) -> ToJsBuffer {
    bytes.into_vec().into()
}

fn op_byte_length(_state: &mut OpState, bytes: JsBuffer) -> f64 {
    bytes.len() as f64
}

fn op_json(_state: &mut OpState, value: serde_json::Value) -> serde_json::Value {
    value
}

fn op_f64(_state: &mut OpState, value: f64) -> f64 {
    value
}

fn op_f32(_state: &mut OpState, value: f32) -> f32 {
    value
}

fn op_u8(_state: &mut OpState, value: u8) -> u8 {
    value
}

fn op_i32(_state: &mut OpState, value: i32) -> i32 {
    value
}

fn op_u32(_state: &mut OpState, value: u32) -> u32 {
    value
}

fn op_i64(_state: &mut OpState, value: i64) -> i64 {
    value
}

fn op_u64(_state: &mut OpState, value: u64) -> u64 {
    value
}

fn op_bool(_state: &mut OpState, value: bool) -> bool {
    value
}

fn op_string(_state: &mut OpState, value: String) -> String {
    value
}

fn op_strings(_state: &mut OpState, values: Vec<String>) -> Vec<String> {
    values
}

#[derive(Debug, Deserialize, Serialize)]
struct Shape {
    name: String,
    #[serde(default)]
    tags: Vec<String>,
    size: Option<f64>,
}

fn op_shape(_state: &mut OpState, shape: Shape) -> Shape {
    shape
}

#[derive(Debug, Deserialize, Serialize)]
enum Command {
    Stop,
    Move { x: i32 },
    Say(String),
    Pair(u8, u8),
}

fn op_command(_state: &mut OpState, command: Command) -> Command {
    command
}

fn op_big_numbers(_state: &mut OpState) -> (u64, u64, i64, i64, u64) {
    ((1 << 53) - 1, 1 << 53, i64::MIN, i64::MAX, u64::MAX)
}

fn throw_type_error(scope: &mut v8::PinScope<'_, '_>, message: &str) {
    let message = v8::String::new(scope, message).unwrap();
    let exception = v8::Exception::type_error(scope, message);
    scope.throw_exception(exception);
}

/// `op_raw_copy(buffer)`: the bytes `buffer_bytes` copies, as a new array.
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

/// `op_raw_fill(buffer)`: fills the buffer in place with 0xab through
/// `with_buffer_mut` and returns how many bytes it saw.
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

/// `op_raw_write_u32s(buffer)`: writes the words 0x04030201 through
/// `write_u32s`.
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

pub fn test_ops() -> Vec<OpDecl> {
    vec![
        op_sync!(op_echo_bytes),
        op_sync!(op_echo_vec),
        op_sync!(op_echo_optional),
        op_sync!(op_echo_field),
        op_async!(op_echo_async),
        op_sync!(op_byte_length),
        op_sync!(op_json),
        op_sync!(op_f64),
        op_sync!(op_f32),
        op_sync!(op_u8),
        op_sync!(op_i32),
        op_sync!(op_u32),
        op_sync!(op_i64),
        op_sync!(op_u64),
        op_sync!(op_bool),
        op_sync!(op_string),
        op_sync!(op_strings),
        op_sync!(op_shape),
        op_sync!(op_command),
        op_sync!(op_big_numbers),
        op_raw!(op_raw_copy),
        op_raw!(op_raw_fill),
        op_raw!(op_raw_write_u32s),
    ]
}

/// Helpers every suite's JavaScript uses. `check` records a failure instead
/// of throwing, so one run reports every broken case.
const PRELUDE: &str = r#"
globalThis.failures = [];
globalThis.check = (condition, message) => {
  if (!condition) failures.push(message);
};
globalThis.attempt = (f) => {
  try {
    return { value: f() };
  } catch (error) {
    return { error };
  }
};
globalThis.describe = (value) => {
  try {
    if (typeof value === "bigint") return `${value}n`;
    if (typeof value === "symbol") return value.toString();
    if (value instanceof Error) return `${value.name}: ${value.message}`;
    return String(value);
  } catch {
    return Object.prototype.toString.call(value);
  }
};
globalThis.sameBytes = (actual, expected) =>
  actual instanceof Uint8Array &&
  actual.length === expected.length &&
  actual.every((byte, index) => byte === expected[index]);
globalThis.isCleanError = (error) => error instanceof Error;
globalThis.report = () => {
  const lines = failures.slice(0, 50);
  if (failures.length > 50) lines.push(`... and ${failures.length - 50} more`);
  failures.length = 0;
  return lines.join("\n");
};
"#;

pub fn runtime() -> JsRuntime {
    runtime_with(RuntimeOptions::default())
}

/// A test runtime built from `options`, with the test ops added.
pub fn runtime_with(options: RuntimeOptions) -> JsRuntime {
    let mut ops = dd_v8::builtins::ops();
    ops.extend(test_ops());
    let mut runtime = JsRuntime::new(RuntimeOptions { ops, ..options }).expect("runtime");
    runtime
        .execute_with_ops("<ops>", "globalThis.ops = ops;")
        .expect("expose ops");
    runtime
        .execute_script("<prelude>", PRELUDE)
        .expect("prelude");
    runtime
}

pub fn string(runtime: &mut JsRuntime, value: v8::Global<v8::Value>) -> String {
    dd_v8::scope!(scope, runtime);
    let value = v8::Local::new(scope, value);
    value.to_rust_string_lossy(scope)
}

pub fn eval(runtime: &mut JsRuntime, source: &str) -> String {
    let value = runtime
        .execute_script("<test>", source)
        .unwrap_or_else(|error| panic!("script failed: {error}"));
    string(runtime, value)
}

pub async fn eval_async(runtime: &mut JsRuntime, source: &str) -> String {
    let value = runtime
        .execute_script("<test>", source)
        .unwrap_or_else(|error| panic!("script failed: {error}"));
    let value = runtime
        .resolve(value)
        .await
        .unwrap_or_else(|error| panic!("promise rejected: {error}"));
    string(runtime, value)
}

/// Runs `source`, then fails the test with every check it recorded.
pub fn assert_checks(runtime: &mut JsRuntime, source: &str) {
    eval(runtime, source);
    let failures = eval(runtime, "report()");
    assert!(failures.is_empty(), "failed checks:\n{failures}");
}

/// [`assert_checks`] for a script whose completion value is a promise.
pub async fn assert_checks_async(runtime: &mut JsRuntime, source: &str) {
    eval_async(runtime, source).await;
    let failures = eval(runtime, "report()");
    assert!(failures.is_empty(), "failed checks:\n{failures}");
}
