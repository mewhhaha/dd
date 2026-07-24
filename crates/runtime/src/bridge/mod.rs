//! Name-keyed bridge dispatcher for Perry-compiled wasm modules.
//!
//! Generated code funnels almost every runtime operation through the
//! `rt.mem_call` import as `(bridge_name, args...)`; the same names are also
//! declared as individual `rt.*` imports for index stability. Both entry
//! points land in [`dispatch`]. Semantics mirror `wasm_runtime.js` from the
//! Perry compiler — that file is the reference implementation of this ABI.

use crate::heap::{HostValue, ObjectData, PromiseState};
use crate::nanbox::{JsValue, TAG_NULL, TAG_UNDEFINED, decode, encode_bool, encode_number};
use crate::state::{HostState, Timer};
use base64::Engine as _;
use sha2::Digest;
use std::time::{Duration, Instant};
use wasmtime::{AsContextMut, Caller, Val};

pub(crate) mod fetch;
mod methods;
mod values;

use methods::{
    array_items, array_op, bytes_of, map_entries, map_position, method_call, method_call_string,
    set_items, slice_bounds,
};
use values::{
    CivilDateTime, date_ms, decode_hex, epoch_ms, form_encode, heap_to_json, iso_from_ms,
    json_to_heap, regex_lite_compile, throw_in_guest, url_object,
};
pub use values::{heap_to_json_value, json_stringify_bits, json_value_to_heap, resolve_promise};

/// `dd_sleep` entry point for the ffi dispatcher.
pub(crate) fn sleep_op(
    caller: &mut Caller<'_, HostState>,
    args: &[u64],
) -> Result<u64, wasmtime::Error> {
    fetch::sleep(caller, args)
}

pub(super) type HostResult = Result<u64, wasmtime::Error>;

pub(super) fn fail(name: &str, detail: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(format!("perry bridge {name}: {detail}"))
}

pub(super) fn arg(args: &[u64], index: usize) -> u64 {
    args.get(index).copied().unwrap_or(TAG_UNDEFINED)
}

/// Call a guest function through `__indirect_function_table`. Arguments are
/// raw NaN-box bits passed as i64; missing trailing parameters are padded
/// with `undefined`, matching `__padBigintArgs` in the JS glue. Works both
/// from inside host imports (via `Caller`) and from the engine's event loop
/// (via the store), because the table handle is stashed in [`HostState`].
pub fn call_guest(
    mut ctx: impl AsContextMut<Data = HostState>,
    func_idx: u32,
    args: &[u64],
) -> HostResult {
    let table = ctx
        .as_context()
        .data()
        .table
        .ok_or_else(|| fail("call_guest", "__indirect_function_table is not initialized"))?;
    let func = table
        .get(&mut ctx, u64::from(func_idx))
        .and_then(|value| value.as_func().and_then(|f| f.copied()))
        .ok_or_else(|| {
            fail(
                "call_guest",
                format!("no function at table index {func_idx}"),
            )
        })?;

    let ty = func.ty(ctx.as_context());
    let arity = ty.params().len();
    let mut params = Vec::with_capacity(arity);
    for i in 0..arity {
        params.push(Val::I64(arg(args, i) as i64));
    }
    let mut results = vec![Val::I64(0); ty.results().len()];
    func.call(&mut ctx, &params, &mut results)?;
    Ok(results
        .first()
        .and_then(|v| v.i64())
        .map(|v| v as u64)
        .unwrap_or(TAG_UNDEFINED))
}

/// Call a closure handle (captures + args), the way `closure_call_N` does.
pub fn call_closure(
    ctx: impl AsContextMut<Data = HostState>,
    closure_bits: u64,
    args: &[u64],
) -> HostResult {
    let (func_idx, captures) = match decode(closure_bits) {
        JsValue::Handle(id) => match ctx.as_context().data().heap.handle(id) {
            Some(HostValue::Closure { func_idx, captures }) => (*func_idx, captures.clone()),
            other => {
                return Err(fail(
                    "call_closure",
                    format!("handle {id} is not a closure: {other:?}"),
                ));
            }
        },
        other => {
            return Err(fail(
                "call_closure",
                format!("not a closure value: {other:?}"),
            ));
        }
    };
    let mut full = captures;
    full.extend_from_slice(args);
    call_guest(ctx, func_idx, &full)
}

pub(super) fn class_of(state: &HostState, bits: u64) -> Option<String> {
    let JsValue::Handle(id) = decode(bits) else {
        return None;
    };
    match state.heap.handle(id)? {
        HostValue::Object(object) => object.class.clone(),
        _ => None,
    }
}

/// Look up a method in the class table, walking the parent chain.
pub(super) fn find_class_method(state: &HostState, class: &str, method: &str) -> Option<u32> {
    let mut current = Some(class.to_string());
    while let Some(name) = current {
        if let Some(idx) = state
            .class_methods
            .get(&name)
            .and_then(|methods| methods.get(method))
        {
            return Some(*idx);
        }
        current = state.class_parents.get(&name).cloned();
    }
    None
}

pub(super) fn string_arg(state: &HostState, bits: u64) -> String {
    state.heap.display(bits)
}

/// Resolve a bridge argument that names something: either a NaN-boxed string
/// or a plain number indexing the string table (both occur in generated code).
pub(super) fn name_arg(state: &HostState, bits: u64) -> Option<String> {
    match decode(bits) {
        JsValue::Str(id) => state.heap.string(id).map(str::to_string),
        JsValue::Number(n) => state.heap.string(n as u32).map(str::to_string),
        _ => None,
    }
}

pub fn dispatch(caller: &mut Caller<'_, HostState>, name: &str, args: &[u64]) -> HostResult {
    if tracing::enabled!(target: "perry_bridge", tracing::Level::TRACE) {
        let summary: Vec<String> = args
            .iter()
            .map(|bits| {
                let state = caller.data();
                match decode(*bits) {
                    JsValue::Handle(id) => format!("#{id}"),
                    _ => state.heap.display(*bits),
                }
            })
            .collect();
        tracing::trace!(target: "perry_bridge", "{name}({})", summary.join(", "));
    }
    match name {
        // ===== console =====
        "console_log" | "console_warn" | "console_error" => {
            let text = caller.data().heap.display(arg(args, 0));
            match name {
                "console_error" => tracing::error!(target: "perry_worker", "{text}"),
                "console_warn" => tracing::warn!(target: "perry_worker", "{text}"),
                _ => tracing::info!(target: "perry_worker", "{text}"),
            }
            Ok(TAG_UNDEFINED)
        }
        "console_log_multi" => {
            let state = caller.data();
            let text = match decode(arg(args, 0)) {
                JsValue::Handle(id) => match state.heap.handle(id) {
                    Some(HostValue::Array(items)) => items
                        .iter()
                        .map(|item| state.heap.display(*item))
                        .collect::<Vec<_>>()
                        .join(" "),
                    _ => state.heap.display(arg(args, 0)),
                },
                _ => state.heap.display(arg(args, 0)),
            };
            tracing::info!(target: "perry_worker", "{text}");
            Ok(TAG_UNDEFINED)
        }

        // ===== core value ops =====
        "string_concat" => {
            let joined = format!(
                "{}{}",
                caller.data().heap.display(arg(args, 0)),
                caller.data().heap.display(arg(args, 1))
            );
            Ok(caller.data_mut().heap.intern_bits(joined))
        }
        "js_add" => {
            let state = caller.data();
            let a = arg(args, 0);
            let b = arg(args, 1);
            let is_string =
                matches!(decode(a), JsValue::Str(_)) || matches!(decode(b), JsValue::Str(_));
            if is_string {
                let joined = format!("{}{}", state.heap.display(a), state.heap.display(b));
                Ok(caller.data_mut().heap.intern_bits(joined))
            } else {
                Ok(encode_number(
                    state.heap.to_number(a) + state.heap.to_number(b),
                ))
            }
        }
        "js_mod" => {
            let state = caller.data();
            Ok(encode_number(
                state.heap.to_number(arg(args, 0)) % state.heap.to_number(arg(args, 1)),
            ))
        }
        "string_eq" | "js_strict_eq" => Ok(encode_bool(
            caller.data().heap.strict_eq(arg(args, 0), arg(args, 1)),
        )),
        "string_len" => {
            let state = caller.data();
            let value = arg(args, 0);
            if let Some(s) = state.heap.string_of(value) {
                return Ok(encode_number(s.chars().count() as f64));
            }
            if let JsValue::Handle(id) = decode(value)
                && let Some(HostValue::Array(items)) = state.heap.handle(id)
            {
                return Ok(encode_number(items.len() as f64));
            }
            Ok(encode_number(0.0))
        }
        "jsvalue_to_string" => {
            let text = caller.data().heap.display(arg(args, 0));
            Ok(caller.data_mut().heap.intern_bits(text))
        }
        "is_truthy" => Ok(encode_bool(caller.data().heap.is_truthy(arg(args, 0)))),
        "is_null_or_undefined" => Ok(encode_bool(matches!(
            decode(arg(args, 0)),
            JsValue::Null | JsValue::Undefined
        ))),
        "js_typeof" => {
            let state = caller.data();
            let kind = match decode(arg(args, 0)) {
                JsValue::Undefined => "undefined",
                JsValue::Null => "object",
                JsValue::Bool(_) => "boolean",
                JsValue::Number(_) => "number",
                JsValue::Str(_) => "string",
                JsValue::Handle(id) => match state.heap.handle(id) {
                    Some(HostValue::Closure { .. }) => "function",
                    _ => "object",
                },
            };
            Ok(caller.data_mut().heap.intern_bits(kind.to_string()))
        }
        "number_coerce" => Ok(encode_number(caller.data().heap.to_number(arg(args, 0)))),
        "is_nan" => Ok(encode_bool(
            caller.data().heap.to_number(arg(args, 0)).is_nan(),
        )),
        "is_finite" => Ok(encode_bool(
            caller.data().heap.to_number(arg(args, 0)).is_finite(),
        )),
        "parse_int" => {
            let text = string_arg(caller.data(), arg(args, 0));
            let trimmed = text.trim_start();
            let (sign, digits) = match trimmed.strip_prefix('-') {
                Some(rest) => (-1.0, rest),
                None => (1.0, trimmed.strip_prefix('+').unwrap_or(trimmed)),
            };
            let leading: String = digits.chars().take_while(char::is_ascii_digit).collect();
            if leading.is_empty() {
                Ok(encode_number(f64::NAN))
            } else {
                Ok(encode_number(
                    sign * leading.parse::<f64>().unwrap_or(f64::NAN),
                ))
            }
        }
        "parse_float" => {
            let text = string_arg(caller.data(), arg(args, 0));
            let trimmed = text.trim_start();
            let mut end = 0;
            let bytes = trimmed.as_bytes();
            let mut seen_dot = false;
            let mut seen_exp = false;
            while end < bytes.len() {
                let b = bytes[end];
                let ok = b.is_ascii_digit()
                    || (end == 0 && (b == b'-' || b == b'+'))
                    || (b == b'.' && !seen_dot && !seen_exp)
                    || ((b == b'e' || b == b'E') && !seen_exp && end > 0)
                    || ((b == b'-' || b == b'+') && end > 0 && (bytes[end - 1] | 0x20) == b'e');
                if !ok {
                    break;
                }
                seen_dot |= b == b'.';
                seen_exp |= b == b'e' || b == b'E';
                end += 1;
            }
            Ok(encode_number(trimmed[..end].parse().unwrap_or(f64::NAN)))
        }

        // ===== math =====
        "math_random" => {
            let mut bytes = [0u8; 8];
            getrandom::fill(&mut bytes).map_err(|e| fail(name, e))?;
            let value = (u64::from_le_bytes(bytes) >> 11) as f64 / (1u64 << 53) as f64;
            Ok(encode_number(value))
        }
        "math_pow" | "math_min" | "math_max" | "math_atan2" | "math_imul" | "math_hypot" => {
            let state = caller.data();
            let a = state.heap.to_number(arg(args, 0));
            let b = state.heap.to_number(arg(args, 1));
            let value = match name {
                "math_pow" => a.powf(b),
                "math_min" => a.min(b),
                "math_max" => a.max(b),
                "math_atan2" => a.atan2(b),
                "math_imul" => ((a as i64 as i32).wrapping_mul(b as i64 as i32)) as f64,
                _ => a.hypot(b),
            };
            Ok(encode_number(value))
        }
        _ if name.starts_with("math_") => {
            let x = caller.data().heap.to_number(arg(args, 0));
            let value = match name {
                "math_floor" => x.floor(),
                "math_ceil" => x.ceil(),
                "math_round" => (x + 0.5).floor(),
                "math_abs" => x.abs(),
                "math_sqrt" => x.sqrt(),
                "math_cbrt" => x.cbrt(),
                "math_log" => x.ln(),
                "math_log2" => x.log2(),
                "math_log10" => x.log10(),
                "math_log1p" => x.ln_1p(),
                "math_exp" => x.exp(),
                "math_expm1" => x.exp_m1(),
                "math_sin" => x.sin(),
                "math_cos" => x.cos(),
                "math_tan" => x.tan(),
                "math_asin" => x.asin(),
                "math_acos" => x.acos(),
                "math_atan" => x.atan(),
                "math_sinh" => x.sinh(),
                "math_cosh" => x.cosh(),
                "math_tanh" => x.tanh(),
                "math_asinh" => x.asinh(),
                "math_acosh" => x.acosh(),
                "math_atanh" => x.atanh(),
                "math_sign" => {
                    if x > 0.0 {
                        1.0
                    } else if x < 0.0 {
                        -1.0
                    } else {
                        x
                    }
                }
                "math_trunc" => x.trunc(),
                "math_fround" => x as f32 as f64,
                "math_clz32" => f64::from((x as i64 as u32).leading_zeros()),
                _ => return Err(fail(name, "unknown math bridge function")),
            };
            Ok(encode_number(value))
        }

        // ===== objects =====
        "object_new" => Ok(caller
            .data_mut()
            .heap
            .alloc_bits(HostValue::Object(ObjectData {
                class: None,
                props: Vec::new(),
            }))),
        // JS arrays and buffers are indexable objects: generated code reaches
        // `arr[i]` (for-of loops included) through the object_get/set path,
        // so these arms must handle every indexable receiver, not just plain
        // objects — mirroring `obj[key]` in the reference glue.
        "object_set" | "object_set_dynamic" => {
            let key = string_arg(caller.data(), arg(args, 1));
            let value = arg(args, 2);
            let byte = caller.data().heap.to_number(value) as i64 as u8;
            let handle = arg(args, 0);
            if let JsValue::Handle(id) = decode(handle) {
                match caller.data_mut().heap.handle_mut(id) {
                    Some(HostValue::Object(object)) => object.set(&key, value),
                    Some(HostValue::Array(items)) => {
                        if let Ok(index) = key.parse::<usize>() {
                            if index >= items.len() {
                                items.resize(index + 1, TAG_UNDEFINED);
                            }
                            items[index] = value;
                        }
                    }
                    Some(HostValue::Buffer(bytes)) => {
                        if let Ok(index) = key.parse::<usize>()
                            && index < bytes.len()
                        {
                            bytes[index] = byte;
                        }
                    }
                    _ => {}
                }
            }
            Ok(handle)
        }
        "object_get" | "object_get_dynamic" => {
            let key = string_arg(caller.data(), arg(args, 1));
            let state = caller.data();
            if let JsValue::Handle(id) = decode(arg(args, 0)) {
                match state.heap.handle(id) {
                    Some(HostValue::Object(object)) => {
                        return Ok(object.get(&key).unwrap_or(TAG_UNDEFINED));
                    }
                    Some(HostValue::Array(items)) => {
                        if key == "length" {
                            return Ok(encode_number(items.len() as f64));
                        }
                        if let Ok(index) = key.parse::<usize>() {
                            return Ok(items.get(index).copied().unwrap_or(TAG_UNDEFINED));
                        }
                    }
                    Some(HostValue::Buffer(bytes)) => {
                        if key == "length" {
                            return Ok(encode_number(bytes.len() as f64));
                        }
                        if let Ok(index) = key.parse::<usize>() {
                            return Ok(bytes
                                .get(index)
                                .map(|b| encode_number(f64::from(*b)))
                                .unwrap_or(TAG_UNDEFINED));
                        }
                    }
                    _ => {}
                }
            }
            Ok(TAG_UNDEFINED)
        }
        "object_delete" | "object_delete_dynamic" => {
            let key = string_arg(caller.data(), arg(args, 1));
            if let JsValue::Handle(id) = decode(arg(args, 0))
                && let Some(HostValue::Object(object)) = caller.data_mut().heap.handle_mut(id)
            {
                object.delete(&key);
            }
            Ok(TAG_UNDEFINED)
        }
        "object_keys" | "object_values" | "object_entries" => {
            let JsValue::Handle(id) = decode(arg(args, 0)) else {
                return Ok(caller
                    .data_mut()
                    .heap
                    .alloc_bits(HostValue::Array(Vec::new())));
            };
            let props: Vec<(String, u64)> = match caller.data().heap.handle(id) {
                Some(HostValue::Object(object)) => object
                    .props
                    .iter()
                    .filter(|(key, _)| key != "__class__")
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            };
            let heap = &mut caller.data_mut().heap;
            let items: Vec<u64> = match name {
                "object_keys" => props
                    .iter()
                    .map(|(key, _)| heap.intern_bits(key.clone()))
                    .collect(),
                "object_values" => props.iter().map(|(_, bits)| *bits).collect(),
                _ => props
                    .iter()
                    .map(|(key, bits)| {
                        let key_bits = heap.intern_bits(key.clone());
                        heap.alloc_bits(HostValue::Array(vec![key_bits, *bits]))
                    })
                    .collect(),
            };
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(items)))
        }
        "object_has_property" => {
            let key = string_arg(caller.data(), arg(args, 1));
            let state = caller.data();
            if let JsValue::Handle(id) = decode(arg(args, 0))
                && let Some(HostValue::Object(object)) = state.heap.handle(id)
            {
                return Ok(encode_bool(object.get(&key).is_some()));
            }
            Ok(encode_bool(false))
        }
        "object_assign" => {
            let target = arg(args, 0);
            let source_props: Vec<(String, u64)> = match decode(arg(args, 1)) {
                JsValue::Handle(id) => match caller.data().heap.handle(id) {
                    Some(HostValue::Object(object)) => object.props.clone(),
                    _ => Vec::new(),
                },
                _ => Vec::new(),
            };
            if let JsValue::Handle(id) = decode(target)
                && let Some(HostValue::Object(object)) = caller.data_mut().heap.handle_mut(id)
            {
                for (key, bits) in source_props {
                    if key != "__class__" {
                        object.set(&key, bits);
                    }
                }
            }
            Ok(target)
        }

        // ===== classes / typed shapes =====
        "class_new" => {
            let class = string_arg(caller.data(), arg(args, 0));
            Ok(caller
                .data_mut()
                .heap
                .alloc_bits(HostValue::Object(ObjectData {
                    class: Some(class),
                    props: Vec::new(),
                })))
        }
        "class_set_method" => {
            let class = string_arg(caller.data(), arg(args, 0));
            let method = string_arg(caller.data(), arg(args, 1));
            let func_idx = caller.data().heap.to_number(arg(args, 2)) as u32;
            caller
                .data_mut()
                .class_methods
                .entry(class)
                .or_default()
                .insert(method, func_idx);
            Ok(TAG_UNDEFINED)
        }
        "class_set_parent" => {
            let child = string_arg(caller.data(), arg(args, 0));
            let parent = string_arg(caller.data(), arg(args, 1));
            caller.data_mut().class_parents.insert(child, parent);
            Ok(TAG_UNDEFINED)
        }
        "class_get_field" => {
            let receiver = arg(args, 0);
            let Some(field) = name_arg(caller.data(), arg(args, 1)) else {
                return Ok(TAG_UNDEFINED);
            };
            if let Some(class) = class_of(caller.data(), receiver)
                && let Some(getter) =
                    find_class_method(caller.data(), &class, &format!("__get_{field}"))
            {
                return call_guest(&mut *caller, getter, &[receiver]);
            }
            let state = caller.data();
            if let JsValue::Handle(id) = decode(receiver)
                && let Some(HostValue::Object(object)) = state.heap.handle(id)
            {
                return Ok(object.get(&field).unwrap_or(TAG_UNDEFINED));
            }
            Ok(TAG_UNDEFINED)
        }
        "class_set_field" => {
            let receiver = arg(args, 0);
            let Some(field) = name_arg(caller.data(), arg(args, 1)) else {
                return Ok(TAG_UNDEFINED);
            };
            let value = arg(args, 2);
            if let Some(class) = class_of(caller.data(), receiver)
                && let Some(setter) =
                    find_class_method(caller.data(), &class, &format!("__set_{field}"))
            {
                call_guest(&mut *caller, setter, &[receiver, value])?;
                return Ok(TAG_UNDEFINED);
            }
            if let JsValue::Handle(id) = decode(receiver)
                && let Some(HostValue::Object(object)) = caller.data_mut().heap.handle_mut(id)
            {
                object.set(&field, value);
            }
            Ok(TAG_UNDEFINED)
        }
        "class_call_method" => {
            let receiver = arg(args, 0);
            let method = name_arg(caller.data(), arg(args, 1))
                .ok_or_else(|| fail(name, "method name is not a string"))?;
            let call_args: Vec<u64> = match decode(arg(args, 2)) {
                JsValue::Handle(id) => match caller.data().heap.handle(id) {
                    Some(HostValue::Array(items)) => items.clone(),
                    _ => Vec::new(),
                },
                _ => Vec::new(),
            };
            method_call(caller, receiver, &method, &call_args)
        }
        "class_set_static" => {
            let class = string_arg(caller.data(), arg(args, 0));
            let field = string_arg(caller.data(), arg(args, 1));
            let value = arg(args, 2);
            caller
                .data_mut()
                .class_statics
                .entry(class)
                .or_default()
                .insert(field, value);
            Ok(TAG_UNDEFINED)
        }
        "class_get_static" => {
            let class = string_arg(caller.data(), arg(args, 0));
            let field = string_arg(caller.data(), arg(args, 1));
            Ok(caller
                .data()
                .class_statics
                .get(&class)
                .and_then(|statics| statics.get(&field))
                .copied()
                .unwrap_or(TAG_UNDEFINED))
        }
        "class_instanceof" => {
            let target = string_arg(caller.data(), arg(args, 1));
            let mut current = class_of(caller.data(), arg(args, 0));
            while let Some(class) = current {
                if class == target {
                    return Ok(encode_bool(true));
                }
                current = caller.data().class_parents.get(&class).cloned();
            }
            Ok(encode_bool(false))
        }

        // ===== closures =====
        "closure_new" => {
            let func_idx = caller.data().heap.to_number(arg(args, 0)) as u32;
            let capture_count = caller.data().heap.to_number(arg(args, 1)) as usize;
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Closure {
                func_idx,
                captures: vec![TAG_UNDEFINED; capture_count],
            }))
        }
        "closure_set_capture" => {
            let handle = arg(args, 0);
            let index = caller.data().heap.to_number(arg(args, 1)) as usize;
            let value = arg(args, 2);
            if let JsValue::Handle(id) = decode(handle)
                && let Some(HostValue::Closure { captures, .. }) =
                    caller.data_mut().heap.handle_mut(id)
                && index < captures.len()
            {
                captures[index] = value;
            }
            Ok(handle)
        }
        "closure_call_0" | "closure_call_1" | "closure_call_2" | "closure_call_3" => {
            call_closure(&mut *caller, arg(args, 0), &args[1..])
        }
        "closure_call_spread" => {
            let spread: Vec<u64> = match decode(arg(args, 1)) {
                JsValue::Handle(id) => match caller.data().heap.handle(id) {
                    Some(HostValue::Array(items)) => items.clone(),
                    _ => Vec::new(),
                },
                _ => Vec::new(),
            };
            call_closure(&mut *caller, arg(args, 0), &spread)
        }

        // ===== arrays =====
        "array_new" => Ok(caller
            .data_mut()
            .heap
            .alloc_bits(HostValue::Array(Vec::new()))),
        "array_is_array" => {
            let state = caller.data();
            let is_array = matches!(decode(arg(args, 0)), JsValue::Handle(id)
                if matches!(state.heap.handle(id), Some(HostValue::Array(_))));
            Ok(encode_bool(is_array))
        }
        "array_from" => {
            let items = array_items(caller.data(), arg(args, 0)).unwrap_or_default();
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(items)))
        }
        // Chaining conventions: unlike their JS method counterparts, the
        // `array_push`/`array_push_spread` bridge entries return the array
        // (generated code threads the result into the next call), and
        // `array_unshift` returns nothing. Mirrors `__memDispatch`.
        "array_push" | "array_push_spread" => {
            let receiver = arg(args, 0);
            array_op(
                caller,
                name.trim_start_matches("array_"),
                receiver,
                &args[1..],
            )?;
            Ok(receiver)
        }
        "array_unshift" => {
            array_op(caller, "unshift", arg(args, 0), &args[1..])?;
            Ok(TAG_UNDEFINED)
        }
        _ if name.starts_with("array_") => {
            let receiver = arg(args, 0);
            array_op(
                caller,
                name.trim_start_matches("array_"),
                receiver,
                &args[1..],
            )
        }

        // ===== strings (named bridge entries) =====
        "string_fromCharCode" => {
            let code = caller.data().heap.to_number(arg(args, 0)) as u32;
            let text = char::from_u32(code).map(String::from).unwrap_or_default();
            Ok(caller.data_mut().heap.intern_bits(text))
        }
        _ if name.starts_with("string_") => {
            let receiver = arg(args, 0);
            let method = name.trim_start_matches("string_");
            method_call_string(caller, receiver, method, &args[1..])
        }

        // ===== JSON =====
        "json_stringify" => {
            let value = heap_to_json(caller.data(), arg(args, 0), 0)?;
            let text = match value {
                Some(v) => serde_json::to_string(&v).map_err(|e| fail(name, e))?,
                None => return Ok(TAG_UNDEFINED),
            };
            Ok(caller.data_mut().heap.intern_bits(text))
        }
        "json_parse" => {
            let text = string_arg(caller.data(), arg(args, 0));
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(value) => Ok(json_to_heap(caller.data_mut(), &value)),
                Err(error) => {
                    throw_in_guest(caller, &format!("JSON.parse: {error}"));
                    Ok(TAG_UNDEFINED)
                }
            }
        }

        // ===== maps / sets =====
        "map_new" => Ok(caller
            .data_mut()
            .heap
            .alloc_bits(HostValue::MapValue(Vec::new()))),
        "map_set" => {
            let (handle, key, value) = (arg(args, 0), arg(args, 1), arg(args, 2));
            let existing = map_position(caller.data(), handle, key);
            if let JsValue::Handle(id) = decode(handle)
                && let Some(HostValue::MapValue(entries)) = caller.data_mut().heap.handle_mut(id)
            {
                match existing {
                    Some(pos) => entries[pos].1 = value,
                    None => entries.push((key, value)),
                }
            }
            Ok(handle)
        }
        "map_get" => {
            let pos = map_position(caller.data(), arg(args, 0), arg(args, 1));
            Ok(map_entries(caller.data(), arg(args, 0))
                .and_then(|entries| pos.map(|p| entries[p].1))
                .unwrap_or(TAG_UNDEFINED))
        }
        "map_has" => Ok(encode_bool(
            map_position(caller.data(), arg(args, 0), arg(args, 1)).is_some(),
        )),
        "map_delete" => {
            let pos = map_position(caller.data(), arg(args, 0), arg(args, 1));
            if let (Some(pos), JsValue::Handle(id)) = (pos, decode(arg(args, 0)))
                && let Some(HostValue::MapValue(entries)) = caller.data_mut().heap.handle_mut(id)
            {
                entries.remove(pos);
                return Ok(encode_bool(true));
            }
            Ok(encode_bool(false))
        }
        "map_size" => Ok(encode_number(
            map_entries(caller.data(), arg(args, 0)).map_or(0.0, |e| e.len() as f64),
        )),
        "map_clear" => {
            if let JsValue::Handle(id) = decode(arg(args, 0))
                && let Some(HostValue::MapValue(entries)) = caller.data_mut().heap.handle_mut(id)
            {
                entries.clear();
            }
            Ok(TAG_UNDEFINED)
        }
        "map_keys" | "map_values" | "map_entries" => {
            let entries = map_entries(caller.data(), arg(args, 0)).unwrap_or_default();
            let heap = &mut caller.data_mut().heap;
            let items: Vec<u64> = match name {
                "map_keys" => entries.iter().map(|(k, _)| *k).collect(),
                "map_values" => entries.iter().map(|(_, v)| *v).collect(),
                _ => entries
                    .iter()
                    .map(|(k, v)| heap.alloc_bits(HostValue::Array(vec![*k, *v])))
                    .collect(),
            };
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(items)))
        }
        "set_new" => Ok(caller
            .data_mut()
            .heap
            .alloc_bits(HostValue::SetValue(Vec::new()))),
        "set_new_from_array" => {
            let items = array_items(caller.data(), arg(args, 0)).unwrap_or_default();
            let mut unique: Vec<u64> = Vec::new();
            for item in items {
                if !unique
                    .iter()
                    .any(|existing| caller.data().heap.strict_eq(*existing, item))
                {
                    unique.push(item);
                }
            }
            Ok(caller
                .data_mut()
                .heap
                .alloc_bits(HostValue::SetValue(unique)))
        }
        "set_add" => {
            let (handle, value) = (arg(args, 0), arg(args, 1));
            let exists = set_items(caller.data(), handle).is_some_and(|items| {
                items
                    .iter()
                    .any(|i| caller.data().heap.strict_eq(*i, value))
            });
            if !exists
                && let JsValue::Handle(id) = decode(handle)
                && let Some(HostValue::SetValue(items)) = caller.data_mut().heap.handle_mut(id)
            {
                items.push(value);
            }
            Ok(handle)
        }
        "set_has" => {
            let has = set_items(caller.data(), arg(args, 0)).is_some_and(|items| {
                items
                    .iter()
                    .any(|i| caller.data().heap.strict_eq(*i, arg(args, 1)))
            });
            Ok(encode_bool(has))
        }
        "set_delete" => {
            let value = arg(args, 1);
            let pos = set_items(caller.data(), arg(args, 0)).and_then(|items| {
                items
                    .iter()
                    .position(|i| caller.data().heap.strict_eq(*i, value))
            });
            if let (Some(pos), JsValue::Handle(id)) = (pos, decode(arg(args, 0)))
                && let Some(HostValue::SetValue(items)) = caller.data_mut().heap.handle_mut(id)
            {
                items.remove(pos);
                return Ok(encode_bool(true));
            }
            Ok(encode_bool(false))
        }
        "set_size" => Ok(encode_number(
            set_items(caller.data(), arg(args, 0)).map_or(0.0, |i| i.len() as f64),
        )),
        "set_clear" => {
            if let JsValue::Handle(id) = decode(arg(args, 0))
                && let Some(HostValue::SetValue(items)) = caller.data_mut().heap.handle_mut(id)
            {
                items.clear();
            }
            Ok(TAG_UNDEFINED)
        }
        "set_values" => {
            let items = set_items(caller.data(), arg(args, 0)).unwrap_or_default();
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(items)))
        }

        // ===== dates =====
        "date_now" => Ok(encode_number(epoch_ms())),
        "date_new" => Ok(caller
            .data_mut()
            .heap
            .alloc_bits(HostValue::Date(epoch_ms()))),
        "date_new_val" => {
            let ms = match decode(arg(args, 0)) {
                JsValue::Undefined => epoch_ms(),
                _ => caller.data().heap.to_number(arg(args, 0)),
            };
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Date(ms)))
        }
        "date_get_time" | "date_value_of" => {
            Ok(encode_number(date_ms(caller.data(), arg(args, 0))))
        }
        "date_to_iso_string" | "date_to_json" => {
            let text = iso_from_ms(date_ms(caller.data(), arg(args, 0)));
            Ok(caller.data_mut().heap.intern_bits(text))
        }
        _ if name.starts_with("date_get_") => {
            let civil = CivilDateTime::from_ms(date_ms(caller.data(), arg(args, 0)));
            let value = match name
                .trim_start_matches("date_get_")
                .trim_start_matches("utc_")
            {
                "full_year" => civil.year as f64,
                "month" => (civil.month - 1) as f64,
                "date" => civil.day as f64,
                "day" => civil.weekday as f64,
                "hours" => civil.hour as f64,
                "minutes" => civil.minute as f64,
                "seconds" => civil.second as f64,
                "milliseconds" => civil.millisecond as f64,
                "timezone_offset" => 0.0,
                other => return Err(fail(name, format!("unsupported date accessor: {other}"))),
            };
            Ok(encode_number(value))
        }

        // ===== errors / exceptions =====
        "error_new" => {
            let message = arg(args, 0);
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Error {
                message_bits: message,
            }))
        }
        "error_message" => {
            let state = caller.data();
            if let JsValue::Handle(id) = decode(arg(args, 0))
                && let Some(HostValue::Error { message_bits }) = state.heap.handle(id)
            {
                return Ok(*message_bits);
            }
            let text = state.heap.display(arg(args, 0));
            Ok(caller.data_mut().heap.intern_bits(text))
        }
        "try_start" => {
            caller.data_mut().try_depth += 1;
            Ok(TAG_UNDEFINED)
        }
        "try_end" => {
            let state = caller.data_mut();
            state.try_depth = state.try_depth.saturating_sub(1);
            Ok(TAG_UNDEFINED)
        }
        "throw_value" => {
            caller.data_mut().pending_exception = Some(arg(args, 0));
            Ok(TAG_UNDEFINED)
        }
        "has_exception" => Ok(encode_bool(caller.data().pending_exception.is_some())),
        "get_exception" => Ok(caller
            .data_mut()
            .pending_exception
            .take()
            .unwrap_or(TAG_UNDEFINED)),

        // ===== regexp =====
        "regexp_new" => {
            let source = string_arg(caller.data(), arg(args, 0));
            let flags = string_arg(caller.data(), arg(args, 1));
            Ok(caller
                .data_mut()
                .heap
                .alloc_bits(HostValue::Regexp { source, flags }))
        }
        "regexp_test" => {
            let JsValue::Handle(id) = decode(arg(args, 0)) else {
                return Ok(encode_bool(false));
            };
            let Some(HostValue::Regexp { source, flags }) = caller.data().heap.handle(id).cloned()
            else {
                return Ok(encode_bool(false));
            };
            let pattern = if flags.contains('i') {
                format!("(?i){source}")
            } else {
                source.clone()
            };
            let regex = regex_lite_compile(&pattern)
                .map_err(|e| fail(name, format!("unsupported pattern {source:?}: {e}")))?;
            let text = string_arg(caller.data(), arg(args, 1));
            Ok(encode_bool(regex.is_match(&text)))
        }

        // ===== URL =====
        "url_parse" => {
            let text = string_arg(caller.data(), arg(args, 0));
            match url::Url::parse(&text) {
                Ok(parsed) => Ok(url_object(caller.data_mut(), &parsed)),
                Err(error) => {
                    throw_in_guest(caller, &format!("invalid URL {text:?}: {error}"));
                    Ok(TAG_UNDEFINED)
                }
            }
        }
        _ if name.starts_with("url_get_") => {
            let field = match name {
                "url_get_search_params" => "searchParams",
                other => other.trim_start_matches("url_get_"),
            };
            let state = caller.data();
            if let JsValue::Handle(id) = decode(arg(args, 0))
                && let Some(HostValue::Object(object)) = state.heap.handle(id)
            {
                return Ok(object.get(field).unwrap_or(TAG_UNDEFINED));
            }
            Ok(TAG_UNDEFINED)
        }
        _ if name.starts_with("searchparams_") => {
            let handle = arg(args, 0);
            let key = string_arg(caller.data(), arg(args, 1));
            let JsValue::Handle(id) = decode(handle) else {
                return Ok(TAG_UNDEFINED);
            };
            match name {
                "searchparams_get" => {
                    let value = match caller.data().heap.handle(id) {
                        Some(HostValue::SearchParams(pairs)) => pairs
                            .iter()
                            .find(|(k, _)| *k == key)
                            .map(|(_, v)| v.clone()),
                        _ => None,
                    };
                    match value {
                        Some(v) => Ok(caller.data_mut().heap.intern_bits(v)),
                        None => Ok(TAG_NULL),
                    }
                }
                "searchparams_has" => {
                    let has = matches!(caller.data().heap.handle(id), Some(HostValue::SearchParams(pairs))
                        if pairs.iter().any(|(k, _)| *k == key));
                    Ok(encode_bool(has))
                }
                "searchparams_set" | "searchparams_append" => {
                    let value = string_arg(caller.data(), arg(args, 2));
                    if let Some(HostValue::SearchParams(pairs)) =
                        caller.data_mut().heap.handle_mut(id)
                    {
                        if name == "searchparams_set" {
                            pairs.retain(|(k, _)| *k != key);
                        }
                        pairs.push((key, value));
                    }
                    Ok(TAG_UNDEFINED)
                }
                "searchparams_delete" => {
                    if let Some(HostValue::SearchParams(pairs)) =
                        caller.data_mut().heap.handle_mut(id)
                    {
                        pairs.retain(|(k, _)| *k != key);
                    }
                    Ok(TAG_UNDEFINED)
                }
                "searchparams_to_string" => {
                    let text = match caller.data().heap.handle(id) {
                        Some(HostValue::SearchParams(pairs)) => pairs
                            .iter()
                            .map(|(k, v)| format!("{}={}", form_encode(k), form_encode(v)))
                            .collect::<Vec<_>>()
                            .join("&"),
                        _ => String::new(),
                    };
                    Ok(caller.data_mut().heap.intern_bits(text))
                }
                other => Err(fail(other, "unsupported searchparams operation")),
            }
        }

        // ===== crypto =====
        "crypto_random_uuid" => {
            let text = uuid::Uuid::new_v4().to_string();
            Ok(caller.data_mut().heap.intern_bits(text))
        }
        "crypto_random_bytes" => {
            let len = caller.data().heap.to_number(arg(args, 0)) as usize;
            if len > 1_048_576 {
                return Err(fail(
                    name,
                    format!("requested {len} random bytes (max 1 MiB)"),
                ));
            }
            let mut bytes = vec![0u8; len];
            getrandom::fill(&mut bytes).map_err(|e| fail(name, e))?;
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Buffer(bytes)))
        }
        "crypto_sha256" => {
            let input = bytes_of(caller.data(), arg(args, 0));
            let digest = sha2::Sha256::digest(&input);
            let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
            Ok(caller.data_mut().heap.intern_bits(hex))
        }

        // ===== path / process =====
        "path_join" => {
            let a = string_arg(caller.data(), arg(args, 0));
            let b = string_arg(caller.data(), arg(args, 1));
            let joined = format!("{}/{}", a.trim_end_matches('/'), b.trim_start_matches('/'));
            Ok(caller.data_mut().heap.intern_bits(joined))
        }
        "path_dirname" => {
            let p = string_arg(caller.data(), arg(args, 0));
            let dir = p
                .rsplit_once('/')
                .map(|(d, _)| d)
                .unwrap_or(".")
                .to_string();
            let dir = if dir.is_empty() { "/".to_string() } else { dir };
            Ok(caller.data_mut().heap.intern_bits(dir))
        }
        "path_basename" => {
            let p = string_arg(caller.data(), arg(args, 0));
            let base = p.rsplit('/').next().unwrap_or("").to_string();
            Ok(caller.data_mut().heap.intern_bits(base))
        }
        "path_extname" => {
            let p = string_arg(caller.data(), arg(args, 0));
            let base = p.rsplit('/').next().unwrap_or("");
            let ext = match base.rfind('.') {
                Some(0) | None => String::new(),
                Some(pos) => base[pos..].to_string(),
            };
            Ok(caller.data_mut().heap.intern_bits(ext))
        }
        "path_resolve" => {
            let p = string_arg(caller.data(), arg(args, 0));
            let resolved = if p.starts_with('/') {
                p
            } else {
                format!("/{p}")
            };
            Ok(caller.data_mut().heap.intern_bits(resolved))
        }
        "path_is_absolute" => {
            let p = string_arg(caller.data(), arg(args, 0));
            Ok(encode_bool(p.starts_with('/')))
        }
        "os_platform" => Ok(caller.data_mut().heap.intern_bits("linux".to_string())),
        "process_cwd" => Ok(caller.data_mut().heap.intern_bits("/".to_string())),
        "process_argv" => {
            let program = caller
                .data_mut()
                .heap
                .intern_bits("dd-wasm-worker".to_string());
            Ok(caller
                .data_mut()
                .heap
                .alloc_bits(HostValue::Array(vec![program])))
        }

        // ===== buffers / typed arrays =====
        "buffer_alloc" | "uint8array_new" => {
            let len = caller.data().heap.to_number(arg(args, 0)) as usize;
            if len > 64 * 1024 * 1024 {
                return Err(fail(
                    name,
                    format!("allocation of {len} bytes exceeds 64 MiB cap"),
                ));
            }
            Ok(caller
                .data_mut()
                .heap
                .alloc_bits(HostValue::Buffer(vec![0u8; len])))
        }
        "buffer_from_string" => {
            let text = string_arg(caller.data(), arg(args, 0));
            let encoding = match decode(arg(args, 1)) {
                JsValue::Undefined => "utf8".to_string(),
                _ => string_arg(caller.data(), arg(args, 1)),
            };
            let bytes = match encoding.as_str() {
                "utf8" | "utf-8" => text.into_bytes(),
                "base64" => base64::engine::general_purpose::STANDARD
                    .decode(text.as_bytes())
                    .map_err(|e| fail(name, e))?,
                "hex" => decode_hex(&text).map_err(|e| fail(name, e))?,
                other => return Err(fail(name, format!("unsupported encoding {other:?}"))),
            };
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Buffer(bytes)))
        }
        "buffer_to_string" => {
            let bytes = bytes_of(caller.data(), arg(args, 0));
            let encoding = match decode(arg(args, 1)) {
                JsValue::Undefined => "utf8".to_string(),
                _ => string_arg(caller.data(), arg(args, 1)),
            };
            let text = match encoding.as_str() {
                "utf8" | "utf-8" => String::from_utf8_lossy(&bytes).into_owned(),
                "base64" => base64::engine::general_purpose::STANDARD.encode(&bytes),
                "hex" => bytes.iter().map(|b| format!("{b:02x}")).collect(),
                other => return Err(fail(name, format!("unsupported encoding {other:?}"))),
            };
            Ok(caller.data_mut().heap.intern_bits(text))
        }
        "buffer_length" | "buffer_byte_length" | "uint8array_length" => Ok(encode_number(
            bytes_of(caller.data(), arg(args, 0)).len() as f64,
        )),
        "buffer_get" | "uint8array_get" => {
            let index = caller.data().heap.to_number(arg(args, 1)) as usize;
            let bytes = bytes_of(caller.data(), arg(args, 0));
            Ok(bytes
                .get(index)
                .map(|b| encode_number(f64::from(*b)))
                .unwrap_or(TAG_UNDEFINED))
        }
        "buffer_set" | "uint8array_set" => {
            let index = caller.data().heap.to_number(arg(args, 1)) as usize;
            let value = caller.data().heap.to_number(arg(args, 2)) as i64 as u8;
            if let JsValue::Handle(id) = decode(arg(args, 0))
                && let Some(HostValue::Buffer(bytes)) = caller.data_mut().heap.handle_mut(id)
                && index < bytes.len()
            {
                bytes[index] = value;
            }
            Ok(TAG_UNDEFINED)
        }
        "buffer_slice" => {
            let bytes = bytes_of(caller.data(), arg(args, 0));
            let (start, end) = slice_bounds(caller.data(), args, bytes.len());
            Ok(caller
                .data_mut()
                .heap
                .alloc_bits(HostValue::Buffer(bytes[start..end].to_vec())))
        }
        "buffer_concat" => {
            let list = array_items(caller.data(), arg(args, 0)).unwrap_or_default();
            let mut out = Vec::new();
            for item in list {
                out.extend_from_slice(&bytes_of(caller.data(), item));
            }
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Buffer(out)))
        }
        "buffer_equals" => {
            let a = bytes_of(caller.data(), arg(args, 0));
            let b = bytes_of(caller.data(), arg(args, 1));
            Ok(encode_bool(a == b))
        }
        "buffer_is_buffer" => {
            let is_buffer = matches!(decode(arg(args, 0)), JsValue::Handle(id)
                if matches!(caller.data().heap.handle(id), Some(HostValue::Buffer(_))));
            Ok(encode_bool(is_buffer))
        }
        "uint8array_from" => {
            let items = array_items(caller.data(), arg(args, 0)).unwrap_or_default();
            let bytes: Vec<u8> = items
                .iter()
                .map(|bits| caller.data().heap.to_number(*bits) as i64 as u8)
                .collect();
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Buffer(bytes)))
        }

        // ===== timers =====
        "set_timeout" | "set_interval" => {
            let callback_bits = arg(args, 0);
            let delay_ms = caller.data().heap.to_number(arg(args, 1)).max(0.0);
            let state = caller.data_mut();
            state.next_timer_id += 1;
            let id = state.next_timer_id;
            state.timers.push(Timer {
                id,
                due: Instant::now() + Duration::from_millis(delay_ms as u64),
                every: (name == "set_interval")
                    .then(|| Duration::from_millis((delay_ms as u64).max(1))),
                callback_bits,
            });
            Ok(encode_number(f64::from(id)))
        }
        "clear_timeout" | "clear_interval" => {
            let id = caller.data().heap.to_number(arg(args, 0)) as u32;
            caller.data_mut().timers.retain(|timer| timer.id != id);
            Ok(TAG_UNDEFINED)
        }

        // ===== promises =====
        "promise_new" => Ok(caller.data_mut().heap.alloc_bits(HostValue::Promise {
            state: PromiseState::Pending,
            reactions: Vec::new(),
        })),
        "promise_resolve" => {
            resolve_promise(caller.data_mut(), arg(args, 0), arg(args, 1));
            Ok(TAG_UNDEFINED)
        }
        "promise_then" => Ok(values::promise_then(
            caller.data_mut(),
            arg(args, 0),
            arg(args, 1),
        )),
        "await_promise" => Ok(arg(args, 0)),

        // ===== threads (sequential on this host) =====
        "thread_parallel_map" | "thread_parallel_filter" => {
            let items = array_items(caller.data(), arg(args, 0)).unwrap_or_default();
            let closure = arg(args, 1);
            let mut out = Vec::new();
            for item in items {
                let mapped = call_closure(&mut *caller, closure, &[item])?;
                if name == "thread_parallel_map" {
                    out.push(mapped);
                } else if caller.data().heap.is_truthy(mapped) {
                    out.push(item);
                }
            }
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(out)))
        }
        "thread_spawn" => call_closure(&mut *caller, arg(args, 0), &[]),

        // ===== networking =====
        // `fetch(url)` returns a real pending promise; the HTTP work runs on
        // the io runtime and the engine's event loop resolves it, so `.then`
        // chains work even though Perry cannot compile `new Promise`.
        "fetch_url" | "fetch_with_options" => fetch::fetch_async(caller, name, args),
        "response_json"
        | "response_text"
        | "response_status"
        | "response_ok"
        | "response_headers_get"
        | "response_url" => fetch::response_op(caller, name, args),

        _ if name.starts_with("perry_ui_") => Err(fail(
            name,
            "Perry UI bridge functions are not available in dd workers",
        )),

        // Bare method names dispatched through the generic method path
        // (e.g. `"abc".slice(1)` arrives as mem_call("slice", receiver, 1)).
        _ => {
            let receiver = arg(args, 0);
            method_call(caller, receiver, name, &args[1..])
        }
    }
}
