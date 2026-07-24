//! Name-keyed bridge dispatcher for Perry-compiled wasm modules.
//!
//! Generated code funnels almost every runtime operation through the
//! `rt.mem_call` import as `(bridge_name, args...)`; the same names are also
//! declared as individual `rt.*` imports for index stability. Both entry
//! points land in [`dispatch`]. Semantics mirror `wasm_runtime.js` from the
//! Perry compiler — that file is the reference implementation of this ABI.

use crate::heap::{HostValue, ObjectData, PromiseReaction, PromiseState};
use crate::nanbox::{JsValue, TAG_NULL, TAG_UNDEFINED, decode, encode, encode_bool, encode_number};
use crate::state::{HostState, Microtask, Timer};
use base64::Engine as _;
use sha2::Digest;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wasmtime::{AsContextMut, Caller, Val};

type HostResult = Result<u64, wasmtime::Error>;

fn fail(name: &str, detail: impl std::fmt::Display) -> wasmtime::Error {
    wasmtime::Error::msg(format!("perry bridge {name}: {detail}"))
}

fn arg(args: &[u64], index: usize) -> u64 {
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

fn class_of(state: &HostState, bits: u64) -> Option<String> {
    let JsValue::Handle(id) = decode(bits) else {
        return None;
    };
    match state.heap.handle(id)? {
        HostValue::Object(object) => object.class.clone(),
        _ => None,
    }
}

/// Look up a method in the class table, walking the parent chain.
fn find_class_method(state: &HostState, class: &str, method: &str) -> Option<u32> {
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

fn string_arg(state: &HostState, bits: u64) -> String {
    state.heap.display(bits)
}

/// Resolve a bridge argument that names something: either a NaN-boxed string
/// or a plain number indexing the string table (both occur in generated code).
fn name_arg(state: &HostState, bits: u64) -> Option<String> {
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
        "promise_then" => {
            let parent = arg(args, 0);
            let callback_bits = arg(args, 1);
            let JsValue::Handle(parent_id) = decode(parent) else {
                return Ok(parent);
            };
            let downstream = caller.data_mut().heap.alloc(HostValue::Promise {
                state: PromiseState::Pending,
                reactions: Vec::new(),
            });
            let resolved_value = match caller.data().heap.handle(parent_id) {
                Some(HostValue::Promise {
                    state: PromiseState::Resolved(v),
                    ..
                }) => Some(*v),
                Some(HostValue::Promise { .. }) => None,
                _ => return Ok(parent),
            };
            match resolved_value {
                Some(value) => caller.data_mut().microtasks.push_back(Microtask {
                    callback_bits,
                    value_bits: value,
                    downstream,
                }),
                None => {
                    if let Some(HostValue::Promise { reactions, .. }) =
                        caller.data_mut().heap.handle_mut(parent_id)
                    {
                        reactions.push(PromiseReaction {
                            callback_bits,
                            downstream,
                        });
                    }
                }
            }
            Ok(encode(JsValue::Handle(downstream)))
        }
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

        // ===== networking (not part of the experiment yet) =====
        "fetch_url"
        | "fetch_with_options"
        | "response_json"
        | "response_text"
        | "response_status"
        | "response_ok"
        | "response_headers_get"
        | "response_url" => Err(fail(
            name,
            "outbound fetch is not supported by the dd perry-wasm host yet",
        )),

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

/// Generic method dispatch: user class methods first, then built-in string,
/// array, and buffer methods by receiver type.
fn method_call(
    caller: &mut Caller<'_, HostState>,
    receiver: u64,
    method: &str,
    args: &[u64],
) -> HostResult {
    if let Some(class) = class_of(caller.data(), receiver)
        && let Some(func_idx) = find_class_method(caller.data(), &class, method)
    {
        let mut full = vec![receiver];
        full.extend_from_slice(args);
        return call_guest(&mut *caller, func_idx, &full);
    }
    let outcome = match decode(receiver) {
        JsValue::Str(_) => method_call_string(caller, receiver, method, args),
        JsValue::Handle(id) => match caller.data().heap.handle(id) {
            Some(HostValue::Array(_)) => array_op(caller, method, receiver, args),
            Some(other) => Err(fail(
                method,
                format!("no method {method:?} on receiver {other:?}"),
            )),
            None => Err(fail(method, format!("no method {method:?} on dead handle"))),
        },
        other => Err(fail(
            method,
            format!("no method {method:?} on primitive {other:?}"),
        )),
    };
    // Parity with the reference JS glue: unresolvable method dispatch yields
    // `undefined` instead of trapping. Perry's codegen has known gaps (e.g.
    // the class_call_method frame layout bug) that the browser runtime
    // tolerates silently; a hard error here would reject programs that "work"
    // on the web target. The warn line keeps the gap visible. Genuine guest
    // failures (a trap inside a callback) still propagate.
    match outcome {
        Err(error) if is_unknown_method(&error) => {
            tracing::warn!(target: "perry_worker", "method dispatch fell through: {error:#}");
            Ok(TAG_UNDEFINED)
        }
        other => other,
    }
}

/// True for the dispatcher's own "this method/receiver combination does not
/// exist" errors, as opposed to failures raised while running a known method.
fn is_unknown_method(error: &wasmtime::Error) -> bool {
    let message = format!("{error:#}");
    message.contains("no method")
        || message.contains("unsupported array operation")
        || message.contains("unsupported string method")
        || message.contains("array receiver is not a handle")
        || message.contains("receiver is not an array")
}

/// Built-in string methods. Indices are Unicode scalar positions — exact for
/// ASCII and BMP text, diverging from JS UTF-16 indices only for astral chars.
fn method_call_string(
    caller: &mut Caller<'_, HostState>,
    receiver: u64,
    method: &str,
    args: &[u64],
) -> HostResult {
    let text = string_arg(caller.data(), receiver);
    let chars: Vec<char> = text.chars().collect();
    let index_arg = |caller: &Caller<'_, HostState>, i: usize, default: f64| -> f64 {
        match decode(arg(args, i)) {
            JsValue::Undefined => default,
            _ => caller.data().heap.to_number(arg(args, i)),
        }
    };
    let clamp = |n: f64| -> usize {
        let len = chars.len() as f64;
        let n = if n < 0.0 {
            (len + n).max(0.0)
        } else {
            n.min(len)
        };
        n as usize
    };
    let intern = |caller: &mut Caller<'_, HostState>, s: String| -> u64 {
        caller.data_mut().heap.intern_bits(s)
    };

    match method {
        "slice" => {
            let start = clamp(index_arg(caller, 0, 0.0));
            let end = clamp(index_arg(caller, 1, chars.len() as f64));
            let out: String = chars[start..end.max(start)].iter().collect();
            Ok(intern(caller, out))
        }
        "substring" => {
            let a = (index_arg(caller, 0, 0.0).max(0.0) as usize).min(chars.len());
            let b = (index_arg(caller, 1, chars.len() as f64).max(0.0) as usize).min(chars.len());
            let (start, end) = if a <= b { (a, b) } else { (b, a) };
            let out: String = chars[start..end].iter().collect();
            Ok(intern(caller, out))
        }
        "charAt" => {
            let index = index_arg(caller, 0, 0.0);
            let out = if index >= 0.0 {
                chars
                    .get(index as usize)
                    .map(char::to_string)
                    .unwrap_or_default()
            } else {
                String::new()
            };
            Ok(intern(caller, out))
        }
        "at" => {
            let index = index_arg(caller, 0, 0.0);
            let pos = if index < 0.0 {
                chars.len() as f64 + index
            } else {
                index
            };
            if pos < 0.0 || pos >= chars.len() as f64 {
                return Ok(TAG_UNDEFINED);
            }
            Ok(intern(caller, chars[pos as usize].to_string()))
        }
        "charCodeAt" => {
            let index = index_arg(caller, 0, 0.0);
            match chars.get(index.max(0.0) as usize) {
                Some(c) => Ok(encode_number(f64::from(*c as u32))),
                None => Ok(encode_number(f64::NAN)),
            }
        }
        "indexOf" | "lastIndexOf" => {
            let needle = string_arg(caller.data(), arg(args, 0));
            let found = if method == "indexOf" {
                text.find(&needle)
            } else {
                text.rfind(&needle)
            };
            let index = found
                .map(|byte| text[..byte].chars().count() as f64)
                .unwrap_or(-1.0);
            Ok(encode_number(index))
        }
        "includes" => {
            let needle = string_arg(caller.data(), arg(args, 0));
            Ok(encode_bool(text.contains(&needle)))
        }
        "startsWith" => {
            let needle = string_arg(caller.data(), arg(args, 0));
            Ok(encode_bool(text.starts_with(&needle)))
        }
        "endsWith" => {
            let needle = string_arg(caller.data(), arg(args, 0));
            Ok(encode_bool(text.ends_with(&needle)))
        }
        "toLowerCase" => Ok(intern(caller, text.to_lowercase())),
        "toUpperCase" => Ok(intern(caller, text.to_uppercase())),
        "trim" => Ok(intern(caller, text.trim().to_string())),
        "trimStart" => Ok(intern(caller, text.trim_start().to_string())),
        "trimEnd" => Ok(intern(caller, text.trim_end().to_string())),
        "replace" => {
            let from = string_arg(caller.data(), arg(args, 0));
            let to = string_arg(caller.data(), arg(args, 1));
            Ok(intern(caller, text.replacen(&from, &to, 1)))
        }
        "replaceAll" => {
            let from = string_arg(caller.data(), arg(args, 0));
            let to = string_arg(caller.data(), arg(args, 1));
            Ok(intern(caller, text.replace(&from, &to)))
        }
        "split" => {
            let separator = arg(args, 0);
            let parts: Vec<String> = match decode(separator) {
                JsValue::Undefined => vec![text.clone()],
                _ => {
                    let sep = string_arg(caller.data(), separator);
                    if sep.is_empty() {
                        chars.iter().map(char::to_string).collect()
                    } else {
                        text.split(&sep).map(str::to_string).collect()
                    }
                }
            };
            let bits: Vec<u64> = parts
                .into_iter()
                .map(|part| caller.data_mut().heap.intern_bits(part))
                .collect();
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(bits)))
        }
        "repeat" => {
            let count = index_arg(caller, 0, 0.0);
            if !(0.0..=10_000_000.0).contains(&count) {
                return Err(fail(method, format!("invalid repeat count {count}")));
            }
            Ok(intern(caller, text.repeat(count as usize)))
        }
        "padStart" | "padEnd" => {
            let target = index_arg(caller, 0, 0.0).max(0.0) as usize;
            let pad = match decode(arg(args, 1)) {
                JsValue::Undefined => " ".to_string(),
                _ => string_arg(caller.data(), arg(args, 1)),
            };
            let mut out = text.clone();
            if !pad.is_empty() {
                let pad_chars: Vec<char> = pad.chars().collect();
                let mut fill = String::new();
                let mut i = 0;
                while chars.len() + fill.chars().count() < target {
                    fill.push(pad_chars[i % pad_chars.len()]);
                    i += 1;
                }
                out = if method == "padStart" {
                    format!("{fill}{text}")
                } else {
                    format!("{text}{fill}")
                };
            }
            Ok(intern(caller, out))
        }
        "concat" => {
            let mut out = text.clone();
            for i in 0..args.len() {
                out.push_str(&string_arg(caller.data(), arg(args, i)));
            }
            Ok(intern(caller, out))
        }
        "toString" => Ok(receiver),
        "match" => Err(fail(
            method,
            "String.prototype.match is not supported by the dd perry-wasm host",
        )),
        other => Err(fail(
            other,
            format!("unsupported string method on {text:?}"),
        )),
    }
}

/// Built-in array operations, shared by `array_*` bridge names and bare
/// method-name dispatch.
fn array_op(
    caller: &mut Caller<'_, HostState>,
    op: &str,
    receiver: u64,
    args: &[u64],
) -> HostResult {
    let op = match op {
        "for" | "forEach" => "forEach",
        "index_of" | "indexOf" => "indexOf",
        "find_index" | "findIndex" => "findIndex",
        "push_spread" => "push_spread",
        other => other,
    };
    let JsValue::Handle(id) = decode(receiver) else {
        return Err(fail(op, "array receiver is not a handle"));
    };
    let items: Vec<u64> = match caller.data().heap.handle(id) {
        Some(HostValue::Array(items)) => items.clone(),
        other => return Err(fail(op, format!("receiver is not an array: {other:?}"))),
    };

    let store = |caller: &mut Caller<'_, HostState>, id: u32, items: Vec<u64>| {
        if let Some(HostValue::Array(slot)) = caller.data_mut().heap.handle_mut(id) {
            *slot = items;
        }
    };

    match op {
        "push" => {
            let mut items = items;
            items.push(arg(args, 0));
            let len = items.len();
            store(caller, id, items);
            Ok(encode_number(len as f64))
        }
        "push_spread" => {
            let extra = array_items(caller.data(), arg(args, 0)).unwrap_or_default();
            let mut items = items;
            items.extend(extra);
            let len = items.len();
            store(caller, id, items);
            Ok(encode_number(len as f64))
        }
        "pop" => {
            let mut items = items;
            let out = items.pop().unwrap_or(TAG_UNDEFINED);
            store(caller, id, items);
            Ok(out)
        }
        "shift" => {
            let mut items = items;
            let out = if items.is_empty() {
                TAG_UNDEFINED
            } else {
                items.remove(0)
            };
            store(caller, id, items);
            Ok(out)
        }
        "unshift" => {
            let mut items = items;
            items.insert(0, arg(args, 0));
            let len = items.len();
            store(caller, id, items);
            Ok(encode_number(len as f64))
        }
        "get" | "at" => {
            let index = caller.data().heap.to_number(arg(args, 0));
            let pos = if index < 0.0 && op == "at" {
                items.len() as f64 + index
            } else {
                index
            };
            if pos < 0.0 || pos >= items.len() as f64 {
                return Ok(TAG_UNDEFINED);
            }
            Ok(items[pos as usize])
        }
        "set" => {
            let index = caller.data().heap.to_number(arg(args, 0)) as usize;
            let mut items = items;
            if index >= items.len() {
                items.resize(index + 1, TAG_UNDEFINED);
            }
            items[index] = arg(args, 1);
            store(caller, id, items);
            Ok(TAG_UNDEFINED)
        }
        "length" => Ok(encode_number(items.len() as f64)),
        "slice" => {
            let (start, end) = slice_bounds(caller.data(), &prepend(receiver, args), items.len());
            Ok(caller
                .data_mut()
                .heap
                .alloc_bits(HostValue::Array(items[start..end].to_vec())))
        }
        "splice" => {
            let start_raw = caller.data().heap.to_number(arg(args, 0));
            let len = items.len() as f64;
            let start = if start_raw < 0.0 {
                (len + start_raw).max(0.0)
            } else {
                start_raw.min(len)
            } as usize;
            let delete_count = match decode(arg(args, 1)) {
                JsValue::Undefined => items.len() - start,
                _ => (caller.data().heap.to_number(arg(args, 1)).max(0.0) as usize)
                    .min(items.len() - start),
            };
            let mut items = items;
            let removed: Vec<u64> = items.drain(start..start + delete_count).collect();
            store(caller, id, items);
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(removed)))
        }
        "join" => {
            let separator = match decode(arg(args, 0)) {
                JsValue::Undefined => ",".to_string(),
                _ => string_arg(caller.data(), arg(args, 0)),
            };
            let state = caller.data();
            let joined = items
                .iter()
                .map(|item| match decode(*item) {
                    JsValue::Undefined | JsValue::Null => String::new(),
                    _ => state.heap.display(*item),
                })
                .collect::<Vec<_>>()
                .join(&separator);
            Ok(caller.data_mut().heap.intern_bits(joined))
        }
        "indexOf" => {
            let target = arg(args, 0);
            let index = items
                .iter()
                .position(|item| caller.data().heap.strict_eq(*item, target))
                .map(|i| i as f64)
                .unwrap_or(-1.0);
            Ok(encode_number(index))
        }
        "includes" => {
            let target = arg(args, 0);
            Ok(encode_bool(
                items
                    .iter()
                    .any(|item| caller.data().heap.strict_eq(*item, target)),
            ))
        }
        "concat" => {
            let extra = array_items(caller.data(), arg(args, 0)).unwrap_or_default();
            let mut out = items;
            out.extend(extra);
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(out)))
        }
        "reverse" => {
            let mut items = items;
            items.reverse();
            store(caller, id, items);
            Ok(receiver)
        }
        "flat" => {
            let mut out = Vec::new();
            for item in &items {
                match array_items(caller.data(), *item) {
                    Some(nested) => out.extend(nested),
                    None => out.push(*item),
                }
            }
            Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(out)))
        }
        "map" | "filter" | "forEach" | "find" | "findIndex" | "some" | "every" => {
            let callback = arg(args, 0);
            let mut out = Vec::new();
            for (index, item) in items.iter().enumerate() {
                let result = call_closure(
                    &mut *caller,
                    callback,
                    &[*item, encode_number(index as f64)],
                )?;
                let truthy = caller.data().heap.is_truthy(result);
                match op {
                    "map" => out.push(result),
                    "filter" if truthy => out.push(*item),
                    "find" if truthy => return Ok(*item),
                    "findIndex" if truthy => return Ok(encode_number(index as f64)),
                    "some" if truthy => return Ok(encode_bool(true)),
                    "every" if !truthy => return Ok(encode_bool(false)),
                    _ => {}
                }
            }
            match op {
                "map" | "filter" => Ok(caller.data_mut().heap.alloc_bits(HostValue::Array(out))),
                "find" => Ok(TAG_UNDEFINED),
                "findIndex" => Ok(encode_number(-1.0)),
                "some" => Ok(encode_bool(false)),
                "every" => Ok(encode_bool(true)),
                _ => Ok(TAG_UNDEFINED),
            }
        }
        "reduce" => {
            let callback = arg(args, 0);
            let mut iter = items.iter().copied().enumerate();
            let mut acc = match decode(arg(args, 1)) {
                JsValue::Undefined => match iter.next() {
                    Some((_, first)) => first,
                    None => return Err(fail(op, "reduce of empty array with no initial value")),
                },
                _ => arg(args, 1),
            };
            for (index, item) in iter {
                acc = call_closure(
                    &mut *caller,
                    callback,
                    &[acc, item, encode_number(index as f64)],
                )?;
            }
            Ok(acc)
        }
        "sort" => {
            let comparator = arg(args, 0);
            let has_comparator = !matches!(decode(comparator), JsValue::Undefined | JsValue::Null);
            let mut items = items;
            if has_comparator {
                // Insertion sort so the comparator (a guest call that can
                // fail) never runs inside a Rust sort closure.
                for i in 1..items.len() {
                    let mut j = i;
                    while j > 0 {
                        let ordering =
                            call_closure(&mut *caller, comparator, &[items[j - 1], items[j]])?;
                        if caller.data().heap.to_number(ordering) > 0.0 {
                            items.swap(j - 1, j);
                            j -= 1;
                        } else {
                            break;
                        }
                    }
                }
            } else {
                let state = caller.data();
                let mut keyed: Vec<(String, u64)> = items
                    .iter()
                    .map(|item| (state.heap.display(*item), *item))
                    .collect();
                keyed.sort_by(|a, b| a.0.cmp(&b.0));
                items = keyed.into_iter().map(|(_, bits)| bits).collect();
            }
            store(caller, id, items);
            Ok(receiver)
        }
        other => Err(fail(other, "unsupported array operation")),
    }
}

fn prepend(first: u64, rest: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(rest.len() + 1);
    out.push(first);
    out.extend_from_slice(rest);
    out
}

/// Bounds for `slice(start, end)` where args[1]/args[2] follow the receiver.
fn slice_bounds(state: &HostState, args: &[u64], len: usize) -> (usize, usize) {
    let resolve = |bits: u64, default: f64| -> usize {
        let n = match decode(bits) {
            JsValue::Undefined => default,
            _ => state.heap.to_number(bits),
        };
        let n = if n < 0.0 {
            (len as f64 + n).max(0.0)
        } else {
            n.min(len as f64)
        };
        n as usize
    };
    let start = resolve(arg(args, 1), 0.0);
    let end = resolve(arg(args, 2), len as f64).max(start);
    (start, end)
}

fn array_items(state: &HostState, bits: u64) -> Option<Vec<u64>> {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id)? {
            HostValue::Array(items) => Some(items.clone()),
            HostValue::SetValue(items) => Some(items.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn map_entries(state: &HostState, bits: u64) -> Option<Vec<(u64, u64)>> {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id)? {
            HostValue::MapValue(entries) => Some(entries.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn map_position(state: &HostState, handle: u64, key: u64) -> Option<usize> {
    map_entries(state, handle)?
        .iter()
        .position(|(k, _)| state.heap.strict_eq(*k, key))
}

fn set_items(state: &HostState, bits: u64) -> Option<Vec<u64>> {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id)? {
            HostValue::SetValue(items) => Some(items.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn bytes_of(state: &HostState, bits: u64) -> Vec<u8> {
    match decode(bits) {
        JsValue::Str(id) => state.heap.string(id).unwrap_or("").as_bytes().to_vec(),
        JsValue::Handle(id) => match state.heap.handle(id) {
            Some(HostValue::Buffer(bytes)) => bytes.clone(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Resolve a promise and queue its reactions as microtasks.
pub fn resolve_promise(state: &mut HostState, promise_bits: u64, value_bits: u64) {
    let JsValue::Handle(id) = decode(promise_bits) else {
        return;
    };
    let reactions = match state.heap.handle_mut(id) {
        Some(HostValue::Promise {
            state: promise_state,
            reactions,
        }) => {
            if *promise_state != PromiseState::Pending {
                return;
            }
            *promise_state = PromiseState::Resolved(value_bits);
            std::mem::take(reactions)
        }
        _ => return,
    };
    for reaction in reactions {
        state.microtasks.push_back(Microtask {
            callback_bits: reaction.callback_bits,
            value_bits,
            downstream: reaction.downstream,
        });
    }
}

/// Record a guest-visible exception the way the JS glue does: only inside a
/// `try` block; otherwise the failure surfaces as `undefined` plus a log line.
fn throw_in_guest(caller: &mut Caller<'_, HostState>, message: &str) {
    let state = caller.data_mut();
    if state.try_depth > 0 {
        let message_bits = state.heap.intern_bits(message.to_string());
        let error = state.heap.alloc_bits(HostValue::Error { message_bits });
        state.pending_exception = Some(error);
    } else {
        tracing::warn!(target: "perry_worker", "uncaught in worker: {message}");
    }
}

fn epoch_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

fn date_ms(state: &HostState, bits: u64) -> f64 {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id) {
            Some(HostValue::Date(ms)) => *ms,
            _ => f64::NAN,
        },
        _ => state.heap.to_number(bits),
    }
}

/// Civil date-time in UTC decomposed from epoch milliseconds, via the
/// days-from-civil algorithm (Howard Hinnant's date algorithms).
struct CivilDateTime {
    year: i64,
    month: u32,
    day: u32,
    weekday: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millisecond: u32,
}

impl CivilDateTime {
    fn from_ms(ms: f64) -> Self {
        let total_ms = ms as i64;
        let days = total_ms.div_euclid(86_400_000);
        let in_day = total_ms.rem_euclid(86_400_000);
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let year = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
        let year = if month <= 2 { year + 1 } else { year };
        let weekday = ((days % 7 + 7 + 4) % 7) as u32;
        CivilDateTime {
            year,
            month,
            day,
            weekday,
            hour: (in_day / 3_600_000) as u32,
            minute: (in_day / 60_000 % 60) as u32,
            second: (in_day / 1000 % 60) as u32,
            millisecond: (in_day % 1000) as u32,
        }
    }
}

fn iso_from_ms(ms: f64) -> String {
    let civil = CivilDateTime::from_ms(ms);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        civil.year,
        civil.month,
        civil.day,
        civil.hour,
        civil.minute,
        civil.second,
        civil.millisecond
    )
}

fn url_object(state: &mut HostState, parsed: &url::Url) -> u64 {
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let search_params = state.heap.alloc_bits(HostValue::SearchParams(pairs));
    let mut props: Vec<(String, u64)> = Vec::new();
    let put = |state: &mut HostState, props: &mut Vec<(String, u64)>, key: &str, value: String| {
        let bits = state.heap.intern_bits(value);
        props.push((key.to_string(), bits));
    };
    put(state, &mut props, "href", parsed.to_string());
    put(
        state,
        &mut props,
        "protocol",
        format!("{}:", parsed.scheme()),
    );
    put(
        state,
        &mut props,
        "hostname",
        parsed.host_str().unwrap_or("").to_string(),
    );
    put(
        state,
        &mut props,
        "port",
        parsed.port().map(|p| p.to_string()).unwrap_or_default(),
    );
    put(state, &mut props, "pathname", parsed.path().to_string());
    put(
        state,
        &mut props,
        "search",
        parsed.query().map(|q| format!("?{q}")).unwrap_or_default(),
    );
    put(
        state,
        &mut props,
        "hash",
        parsed
            .fragment()
            .map(|f| format!("#{f}"))
            .unwrap_or_default(),
    );
    put(
        state,
        &mut props,
        "origin",
        parsed.origin().ascii_serialization(),
    );
    props.push(("searchParams".to_string(), search_params));
    state.heap.alloc_bits(HostValue::Object(ObjectData {
        class: Some("URL".to_string()),
        props,
    }))
}

fn form_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err(format!("odd-length hex string ({} chars)", text.len()));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

/// Convert a heap value to JSON. Returns `None` for values JS omits
/// (`undefined`, functions), matching `JSON.stringify` semantics.
fn heap_to_json(
    state: &HostState,
    bits: u64,
    depth: usize,
) -> Result<Option<serde_json::Value>, wasmtime::Error> {
    if depth > 128 {
        return Err(fail(
            "json_stringify",
            "value nesting exceeds 128 levels (cycle?)",
        ));
    }
    let value = match decode(bits) {
        JsValue::Undefined => return Ok(None),
        JsValue::Null => serde_json::Value::Null,
        JsValue::Bool(b) => serde_json::Value::Bool(b),
        JsValue::Number(n) => {
            // JS prints integral numbers without a decimal point.
            if n == n.trunc() && n.abs() <= i64::MAX as f64 {
                serde_json::Value::Number(serde_json::Number::from(n as i64))
            } else {
                serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
        }
        JsValue::Str(id) => {
            serde_json::Value::String(state.heap.string(id).unwrap_or("").to_string())
        }
        JsValue::Handle(id) => match state.heap.handle(id) {
            Some(HostValue::Array(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(
                        heap_to_json(state, *item, depth + 1)?.unwrap_or(serde_json::Value::Null),
                    );
                }
                serde_json::Value::Array(out)
            }
            Some(HostValue::Object(object)) => {
                let mut out = serde_json::Map::new();
                for (key, prop_bits) in &object.props {
                    if key == "__class__" {
                        continue;
                    }
                    if let Some(v) = heap_to_json(state, *prop_bits, depth + 1)? {
                        out.insert(key.clone(), v);
                    }
                }
                serde_json::Value::Object(out)
            }
            Some(HostValue::Date(ms)) => serde_json::Value::String(iso_from_ms(*ms)),
            Some(HostValue::Closure { .. }) => return Ok(None),
            Some(HostValue::MapValue(_)) | Some(HostValue::SetValue(_)) => {
                serde_json::Value::Object(serde_json::Map::new())
            }
            Some(HostValue::SearchParams(_)) | Some(HostValue::Regexp { .. }) => {
                serde_json::Value::Object(serde_json::Map::new())
            }
            Some(HostValue::Error { message_bits }) => {
                serde_json::Value::String(state.heap.display(*message_bits))
            }
            Some(HostValue::Buffer(_)) | Some(HostValue::Promise { .. }) | None => {
                serde_json::Value::Object(serde_json::Map::new())
            }
        },
    };
    Ok(Some(value))
}

fn json_to_heap(state: &mut HostState, value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Null => TAG_NULL,
        serde_json::Value::Bool(b) => encode_bool(*b),
        serde_json::Value::Number(n) => encode_number(n.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(s) => state.heap.intern_bits(s.clone()),
        serde_json::Value::Array(items) => {
            let bits: Vec<u64> = items.iter().map(|item| json_to_heap(state, item)).collect();
            state.heap.alloc_bits(HostValue::Array(bits))
        }
        serde_json::Value::Object(map) => {
            let props: Vec<(String, u64)> = map
                .iter()
                .map(|(key, item)| (key.clone(), json_to_heap(state, item)))
                .collect();
            state
                .heap
                .alloc_bits(HostValue::Object(ObjectData { class: None, props }))
        }
    }
}

/// Serialize a heap value with host-side `JSON.stringify` semantics; entry
/// point for the `ffi.dd_json` helper.
pub fn json_stringify_bits(
    state: &HostState,
    bits: u64,
) -> Result<Option<String>, wasmtime::Error> {
    match heap_to_json(state, bits, 0)? {
        Some(value) => Ok(Some(
            serde_json::to_string(&value).map_err(|e| fail("dd_json", e))?,
        )),
        None => Ok(None),
    }
}

/// Decode a heap value to JSON (`None` for `undefined`/functions), used by
/// the tvar persistence in `dd_memory_atomic`.
pub fn heap_to_json_value(
    state: &HostState,
    bits: u64,
) -> Result<Option<serde_json::Value>, wasmtime::Error> {
    heap_to_json(state, bits, 0)
}

/// Materialize a JSON value into the heap, used to hand persisted tvars back
/// to the guest.
pub fn json_value_to_heap(state: &mut HostState, value: &serde_json::Value) -> u64 {
    json_to_heap(state, value)
}

/// Extremely small regex support: Perry's `regexp_test` goes through the host,
/// and full JS regex is out of scope for the experiment. Literal text plus
/// `^`/`$` anchors and `.` are handled; anything else is rejected loudly.
struct LiteRegex {
    anchored_start: bool,
    anchored_end: bool,
    case_insensitive: bool,
    pattern: Vec<Option<char>>,
}

fn regex_lite_compile(pattern: &str) -> Result<LiteRegex, String> {
    let case_insensitive = pattern.starts_with("(?i)");
    let body = pattern.strip_prefix("(?i)").unwrap_or(pattern);
    let anchored_start = body.starts_with('^');
    let body = body.strip_prefix('^').unwrap_or(body);
    let anchored_end = body.ends_with('$') && !body.ends_with("\\$");
    let body = if anchored_end {
        &body[..body.len() - 1]
    } else {
        body
    };
    let mut chars = body.chars().peekable();
    let mut out = Vec::new();
    while let Some(c) = chars.next() {
        match c {
            '.' => out.push(None),
            '\\' => match chars.next() {
                Some(escaped) => out.push(Some(escaped)),
                None => return Err("trailing backslash".to_string()),
            },
            '[' | ']' | '(' | ')' | '{' | '}' | '*' | '+' | '?' | '|' => {
                return Err(format!("unsupported regex construct {c:?}"));
            }
            other => out.push(Some(other)),
        }
    }
    Ok(LiteRegex {
        anchored_start,
        anchored_end,
        case_insensitive,
        pattern: out,
    })
}

impl LiteRegex {
    fn is_match(&self, text: &str) -> bool {
        let haystack: Vec<char> = if self.case_insensitive {
            text.to_lowercase().chars().collect()
        } else {
            text.chars().collect()
        };
        let needle: Vec<Option<char>> = if self.case_insensitive {
            self.pattern
                .iter()
                .map(|c| c.map(|c| c.to_lowercase().next().unwrap_or(c)))
                .collect()
        } else {
            self.pattern.clone()
        };
        let fits = |start: usize| -> bool {
            if start + needle.len() > haystack.len() {
                return false;
            }
            needle
                .iter()
                .zip(&haystack[start..])
                .all(|(want, have)| want.is_none_or(|c| c == *have))
        };
        match (self.anchored_start, self.anchored_end) {
            (true, true) => haystack.len() == needle.len() && fits(0),
            (true, false) => fits(0),
            (false, true) => haystack.len() >= needle.len() && fits(haystack.len() - needle.len()),
            (false, false) => (0..=haystack.len().saturating_sub(needle.len())).any(fits),
        }
    }
}
