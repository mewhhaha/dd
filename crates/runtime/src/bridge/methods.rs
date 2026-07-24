//! Generic method dispatch: user class methods, then built-in string and
//! array methods by receiver type. Split from the bridge dispatcher for
//! navigability; semantics unchanged.

use super::{
    HostResult, arg, call_closure, call_guest, class_of, fail, find_class_method, string_arg,
};
use crate::heap::HostValue;
use crate::nanbox::{JsValue, TAG_UNDEFINED, decode, encode_bool, encode_number};
use crate::state::HostState;
use wasmtime::Caller;

/// Generic method dispatch: user class methods first, then built-in string,
/// array, and buffer methods by receiver type.
pub(super) fn method_call(
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
            // Perry emits `p.then(cb)` as a bare method call; route it into
            // the promise machinery (the reference glue drops it — a gap, not
            // a behavior to copy). There is no rejection path in Perry's wasm
            // target, so `catch` is a no-op chain and `finally` behaves as
            // `then`.
            Some(HostValue::Promise { .. }) if method == "then" || method == "finally" => Ok(
                super::values::promise_then(caller.data_mut(), receiver, arg(args, 0)),
            ),
            Some(HostValue::Promise { .. }) if method == "catch" => Ok(receiver),
            Some(HostValue::Object(object))
                if object.class.as_deref() == Some("Response")
                    && matches!(method, "text" | "json") =>
            {
                super::fetch::response_method(caller, method, receiver, args)
                    .expect("text/json are Response methods")
            }
            Some(other) => Err(unknown_method(format!(
                "no method {method:?} on receiver {other:?}"
            ))),
            None => Err(unknown_method(format!(
                "no method {method:?} on dead handle"
            ))),
        },
        other => Err(unknown_method(format!(
            "no method {method:?} on primitive {other:?}"
        ))),
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

/// Marker error for "this method/receiver combination does not exist", as
/// opposed to failures raised while running a known method. The dispatcher
/// softens the former to `undefined` (reference-glue parity) and propagates
/// the latter.
#[derive(Debug)]
pub(super) struct UnknownMethod(pub String);

impl std::fmt::Display for UnknownMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UnknownMethod {}

pub(super) fn unknown_method(detail: impl Into<String>) -> wasmtime::Error {
    wasmtime::Error::new(UnknownMethod(detail.into()))
}

pub(super) fn is_unknown_method(error: &wasmtime::Error) -> bool {
    error.downcast_ref::<UnknownMethod>().is_some()
}

/// Built-in string methods. Indices are Unicode scalar positions — exact for
/// ASCII and BMP text, diverging from JS UTF-16 indices only for astral chars.
pub(super) fn method_call_string(
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
        other => Err(unknown_method(format!(
            "unsupported string method {other:?} on {text:?}"
        ))),
    }
}

/// Built-in array operations, shared by `array_*` bridge names and bare
/// method-name dispatch.
pub(super) fn array_op(
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
        return Err(unknown_method(format!(
            "array receiver for {op:?} is not a handle"
        )));
    };
    let items: Vec<u64> = match caller.data().heap.handle(id) {
        Some(HostValue::Array(items)) => items.clone(),
        other => {
            return Err(unknown_method(format!(
                "receiver for {op:?} is not an array: {other:?}"
            )));
        }
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
        other => Err(unknown_method(format!(
            "unsupported array operation {other:?}"
        ))),
    }
}

pub(super) fn prepend(first: u64, rest: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(rest.len() + 1);
    out.push(first);
    out.extend_from_slice(rest);
    out
}

/// Bounds for `slice(start, end)` where args[1]/args[2] follow the receiver.
pub(super) fn slice_bounds(state: &HostState, args: &[u64], len: usize) -> (usize, usize) {
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

pub(super) fn array_items(state: &HostState, bits: u64) -> Option<Vec<u64>> {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id)? {
            HostValue::Array(items) => Some(items.clone()),
            HostValue::SetValue(items) => Some(items.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn map_entries(state: &HostState, bits: u64) -> Option<Vec<(u64, u64)>> {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id)? {
            HostValue::MapValue(entries) => Some(entries.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn map_position(state: &HostState, handle: u64, key: u64) -> Option<usize> {
    map_entries(state, handle)?
        .iter()
        .position(|(k, _)| state.heap.strict_eq(*k, key))
}

pub(super) fn set_items(state: &HostState, bits: u64) -> Option<Vec<u64>> {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id)? {
            HostValue::SetValue(items) => Some(items.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn bytes_of(state: &HostState, bits: u64) -> Vec<u8> {
    match decode(bits) {
        JsValue::Str(id) => state.heap.string(id).unwrap_or("").as_bytes().to_vec(),
        JsValue::Handle(id) => match state.heap.handle(id) {
            Some(HostValue::Buffer(bytes)) => bytes.clone(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}
