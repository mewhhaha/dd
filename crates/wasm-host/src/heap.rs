//! Host-side value heap for one Perry wasm instance.
//!
//! The guest never owns objects: every string, object, array, closure, and
//! promise lives here and is referenced from wasm as a NaN-boxed id (see
//! [`crate::nanbox`]). String ids are assigned in interning order; the guest
//! relies on that ordering because `__init_strings` registers its literals
//! sequentially during `_start`.

use crate::nanbox::{JsValue, decode, encode};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum PromiseState {
    Pending,
    Resolved(u64),
}

/// A `.then` registration: when the promise resolves, call `callback_bits`
/// (a closure handle) with the value, and resolve `downstream` with the result.
#[derive(Debug, Clone)]
pub struct PromiseReaction {
    pub callback_bits: u64,
    pub downstream: u32,
}

#[derive(Debug, Clone)]
pub struct ObjectData {
    pub class: Option<String>,
    pub props: Vec<(String, u64)>,
}

impl ObjectData {
    pub fn get(&self, key: &str) -> Option<u64> {
        self.props
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, bits)| *bits)
    }

    pub fn set(&mut self, key: &str, bits: u64) {
        if let Some(entry) = self.props.iter_mut().find(|(name, _)| name == key) {
            entry.1 = bits;
        } else {
            self.props.push((key.to_string(), bits));
        }
    }

    pub fn delete(&mut self, key: &str) {
        self.props.retain(|(name, _)| name != key);
    }
}

#[derive(Debug, Clone)]
pub enum HostValue {
    Object(ObjectData),
    Array(Vec<u64>),
    Closure {
        func_idx: u32,
        captures: Vec<u64>,
    },
    Promise {
        state: PromiseState,
        reactions: Vec<PromiseReaction>,
    },
    Buffer(Vec<u8>),
    Date(f64),
    MapValue(Vec<(u64, u64)>),
    SetValue(Vec<u64>),
    Error {
        message_bits: u64,
    },
    Regexp {
        source: String,
        flags: String,
    },
    SearchParams(Vec<(String, String)>),
}

#[derive(Default)]
pub struct Heap {
    strings: Vec<String>,
    handles: HashMap<u32, HostValue>,
    next_handle: u32,
}

impl Heap {
    pub fn intern_string(&mut self, value: String) -> u32 {
        self.strings.push(value);
        (self.strings.len() - 1) as u32
    }

    pub fn string(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(String::as_str)
    }

    pub fn intern_bits(&mut self, value: String) -> u64 {
        let id = self.intern_string(value);
        encode(JsValue::Str(id))
    }

    pub fn alloc(&mut self, value: HostValue) -> u32 {
        self.next_handle += 1;
        let id = self.next_handle;
        self.handles.insert(id, value);
        id
    }

    pub fn alloc_bits(&mut self, value: HostValue) -> u64 {
        encode(JsValue::Handle(self.alloc(value)))
    }

    pub fn handle(&self, id: u32) -> Option<&HostValue> {
        self.handles.get(&id)
    }

    pub fn handle_mut(&mut self, id: u32) -> Option<&mut HostValue> {
        self.handles.get_mut(&id)
    }

    /// Resolve bits to a string when they carry one (string id, or handle to
    /// an Error/Buffer is not included — only true strings).
    pub fn string_of(&self, bits: u64) -> Option<&str> {
        match decode(bits) {
            JsValue::Str(id) => self.string(id),
            _ => None,
        }
    }

    /// JS-style display conversion (`String(value)`), used for concatenation,
    /// `console.log`, and header serialization.
    pub fn display(&self, bits: u64) -> String {
        match decode(bits) {
            JsValue::Undefined => "undefined".to_string(),
            JsValue::Null => "null".to_string(),
            JsValue::Bool(b) => b.to_string(),
            JsValue::Number(n) => format_number(n),
            JsValue::Str(id) => self.string(id).unwrap_or("").to_string(),
            JsValue::Handle(id) => match self.handle(id) {
                Some(HostValue::Array(items)) => items
                    .iter()
                    .map(|item| match decode(*item) {
                        JsValue::Undefined | JsValue::Null => String::new(),
                        _ => self.display(*item),
                    })
                    .collect::<Vec<_>>()
                    .join(","),
                Some(HostValue::Error { message_bits }) => {
                    format!("Error: {}", self.display(*message_bits))
                }
                Some(HostValue::Date(ms)) => format!("[Date {ms}]"),
                Some(HostValue::Closure { .. }) => "[Function]".to_string(),
                Some(_) => "[object Object]".to_string(),
                None => "undefined".to_string(),
            },
        }
    }

    /// JS truthiness.
    pub fn is_truthy(&self, bits: u64) -> bool {
        match decode(bits) {
            JsValue::Undefined | JsValue::Null | JsValue::Bool(false) => false,
            JsValue::Bool(true) => true,
            JsValue::Number(n) => n != 0.0 && !n.is_nan(),
            JsValue::Str(id) => self.string(id).is_some_and(|s| !s.is_empty()),
            JsValue::Handle(_) => true,
        }
    }

    /// JS strict equality on decoded values (string content, not id).
    pub fn strict_eq(&self, a: u64, b: u64) -> bool {
        match (decode(a), decode(b)) {
            (JsValue::Str(x), JsValue::Str(y)) => self.string(x) == self.string(y),
            (JsValue::Number(x), JsValue::Number(y)) => x == y,
            (x, y) => x == y,
        }
    }

    /// JS `Number(value)` coercion.
    pub fn to_number(&self, bits: u64) -> f64 {
        match decode(bits) {
            JsValue::Undefined => f64::NAN,
            JsValue::Null => 0.0,
            JsValue::Bool(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            JsValue::Number(n) => n,
            JsValue::Str(id) => {
                let s = self.string(id).unwrap_or("").trim();
                if s.is_empty() {
                    0.0
                } else {
                    s.parse().unwrap_or(f64::NAN)
                }
            }
            JsValue::Handle(id) => match self.handle(id) {
                Some(HostValue::Date(ms)) => *ms,
                _ => f64::NAN,
            },
        }
    }
}

/// Format a number the way JS string conversion does for the common cases:
/// integral values print without a decimal point.
pub fn format_number(n: f64) -> String {
    if n.is_nan() {
        return "NaN".to_string();
    }
    if n.is_infinite() {
        return if n > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if n == n.trunc() && n.abs() < 1e21 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_ids_are_sequential_from_zero() {
        let mut heap = Heap::default();
        assert_eq!(heap.intern_string("a".into()), 0);
        assert_eq!(heap.intern_string("b".into()), 1);
        assert_eq!(heap.string(1), Some("b"));
    }

    #[test]
    fn strict_eq_compares_string_content_across_ids() {
        let mut heap = Heap::default();
        let a = heap.intern_bits("same".into());
        let b = heap.intern_bits("same".into());
        assert!(heap.strict_eq(a, b));
    }

    #[test]
    fn display_joins_arrays_like_js() {
        let mut heap = Heap::default();
        let s = heap.intern_bits("x".into());
        let arr = heap.alloc_bits(HostValue::Array(vec![
            s,
            crate::nanbox::encode_number(2.0),
            crate::nanbox::TAG_NULL,
        ]));
        assert_eq!(heap.display(arr), "x,2,");
    }
}
