//! Arbitrary Rust values through the serde bridge: to V8 and straight back,
//! back through `deserialize_any` (as JSON), and back after V8's own
//! serializer copied them in both modes and op_structured_clone did. Every
//! path must give the value it started from.

#![no_main]

use arbitrary::Arbitrary;
use dd_v8::{serde_v8, v8};
use dd_v8_fuzz::{Entry, with};
use libfuzzer_sys::fuzz_target;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Floats compare by bits, so -0 differs from 0, except that every NaN is
/// equal: V8 may canonicalize NaN payloads.
macro_rules! float {
    ($name:ident, $ty:ty) => {
        #[derive(Arbitrary, Clone, Copy, Debug, Deserialize, Serialize)]
        #[serde(transparent)]
        struct $name($ty);

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.0.to_bits() == other.0.to_bits() || (self.0.is_nan() && other.0.is_nan())
            }
        }
    };
}

float!(F32, f32);
float!(F64, f64);

#[derive(Arbitrary, Debug, Deserialize, PartialEq, Serialize)]
struct Marker;

#[derive(Arbitrary, Debug, Deserialize, PartialEq, Serialize)]
struct Record {
    id: u64,
    name: String,
    score: Option<F64>,
    #[serde(with = "serde_bytes")]
    blob: Vec<u8>,
    children: Vec<Node>,
    unit: (),
    marker: Marker,
}

#[derive(Arbitrary, Debug, Deserialize, PartialEq, Serialize)]
enum Node {
    Unit,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(F32),
    F64(F64),
    Char(char),
    Str(String),
    Bytes(#[serde(with = "serde_bytes")] Vec<u8>),
    Option(Option<Box<Node>>),
    List(Vec<Node>),
    Tuple(i64, String, Box<Node>),
    Map(BTreeMap<String, Node>),
    Record(Box<Record>),
    Struct { left: Box<Node>, right: Option<u64> },
}

impl Node {
    /// How deeply the JavaScript form nests: every variant is an object
    /// `{ Variant: value }`, and lists, maps, tuples and structs add a level.
    fn depth(&self) -> usize {
        1 + match self {
            Node::Option(Some(node)) => node.depth(),
            Node::Tuple(_, _, node) => 1 + node.depth(),
            Node::List(nodes) => 1 + nodes.iter().map(Node::depth).max().unwrap_or(0),
            Node::Map(nodes) => 1 + nodes.values().map(Node::depth).max().unwrap_or(0),
            Node::Record(record) => 2 + record.children.iter().map(Node::depth).max().unwrap_or(0),
            Node::Struct { left, .. } => 1 + left.depth(),
            _ => 1,
        }
    }

    fn has_bytes(&self) -> bool {
        match self {
            Node::Bytes(_) | Node::Record(_) => true,
            Node::Option(Some(node)) | Node::Tuple(_, _, node) => node.has_bytes(),
            Node::List(nodes) => nodes.iter().any(Node::has_bytes),
            Node::Map(nodes) => nodes.values().any(Node::has_bytes),
            Node::Struct { left, .. } => left.has_bytes(),
            _ => false,
        }
    }
}

/// JSON equality that compares numbers as numbers: V8 has one number type,
/// so `deserialize_any` reads 3.0 back as the integer 3.
fn json_eq(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_eq(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| json_eq(a, b)))
        }
        (a, b) => a == b,
    }
}

fuzz_target!(|node: Node| {
    // serde_v8 stops reading at depth 128; stay inside it.
    if node.depth() > 120 {
        return;
    }
    with(|harness| {
        let value = {
            let runtime = harness.runtime();
            dd_v8::scope!(scope, runtime);
            let value = serde_v8::to_v8(scope, &node).expect("every Node converts to V8");
            let back: Node = serde_v8::from_v8(scope, value).expect("a converted Node reads back");
            assert_eq!(back, node, "to_v8 then from_v8");

            match serde_v8::from_v8::<serde_json::Value>(scope, value) {
                Ok(json) => {
                    let expected = serde_json::to_value(&node).expect("Node is JSON");
                    assert!(
                        json_eq(&json, &expected),
                        "deserialize_any gave {json} for {expected}"
                    );
                }
                Err(error) => assert!(
                    node.has_bytes(),
                    "deserialize_any failed without bytes: {error}"
                ),
            }
            v8::Global::new(scope, value)
        };
        let copies = harness.call_value(Entry::Clone, value);
        let runtime = harness.runtime();
        dd_v8::scope!(scope, runtime);
        let copies = v8::Local::new(scope, copies);
        let copies = v8::Local::<v8::Array>::try_from(copies).expect("clone returns an array");
        for (index, path) in ["op_structured_clone", "message", "storage"]
            .into_iter()
            .enumerate()
        {
            let copy = copies.get_index(scope, index as u32).expect("copy");
            let back: Node = serde_v8::from_v8(scope, copy)
                .unwrap_or_else(|error| panic!("{path} copy does not read back: {error}"));
            assert_eq!(back, node, "{path} copy");
        }
    });
});
