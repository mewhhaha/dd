//! Compact binary Serde wire codec between Rust ops and Scriptw JS (`kernel.mjs`).
//!
//! Structs and maps become plain objects, sequences and tuples become arrays,
//! `None` and `()` become `null`, unit variants become their name, and other
//! variants become `{ Variant: value }`, matching `serde_json` and `dd`'s
//! original `serde_v8` data model. Integers past `Number.MAX_SAFE_INTEGER`
//! become BigInts. [`ToJsBuffer`] and serde's `serialize_bytes` become
//! `Uint8Array`s; byte arrays deserialize from `Uint8Array` / `ArrayBuffer` /
//! `ArrayBufferView` or plain arrays of bytes.
//!
//! Reading a struct skips fields whose value is `undefined`, so serde defaults
//! apply to them.

use serde::de::{self, DeserializeSeed, IntoDeserializer, Visitor};
use serde::ser::{self, Serialize};
use std::fmt;

const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;
const MIN_SAFE_INTEGER: i64 = -MAX_SAFE_INTEGER;
const MAX_DEPTH: usize = 128;

/// The most a single op argument may copy into Rust, unless the runtime sets
/// its own.
pub const DEFAULT_MAX_OP_ARGUMENT_BYTES: usize = 256 * 1024 * 1024;

const VALUE_COST: usize = 16;

// Wire value tags (must stay in sync with `src/kernel.mjs`):
pub const TAG_UNDEFINED: u8 = 0;
pub const TAG_NULL: u8 = 1;
pub const TAG_FALSE: u8 = 2;
pub const TAG_TRUE: u8 = 3;
pub const TAG_I32: u8 = 4;
pub const TAG_F64: u8 = 5;
pub const TAG_BIGINT: u8 = 6;
pub const TAG_STRING: u8 = 7;
pub const TAG_BYTES: u8 = 8;
pub const TAG_ARRAY: u8 = 9;
pub const TAG_OBJECT: u8 = 10;
pub const TAG_ERROR: u8 = 11;

// Wire envelope status bytes:
pub const ENV_OK: u8 = 0;
pub const ENV_ERR: u8 = 1;
pub const ENV_PENDING: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub(crate) String);

impl Error {
    pub(crate) fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }

    fn expected(what: &str, value: &WireValue) -> Self {
        Self(format!("expected {what}, got {}", value.type_repr()))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Bytes that serialize as a `Uint8Array` that owns them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToJsBuffer(Box<[u8]>);

impl From<Vec<u8>> for ToJsBuffer {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into_boxed_slice())
    }
}

impl From<Box<[u8]>> for ToJsBuffer {
    fn from(value: Box<[u8]>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for ToJsBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for ToJsBuffer {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

/// Owned value representation crossing the JS <-> Rust wire boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum WireValue {
    Undefined,
    Null,
    Bool(bool),
    I32(i32),
    F64(f64),
    BigInt(String),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<WireValue>),
    Object(Vec<(String, WireValue)>),
    Error(String),
}

impl WireValue {
    pub fn type_repr(&self) -> &'static str {
        match self {
            Self::Undefined => "undefined",
            Self::Null => "null",
            Self::Bool(_) => "boolean",
            Self::I32(_) | Self::F64(_) => "number",
            Self::BigInt(_) => "bigint",
            Self::String(_) => "string",
            Self::Bytes(_) => "Uint8Array",
            Self::Array(_) => "Array",
            Self::Object(_) => "object",
            Self::Error(_) => "error",
        }
    }
}

pub fn encode_wire(val: &WireValue, out: &mut Vec<u8>) {
    match val {
        WireValue::Undefined => out.push(TAG_UNDEFINED),
        WireValue::Null => out.push(TAG_NULL),
        WireValue::Bool(false) => out.push(TAG_FALSE),
        WireValue::Bool(true) => out.push(TAG_TRUE),
        WireValue::I32(n) => {
            out.push(TAG_I32);
            out.extend_from_slice(&n.to_le_bytes());
        }
        WireValue::F64(n) => {
            out.push(TAG_F64);
            out.extend_from_slice(&n.to_le_bytes());
        }
        WireValue::BigInt(s) => {
            out.push(TAG_BIGINT);
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        WireValue::String(s) => {
            out.push(TAG_STRING);
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        WireValue::Bytes(b) => {
            out.push(TAG_BYTES);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        WireValue::Array(items) => {
            out.push(TAG_ARRAY);
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for item in items {
                encode_wire(item, out);
            }
        }
        WireValue::Object(entries) => {
            out.push(TAG_OBJECT);
            out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for (k, v) in entries {
                let kb = k.as_bytes();
                out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                out.extend_from_slice(kb);
                encode_wire(v, out);
            }
        }
        WireValue::Error(msg) => {
            out.push(TAG_ERROR);
            let mb = msg.as_bytes();
            out.extend_from_slice(&(mb.len() as u32).to_le_bytes());
            out.extend_from_slice(mb);
        }
    }
}

pub fn encode_ok_envelope(val: &WireValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(ENV_OK);
    encode_wire(val, &mut out);
    out
}

pub fn encode_err_envelope(error: &crate::ops::OpError) -> Vec<u8> {
    use crate::ops::OpErrorClass;
    let mut out = Vec::with_capacity(64);
    out.push(ENV_ERR);
    match error.class() {
        OpErrorClass::Error => out.push(0),
        OpErrorClass::TypeError => out.push(1),
        OpErrorClass::RangeError => out.push(2),
        OpErrorClass::Custom(class) => {
            out.push(3);
            let cb = class.as_bytes();
            out.extend_from_slice(&(cb.len() as u32).to_le_bytes());
            out.extend_from_slice(cb);
        }
    }
    let mb = error.message().as_bytes();
    out.extend_from_slice(&(mb.len() as u32).to_le_bytes());
    out.extend_from_slice(mb);
    out
}

pub fn encode_pending_envelope(pending_id: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.push(ENV_PENDING);
    out.extend_from_slice(&pending_id.to_le_bytes());
    out
}

struct WireReader<'a> {
    buf: &'a [u8],
    pos: usize,
    budget: usize,
}

impl<'a> WireReader<'a> {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        match self.budget.checked_sub(bytes) {
            Some(rem) => {
                self.budget = rem;
                Ok(())
            }
            None => Err(Error::new("op argument exceeds maximum size")),
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| Error::new("unexpected EOF in wire data"))?;
        self.pos += 1;
        Ok(b)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let end = self
            .pos
            .checked_add(4)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| Error::new("unexpected EOF reading u32"))?;
        let arr: [u8; 4] = self.buf[self.pos..end].try_into().unwrap();
        self.pos = end;
        Ok(u32::from_le_bytes(arr))
    }

    fn read_i32(&mut self) -> Result<i32> {
        Ok(self.read_u32()? as i32)
    }

    fn read_f64(&mut self) -> Result<f64> {
        let end = self
            .pos
            .checked_add(8)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| Error::new("unexpected EOF reading f64"))?;
        let arr: [u8; 8] = self.buf[self.pos..end].try_into().unwrap();
        self.pos = end;
        Ok(f64::from_le_bytes(arr))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.charge(len)?;
        let end = self
            .pos
            .checked_add(len)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| Error::new("wire byte length exceeds payload"))?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }

    fn read_value(&mut self, depth: usize) -> Result<WireValue> {
        if depth > MAX_DEPTH {
            return Err(Error::new("value is nested too deeply"));
        }
        self.charge(VALUE_COST)?;
        let tag = self.read_u8()?;
        match tag {
            TAG_UNDEFINED => Ok(WireValue::Undefined),
            TAG_NULL => Ok(WireValue::Null),
            TAG_FALSE => Ok(WireValue::Bool(false)),
            TAG_TRUE => Ok(WireValue::Bool(true)),
            TAG_I32 => Ok(WireValue::I32(self.read_i32()?)),
            TAG_F64 => Ok(WireValue::F64(self.read_f64()?)),
            TAG_BIGINT => Ok(WireValue::BigInt(self.read_string()?)),
            TAG_STRING => Ok(WireValue::String(self.read_string()?)),
            TAG_BYTES => {
                let len = self.read_u32()? as usize;
                let slice = self.read_bytes(len)?;
                Ok(WireValue::Bytes(slice.to_vec()))
            }
            TAG_ARRAY => {
                let len = self.read_u32()? as usize;
                let cap = len.min(self.buf.len().saturating_sub(self.pos)).min(1 << 20);
                let mut out = Vec::with_capacity(cap);
                for _ in 0..len {
                    out.push(self.read_value(depth + 1)?);
                }
                Ok(WireValue::Array(out))
            }
            TAG_OBJECT => {
                let len = self.read_u32()? as usize;
                let cap = len.min(self.buf.len().saturating_sub(self.pos)).min(1 << 20);
                let mut out = Vec::with_capacity(cap);
                for _ in 0..len {
                    let k = self.read_string()?;
                    let v = self.read_value(depth + 1)?;
                    out.push((k, v));
                }
                Ok(WireValue::Object(out))
            }
            TAG_ERROR => Ok(WireValue::Error(self.read_string()?)),
            other => Err(Error::new(format!("unknown wire value tag {other}"))),
        }
    }
}

pub fn decode_wire(bytes: &[u8], max_bytes: usize) -> Result<WireValue> {
    let mut reader = WireReader {
        buf: bytes,
        pos: 0,
        budget: max_bytes,
    };
    reader.read_value(0)
}

pub fn decode_wire_args(bytes: &[u8], max_bytes: usize) -> Result<Vec<WireValue>> {
    let mut reader = WireReader {
        buf: bytes,
        pos: 0,
        budget: max_bytes,
    };
    let count = reader.read_u32()? as usize;
    let cap = count.min(bytes.len()).min(1024);
    let mut out = Vec::with_capacity(cap);
    for _ in 0..count {
        out.push(reader.read_value(0)?);
    }
    Ok(out)
}

pub fn to_wire<T: Serialize + ?Sized>(value: &T) -> Result<WireValue> {
    value.serialize(Serializer)
}

pub fn from_wire<T: de::DeserializeOwned>(value: &WireValue) -> Result<T> {
    T::deserialize(Deserializer { value, depth: 0 })
}

// =========================================================================
// Serde Serializer -> WireValue
// =========================================================================

struct Serializer;

impl Serializer {
    fn wrap_variant(variant: &'static str, value: WireValue) -> WireValue {
        WireValue::Object(vec![(variant.to_string(), value)])
    }
}

impl ser::Serializer for Serializer {
    type Ok = WireValue;
    type Error = Error;
    type SerializeSeq = SeqSer;
    type SerializeTuple = SeqSer;
    type SerializeTupleStruct = SeqSer;
    type SerializeTupleVariant = TupleVariantSer;
    type SerializeMap = MapSer;
    type SerializeStruct = MapSer;
    type SerializeStructVariant = StructVariantSer;

    fn serialize_bool(self, v: bool) -> Result<WireValue> {
        Ok(WireValue::Bool(v))
    }
    fn serialize_i8(self, v: i8) -> Result<WireValue> {
        Ok(WireValue::I32(i32::from(v)))
    }
    fn serialize_i16(self, v: i16) -> Result<WireValue> {
        Ok(WireValue::I32(i32::from(v)))
    }
    fn serialize_i32(self, v: i32) -> Result<WireValue> {
        Ok(WireValue::I32(v))
    }
    fn serialize_i64(self, v: i64) -> Result<WireValue> {
        if (MIN_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&v) {
            Ok(WireValue::F64(v as f64))
        } else {
            Ok(WireValue::BigInt(v.to_string()))
        }
    }
    fn serialize_u8(self, v: u8) -> Result<WireValue> {
        Ok(WireValue::I32(i32::from(v)))
    }
    fn serialize_u16(self, v: u16) -> Result<WireValue> {
        Ok(WireValue::I32(i32::from(v)))
    }
    fn serialize_u32(self, v: u32) -> Result<WireValue> {
        if v <= i32::MAX as u32 {
            Ok(WireValue::I32(v as i32))
        } else {
            Ok(WireValue::F64(f64::from(v)))
        }
    }
    fn serialize_u64(self, v: u64) -> Result<WireValue> {
        if v <= MAX_SAFE_INTEGER as u64 {
            Ok(WireValue::F64(v as f64))
        } else {
            Ok(WireValue::BigInt(v.to_string()))
        }
    }
    fn serialize_f32(self, v: f32) -> Result<WireValue> {
        Ok(WireValue::F64(f64::from(v)))
    }
    fn serialize_f64(self, v: f64) -> Result<WireValue> {
        Ok(WireValue::F64(v))
    }
    fn serialize_char(self, v: char) -> Result<WireValue> {
        Ok(WireValue::String(v.to_string()))
    }
    fn serialize_str(self, v: &str) -> Result<WireValue> {
        Ok(WireValue::String(v.to_string()))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<WireValue> {
        Ok(WireValue::Bytes(v.to_vec()))
    }
    fn serialize_none(self) -> Result<WireValue> {
        Ok(WireValue::Null)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<WireValue> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<WireValue> {
        Ok(WireValue::Null)
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<WireValue> {
        Ok(WireValue::Null)
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<WireValue> {
        Ok(WireValue::String(variant.to_string()))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<WireValue> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<WireValue> {
        Ok(Self::wrap_variant(variant, value.serialize(self)?))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(SeqSer(Vec::with_capacity(len.unwrap_or(0))))
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        Ok(SeqSer(Vec::with_capacity(len)))
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(SeqSer(Vec::with_capacity(len)))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(TupleVariantSer {
            variant,
            items: Vec::with_capacity(len),
        })
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(MapSer {
            entries: Vec::with_capacity(len.unwrap_or(0)),
            key: None,
        })
    }
    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        Ok(MapSer {
            entries: Vec::with_capacity(len),
            key: None,
        })
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(StructVariantSer {
            variant,
            entries: Vec::with_capacity(len),
        })
    }
}

struct SeqSer(Vec<WireValue>);

impl ser::SerializeSeq for SeqSer {
    type Ok = WireValue;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.push(value.serialize(Serializer)?);
        Ok(())
    }
    fn end(self) -> Result<WireValue> {
        Ok(WireValue::Array(self.0))
    }
}

impl ser::SerializeTuple for SeqSer {
    type Ok = WireValue;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.push(value.serialize(Serializer)?);
        Ok(())
    }
    fn end(self) -> Result<WireValue> {
        Ok(WireValue::Array(self.0))
    }
}

impl ser::SerializeTupleStruct for SeqSer {
    type Ok = WireValue;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.push(value.serialize(Serializer)?);
        Ok(())
    }
    fn end(self) -> Result<WireValue> {
        Ok(WireValue::Array(self.0))
    }
}

struct TupleVariantSer {
    variant: &'static str,
    items: Vec<WireValue>,
}

impl ser::SerializeTupleVariant for TupleVariantSer {
    type Ok = WireValue;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.items.push(value.serialize(Serializer)?);
        Ok(())
    }
    fn end(self) -> Result<WireValue> {
        Ok(Serializer::wrap_variant(
            self.variant,
            WireValue::Array(self.items),
        ))
    }
}

struct MapSer {
    entries: Vec<(String, WireValue)>,
    key: Option<String>,
}

impl ser::SerializeMap for MapSer {
    type Ok = WireValue;
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        let key_val = key.serialize(Serializer)?;
        let key_str = match key_val {
            WireValue::String(s) => s,
            WireValue::I32(n) => n.to_string(),
            WireValue::F64(n) => n.to_string(),
            WireValue::BigInt(s) => s,
            WireValue::Bool(b) => b.to_string(),
            other => return Err(Error::expected("string map key", &other)),
        };
        self.key = Some(key_str);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let k = self
            .key
            .take()
            .ok_or_else(|| Error::new("serialize_value called before serialize_key"))?;
        self.entries.push((k, value.serialize(Serializer)?));
        Ok(())
    }
    fn end(self) -> Result<WireValue> {
        Ok(WireValue::Object(self.entries))
    }
}

impl ser::SerializeStruct for MapSer {
    type Ok = WireValue;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.entries
            .push((key.to_string(), value.serialize(Serializer)?));
        Ok(())
    }
    fn end(self) -> Result<WireValue> {
        Ok(WireValue::Object(self.entries))
    }
}

struct StructVariantSer {
    variant: &'static str,
    entries: Vec<(String, WireValue)>,
}

impl ser::SerializeStructVariant for StructVariantSer {
    type Ok = WireValue;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.entries
            .push((key.to_string(), value.serialize(Serializer)?));
        Ok(())
    }
    fn end(self) -> Result<WireValue> {
        Ok(Serializer::wrap_variant(
            self.variant,
            WireValue::Object(self.entries),
        ))
    }
}

// =========================================================================
// Serde Deserializer <- WireValue
// =========================================================================

struct Deserializer<'a> {
    value: &'a WireValue,
    depth: usize,
}

impl<'a> Deserializer<'a> {
    fn next_depth(&self) -> Result<usize> {
        let next = self.depth + 1;
        if next > MAX_DEPTH {
            return Err(Error::new("value is nested too deeply"));
        }
        Ok(next)
    }

    fn as_i64(&self) -> Result<i64> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::I32(n) => Ok(i64::from(*n)),
            WireValue::F64(n) => {
                if n.is_nan() {
                    return Ok(0);
                }
                if !n.is_finite() {
                    return Err(Error::new("expected finite integer"));
                }
                let t = n.trunc();
                if t < (i64::MIN as f64) || t >= 9223372036854775808.0 {
                    return Err(Error::new("integer out of i64 range"));
                }
                Ok(t as i64)
            }
            WireValue::BigInt(s) => s
                .parse::<i64>()
                .map_err(|_| Error::new("BigInt out of i64 range")),
            other => Err(Error::expected("number", other)),
        }
    }

    fn as_u64(&self) -> Result<u64> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::I32(n) => {
                u64::try_from(*n).map_err(|_| Error::new("negative integer for unsigned type"))
            }
            WireValue::F64(n) => {
                if n.is_nan() {
                    return Ok(0);
                }
                if !n.is_finite() {
                    return Err(Error::new("expected finite integer"));
                }
                let t = n.trunc();
                if t < 0.0 || t >= 18446744073709551616.0 {
                    return Err(Error::new("integer out of u64 range"));
                }
                Ok(t as u64)
            }
            WireValue::BigInt(s) => s
                .parse::<u64>()
                .map_err(|_| Error::new("BigInt out of u64 range")),
            other => Err(Error::expected("number", other)),
        }
    }

    fn as_f64(&self) -> Result<f64> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::I32(n) => Ok(f64::from(*n)),
            WireValue::F64(n) => Ok(*n),
            WireValue::BigInt(s) => {
                if let Ok(v) = s.parse::<i64>() {
                    Ok(v as f64)
                } else if let Ok(v) = s.parse::<u64>() {
                    Ok(v as f64)
                } else {
                    Err(Error::new("BigInt out of f64 range"))
                }
            }
            other => Err(Error::expected("number", other)),
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for Deserializer<'a> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Undefined | WireValue::Null => visitor.visit_unit(),
            WireValue::Bool(b) => visitor.visit_bool(*b),
            WireValue::I32(n) => visitor.visit_i32(*n),
            WireValue::F64(n) => {
                if n.is_finite() && n.fract() == 0.0 && *n >= (MIN_SAFE_INTEGER as f64) && *n <= (MAX_SAFE_INTEGER as f64) {
                    if *n >= 0.0 {
                        visitor.visit_u64(*n as u64)
                    } else {
                        visitor.visit_i64(*n as i64)
                    }
                } else {
                    visitor.visit_f64(*n)
                }
            }
            WireValue::BigInt(s) => {
                if let Ok(v) = s.parse::<i64>() {
                    visitor.visit_i64(v)
                } else if let Ok(v) = s.parse::<u64>() {
                    visitor.visit_u64(v)
                } else {
                    Err(Error::new("BigInt out of i64/u64 range"))
                }
            }
            WireValue::String(s) => visitor.visit_str(s),
            WireValue::Bytes(b) => visitor.visit_bytes(b),
            WireValue::Array(items) => {
                let d = self.next_depth()?;
                visitor.visit_seq(SeqDe {
                    items,
                    index: 0,
                    depth: d,
                })
            }
            WireValue::Object(entries) => {
                let d = self.next_depth()?;
                visitor.visit_map(MapDe {
                    entries,
                    index: 0,
                    depth: d,
                    skip_undefined: true,
                })
            }
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Bool(b) => visitor.visit_bool(*b),
            WireValue::Undefined | WireValue::Null => visitor.visit_bool(false),
            WireValue::I32(n) => visitor.visit_bool(*n != 0),
            WireValue::F64(n) => visitor.visit_bool(*n != 0.0 && !n.is_nan()),
            WireValue::BigInt(s) => visitor.visit_bool(s != "0" && s != "-0"),
            WireValue::String(s) => visitor.visit_bool(!s.is_empty()),
            WireValue::Bytes(_) | WireValue::Array(_) | WireValue::Object(_) => {
                visitor.visit_bool(true)
            }
        }
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let n = self.as_i64()?;
        let v = i8::try_from(n).map_err(|_| Error::new("integer out of i8 range"))?;
        visitor.visit_i8(v)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let n = self.as_i64()?;
        let v = i16::try_from(n).map_err(|_| Error::new("integer out of i16 range"))?;
        visitor.visit_i16(v)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let n = self.as_i64()?;
        let v = i32::try_from(n).map_err(|_| Error::new("integer out of i32 range"))?;
        visitor.visit_i32(v)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.as_i64()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let n = self.as_u64()?;
        let v = u8::try_from(n).map_err(|_| Error::new("integer out of u8 range"))?;
        visitor.visit_u8(v)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let n = self.as_u64()?;
        let v = u16::try_from(n).map_err(|_| Error::new("integer out of u16 range"))?;
        visitor.visit_u16(v)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let n = self.as_u64()?;
        let v = u32::try_from(n).map_err(|_| Error::new("integer out of u32 range"))?;
        visitor.visit_u32(v)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.as_u64()?)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(self.as_f64()? as f32)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(self.as_f64()?)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::String(s) => {
                let mut chars = s.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) => visitor.visit_char(c),
                    _ => Err(Error::new("expected single-character string")),
                }
            }
            other => Err(Error::expected("string", other)),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::String(s) => visitor.visit_str(s),
            other => Err(Error::expected("string", other)),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::String(s) => visitor.visit_string(s.clone()),
            other => Err(Error::expected("string", other)),
        }
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Bytes(b) => visitor.visit_byte_buf(b.clone()),
            WireValue::Array(items) => {
                let d = self.next_depth()?;
                visitor.visit_seq(SeqDe {
                    items,
                    index: 0,
                    depth: d,
                })
            }
            other => Err(Error::expected("an ArrayBuffer or ArrayBufferView", other)),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Undefined | WireValue::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Undefined | WireValue::Null => visitor.visit_unit(),
            other => Err(Error::expected("null or undefined", other)),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Array(items) => {
                let d = self.next_depth()?;
                visitor.visit_seq(SeqDe {
                    items,
                    index: 0,
                    depth: d,
                })
            }
            WireValue::Bytes(bytes) => {
                let d = self.next_depth()?;
                visitor.visit_seq(BytesSeqDe {
                    bytes,
                    index: 0,
                    depth: d,
                })
            }
            other => Err(Error::expected("array", other)),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Object(entries) => {
                let d = self.next_depth()?;
                visitor.visit_map(MapDe {
                    entries,
                    index: 0,
                    depth: d,
                    skip_undefined: true,
                })
            }
            other => Err(Error::expected("object", other)),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::Object(entries) => {
                let d = self.next_depth()?;
                visitor.visit_map(MapDe {
                    entries,
                    index: 0,
                    depth: d,
                    skip_undefined: true,
                })
            }
            other => Err(Error::expected("object", other)),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self.value {
            WireValue::Error(msg) => Err(Error::new(msg.clone())),
            WireValue::String(s) => visitor.visit_enum(s.as_str().into_deserializer()),
            WireValue::Object(entries) if entries.len() == 1 => {
                let d = self.next_depth()?;
                visitor.visit_enum(EnumDe {
                    variant: &entries[0].0,
                    payload: &entries[0].1,
                    depth: d,
                })
            }
            other => Err(Error::expected("enum string or single-key object", other)),
        }
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }
}

struct SeqDe<'a> {
    items: &'a [WireValue],
    index: usize,
    depth: usize,
}

impl<'de, 'a> de::SeqAccess<'de> for SeqDe<'a> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        let Some(value) = self.items.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        seed.deserialize(Deserializer {
            value,
            depth: self.depth,
        })
        .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.items.len() - self.index)
    }
}

struct BytesSeqDe<'a> {
    bytes: &'a [u8],
    index: usize,
    depth: usize,
}

impl<'de, 'a> de::SeqAccess<'de> for BytesSeqDe<'a> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        let Some(&byte) = self.bytes.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        let val = WireValue::I32(i32::from(byte));
        seed.deserialize(Deserializer {
            value: &val,
            depth: self.depth,
        })
        .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.bytes.len() - self.index)
    }
}

struct MapDe<'a> {
    entries: &'a [(String, WireValue)],
    index: usize,
    depth: usize,
    skip_undefined: bool,
}

impl<'de, 'a> de::MapAccess<'de> for MapDe<'a> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        while let Some((k, v)) = self.entries.get(self.index) {
            if self.skip_undefined && matches!(v, WireValue::Undefined) {
                self.index += 1;
                continue;
            }
            let key_val = WireValue::String(k.clone());
            return seed
                .deserialize(Deserializer {
                    value: &key_val,
                    depth: self.depth,
                })
                .map(Some);
        }
        Ok(None)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let (_, v) = &self.entries[self.index];
        self.index += 1;
        seed.deserialize(Deserializer {
            value: v,
            depth: self.depth,
        })
    }
}

struct EnumDe<'a> {
    variant: &'a str,
    payload: &'a WireValue,
    depth: usize,
}

impl<'de, 'a> de::EnumAccess<'de> for EnumDe<'a> {
    type Error = Error;
    type Variant = VariantDe<'a>;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let key_val = WireValue::String(self.variant.to_string());
        let val = seed.deserialize(Deserializer {
            value: &key_val,
            depth: self.depth,
        })?;
        Ok((
            val,
            VariantDe {
                payload: self.payload,
                depth: self.depth,
            },
        ))
    }
}

struct VariantDe<'a> {
    payload: &'a WireValue,
    depth: usize,
}

impl<'de, 'a> de::VariantAccess<'de> for VariantDe<'a> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(Deserializer {
            value: self.payload,
            depth: self.depth,
        })
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        de::Deserializer::deserialize_seq(
            Deserializer {
                value: self.payload,
                depth: self.depth,
            },
            visitor,
        )
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        de::Deserializer::deserialize_struct(
            Deserializer {
                value: self.payload,
                depth: self.depth,
            },
            "",
            fields,
            visitor,
        )
    }
}
