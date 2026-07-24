//! NaN-boxed value encoding shared with Perry-compiled wasm modules.
//!
//! Every JS value crosses the wasm boundary as 64 raw bits (declared `i64`).
//! The tag constants must match `wasm_runtime.js` in the Perry compiler and
//! `perry-runtime/src/value.rs`; they are part of the compiled module's ABI.

pub const TAG_UNDEFINED: u64 = 0x7FFC_0000_0000_0001;
pub const TAG_NULL: u64 = 0x7FFC_0000_0000_0002;
pub const TAG_FALSE: u64 = 0x7FFC_0000_0000_0003;
pub const TAG_TRUE: u64 = 0x7FFC_0000_0000_0004;

const STRING_TAG: u64 = 0x7FFF;
const POINTER_TAG: u64 = 0x7FFD;
const INT32_TAG: u64 = 0x7FFE;

/// A decoded boundary value. `Str` and `Handle` are indexes into the
/// per-instance [`crate::heap::Heap`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JsValue {
    Undefined,
    Null,
    Bool(bool),
    Number(f64),
    Str(u32),
    Handle(u32),
}

pub fn decode(bits: u64) -> JsValue {
    match bits {
        TAG_UNDEFINED => return JsValue::Undefined,
        TAG_NULL => return JsValue::Null,
        TAG_FALSE => return JsValue::Bool(false),
        TAG_TRUE => return JsValue::Bool(true),
        _ => {}
    }
    let id = (bits & 0xFFFF_FFFF) as u32;
    match bits >> 48 {
        STRING_TAG => JsValue::Str(id),
        POINTER_TAG => JsValue::Handle(id),
        INT32_TAG => JsValue::Number(id as i32 as f64),
        _ => JsValue::Number(f64::from_bits(bits)),
    }
}

pub fn encode(value: JsValue) -> u64 {
    match value {
        JsValue::Undefined => TAG_UNDEFINED,
        JsValue::Null => TAG_NULL,
        JsValue::Bool(false) => TAG_FALSE,
        JsValue::Bool(true) => TAG_TRUE,
        JsValue::Number(n) => n.to_bits(),
        JsValue::Str(id) => (STRING_TAG << 48) | u64::from(id),
        JsValue::Handle(id) => (POINTER_TAG << 48) | u64::from(id),
    }
}

pub fn encode_number(n: f64) -> u64 {
    encode(JsValue::Number(n))
}

pub fn encode_bool(b: bool) -> u64 {
    if b { TAG_TRUE } else { TAG_FALSE }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_value_kind() {
        for value in [
            JsValue::Undefined,
            JsValue::Null,
            JsValue::Bool(true),
            JsValue::Bool(false),
            JsValue::Number(42.5),
            JsValue::Number(0.0),
            JsValue::Str(7),
            JsValue::Handle(19),
        ] {
            assert_eq!(decode(encode(value)), value);
        }
    }

    #[test]
    fn decodes_int32_tag_with_sign_extension() {
        let bits = (INT32_TAG << 48) | u64::from((-5i32) as u32);
        assert_eq!(decode(bits), JsValue::Number(-5.0));
    }

    #[test]
    fn plain_nan_stays_a_number() {
        let bits = f64::NAN.to_bits();
        match decode(bits) {
            JsValue::Number(n) => assert!(n.is_nan()),
            other => panic!("expected NaN number, got {other:?}"),
        }
    }
}
