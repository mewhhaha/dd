// `TextEncoder`, `TextDecoder`, and forgiving base64, ported from deno_web's
// lib.rs (Copyright 2018-2026 the Deno authors, MIT license). Streaming
// decoders live in the op state behind a numeric handle instead of a
// garbage-collected wrapper; the decoder's JS object frees it.

use base64::Engine;
use dd_v8::builtins::{buffer_bytes, with_buffer_mut};
use dd_v8::{JsBuffer, OpError, OpState, ToJsBuffer, v8};
use encoding_rs::{CoderResult, Decoder, DecoderResult, Encoding};
use std::collections::HashMap;

#[derive(Default)]
struct Decoders {
    next: u32,
    decoders: HashMap<u32, (Decoder, bool)>,
}

pub(super) fn op_encoding_normalize_label(
    _state: &mut OpState,
    label: String,
) -> Result<String, OpError> {
    let encoding = Encoding::for_label_no_replacement(label.as_bytes()).ok_or_else(|| {
        OpError::range_error(format!(
            "The encoding label provided ('{label}') is invalid."
        ))
    })?;
    Ok(encoding.name().to_lowercase())
}

fn throw(scope: &mut v8::PinScope<'_, '_>, error: OpError) {
    let exception = error.to_exception(scope);
    scope.throw_exception(exception);
}

/// `op_encoding_decode_utf8(bytes, ignoreBOM) -> string`
pub(super) fn op_encoding_decode_utf8<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Some(bytes) = buffer_bytes(args.get(0)) else {
        return throw(scope, OpError::type_error("expected a buffer source"));
    };
    let ignore_bom = args.get(1).is_true();
    let bytes = if !ignore_bom {
        bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(&bytes)
    } else {
        &bytes
    };
    match v8::String::new_from_utf8(scope, bytes, v8::NewStringType::Normal) {
        Some(text) => rv.set(text.into()),
        None => throw(scope, OpError::range_error("The string is too long")),
    }
}

/// `op_encoding_decode_utf8_ascii_only(bytes) -> string | null`
pub(super) fn op_encoding_decode_utf8_ascii_only<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Some(bytes) = buffer_bytes(args.get(0)) else {
        return throw(scope, OpError::type_error("expected a buffer source"));
    };
    if !bytes.is_ascii() {
        return rv.set_null();
    }
    match v8::String::new_from_one_byte(scope, &bytes, v8::NewStringType::Normal) {
        Some(text) => rv.set(text.into()),
        None => throw(scope, OpError::range_error("The string is too long")),
    }
}

fn decode(decoder: &mut Decoder, fatal: bool, data: &[u8], last: bool) -> Result<String, OpError> {
    let max_length = decoder
        .max_utf16_buffer_length(data.len())
        .ok_or_else(|| OpError::range_error("Value too large to decode"))?;
    let mut output = vec![0u16; max_length];
    let written = if fatal {
        let (result, _, written) =
            decoder.decode_to_utf16_without_replacement(data, &mut output, last);
        match result {
            DecoderResult::InputEmpty => written,
            DecoderResult::OutputFull => {
                return Err(OpError::range_error("Provided buffer too small"));
            }
            DecoderResult::Malformed(_, _) => {
                return Err(OpError::type_error("The encoded data is not valid"));
            }
        }
    } else {
        let (result, _, written, _) = decoder.decode_to_utf16(data, &mut output, last);
        match result {
            CoderResult::InputEmpty => written,
            CoderResult::OutputFull => {
                return Err(OpError::range_error("Provided buffer too small"));
            }
        }
    };
    output.truncate(written);
    Ok(String::from_utf16_lossy(&output))
}

fn new_decoder(label: &str, ignore_bom: bool) -> Result<Decoder, OpError> {
    let encoding = Encoding::for_label(label.as_bytes()).ok_or_else(|| {
        OpError::range_error(format!(
            "The encoding label provided ('{label}') is invalid."
        ))
    })?;
    Ok(if ignore_bom {
        encoding.new_decoder_without_bom_handling()
    } else {
        encoding.new_decoder_with_bom_removal()
    })
}

pub(super) fn op_encoding_decode_single(
    _state: &mut OpState,
    data: JsBuffer,
    label: String,
    fatal: bool,
    ignore_bom: bool,
) -> Result<String, OpError> {
    let mut decoder = new_decoder(&label, ignore_bom)?;
    decode(&mut decoder, fatal, &data, true)
}

pub(super) fn op_encoding_new_decoder(
    state: &mut OpState,
    label: String,
    fatal: bool,
    ignore_bom: bool,
) -> Result<u32, OpError> {
    let decoder = new_decoder(&label, ignore_bom)?;
    if !state.has::<Decoders>() {
        state.put(Decoders::default());
    }
    let decoders = state.borrow_mut::<Decoders>();
    decoders.next = decoders.next.wrapping_add(1).max(1);
    let handle = decoders.next;
    decoders.decoders.insert(handle, (decoder, fatal));
    Ok(handle)
}

/// Decodes a chunk with a streaming decoder; the last chunk (`stream ==
/// false`) frees it.
pub(super) fn op_encoding_decode(
    state: &mut OpState,
    data: JsBuffer,
    handle: u32,
    stream: bool,
) -> Result<String, OpError> {
    let decoders = state
        .try_borrow_mut::<Decoders>()
        .ok_or_else(|| OpError::type_error("TextDecoder state is gone"))?;
    let (decoder, fatal) = decoders
        .decoders
        .get_mut(&handle)
        .ok_or_else(|| OpError::type_error("TextDecoder state is gone"))?;
    let result = decode(decoder, *fatal, &data, !stream);
    if !stream || result.is_err() {
        decoders.decoders.remove(&handle);
    }
    result
}

pub(super) fn op_encoding_drop_decoder(state: &mut OpState, handle: u32) {
    if let Some(decoders) = state.try_borrow_mut::<Decoders>() {
        decoders.decoders.remove(&handle);
    }
}

/// `op_encoding_encode_into(source, destination) -> [read, written]`
pub(super) fn op_encoding_encode_into<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Ok(source) = v8::Local::<v8::String>::try_from(args.get(0)) else {
        return throw(scope, OpError::type_error("expected a string"));
    };
    let mut read = 0;
    let written = with_buffer_mut(args.get(1), |destination| {
        source.write_utf8_v2(
            scope,
            destination,
            v8::WriteFlags::kReplaceInvalidUtf8,
            Some(&mut read),
        )
    });
    let Some(written) = written else {
        return throw(scope, OpError::type_error("expected a Uint8Array"));
    };
    let read = v8::Number::new(scope, read as f64).into();
    let written = v8::Number::new(scope, written as f64).into();
    rv.set(v8::Array::new_with_elements(scope, &[read, written]).into());
}

fn is_ascii_whitespace(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | 0x0c | b'\r' | b' ')
}

/// <https://infra.spec.whatwg.org/#forgiving-base64-decode>
pub(super) fn forgiving_base64_decode(input: &str) -> Result<Vec<u8>, OpError> {
    let invalid = || OpError::new("Failed to decode base64");
    let mut data = input
        .bytes()
        .filter(|byte| !is_ascii_whitespace(*byte))
        .collect::<Vec<_>>();
    if data.len() % 4 == 0 {
        if data.ends_with(b"==") {
            data.truncate(data.len() - 2);
        } else if data.ends_with(b"=") {
            data.truncate(data.len() - 1);
        }
    }
    if data.len() % 4 == 1
        || data
            .iter()
            .any(|byte| !(byte.is_ascii_alphanumeric() || *byte == b'+' || *byte == b'/'))
    {
        return Err(invalid());
    }
    let engine = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new()
            .with_decode_allow_trailing_bits(true)
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::RequireNone),
    );
    engine.decode(&data).map_err(|_| invalid())
}

pub(super) fn op_base64_decode(_state: &mut OpState, input: String) -> Result<ToJsBuffer, OpError> {
    forgiving_base64_decode(&input).map(Into::into)
}

pub(super) fn op_base64_encode_from_buffer(
    _state: &mut OpState,
    data: JsBuffer,
    offset: u32,
    length: u32,
) -> Result<String, OpError> {
    let start = offset as usize;
    let end = start
        .checked_add(length as usize)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| OpError::range_error("Buffer too small"))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(&data[start..end]))
}

#[cfg(test)]
mod tests {
    use super::forgiving_base64_decode;

    #[test]
    fn forgiving_base64_follows_the_infra_spec() {
        assert_eq!(forgiving_base64_decode("aGk=").unwrap(), b"hi");
        assert_eq!(forgiving_base64_decode(" aG k ").unwrap(), b"hi");
        assert_eq!(forgiving_base64_decode("aGk").unwrap(), b"hi");
        assert_eq!(forgiving_base64_decode("YQ").unwrap(), b"a");
        assert_eq!(forgiving_base64_decode("YR==").unwrap(), b"a");
        assert!(forgiving_base64_decode("a").is_err());
        assert!(forgiving_base64_decode("a===").is_err());
        assert!(forgiving_base64_decode("a-b_").is_err());
    }
}
