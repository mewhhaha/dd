//! Ops every JavaScript layer on top of a runtime needs: structured
//! serialization, text encoding, value type checks, the async context that
//! follows promise continuations, cancellable timers, and printing.

use crate::serde_v8;
use crate::{OpDecl, OpState, op_async, op_raw, op_sync};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::oneshot;
use v8::{ValueDeserializerHelper, ValueSerializerHelper};

/// The symbol objects use to describe how to clone themselves.
pub const HOST_OBJECT_BRAND: &str = "Deno.core.hostObject";

pub fn ops() -> Vec<OpDecl> {
    vec![
        op_raw!(op_encode),
        op_raw!(op_decode),
        op_raw!(op_serialize),
        op_raw!(op_deserialize),
        op_raw!(op_structured_clone),
        op_raw!(op_get_async_context),
        op_raw!(op_set_async_context),
        op_raw!(op_is_any_array_buffer),
        op_raw!(op_is_array_buffer),
        op_raw!(op_is_array_buffer_view),
        op_raw!(op_is_data_view),
        op_raw!(op_is_date),
        op_raw!(op_is_map),
        op_raw!(op_is_native_error),
        op_raw!(op_is_promise),
        op_raw!(op_is_proxy),
        op_raw!(op_is_reg_exp),
        op_raw!(op_is_set),
        op_raw!(op_is_shared_array_buffer),
        op_raw!(op_is_string_object),
        op_raw!(op_is_typed_array),
        op_raw!(op_is_boxed_primitive),
        op_raw!(op_register_error_builder),
        op_raw!(op_set_wasm_streaming_handler),
        op_raw!(op_wasm_streaming_feed),
        op_raw!(op_wasm_streaming_set_url),
        op_raw!(op_wasm_streaming_finish),
        op_raw!(op_wasm_streaming_abort),
        op_sync!(op_print),
        op_async!(op_timer_sleep),
        op_sync!(op_timer_cancel),
    ]
}

const ERROR_BUILDERS: &str = "dd_v8.errorBuilders";
const WASM_STREAMING_HANDLER: &str = "dd_v8.wasmStreamingHandler";

fn private_key<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    name: &str,
) -> Option<v8::Local<'s, v8::Private>> {
    let name = v8::String::new(scope, name)?;
    Some(v8::Private::for_api(scope, Some(name)))
}

/// The object mapping error class names to builders, kept on the ops object
/// under a private key so it is part of snapshots and out of user code's
/// reach.
fn error_builders<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    create: bool,
) -> Option<v8::Local<'s, v8::Object>> {
    let ops = crate::runtime::runtime_ops_object(scope)?;
    let key = private_key(scope, ERROR_BUILDERS)?;
    match ops.get_private(scope, key) {
        Some(value) if value.is_object() => value.to_object(scope),
        _ if create => {
            let builders = v8::Object::new(scope);
            ops.set_private(scope, key, builders.into());
            Some(builders)
        }
        _ => None,
    }
}

/// An exception of a registered class, or an `Error` named after the class
/// when nothing registered it (or its builder threw).
pub(crate) fn build_custom_error<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    class: &str,
    message: v8::Local<'s, v8::String>,
) -> v8::Local<'s, v8::Value> {
    let built = (|| {
        let builders = error_builders(scope, false)?;
        let key = v8::String::new(scope, class)?;
        let builder = v8::Local::<v8::Function>::try_from(builders.get(scope, key.into())?).ok()?;
        v8::tc_scope!(let tc, scope);
        let receiver = v8::undefined(tc).into();
        let built = builder.call(tc, receiver, &[message.into()]);
        if tc.has_caught() {
            return None;
        }
        built
    })();
    if let Some(built) = built {
        return built;
    }
    let error = v8::Exception::error(scope, message);
    if let (Ok(object), Some(name), Some(class)) = (
        v8::Local::<v8::Object>::try_from(error),
        v8::String::new(scope, "name"),
        v8::String::new(scope, class),
    ) {
        object.set(scope, name.into(), class.into());
    }
    error
}

/// `op_register_error_builder(className, builder)`: ops failing with
/// `OpError::custom(className, message)` throw `builder(message)`.
fn op_register_error_builder<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    if !args.get(1).is_function() {
        return throw_type_error(scope, "error builder must be a function");
    }
    let Some(builders) = error_builders(scope, true) else {
        return;
    };
    builders.set(scope, args.get(0), args.get(1));
}

fn throw_type_error(scope: &mut v8::PinScope<'_, '_>, message: &str) {
    let message = v8::String::new(scope, message).unwrap_or_else(|| v8::String::empty(scope));
    let exception = v8::Exception::type_error(scope, message);
    scope.throw_exception(exception);
}

fn throw_range_error(scope: &mut v8::PinScope<'_, '_>, message: &str) {
    let message = v8::String::new(scope, message).unwrap_or_else(|| v8::String::empty(scope));
    let exception = v8::Exception::range_error(scope, message);
    scope.throw_exception(exception);
}

/// Copies the bytes of an `ArrayBuffer` or view.
pub fn buffer_bytes(value: v8::Local<v8::Value>) -> Option<Vec<u8>> {
    if let Ok(view) = v8::Local::<v8::ArrayBufferView>::try_from(value) {
        let mut bytes = vec![0; view.byte_length()];
        view.copy_contents(&mut bytes);
        return Some(bytes);
    }
    let buffer = v8::Local::<v8::ArrayBuffer>::try_from(value).ok()?;
    let store = buffer.get_backing_store();
    let Some(data) = store.data() else {
        return Some(Vec::new());
    };
    // SAFETY: the backing store is alive here and holds `byte_length` bytes.
    let bytes =
        unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), store.byte_length()) };
    Some(bytes.to_vec())
}

/// Runs `f` on the bytes of an `ArrayBuffer` or view in place, so writes land
/// in JavaScript memory. `None` when `value` holds no buffer.
pub fn with_buffer_mut<R>(
    value: v8::Local<v8::Value>,
    f: impl FnOnce(&mut [u8]) -> R,
) -> Option<R> {
    let (data, length) = if let Ok(view) = v8::Local::<v8::ArrayBufferView>::try_from(value) {
        (view.data().cast::<u8>(), view.byte_length())
    } else {
        let buffer = v8::Local::<v8::ArrayBuffer>::try_from(value).ok()?;
        let data = buffer
            .data()
            .map(|data| data.as_ptr().cast::<u8>())
            .unwrap_or(std::ptr::null_mut());
        (data, buffer.byte_length())
    };
    if data.is_null() || length == 0 {
        return Some(f(&mut []));
    }
    // SAFETY: the buffer is reachable from the caller's handle scope, so its
    // memory stays alive and unmoved while `f` runs on this thread.
    Some(f(unsafe { std::slice::from_raw_parts_mut(data, length) }))
}

/// Writes `values` into a `Uint32Array` (or any view) as native-endian words.
pub fn write_u32s(value: v8::Local<v8::Value>, values: &[u32]) -> bool {
    with_buffer_mut(value, |bytes| {
        for (chunk, word) in bytes.as_chunks_mut::<4>().0.iter_mut().zip(values) {
            *chunk = word.to_ne_bytes();
        }
    })
    .is_some()
}

fn op_encode<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Ok(text) = v8::Local::<v8::String>::try_from(args.get(0)) else {
        return throw_type_error(scope, "Invalid argument");
    };
    let text = text.to_rust_string_lossy(scope);
    rv.set(serde_v8::uint8_array(scope, text.into_bytes()).into());
}

fn op_decode<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Some(bytes) = buffer_bytes(args.get(0)) else {
        return throw_type_error(scope, "expected an ArrayBuffer or ArrayBufferView");
    };
    let bytes = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(&bytes);
    match v8::String::new_from_utf8(scope, bytes, v8::NewStringType::Normal) {
        Some(text) => rv.set(text.into()),
        None => throw_range_error(scope, "string too long"),
    }
}

fn host_object_brand<'s>(scope: &v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Symbol> {
    let key = v8::String::new(scope, HOST_OBJECT_BRAND).expect("brand name");
    v8::Symbol::for_key(scope, key)
}

/// The serializer delegate: branded objects serialize the value their brand
/// function returns, tagged `u32::MAX`; other host objects are looked up in
/// `host_objects` and written as their index.
struct Delegate<'a> {
    brand: v8::Local<'a, v8::Symbol>,
    host_objects: Option<v8::Local<'a, v8::Array>>,
    error_callback: Option<v8::Local<'a, v8::Function>>,
    deserializers: Option<v8::Local<'a, v8::Object>>,
    for_storage: bool,
}

impl v8::ValueSerializerImpl for Delegate<'_> {
    fn throw_data_clone_error<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        message: v8::Local<'s, v8::String>,
    ) {
        if let Some(callback) = self.error_callback {
            v8::tc_scope!(let scope, scope);
            let receiver = v8::undefined(scope).into();
            callback.call(scope, receiver, &[message.into()]);
            if scope.has_caught() || scope.has_terminated() {
                scope.rethrow();
                return;
            }
        }
        let error = v8::Exception::type_error(scope, message);
        scope.throw_exception(error);
    }

    fn get_shared_array_buffer_id<'s>(
        &self,
        _scope: &mut v8::PinScope<'s, '_>,
        _shared_array_buffer: v8::Local<'s, v8::SharedArrayBuffer>,
    ) -> Option<u32> {
        None
    }

    fn get_wasm_module_transfer_id(
        &self,
        scope: &mut v8::PinScope<'_, '_>,
        _module: v8::Local<v8::WasmModuleObject>,
    ) -> Option<u32> {
        if self.for_storage {
            let message = v8::String::new(scope, "Wasm modules cannot be stored")?;
            self.throw_data_clone_error(scope, message);
        }
        None
    }

    fn has_custom_host_object(&self, _isolate: &v8::Isolate) -> bool {
        true
    }

    fn is_host_object<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        object: v8::Local<'s, v8::Object>,
    ) -> Option<bool> {
        object.has(scope, self.brand.into())
    }

    fn write_host_object<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        object: v8::Local<'s, v8::Object>,
        serializer: &dyn v8::ValueSerializerHelper,
    ) -> Option<bool> {
        let brand = object.get(scope, self.brand.into())?;
        if let Ok(describe) = v8::Local::<v8::Function>::try_from(brand) {
            let description = describe.call(scope, object.into(), &[])?;
            serializer.write_uint32(u32::MAX);
            serializer.write_value(scope.get_current_context(), description);
            return Some(true);
        }
        if let Some(host_objects) = self.host_objects {
            for index in 0..host_objects.length() {
                if host_objects.get_index(scope, index)? == v8::Local::<v8::Value>::from(object) {
                    serializer.write_uint32(index);
                    return Some(true);
                }
            }
        }
        let message = v8::String::new(scope, "Unsupported object type")?;
        self.throw_data_clone_error(scope, message);
        None
    }
}

impl v8::ValueDeserializerImpl for Delegate<'_> {
    fn get_shared_array_buffer_from_id<'s>(
        &self,
        _scope: &mut v8::PinScope<'s, '_>,
        _transfer_id: u32,
    ) -> Option<v8::Local<'s, v8::SharedArrayBuffer>> {
        None
    }

    fn get_wasm_module_from_id<'s>(
        &self,
        _scope: &mut v8::PinScope<'s, '_>,
        _clone_id: u32,
    ) -> Option<v8::Local<'s, v8::WasmModuleObject>> {
        None
    }

    fn read_host_object<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        deserializer: &dyn v8::ValueDeserializerHelper,
    ) -> Option<v8::Local<'s, v8::Object>> {
        let mut index = 0;
        if !deserializer.read_uint32(&mut index) {
            return None;
        }
        if index == u32::MAX {
            if let Some(deserializers) = self.deserializers
                && let Some(description) = deserializer.read_value(scope.get_current_context())
                && let Some(object) = description.to_object(scope)
            {
                let kind = object.get(scope, serde_v8::key(scope, "type").into())?;
                let revive = deserializers.get(scope, kind)?;
                if let Ok(revive) = v8::Local::<v8::Function>::try_from(revive) {
                    let receiver = v8::null(scope).into();
                    v8::allow_javascript_execution_scope!(let scope, scope);
                    let revived = revive.call(scope, receiver, &[description])?;
                    return revived.to_object(scope);
                }
            }
        } else if let Some(host_objects) = self.host_objects
            && let Some(value) = host_objects.get_index(scope, index)
        {
            return value.to_object(scope);
        }
        let message = v8::String::new(scope, "Failed to deserialize host object")?;
        let error = v8::Exception::error(scope, message);
        scope.throw_exception(error);
        None
    }
}

fn optional<'s, T>(value: v8::Local<'s, v8::Value>) -> Result<Option<v8::Local<'s, T>>, ()>
where
    v8::Local<'s, T>: TryFrom<v8::Local<'s, v8::Value>>,
{
    if value.is_null_or_undefined() {
        return Ok(None);
    }
    v8::Local::<T>::try_from(value).map(Some).map_err(|_| ())
}

fn serialize<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
    delegate: Delegate<'s>,
) -> Option<Vec<u8>> {
    let serializer = v8::ValueSerializer::new(scope, Box::new(delegate));
    serializer.write_header();
    v8::tc_scope!(let tc, scope);
    let written = serializer.write_value(tc.get_current_context(), value);
    if tc.has_caught() || tc.has_terminated() {
        tc.rethrow();
        return None;
    }
    if written != Some(true) {
        throw_type_error(tc, "Failed to serialize response");
        return None;
    }
    Some(serializer.release())
}

fn deserialize<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    bytes: &[u8],
    delegate: Delegate<'s>,
) -> Option<v8::Local<'s, v8::Value>> {
    let deserializer = v8::ValueDeserializer::new(scope, Box::new(delegate), bytes);
    let context = scope.get_current_context();
    if deserializer.read_header(context) != Some(true) {
        throw_range_error(scope, "could not deserialize value");
        return None;
    }
    v8::tc_scope!(let tc, scope);
    match deserializer.read_value(tc.get_current_context()) {
        Some(value) => Some(value),
        None => {
            if tc.has_caught() || tc.has_terminated() {
                tc.rethrow();
            } else {
                throw_range_error(tc, "could not deserialize value");
            }
            None
        }
    }
}

/// `op_serialize(value, hostObjects?, transferredArrayBuffers?, forStorage, errorCallback?)`
fn op_serialize<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Ok(host_objects) = optional::<v8::Array>(args.get(1)) else {
        return throw_type_error(scope, "hostObjects not an array");
    };
    let Ok(error_callback) = optional::<v8::Function>(args.get(4)) else {
        return throw_type_error(scope, "Invalid error callback");
    };
    let delegate = Delegate {
        brand: host_object_brand(scope),
        host_objects,
        error_callback,
        deserializers: None,
        for_storage: args.get(3).is_true(),
    };
    if let Some(bytes) = serialize(scope, args.get(0), delegate) {
        rv.set(serde_v8::uint8_array(scope, bytes).into());
    }
}

/// `op_deserialize(buffer, hostObjects?, transferredArrayBuffers?, deserializers?, forStorage)`
fn op_deserialize<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Some(bytes) = buffer_bytes(args.get(0)) else {
        return throw_type_error(scope, "expected an ArrayBuffer or ArrayBufferView");
    };
    let Ok(host_objects) = optional::<v8::Array>(args.get(1)) else {
        return throw_type_error(scope, "hostObjects not an array");
    };
    let Ok(deserializers) = optional::<v8::Object>(args.get(3)) else {
        return throw_type_error(scope, "deserializers not an object");
    };
    let delegate = Delegate {
        brand: host_object_brand(scope),
        host_objects,
        error_callback: None,
        deserializers,
        for_storage: args.get(4).is_true(),
    };
    if let Some(value) = deserialize(scope, &bytes, delegate) {
        rv.set(value);
    }
}

/// `op_structured_clone(value, deserializers)`
fn op_structured_clone<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Ok(deserializers) = optional::<v8::Object>(args.get(1)) else {
        return throw_type_error(scope, "deserializers not an object");
    };
    let delegate = Delegate {
        brand: host_object_brand(scope),
        host_objects: None,
        error_callback: None,
        deserializers: None,
        for_storage: false,
    };
    let Some(bytes) = serialize(scope, args.get(0), delegate) else {
        return;
    };
    let delegate = Delegate {
        brand: host_object_brand(scope),
        host_objects: None,
        error_callback: None,
        deserializers,
        for_storage: false,
    };
    if let Some(value) = deserialize(scope, &bytes, delegate) {
        rv.set(value);
    }
}

fn op_get_async_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    rv.set(scope.get_continuation_preserved_embedder_data());
}

fn op_set_async_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    scope.set_continuation_preserved_embedder_data(args.get(0));
}

macro_rules! type_checks {
    ($($op:ident => $check:expr),* $(,)?) => {
        $(
            fn $op<'s>(
                _scope: &mut v8::PinScope<'s, '_>,
                args: &v8::FunctionCallbackArguments<'s>,
                rv: &mut v8::ReturnValue<'s, v8::Value>,
            ) {
                let value = args.get(0);
                let check: fn(v8::Local<v8::Value>) -> bool = $check;
                rv.set_bool(check(value));
            }
        )*
    };
}

type_checks! {
    op_is_any_array_buffer => |value| value.is_array_buffer() || value.is_shared_array_buffer(),
    op_is_array_buffer => |value| value.is_array_buffer(),
    op_is_array_buffer_view => |value| value.is_array_buffer_view(),
    op_is_data_view => |value| value.is_data_view(),
    op_is_date => |value| value.is_date(),
    op_is_map => |value| value.is_map(),
    op_is_native_error => |value| value.is_native_error(),
    op_is_promise => |value| value.is_promise(),
    op_is_proxy => |value| value.is_proxy(),
    op_is_reg_exp => |value| value.is_reg_exp(),
    op_is_set => |value| value.is_set(),
    op_is_shared_array_buffer => |value| value.is_shared_array_buffer(),
    op_is_string_object => |value| value.is_string_object(),
    op_is_typed_array => |value| value.is_typed_array(),
    op_is_boxed_primitive => |value| value.is_number_object()
        || value.is_string_object()
        || value.is_boolean_object()
        || value.is_big_int_object()
        || value.is_symbol_object(),
}

fn op_print(_state: &mut OpState, message: String, is_err: bool) {
    use std::io::Write;
    if is_err {
        let _ = std::io::stderr().write_all(message.as_bytes());
    } else {
        let _ = std::io::stdout().write_all(message.as_bytes());
    }
}

#[derive(Default)]
struct Timers {
    cancels: HashMap<u32, oneshot::Sender<()>>,
}

/// Sleeps for `millis`; resolves `true` when the time passed and `false` when
/// the timer was cancelled first.
async fn op_timer_sleep(state: Rc<RefCell<OpState>>, id: u32, millis: f64) -> bool {
    let (cancel, cancelled) = oneshot::channel();
    {
        let mut state = state.borrow_mut();
        if !state.has::<Timers>() {
            state.put(Timers::default());
        }
        state.borrow_mut::<Timers>().cancels.insert(id, cancel);
    }
    let millis = if millis.is_finite() && millis > 0.0 {
        millis
    } else {
        0.0
    };
    let fired = tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs_f64(millis / 1000.0)) => true,
        _ = cancelled => false,
    };
    if let Some(timers) = state.borrow_mut().try_borrow_mut::<Timers>() {
        timers.cancels.remove(&id);
    }
    fired
}

fn op_timer_cancel(state: &mut OpState, id: u32) {
    if let Some(timers) = state.try_borrow_mut::<Timers>()
        && let Some(cancel) = timers.cancels.remove(&id)
    {
        let _ = cancel.send(());
    }
}

/// `op_set_wasm_streaming_handler(handler)`: `WebAssembly.compileStreaming`
/// and `instantiateStreaming` call `handler(source, streamId)`, which feeds
/// the response's bytes through the `op_wasm_streaming_*` ops.
fn op_set_wasm_streaming_handler<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    if !args.get(0).is_function() {
        return throw_type_error(
            scope,
            "the WebAssembly streaming handler must be a function",
        );
    }
    let (Some(ops), Some(key)) = (
        crate::runtime::runtime_ops_object(scope),
        private_key(scope, WASM_STREAMING_HANDLER),
    ) else {
        return;
    };
    ops.set_private(scope, key, args.get(0));
}

pub(crate) fn wasm_streaming_callback<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    source: v8::Local<'s, v8::Value>,
    streaming: v8::WasmStreaming<false>,
) {
    let handler = crate::runtime::runtime_ops_object(scope)
        .zip(private_key(scope, WASM_STREAMING_HANDLER))
        .and_then(|(ops, key)| ops.get_private(scope, key))
        .and_then(|handler| v8::Local::<v8::Function>::try_from(handler).ok());
    let Some(handler) = handler else {
        let message = v8::String::new(scope, "WebAssembly streaming compilation is not supported")
            .unwrap_or_else(|| v8::String::empty(scope));
        let error = v8::Exception::type_error(scope, message);
        streaming.abort(Some(error));
        return;
    };
    let state = crate::runtime::runtime_state(scope);
    let id = {
        let mut streams = state.wasm_streams.borrow_mut();
        streams.next = streams.next.wrapping_add(1).max(1);
        let id = streams.next;
        streams.streams.insert(id, streaming);
        id
    };
    let receiver = v8::undefined(scope).into();
    let id_value = v8::Integer::new_from_unsigned(scope, id).into();
    v8::tc_scope!(let tc, scope);
    handler.call(tc, receiver, &[source, id_value]);
    if let Some(exception) = tc.exception()
        && let Some(streaming) = state.wasm_streams.borrow_mut().streams.remove(&id)
    {
        streaming.abort(Some(exception));
    }
}

fn wasm_stream_id(scope: &mut v8::PinScope<'_, '_>, value: v8::Local<v8::Value>) -> u32 {
    value.uint32_value(scope).unwrap_or(0)
}

/// `op_wasm_streaming_feed(streamId, bytes)`
fn op_wasm_streaming_feed<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let id = wasm_stream_id(scope, args.get(0));
    let Some(bytes) = buffer_bytes(args.get(1)) else {
        return throw_type_error(scope, "expected an ArrayBuffer or ArrayBufferView");
    };
    let state = crate::runtime::runtime_state(scope);
    if let Some(streaming) = state.wasm_streams.borrow_mut().streams.get_mut(&id) {
        streaming.on_bytes_received(&bytes);
    }
}

/// `op_wasm_streaming_set_url(streamId, url)`
fn op_wasm_streaming_set_url<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let id = wasm_stream_id(scope, args.get(0));
    let url = args.get(1).to_rust_string_lossy(scope);
    let state = crate::runtime::runtime_state(scope);
    if let Some(streaming) = state.wasm_streams.borrow_mut().streams.get_mut(&id) {
        streaming.set_url(&url);
    }
}

/// `op_wasm_streaming_finish(streamId)`: all bytes were fed.
fn op_wasm_streaming_finish<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let id = wasm_stream_id(scope, args.get(0));
    let state = crate::runtime::runtime_state(scope);
    let streaming = state.wasm_streams.borrow_mut().streams.remove(&id);
    if let Some(streaming) = streaming {
        streaming.finish();
    }
}

/// `op_wasm_streaming_abort(streamId, error)`
fn op_wasm_streaming_abort<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let id = wasm_stream_id(scope, args.get(0));
    let state = crate::runtime::runtime_state(scope);
    let streaming = state.wasm_streams.borrow_mut().streams.remove(&id);
    if let Some(streaming) = streaming {
        streaming.abort(Some(args.get(1)));
    }
}
