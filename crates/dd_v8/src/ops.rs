//! Host functions callable from JavaScript.
//!
//! An op is a plain Rust function. Sync ops take `&mut OpState` (or, to
//! hand the state to a task, `Rc<RefCell<OpState>>`) first and async ops
//! take `Rc<RefCell<OpState>>` first; every other argument and the
//! return value convert through serde (see [`crate::serde_v8`]). A return of
//! `()` is `undefined`, and an `Err` (of any error convertible to
//! [`OpError`]) throws (sync) or rejects (async).
//! Ops that need V8 values directly register as raw ops.
//!
//! All ops share one native callback, so a snapshot needs a single external
//! reference; the op's index rides along as the function's data.

use crate::runtime::{PendingOp, runtime_state};
use crate::serde_v8;
use crate::state::OpState;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::TypeId;
use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

pub(crate) type OpHandler = Box<
    dyn for<'s, 'i> Fn(
        &mut v8::PinScope<'s, 'i>,
        &v8::FunctionCallbackArguments<'s>,
        &mut v8::ReturnValue<'s, v8::Value>,
    ),
>;

/// A raw op reads its arguments and writes its return value itself.
pub type RawOp = for<'s, 'i> fn(
    &mut v8::PinScope<'s, 'i>,
    &v8::FunctionCallbackArguments<'s>,
    &mut v8::ReturnValue<'s, v8::Value>,
);

pub struct OpDecl {
    pub(crate) name: &'static str,
    pub(crate) handler: OpHandler,
}

impl OpDecl {
    pub fn sync<Args>(name: &'static str, op: impl SyncOp<Args>) -> Self {
        Self {
            name,
            handler: op.into_handler(name),
        }
    }

    pub fn async_op<Args>(name: &'static str, op: impl AsyncOp<Args>) -> Self {
        Self {
            name,
            handler: op.into_handler(name),
        }
    }

    pub fn raw(name: &'static str, op: RawOp) -> Self {
        Self {
            name,
            handler: Box::new(op),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// Declares a sync op named after the function.
#[macro_export]
macro_rules! op_sync {
    ($op:ident) => {
        $crate::OpDecl::sync(stringify!($op), $op)
    };
}

/// Declares an async op named after the function.
#[macro_export]
macro_rules! op_async {
    ($op:ident) => {
        $crate::OpDecl::async_op(stringify!($op), $op)
    };
}

/// Declares a raw op named after the function.
#[macro_export]
macro_rules! op_raw {
    ($op:ident) => {
        $crate::OpDecl::raw(stringify!($op), $op)
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpErrorClass {
    Error,
    TypeError,
    RangeError,
    /// A class JavaScript registered a builder for with
    /// `op_register_error_builder` (e.g. `DOMExceptionOperationError`).
    Custom(&'static str),
}

/// An error an op throws into JavaScript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpError {
    class: OpErrorClass,
    message: String,
}

impl OpError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            class: OpErrorClass::Error,
            message: message.into(),
        }
    }

    pub fn type_error(message: impl Into<String>) -> Self {
        Self {
            class: OpErrorClass::TypeError,
            message: message.into(),
        }
    }

    pub fn range_error(message: impl Into<String>) -> Self {
        Self {
            class: OpErrorClass::RangeError,
            message: message.into(),
        }
    }

    pub fn custom(class: &'static str, message: impl Into<String>) -> Self {
        Self {
            class: OpErrorClass::Custom(class),
            message: message.into(),
        }
    }

    pub fn class(&self) -> OpErrorClass {
        self.class
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn to_exception<'s>(&self, scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Value> {
        let message =
            v8::String::new(scope, &self.message).unwrap_or_else(|| v8::String::empty(scope));
        match self.class {
            OpErrorClass::Error => v8::Exception::error(scope, message),
            OpErrorClass::TypeError => v8::Exception::type_error(scope, message),
            OpErrorClass::RangeError => v8::Exception::range_error(scope, message),
            OpErrorClass::Custom(class) => {
                crate::builtins::build_custom_error(scope, class, message)
            }
        }
    }
}

impl fmt::Display for OpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for OpError {}

impl From<String> for OpError {
    fn from(message: String) -> Self {
        Self::new(message)
    }
}

impl From<&str> for OpError {
    fn from(message: &str) -> Self {
        Self::new(message)
    }
}

impl From<serde_v8::Error> for OpError {
    fn from(error: serde_v8::Error) -> Self {
        Self::type_error(error.to_string())
    }
}

/// An op argument.
pub trait FromV8: Sized {
    fn from_v8<'s>(
        scope: &mut v8::PinScope<'s, '_>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, OpError>;
}

impl<T: DeserializeOwned> FromV8 for T {
    fn from_v8<'s>(
        scope: &mut v8::PinScope<'s, '_>,
        value: v8::Local<'s, v8::Value>,
    ) -> Result<Self, OpError> {
        Ok(serde_v8::from_v8(scope, value)?)
    }
}

/// An op's return value. `Marker` tells plain values from `Result`s.
pub trait OpResult<Marker>: 'static {
    /// `None` means `undefined`.
    fn into_v8<'s>(
        self,
        scope: &mut v8::PinScope<'s, '_>,
    ) -> Result<Option<v8::Local<'s, v8::Value>>, OpError>;
}

#[doc(hidden)]
pub struct Value;

#[doc(hidden)]
pub struct Fallible;

impl<T: Serialize + 'static> OpResult<Value> for T {
    fn into_v8<'s>(
        self,
        scope: &mut v8::PinScope<'s, '_>,
    ) -> Result<Option<v8::Local<'s, v8::Value>>, OpError> {
        if TypeId::of::<T>() == TypeId::of::<()>() {
            return Ok(None);
        }
        Ok(Some(serde_v8::to_v8(scope, &self)?))
    }
}

impl<T: Serialize + 'static, E: Into<OpError> + 'static> OpResult<Fallible> for Result<T, E> {
    fn into_v8<'s>(
        self,
        scope: &mut v8::PinScope<'s, '_>,
    ) -> Result<Option<v8::Local<'s, v8::Value>>, OpError> {
        OpResult::<Value>::into_v8(self.map_err(Into::into)?, scope)
    }
}

/// Bytes copied out of an `ArrayBuffer` or view passed to an op.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JsBuffer(Vec<u8>);

impl JsBuffer {
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl std::ops::Deref for JsBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for JsBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'de> serde::Deserialize<'de> for JsBuffer {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = JsBuffer;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an ArrayBuffer or ArrayBufferView")
            }

            fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<JsBuffer, E> {
                Ok(JsBuffer(bytes))
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<JsBuffer, E> {
                Ok(JsBuffer(bytes.to_vec()))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<JsBuffer, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(JsBuffer(bytes))
            }
        }

        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

fn handler<F>(f: F) -> OpHandler
where
    F: for<'s, 'i> Fn(
            &mut v8::PinScope<'s, 'i>,
            &v8::FunctionCallbackArguments<'s>,
            &mut v8::ReturnValue<'s, v8::Value>,
        ) + 'static,
{
    Box::new(f)
}

fn throw(scope: &mut v8::PinScope<'_, '_>, error: &OpError) {
    let exception = error.to_exception(scope);
    scope.throw_exception(exception);
}

fn throw_argument_error(scope: &mut v8::PinScope<'_, '_>, op: &str, index: usize, error: OpError) {
    throw(
        scope,
        &OpError::type_error(format!("{op}: argument {}: {}", index + 1, error.message)),
    );
}

fn return_result<'s, M>(
    scope: &mut v8::PinScope<'s, '_>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
    result: impl OpResult<M>,
) {
    match result.into_v8(scope) {
        Ok(Some(value)) => rv.set(value),
        Ok(None) => rv.set_undefined(),
        Err(error) => throw(scope, &error),
    }
}

/// The settled value of an async op, converted once a scope is available.
pub(crate) trait AsyncResult {
    fn into_v8<'s>(
        self: Box<Self>,
        scope: &mut v8::PinScope<'s, '_>,
    ) -> Result<Option<v8::Local<'s, v8::Value>>, OpError>;
}

struct Settled<R, M>(R, PhantomData<M>);

impl<R: OpResult<M>, M: 'static> AsyncResult for Settled<R, M> {
    fn into_v8<'s>(
        self: Box<Self>,
        scope: &mut v8::PinScope<'s, '_>,
    ) -> Result<Option<v8::Local<'s, v8::Value>>, OpError> {
        self.0.into_v8(scope)
    }
}

pub(crate) fn settle<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    resolver: v8::Local<'s, v8::PromiseResolver>,
    result: Box<dyn AsyncResult>,
) {
    match result.into_v8(scope) {
        Ok(value) => {
            let value = value.unwrap_or_else(|| v8::undefined(scope).into());
            resolver.resolve(scope, value);
        }
        Err(error) => {
            let exception = error.to_exception(scope);
            resolver.reject(scope, exception);
        }
    }
}

/// Starts an async op: polls it once, and either settles the returned promise
/// now or parks the future on the event loop.
fn submit<'s, R: OpResult<M>, M: 'static>(
    scope: &mut v8::PinScope<'s, '_>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
    future: impl Future<Output = R> + 'static,
) {
    let Some(resolver) = v8::PromiseResolver::new(scope) else {
        return;
    };
    rv.set(resolver.get_promise(scope).into());
    let mut future: std::pin::Pin<Box<dyn Future<Output = Box<dyn AsyncResult>>>> = Box::pin(
        async move { Box::new(Settled(future.await, PhantomData)) as Box<dyn AsyncResult> },
    );
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => settle(scope, resolver, result),
        Poll::Pending => {
            let state = runtime_state(scope);
            state
                .pending_ops
                .borrow_mut()
                .push(PendingOp::new(v8::Global::new(scope, resolver), future));
            // The first poll registered no waker; the loop must poll the
            // future again before it can wake anything.
            state.waker.wake();
        }
    }
}

/// Marks sync ops that take `&mut OpState`.
#[doc(hidden)]
pub struct ExclusiveState;

/// Marks sync ops that take the shared `Rc<RefCell<OpState>>`.
#[doc(hidden)]
pub struct SharedState;

pub trait SyncOp<Args>: 'static {
    fn into_handler(self, name: &'static str) -> OpHandler;
}

pub trait AsyncOp<Args>: 'static {
    fn into_handler(self, name: &'static str) -> OpHandler;
}

macro_rules! impl_ops {
    ($($arg:ident),*) => {
        impl<F, R, M, $($arg,)*> SyncOp<(ExclusiveState, M, R, $($arg,)*)> for F
        where
            F: Fn(&mut OpState, $($arg),*) -> R + 'static,
            R: OpResult<M>,
            M: 'static,
            $($arg: FromV8,)*
        {
            #[allow(non_snake_case, unused_mut, unused_variables, unused_assignments)]
            fn into_handler(self, name: &'static str) -> OpHandler {
                handler(move |scope, args, rv| {
                    let mut index = 0;
                    $(
                        let $arg = match <$arg as FromV8>::from_v8(scope, args.get(index as i32)) {
                            Ok(value) => value,
                            Err(error) => return throw_argument_error(scope, name, index, error),
                        };
                        index += 1;
                    )*
                    let op_state = Rc::clone(&runtime_state(scope).op_state);
                    let result = (self)(&mut op_state.borrow_mut(), $($arg),*);
                    return_result(scope, rv, result);
                })
            }
        }

        impl<F, R, M, $($arg,)*> SyncOp<(SharedState, M, R, $($arg,)*)> for F
        where
            F: Fn(Rc<RefCell<OpState>>, $($arg),*) -> R + 'static,
            R: OpResult<M>,
            M: 'static,
            $($arg: FromV8,)*
        {
            #[allow(non_snake_case, unused_mut, unused_variables, unused_assignments)]
            fn into_handler(self, name: &'static str) -> OpHandler {
                handler(move |scope, args, rv| {
                    let mut index = 0;
                    $(
                        let $arg = match <$arg as FromV8>::from_v8(scope, args.get(index as i32)) {
                            Ok(value) => value,
                            Err(error) => return throw_argument_error(scope, name, index, error),
                        };
                        index += 1;
                    )*
                    let op_state = Rc::clone(&runtime_state(scope).op_state);
                    let result = (self)(op_state, $($arg),*);
                    return_result(scope, rv, result);
                })
            }
        }

        impl<F, Fut, R, M, $($arg,)*> AsyncOp<(Fut, M, R, $($arg,)*)> for F
        where
            F: Fn(Rc<RefCell<OpState>>, $($arg),*) -> Fut + 'static,
            Fut: Future<Output = R> + 'static,
            R: OpResult<M>,
            M: 'static,
            $($arg: FromV8,)*
        {
            #[allow(non_snake_case, unused_mut, unused_variables, unused_assignments)]
            fn into_handler(self, name: &'static str) -> OpHandler {
                handler(move |scope, args, rv| {
                    let mut index = 0;
                    $(
                        let $arg = match <$arg as FromV8>::from_v8(scope, args.get(index as i32)) {
                            Ok(value) => value,
                            Err(error) => return throw_argument_error(scope, name, index, error),
                        };
                        index += 1;
                    )*
                    let op_state = Rc::clone(&runtime_state(scope).op_state);
                    let future = (self)(op_state, $($arg),*);
                    submit(scope, rv, future);
                })
            }
        }
    };
}

impl_ops!();
impl_ops!(A1);
impl_ops!(A1, A2);
impl_ops!(A1, A2, A3);
impl_ops!(A1, A2, A3, A4);
impl_ops!(A1, A2, A3, A4, A5);
impl_ops!(A1, A2, A3, A4, A5, A6);
impl_ops!(A1, A2, A3, A4, A5, A6, A7);
impl_ops!(A1, A2, A3, A4, A5, A6, A7, A8);
impl_ops!(A1, A2, A3, A4, A5, A6, A7, A8, A9);
impl_ops!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10);

/// The callback behind every op function.
pub(crate) fn dispatch<'s, 'i>(
    scope: &mut v8::PinScope<'s, 'i>,
    args: v8::FunctionCallbackArguments<'s>,
    mut rv: v8::ReturnValue<'s, v8::Value>,
) {
    let state = runtime_state(scope);
    let Some(index) = v8::Local::<v8::Integer>::try_from(args.data())
        .ok()
        .and_then(|index| usize::try_from(index.value()).ok())
    else {
        return;
    };
    let Some(op) = state.ops.get(index) else {
        return;
    };
    (op.handler)(scope, &args, &mut rv);
}

/// Builds the object holding one function per op, keyed by op name.
pub(crate) fn ops_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    ops: &[OpDecl],
) -> v8::Local<'s, v8::Object> {
    let object = v8::Object::new(scope);
    for (index, op) in ops.iter().enumerate() {
        let data = v8::Integer::new_from_unsigned(scope, index as u32);
        let function = v8::Function::builder(dispatch)
            .data(data.into())
            .build(scope)
            .expect("op function");
        let name = serde_v8::key(scope, op.name);
        function.set_name(name);
        object.create_data_property(scope, name.into(), function.into());
    }
    object
}
