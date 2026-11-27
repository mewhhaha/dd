//! Host functions callable from JavaScript.
//!
//! An op is a plain Rust function. Sync ops take `&mut OpState` (or
//! `Rc<RefCell<OpState>>`) first and async ops take `Rc<RefCell<OpState>>`
//! first; every other argument and the return value convert through serde
//! (see [`crate::serde_v8`]). A return of `()` is `undefined`, and an `Err`
//! (of any error convertible to [`OpError`]) throws (sync) or rejects (async).

use crate::serde_v8::{self, WireValue};
use crate::state::OpState;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::TypeId;
use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;

pub(crate) type SyncOpFn =
    Box<dyn Fn(&Rc<RefCell<OpState>>, &[WireValue]) -> Result<WireValue, OpError>>;

pub(crate) type AsyncOpFn = Box<
    dyn Fn(
        &Rc<RefCell<OpState>>,
        &[WireValue],
    ) -> Result<Pin<Box<dyn Future<Output = Result<WireValue, OpError>>>>, OpError>,
>;

pub(crate) enum OpKind {
    Sync(SyncOpFn),
    Async(AsyncOpFn),
    /// Handled directly inside `kernel.mjs` (pure JS builtin or wrapped op).
    BuiltinJs,
}

pub struct OpDecl {
    pub(crate) name: &'static str,
    pub(crate) kind: OpKind,
}

impl OpDecl {
    pub fn sync<Args>(name: &'static str, op: impl SyncOp<Args>) -> Self {
        Self {
            name,
            kind: OpKind::Sync(op.into_handler(name)),
        }
    }

    pub fn async_op<Args>(name: &'static str, op: impl AsyncOp<Args>) -> Self {
        Self {
            name,
            kind: OpKind::Async(op.into_handler(name)),
        }
    }

    /// Declares an op whose implementation lives in the Scriptw JS host kernel.
    pub fn builtin_js(name: &'static str) -> Self {
        Self {
            name,
            kind: OpKind::BuiltinJs,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn is_async(&self) -> bool {
        matches!(self.kind, OpKind::Async(_))
    }

    pub(crate) fn is_builtin_js(&self) -> bool {
        matches!(self.kind, OpKind::BuiltinJs)
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

/// An op argument deserialized from JS wire data.
pub trait FromV8: Sized {
    fn from_wire(value: &WireValue) -> Result<Self, OpError>;
}

impl<T: DeserializeOwned> FromV8 for T {
    fn from_wire(value: &WireValue) -> Result<Self, OpError> {
        Ok(serde_v8::from_wire(value)?)
    }
}

/// An op's return value. `Marker` tells plain values from `Result`s.
pub trait OpResult<Marker>: 'static {
    fn into_wire(self) -> Result<WireValue, OpError>;
}

#[doc(hidden)]
pub struct Value;

#[doc(hidden)]
pub struct Fallible;

impl<T: Serialize + 'static> OpResult<Value> for T {
    fn into_wire(self) -> Result<WireValue, OpError> {
        if TypeId::of::<T>() == TypeId::of::<()>() {
            return Ok(WireValue::Undefined);
        }
        Ok(serde_v8::to_wire(&self)?)
    }
}

impl<T: Serialize + 'static, E: Into<OpError> + 'static> OpResult<Fallible> for Result<T, E> {
    fn into_wire(self) -> Result<WireValue, OpError> {
        OpResult::<Value>::into_wire(self.map_err(Into::into)?)
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
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(1 << 20));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(JsBuffer(bytes))
            }
        }

        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

fn arg_error(op: &str, index: usize, error: OpError) -> OpError {
    OpError::type_error(format!("{op}: argument {}: {}", index + 1, error.message))
}

fn get_arg(args: &[WireValue], index: usize) -> &WireValue {
    args.get(index).unwrap_or(&WireValue::Undefined)
}

/// Marks sync ops that take `&mut OpState`.
#[doc(hidden)]
pub struct ExclusiveState;

/// Marks sync ops that take the shared `Rc<RefCell<OpState>>`.
#[doc(hidden)]
pub struct SharedState;

pub trait SyncOp<Args>: 'static {
    fn into_handler(self, name: &'static str) -> SyncOpFn;
}

pub trait AsyncOp<Args>: 'static {
    fn into_handler(self, name: &'static str) -> AsyncOpFn;
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
            fn into_handler(self, name: &'static str) -> SyncOpFn {
                let _marker = PhantomData::<M>;
                Box::new(move |op_state, args| {
                    let mut index = 0usize;
                    $(
                        let $arg = <$arg as FromV8>::from_wire(get_arg(args, index))
                            .map_err(|e| arg_error(name, index, e))?;
                        index += 1;
                    )*
                    let result = (self)(&mut op_state.borrow_mut(), $($arg),*);
                    result.into_wire()
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
            fn into_handler(self, name: &'static str) -> SyncOpFn {
                let _marker = PhantomData::<M>;
                Box::new(move |op_state, args| {
                    let mut index = 0usize;
                    $(
                        let $arg = <$arg as FromV8>::from_wire(get_arg(args, index))
                            .map_err(|e| arg_error(name, index, e))?;
                        index += 1;
                    )*
                    let result = (self)(Rc::clone(op_state), $($arg),*);
                    result.into_wire()
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
            fn into_handler(self, name: &'static str) -> AsyncOpFn {
                let _marker = PhantomData::<M>;
                Box::new(move |op_state, args| {
                    let mut index = 0usize;
                    $(
                        let $arg = <$arg as FromV8>::from_wire(get_arg(args, index))
                            .map_err(|e| arg_error(name, index, e))?;
                        index += 1;
                    )*
                    let fut = (self)(Rc::clone(op_state), $($arg),*);
                    Ok(Box::pin(async move { fut.await.into_wire() }))
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
