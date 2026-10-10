//! dd's JavaScript runtime core, built directly on rusty_v8.
//!
//! A [`JsRuntime`] owns one isolate and one context. It exposes Rust
//! functions to JavaScript as ops, loads ES modules through a
//! [`ModuleLoader`], drives async ops and dynamic imports from
//! [`JsRuntime::poll_event_loop`], and can capture its context as a startup
//! snapshot.

pub mod builtins;
mod error;
mod modules;
mod ops;
mod platform;
mod runtime;
pub mod serde_v8;
mod state;

pub use error::Error;
pub use modules::{ModuleCode, ModuleId, ModuleLoader, ModuleSource, ModuleType, NoModules};
pub use ops::{AsyncOp, FromV8, JsBuffer, OpDecl, OpError, OpErrorClass, OpResult, RawOp, SyncOp};
pub use platform::{init, set_flags};
pub use runtime::{JsRuntime, RuntimeOptions, runtime_op_state};
pub use serde_v8::ToJsBuffer;
pub use state::OpState;
pub use v8;
