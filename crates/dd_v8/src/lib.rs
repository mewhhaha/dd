//! dd's JavaScript runtime core, powered by Scriptw + wtime x64 AOT.
//!
//! A [`JsRuntime`] owns one isolated `wtime-aot-host` Store and Scriptw Realm.
//! It exposes Rust functions to JavaScript as ops, loads ES modules through a
//! [`ModuleLoader`], drives async ops and dynamic imports from
//! [`JsRuntime::poll_event_loop`], and AOT-compiles bootstrap snapshots once
//! per process into shared native x64 code.

pub mod builtins;
mod error;
mod inspector;
mod modules;
mod ops;
mod platform;
mod runtime;
pub mod serde_v8;
mod state;
mod wasm_api;

pub use error::Error;
pub use inspector::{InspectorHandle, InspectorSession};
pub use modules::{ModuleCode, ModuleId, ModuleLoader, ModuleSource, ModuleType, NoModules};
pub use ops::{AsyncOp, FromV8, JsBuffer, OpDecl, OpError, OpErrorClass, OpResult, SyncOp};
pub use platform::{init, set_flags};
pub use runtime::{JsRuntime, JsValue, RuntimeHandle, RuntimeOptions};
pub use serde_v8::ToJsBuffer;
pub use state::OpState;
