//! The dd worker runtime: executes TypeScript compiled to WebAssembly by
//! [Perry](https://github.com/PerryTS/perry) — no JavaScript engine in the
//! loop.
//!
//! The host implements Perry's wasm import ABI (the `rt` bridge catalog and
//! `mem_call` dispatch bus) natively in Rust, plus a small dd-specific `ffi`
//! surface (`dd_register`, `dd_header`, `dd_json`) that workers reach through
//! plain `declare function` statements. See
//! `docs/wasm-runtime.md` for the worker contract and limitations.

mod bridge;
mod engine;
mod heap;
mod host_api;
mod nanbox;
mod state;
mod ws;

pub use engine::{InvokeOptions, WorkerModule, WorkerOptions};
pub use state::{WorkerRegistry, WorkerStores, WsConnections};
pub use ws::{WsEvent, WsOutbound};
