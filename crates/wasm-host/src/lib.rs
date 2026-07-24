//! Experimental dd worker runtime that executes TypeScript compiled to
//! WebAssembly by [Perry](https://github.com/PerryTS/perry) instead of
//! running JavaScript in V8 isolates.
//!
//! The host implements Perry's wasm import ABI (the `rt` bridge catalog and
//! `mem_call` dispatch bus) natively in Rust, plus a small dd-specific `ffi`
//! surface (`dd_register`, `dd_header`, `dd_json`) that workers reach through
//! plain `declare function` statements. See
//! `docs/perry-wasm-experiment.md` for the worker contract and limitations.

mod bridge;
mod engine;
mod heap;
mod nanbox;
mod state;

pub use engine::{InvokeOptions, WorkerModule};
