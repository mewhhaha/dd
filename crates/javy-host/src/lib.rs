//! Experimental dd runtime for JavaScript workers compiled to WebAssembly by
//! Javy.
//!
//! Javy embeds QuickJS bytecode in a Wasm module. The runtime pools complete
//! Wasmtime instances and resets their JSON request and response envelope over
//! WASI stdin/stdout before every invocation.

mod runtime;

pub use runtime::{InvokeOptions, JavyWorker, WorkerOptions};
