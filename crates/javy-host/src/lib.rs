//! Experimental dd runtime for JavaScript workers compiled to WebAssembly by
//! Javy.
//!
//! Javy embeds QuickJS bytecode in a Wasm module. The first runtime milestone
//! uses a JSON request and response envelope over WASI stdin/stdout and creates
//! a fresh instance for each request. This keeps the ABI narrow while the
//! worker compatibility layer is still changing.

mod runtime;

pub use runtime::{InvokeOptions, JavyWorker, WorkerOptions};
