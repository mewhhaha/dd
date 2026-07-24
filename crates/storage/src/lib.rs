//! Disk-backed storage for dd workers: KV, keyed memory namespaces, the
//! worker response cache, and blob storage — all on turso. Deliberately free
//! of any JS-engine dependency so both the V8 runtime and the Perry wasm
//! host can share it.

pub mod blob;
pub mod cache;
pub mod json;
pub mod kv;
pub mod memory;
pub mod turso_util;
