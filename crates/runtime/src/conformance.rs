//! A production-configured worker isolate, for conformance testing.
//!
//! This exists for the web-platform-tests runner (`src/bin/wpt`). A
//! [`WorkerIsolate`] starts from the same bootstrap snapshot, with the same
//! isolate policy (no code generation from strings, no unscoped fetch, no
//! internals, the same op argument budget) and the same heap limit as a
//! deployed worker's isolate under the default [`RuntimeConfig`].
//! Scripts run as classic scripts in its global scope, the way a worker's
//! `importScripts` runs them, and reach only what worker code reaches. Nothing
//! here exposes the runtime's ops or internals.

use crate::RuntimeConfig;
use crate::engine::{self, IsolatePolicy};
use crate::module_registry::ModuleRegistry;
pub use dd_v8::Error;
use dd_v8::{JsRuntime, v8};
use std::task::{Context, Poll};

/// One worker isolate with its event loop.
pub struct WorkerIsolate {
    runtime: JsRuntime,
}

impl WorkerIsolate {
    /// Creates an isolate the way the runtime creates one for a deployed
    /// worker, building the process's bootstrap snapshot first if needed.
    pub async fn new() -> Result<Self, Error> {
        let snapshot = engine::build_bootstrap_snapshot()
            .await
            .map_err(|error| Error::new(error.to_string()))?;
        let config = RuntimeConfig::default();
        let policy = IsolatePolicy {
            max_op_argument_bytes: engine::op_argument_budget(
                config.max_request_body_bytes,
                config.max_response_body_bytes,
            ),
            ..IsolatePolicy::default()
        };
        let runtime = engine::new_runtime_from_snapshot_with_heap_limit(
            snapshot,
            policy,
            config.max_isolate_heap_bytes,
            ModuleRegistry::default(),
        )
        .map_err(|error| Error::new(error.to_string()))?;
        Ok(Self { runtime })
    }

    /// Runs `source` as a classic script named `name` in the global scope and
    /// returns its completion value converted with `String()`. Microtasks it
    /// queues run on the next turn of the event loop.
    pub fn execute_script(&mut self, name: &str, source: &str) -> Result<String, Error> {
        let value = self.runtime.execute_script(name, source)?;
        dd_v8::scope!(scope, self.runtime);
        let value = v8::Local::new(scope, value);
        v8::tc_scope!(let scope, scope);
        match value.to_string(scope) {
            Some(text) => Ok(text.to_rust_string_lossy(scope)),
            None if scope.is_execution_terminating() => Err(Error::new(
                "execution terminated while converting a script result",
            )),
            None => Err(Error::new("the script's completion value is not printable")),
        }
    }

    /// One turn of the event loop: timers, settled async work, microtasks.
    /// Ready once nothing is pending. An unhandled promise rejection ends the
    /// turn with an error; the loop can be polled again afterwards.
    pub fn poll_event_loop(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.runtime.poll_event_loop(cx)
    }

    /// A handle another thread can use to stop runaway JavaScript.
    pub fn terminate_handle(&mut self) -> TerminateHandle {
        TerminateHandle(self.runtime.v8_isolate().thread_safe_handle())
    }

    /// Lets the isolate run JavaScript again after [`TerminateHandle::terminate`].
    pub fn cancel_termination(&mut self) {
        self.runtime.v8_isolate().cancel_terminate_execution();
    }
}

/// Stops the JavaScript running in a [`WorkerIsolate`] from any thread.
#[derive(Clone)]
pub struct TerminateHandle(v8::IsolateHandle);

impl TerminateHandle {
    /// Terminates the running script, if any; the isolate's next poll or
    /// script fails with a terminated [`Error`]. Returns false when the
    /// isolate is gone.
    pub fn terminate(&self) -> bool {
        self.0.terminate_execution()
    }
}
