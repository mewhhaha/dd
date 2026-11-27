//! A production-configured worker isolate, for conformance testing.

use crate::RuntimeConfig;
use crate::engine::{self, IsolatePolicy};
use crate::module_registry::ModuleRegistry;
pub use dd_v8::Error;
use dd_v8::{JsRuntime, RuntimeHandle};
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
        let runtime = engine::new_runtime_from_snapshot_with_heap_limit(
            snapshot,
            IsolatePolicy::for_config(&config),
            config.max_isolate_heap_bytes,
            ModuleRegistry::default(),
        )
        .map_err(|error| Error::new(error.to_string()))?;
        Ok(Self { runtime })
    }

    /// Runs `source` as a classic script named `name` in the global scope and
    /// returns its completion value converted with `String()`.
    pub fn execute_script(&mut self, name: &str, source: &str) -> Result<String, Error> {
        let value = self.runtime.execute_script(name, source)?;
        self.runtime.value_to_string(&value)
    }

    /// One turn of the event loop: timers, settled async work, microtasks.
    pub fn poll_event_loop(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.runtime.poll_event_loop(cx)
    }

    /// A handle another thread can use to stop runaway JavaScript.
    pub fn terminate_handle(&mut self) -> TerminateHandle {
        TerminateHandle(self.runtime.handle())
    }

    /// Lets the isolate run JavaScript again after [`TerminateHandle::terminate`].
    pub fn cancel_termination(&mut self) {
        self.runtime.cancel_termination();
    }
}

/// Stops the JavaScript running in a [`WorkerIsolate`] from any thread.
#[derive(Clone)]
pub struct TerminateHandle(RuntimeHandle);

impl TerminateHandle {
    /// Terminates the running script, if any; the isolate's next poll or
    /// script fails with a terminated [`Error`]. Returns false when the
    /// isolate is gone.
    pub fn terminate(&self) -> bool {
        self.0.terminate_execution()
    }
}
