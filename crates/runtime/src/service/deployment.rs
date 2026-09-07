use crate::engine::{load_worker_source, new_runtime_from_snapshot_with_heap_limit};
use crate::module_registry::ModuleRegistry;
use crate::ops::WorkerSource;
use common::{PlatformError, Result};
use deno_core::v8::IsolateHandle;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};

pub(super) struct DeploymentValidator {
    pub(super) snapshot: &'static [u8],
    pub(super) max_heap_bytes: usize,
    pub(super) timeout: Duration,
    pub(super) allow_code_generation: bool,
    pub(super) slots: Arc<Semaphore>,
}

#[derive(Default)]
struct ValidationState {
    isolate: Option<IsolateHandle>,
    cancelled: bool,
}

struct ValidationCancellation(Arc<Mutex<ValidationState>>);

impl Drop for ValidationCancellation {
    fn drop(&mut self) {
        let mut state = self.0.lock().expect("validation state mutex poisoned");
        state.cancelled = true;
        if let Some(isolate) = &state.isolate {
            isolate.terminate_execution();
        }
    }
}

impl DeploymentValidator {
    pub(super) async fn validate(
        &self,
        source: WorkerSource,
        modules: ModuleRegistry,
    ) -> Result<()> {
        let permit = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|_| PlatformError::internal("deployment validator is closed"))?;
        let snapshot = self.snapshot;
        let max_heap_bytes = self.max_heap_bytes;
        let allow_code_generation = self.allow_code_generation;
        let timeout = self.timeout;
        let state = Arc::new(Mutex::new(ValidationState::default()));
        let cancellation = ValidationCancellation(Arc::clone(&state));
        let (reply, receive) = oneshot::channel();
        std::thread::Builder::new()
            .name("dd-deployment-validation".to_string())
            .spawn(move || {
                let _permit = permit;
                let result = (|| {
                    let executor = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| {
                            PlatformError::internal(format!("validation executor: {error}"))
                        })?;
                    executor.block_on(async {
                        let mut runtime = new_runtime_from_snapshot_with_heap_limit(
                            snapshot,
                            allow_code_generation,
                            max_heap_bytes,
                            modules,
                        )?;
                        {
                            let mut state = state.lock().expect("validation state mutex poisoned");
                            if state.cancelled {
                                return Err(PlatformError::runtime(
                                    "deployment validation cancelled",
                                ));
                            }
                            state.isolate = Some(runtime.v8_isolate().thread_safe_handle());
                        }
                        tokio::time::timeout(timeout, load_worker_source(&mut runtime, &source))
                            .await
                            .map_err(|_| {
                                PlatformError::runtime(format!(
                                    "deployment startup exceeded {} ms",
                                    timeout.as_millis()
                                ))
                            })?
                    })
                })();
                let _ = reply.send(result);
            })
            .map_err(|error| {
                PlatformError::internal(format!("starting deployment validator: {error}"))
            })?;
        let result = tokio::time::timeout(timeout, receive)
            .await
            .map_err(|_| {
                PlatformError::runtime(format!(
                    "deployment startup exceeded {} ms",
                    timeout.as_millis()
                ))
            })?
            .map_err(|_| {
                PlatformError::internal("deployment validation thread exited without a result")
            })?;
        drop(cancellation);
        result
    }
}
