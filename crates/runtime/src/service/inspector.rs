//! DevTools targets: one per live worker isolate, for the development
//! runtime's inspector server to bridge.

use super::*;

pub use dd_v8::InspectorSession;

/// Whether worker isolates take a debugger. Only the local development
/// runtime turns this on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InspectorMode {
    #[default]
    Off,
    /// Every worker isolate gets an inspector, listed by
    /// [`RuntimeService::inspector_targets`] while it lives.
    On,
    /// As `On`, and each isolate waits to evaluate its worker until a
    /// debugger attaches and lets it run (`Runtime.runIfWaitingForDebugger`).
    Wait,
}

/// A worker isolate a debugger can attach to.
#[derive(Clone)]
pub struct InspectorTarget {
    /// A random id, unguessable by pages that cannot list the targets.
    pub id: Uuid,
    pub worker: String,
    pub generation: u64,
    pub isolate_id: u64,
    inspector: dd_v8::InspectorHandle,
}

impl InspectorTarget {
    /// Opens a DevTools session on the isolate.
    pub fn connect(&self) -> InspectorSession {
        self.inspector.connect()
    }
}

impl std::fmt::Debug for InspectorTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectorTarget")
            .field("id", &self.id)
            .field("worker", &self.worker)
            .field("generation", &self.generation)
            .field("isolate_id", &self.isolate_id)
            .finish_non_exhaustive()
    }
}

/// The live targets, filled by isolate threads as they start.
#[derive(Clone)]
pub(crate) struct InspectorRegistry {
    targets: Arc<StdMutex<BTreeMap<Uuid, InspectorTarget>>>,
    wait_for_debugger: bool,
}

impl InspectorRegistry {
    pub(super) fn new(mode: InspectorMode) -> Option<Self> {
        (mode != InspectorMode::Off).then(|| Self {
            targets: Arc::default(),
            wait_for_debugger: mode == InspectorMode::Wait,
        })
    }

    pub(super) fn wait_for_debugger(&self) -> bool {
        self.wait_for_debugger
    }

    /// Lists the target until the returned guard drops with its isolate.
    pub(super) fn register(
        &self,
        worker: &str,
        generation: u64,
        isolate_id: u64,
        inspector: dd_v8::InspectorHandle,
    ) -> InspectorTargetGuard {
        let id = Uuid::new_v4();
        self.lock().insert(
            id,
            InspectorTarget {
                id,
                worker: worker.to_string(),
                generation,
                isolate_id,
                inspector,
            },
        );
        InspectorTargetGuard {
            registry: self.clone(),
            id,
        }
    }

    pub(super) fn targets(&self) -> Vec<InspectorTarget> {
        self.lock().values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<Uuid, InspectorTarget>> {
        self.targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(super) struct InspectorTargetGuard {
    registry: InspectorRegistry,
    id: Uuid,
}

impl Drop for InspectorTargetGuard {
    fn drop(&mut self) {
        self.registry.lock().remove(&self.id);
    }
}
