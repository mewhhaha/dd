//! Stub DevTools inspector handle/session for the Scriptw + wtime backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::mpsc;

/// Opens DevTools sessions on a runtime's inspector from any thread.
#[derive(Clone)]
pub struct InspectorHandle {
    closed: Arc<AtomicBool>,
    next_session: Arc<AtomicU64>,
}

impl InspectorHandle {
    pub(crate) fn new() -> Self {
        Self {
            closed: Arc::new(AtomicBool::new(false)),
            next_session: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn connect(&self) -> InspectorSession {
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        let (_tx, receiver) = mpsc::unbounded_channel();
        InspectorSession { id, receiver }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

impl std::fmt::Debug for InspectorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectorHandle").finish_non_exhaustive()
    }
}

/// One DevTools session.
pub struct InspectorSession {
    id: u64,
    receiver: mpsc::UnboundedReceiver<String>,
}

impl InspectorSession {
    pub fn send(&self, _message: impl Into<String>) {}

    pub async fn recv(&mut self) -> Option<String> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Option<String> {
        self.receiver.try_recv().ok()
    }
}

impl std::fmt::Debug for InspectorSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectorSession")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
