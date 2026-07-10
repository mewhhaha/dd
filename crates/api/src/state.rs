use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};

use crate::deploy_tokens::DeployTokenStore;
use runtime::RuntimeService;

#[derive(Clone, Debug)]
pub struct WebSocketSession {
    pub id: String,
    pub worker_name: String,
    pub started_at: Instant,
}

#[derive(Clone, Default)]
pub struct OperationalState {
    inner: Arc<OperationalStateInner>,
}

#[derive(Default)]
struct OperationalStateInner {
    draining: AtomicBool,
    shutting_down: AtomicBool,
    active_requests: AtomicUsize,
    drained: Notify,
}

pub struct ActiveRequestGuard {
    inner: Arc<OperationalStateInner>,
    active: bool,
}

impl OperationalState {
    pub fn is_ready(&self) -> bool {
        !self.inner.draining.load(Ordering::Acquire)
            && !self.inner.shutting_down.load(Ordering::Acquire)
    }

    pub fn is_draining(&self) -> bool {
        self.inner.draining.load(Ordering::Acquire)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::Acquire)
    }

    pub fn active_requests(&self) -> usize {
        self.inner.active_requests.load(Ordering::Acquire)
    }

    pub fn try_begin_request(&self) -> Option<ActiveRequestGuard> {
        if self.is_draining() || self.is_shutting_down() {
            return None;
        }

        self.inner.active_requests.fetch_add(1, Ordering::AcqRel);
        if self.is_draining() || self.is_shutting_down() {
            self.request_finished();
            return None;
        }

        Some(ActiveRequestGuard {
            inner: Arc::clone(&self.inner),
            active: true,
        })
    }

    /// Keep an already-admitted upgraded connection in the drain count until
    /// its session task exits.
    pub fn try_begin_session(&self) -> Option<ActiveRequestGuard> {
        self.try_begin_request()
    }

    pub fn begin_drain(&self) {
        self.inner.draining.store(true, Ordering::Release);
        if self.active_requests() == 0 {
            self.inner.drained.notify_waiters();
        }
    }

    pub fn begin_shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::Release);
        self.begin_drain();
    }

    pub fn resume(&self) -> bool {
        if self.is_shutting_down() {
            return false;
        }
        self.inner.draining.store(false, Ordering::Release);
        true
    }

    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        let wait = async {
            loop {
                if self.active_requests() == 0 {
                    return;
                }
                let notified = self.inner.drained.notified();
                if self.active_requests() == 0 {
                    return;
                }
                notified.await;
            }
        };
        tokio::time::timeout(timeout, wait).await.is_ok()
    }

    fn request_finished(&self) {
        if self.inner.active_requests.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.drained.notify_waiters();
        }
    }
}

impl ActiveRequestGuard {
    pub fn finish(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if self.inner.active_requests.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.drained.notify_waiters();
        }
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Clone)]
pub struct AppState {
    pub runtime: RuntimeService,
    pub deploy_tokens: DeployTokenStore,
    pub front_cache_revalidations: Arc<Mutex<HashSet<String>>>,
    pub invoke_max_body_bytes: usize,
    pub public_base_domain: String,
    pub private_bearer_token: Option<String>,
    pub public_tls_cert_path: Option<PathBuf>,
    pub public_tls_key_path: Option<PathBuf>,
    pub websocket_sessions: Arc<Mutex<HashMap<String, WebSocketSession>>>,
    pub operations: OperationalState,
}

impl AppState {
    pub fn new(
        runtime: RuntimeService,
        deploy_tokens: DeployTokenStore,
        invoke_max_body_bytes: usize,
        public_base_domain: String,
        private_bearer_token: Option<String>,
        public_tls_cert_path: Option<PathBuf>,
        public_tls_key_path: Option<PathBuf>,
    ) -> Self {
        Self {
            runtime,
            deploy_tokens,
            front_cache_revalidations: Arc::new(Mutex::new(HashSet::new())),
            invoke_max_body_bytes,
            public_base_domain: public_base_domain.trim().to_ascii_lowercase(),
            private_bearer_token,
            public_tls_cert_path,
            public_tls_key_path,
            websocket_sessions: Arc::new(Mutex::new(HashMap::new())),
            operations: OperationalState::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OperationalState;
    use std::time::Duration;

    #[tokio::test]
    async fn drain_rejects_new_requests_and_waits_for_existing_request() {
        let state = OperationalState::default();
        let guard = state.try_begin_request().expect("request should start");
        state.begin_drain();
        assert!(!state.is_ready());
        assert!(state.try_begin_request().is_none());
        assert!(!state.wait_for_drain(Duration::from_millis(1)).await);
        drop(guard);
        assert!(state.wait_for_drain(Duration::from_secs(1)).await);
        assert!(state.resume());
        assert!(state.is_ready());
    }

    #[tokio::test]
    async fn upgraded_session_remains_part_of_bounded_drain() {
        let state = OperationalState::default();
        let session = state.try_begin_session().expect("session should start");
        state.begin_drain();
        assert!(!state.wait_for_drain(Duration::from_millis(1)).await);
        drop(session);
        assert!(state.wait_for_drain(Duration::from_secs(1)).await);
    }

    #[test]
    fn shutdown_cannot_be_resumed() {
        let state = OperationalState::default();
        state.begin_shutdown();
        assert!(!state.resume());
        assert!(state.is_shutting_down());
        assert!(!state.is_ready());
    }
}
