use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use runtime::RuntimeService;

#[derive(Clone, Debug)]
pub struct WebSocketSession {
    pub id: String,
    pub worker_name: String,
    pub started_at: Instant,
}

#[derive(Clone)]
pub struct AppState {
    pub runtime: RuntimeService,
    pub edge_revalidations: Arc<Mutex<HashSet<String>>>,
    pub invoke_max_body_bytes: usize,
    pub public_base_domain: String,
    pub public_tls_cert_path: Option<PathBuf>,
    pub public_tls_key_path: Option<PathBuf>,
    pub websocket_sessions: Arc<Mutex<HashMap<String, WebSocketSession>>>,
}

impl AppState {
    pub fn new(
        runtime: RuntimeService,
        invoke_max_body_bytes: usize,
        public_base_domain: String,
        public_tls_cert_path: Option<PathBuf>,
        public_tls_key_path: Option<PathBuf>,
    ) -> Self {
        Self {
            runtime,
            edge_revalidations: Arc::new(Mutex::new(HashSet::new())),
            invoke_max_body_bytes,
            public_base_domain: public_base_domain.trim().to_ascii_lowercase(),
            public_tls_cert_path,
            public_tls_key_path,
            websocket_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
