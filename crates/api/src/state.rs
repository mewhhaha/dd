use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use runtime::RuntimeService;

#[derive(Clone)]
pub struct AppState {
    pub runtime: RuntimeService,
    pub edge_revalidations: Arc<Mutex<HashSet<String>>>,
    pub invoke_max_body_bytes: usize,
    pub public_base_domain: String,
}

impl AppState {
    pub fn new(
        runtime: RuntimeService,
        invoke_max_body_bytes: usize,
        public_base_domain: String,
    ) -> Self {
        Self {
            runtime,
            edge_revalidations: Arc::new(Mutex::new(HashSet::new())),
            invoke_max_body_bytes,
            public_base_domain: public_base_domain.trim().to_ascii_lowercase(),
        }
    }
}
