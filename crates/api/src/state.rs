use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use runtime::RuntimeService;

#[derive(Clone)]
pub struct AppState {
    pub runtime: RuntimeService,
    pub edge_revalidations: Arc<Mutex<HashSet<String>>>,
}

impl AppState {
    pub fn new(runtime: RuntimeService) -> Self {
        Self {
            runtime,
            edge_revalidations: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}
