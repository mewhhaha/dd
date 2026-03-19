use runtime::RuntimeService;

#[derive(Clone)]
pub struct AppState {
    pub runtime: RuntimeService,
}

impl AppState {
    pub fn new(runtime: RuntimeService) -> Self {
        Self { runtime }
    }
}
