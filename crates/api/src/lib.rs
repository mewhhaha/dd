mod app;
mod handlers;
mod state;

use common::Result;
use runtime::{RuntimeService, RuntimeServiceConfig};
use std::net::SocketAddr;

pub use app::serve;
pub use state::AppState;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_public_addr: SocketAddr,
    pub bind_private_addr: SocketAddr,
    pub public_base_domain: String,
    pub runtime: RuntimeServiceConfig,
    pub invoke_max_body_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_public_addr: SocketAddr::from(([127, 0, 0, 1], 3000)),
            bind_private_addr: SocketAddr::from(([127, 0, 0, 1], 3001)),
            public_base_domain: "example.com".to_string(),
            runtime: RuntimeServiceConfig::default(),
            invoke_max_body_bytes: 16 * 1024 * 1024,
        }
    }
}

pub async fn run(config: ServerConfig) -> Result<()> {
    let ServerConfig {
        bind_public_addr,
        bind_private_addr,
        public_base_domain,
        runtime,
        invoke_max_body_bytes,
    } = config;
    let runtime = RuntimeService::start_with_service_config(runtime).await?;
    let state = AppState::new(runtime, invoke_max_body_bytes, public_base_domain);
    serve(bind_public_addr, bind_private_addr, state).await
}
