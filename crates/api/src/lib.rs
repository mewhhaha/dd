mod app;
mod handlers;
mod public_quic;
mod state;
#[cfg(test)]
mod test_support;

use common::Result;
use runtime::{RuntimeService, RuntimeServiceConfig};
use std::net::SocketAddr;
use std::path::PathBuf;

pub use app::serve;
pub use state::AppState;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_public_addr: SocketAddr,
    pub bind_private_addr: SocketAddr,
    pub public_base_domain: String,
    pub public_tls_cert_path: Option<PathBuf>,
    pub public_tls_key_path: Option<PathBuf>,
    pub runtime: RuntimeServiceConfig,
    pub invoke_max_body_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_public_addr: SocketAddr::from(([127, 0, 0, 1], 3000)),
            bind_private_addr: SocketAddr::from(([127, 0, 0, 1], 3001)),
            public_base_domain: "example.com".to_string(),
            public_tls_cert_path: None,
            public_tls_key_path: None,
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
        public_tls_cert_path,
        public_tls_key_path,
        runtime,
        invoke_max_body_bytes,
    } = config;
    let runtime = RuntimeService::start_with_service_config(runtime).await?;
    let state = AppState::new(
        runtime,
        invoke_max_body_bytes,
        public_base_domain,
        public_tls_cert_path,
        public_tls_key_path,
    );
    serve(bind_public_addr, bind_private_addr, state).await
}
