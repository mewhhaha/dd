mod app;
mod handlers;
mod public_quic;
mod state;

use common::PlatformError;
use common::Result;
use runtime::{RuntimeService, RuntimeServiceConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::warn;

pub use app::serve;
pub use state::AppState;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_public_addr: SocketAddr,
    pub bind_private_addr: SocketAddr,
    pub public_base_domain: String,
    pub private_bearer_token: Option<String>,
    pub allow_insecure_private_loopback: bool,
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
            private_bearer_token: None,
            allow_insecure_private_loopback: false,
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
        private_bearer_token,
        allow_insecure_private_loopback,
        public_tls_cert_path,
        public_tls_key_path,
        runtime,
        invoke_max_body_bytes,
    } = config;
    let private_bearer_token = validate_private_control_plane_auth_config(
        bind_private_addr,
        private_bearer_token,
        allow_insecure_private_loopback,
    )?;
    let runtime = RuntimeService::start_with_service_config(runtime).await?;
    let state = AppState::new(
        runtime,
        invoke_max_body_bytes,
        public_base_domain,
        private_bearer_token,
        public_tls_cert_path,
        public_tls_key_path,
    );
    serve(bind_public_addr, bind_private_addr, state).await
}

fn validate_private_control_plane_auth_config(
    bind_private_addr: SocketAddr,
    private_bearer_token: Option<String>,
    allow_insecure_private_loopback: bool,
) -> Result<Option<String>> {
    let private_bearer_token = private_bearer_token
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty());
    if private_bearer_token.is_some() {
        return Ok(private_bearer_token);
    }

    if allow_insecure_private_loopback {
        if !bind_private_addr.ip().is_loopback() {
            return Err(PlatformError::internal(
                "ALLOW_INSECURE_PRIVATE_LOOPBACK requires loopback BIND_PRIVATE_ADDR",
            ));
        }
        warn!(
            private_addr = %bind_private_addr,
            "private control plane running without bearer auth because ALLOW_INSECURE_PRIVATE_LOOPBACK is enabled"
        );
        return Ok(None);
    }

    Err(PlatformError::internal(
        "private control plane bearer auth required; set DD_PRIVATE_TOKEN or PRIVATE_BEARER_TOKEN",
    ))
}

#[cfg(test)]
mod tests {
    use super::validate_private_control_plane_auth_config;
    use std::net::SocketAddr;

    #[test]
    fn private_auth_config_requires_token_by_default() {
        let result = validate_private_control_plane_auth_config(
            SocketAddr::from(([127, 0, 0, 1], 3001)),
            None,
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn private_auth_config_allows_loopback_escape_hatch() {
        let result = validate_private_control_plane_auth_config(
            SocketAddr::from(([127, 0, 0, 1], 3001)),
            None,
            true,
        )
        .expect("loopback escape hatch should succeed");
        assert!(result.is_none());
    }

    #[test]
    fn private_auth_config_rejects_non_loopback_escape_hatch() {
        let result = validate_private_control_plane_auth_config(
            SocketAddr::from(([10, 0, 0, 5], 3001)),
            None,
            true,
        );
        assert!(result.is_err());
    }
}
