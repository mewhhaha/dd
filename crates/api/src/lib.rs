mod app;
mod deploy_tokens;
mod handlers;
#[cfg(test)]
mod handlers_runtime_tests;
#[cfg(feature = "http3")]
mod public_quic;
mod state;
mod trace_health;

use common::{DEFAULT_PRIVATE_BIND_ADDR, DEFAULT_PUBLIC_BIND_ADDR, PlatformError, Result};
use deploy_tokens::DeployTokenStore;
use runtime::{RuntimeService, RuntimeServiceConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn};

pub use app::{serve, serve_until};
pub use state::AppState;

#[doc(hidden)]
pub use trace_health::{
    configure as configure_trace_exporter, record_export as record_trace_export,
    snapshot as trace_exporter_health,
};

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_public_addr: SocketAddr,
    pub bind_private_addr: SocketAddr,
    pub public_base_domain: String,
    pub private_bearer_token: Option<String>,
    pub allow_insecure_private_loopback: bool,
    pub public_tls_cert_path: Option<PathBuf>,
    pub public_tls_key_path: Option<PathBuf>,
    pub token_store_path: Option<PathBuf>,
    pub runtime: RuntimeServiceConfig,
    pub invoke_max_body_bytes: usize,
    pub limits: ServerLimits,
}

#[derive(Clone, Debug)]
pub struct ServerLimits {
    pub max_public_connections: usize,
    pub max_private_connections: usize,
    pub header_read_timeout: Duration,
    pub max_headers: usize,
    pub http2_keep_alive_interval: Duration,
    pub http2_keep_alive_timeout: Duration,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_public_connections: 4_096,
            max_private_connections: 256,
            header_read_timeout: Duration::from_secs(10),
            max_headers: 100,
            http2_keep_alive_interval: Duration::from_secs(30),
            http2_keep_alive_timeout: Duration::from_secs(10),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_public_addr: DEFAULT_PUBLIC_BIND_ADDR
                .parse()
                .expect("default public bind address must parse"),
            bind_private_addr: DEFAULT_PRIVATE_BIND_ADDR
                .parse()
                .expect("default private bind address must parse"),
            public_base_domain: "example.com".to_string(),
            private_bearer_token: None,
            allow_insecure_private_loopback: false,
            public_tls_cert_path: None,
            public_tls_key_path: None,
            token_store_path: None,
            runtime: RuntimeServiceConfig::default(),
            invoke_max_body_bytes: 16 * 1024 * 1024,
            limits: ServerLimits::default(),
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
        token_store_path,
        runtime,
        invoke_max_body_bytes,
        limits,
    } = config;
    let private_bearer_token = validate_private_control_plane_auth_config(
        bind_private_addr,
        private_bearer_token,
        allow_insecure_private_loopback,
    )?;
    let token_store_path =
        token_store_path.unwrap_or_else(|| runtime.storage.store_dir.join("tokens.json"));
    let runtime = RuntimeService::start_with_service_config(runtime).await?;
    let deploy_tokens = match DeployTokenStore::from_control_store(
        runtime.control_store(),
        Some(&token_store_path),
    )
    .await
    {
        Ok(store) => store,
        Err(error) => {
            let _ = runtime.shutdown().await;
            return Err(error);
        }
    };
    let state = AppState::new(
        runtime,
        deploy_tokens,
        invoke_max_body_bytes,
        public_base_domain,
        private_bearer_token,
        public_tls_cert_path,
        public_tls_key_path,
    );
    let serve_result = serve_until(
        bind_public_addr,
        bind_private_addr,
        state.clone(),
        limits,
        shutdown_signal(),
    )
    .await;

    state.operations.begin_shutdown();
    let drain_timeout = Duration::from_secs(20);
    let drain_started_at = std::time::Instant::now();
    let requests_drained = state.operations.wait_for_drain(drain_timeout).await;
    let runtime_drained = state
        .runtime
        .wait_for_quiescence(drain_timeout.saturating_sub(drain_started_at.elapsed()))
        .await;
    if requests_drained && runtime_drained {
        info!("HTTP requests, runtime work, and outbox delivery drained before shutdown");
    } else {
        warn!(
            active_requests = state.operations.active_requests(),
            requests_drained, runtime_drained, "drain deadline reached; forcing runtime shutdown"
        );
    }
    let shutdown_result = state.runtime.shutdown().await;
    serve_result.and(shutdown_result)
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let terminate = signal(SignalKind::terminate());
    match terminate {
        Ok(mut terminate) => {
            tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        warn!(%error, "failed to listen for SIGINT");
                    }
                }
                _ = terminate.recv() => {}
            }
        }
        Err(error) => {
            warn!(%error, "failed to listen for SIGTERM; waiting for SIGINT only");
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "failed to listen for shutdown signal");
    }
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
