mod app;
mod handlers;
mod state;

use common::{PlatformError, Result};
use runtime::RuntimeService;
use std::env;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let addr: SocketAddr = bind_addr
        .parse()
        .map_err(|error| PlatformError::internal(format!("invalid BIND_ADDR: {error}")))?;

    let state = state::AppState::new(RuntimeService::start().await?);
    app::serve(addr, state).await
}
