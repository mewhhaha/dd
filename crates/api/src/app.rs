use crate::handlers::{deploy_worker, invoke_worker};
use crate::state::AppState;
use common::{PlatformError, Result};
use std::net::SocketAddr;
use tracing::info;

pub async fn serve(addr: SocketAddr, state: AppState) -> Result<()> {
    let app = axum::Router::new()
        .route("/deploy", axum::routing::post(deploy_worker))
        .route("/invoke", axum::routing::post(invoke_worker))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    info!("listening on http://{}", addr);
    axum::serve(listener, app)
        .await
        .map_err(|error| PlatformError::internal(error.to_string()))
}
