use crate::handlers::{deploy_worker, invoke_worker_private, invoke_worker_public};
use crate::state::AppState;
use common::{PlatformError, Result};
use std::net::SocketAddr;
use tracing::info;

fn private_router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/v1/deploy", axum::routing::post(deploy_worker))
        .route("/v1/invoke", axum::routing::any(invoke_worker_private))
        .route(
            "/v1/invoke/{*path}",
            axum::routing::any(invoke_worker_private),
        )
        .with_state(state)
}

fn public_router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/", axum::routing::any(invoke_worker_public))
        .route("/{*path}", axum::routing::any(invoke_worker_public))
        .with_state(state)
}

pub async fn serve(
    public_addr: SocketAddr,
    private_addr: SocketAddr,
    state: AppState,
) -> Result<()> {
    let public_listener = tokio::net::TcpListener::bind(public_addr)
        .await
        .map_err(|error| PlatformError::internal(error.to_string()))?;
    let private_listener = tokio::net::TcpListener::bind(private_addr)
        .await
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    let public_app = public_router(state.clone());
    let private_app = private_router(state);
    info!("public listener on http://{}", public_addr);
    info!("private listener on http://{}", private_addr);

    tokio::select! {
        result = axum::serve(public_listener, public_app) => {
            result.map_err(|error| PlatformError::internal(error.to_string()))
        }
        result = axum::serve(private_listener, private_app) => {
            result.map_err(|error| PlatformError::internal(error.to_string()))
        }
    }
}
