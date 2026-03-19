use crate::state::AppState;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{HeaderName, HeaderValue};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use common::{
    DeployRequest, DeployResponse, ErrorBody, ErrorKind, InvokeRequest, PlatformError,
    WorkerInvocation, WorkerOutput,
};
use uuid::Uuid;

pub type ApiResult<T> = std::result::Result<T, ApiError>;

pub struct ApiError(pub PlatformError);

pub async fn deploy_worker(
    State(state): State<AppState>,
    Json(payload): Json<DeployRequest>,
) -> ApiResult<Json<DeployResponse>> {
    let name = payload.name.trim();
    if name.is_empty() {
        return Err(PlatformError::bad_request("Worker name must not be empty").into());
    }

    let deployment_id = state
        .runtime
        .deploy(name.to_string(), payload.source)
        .await?;

    Ok(Json(DeployResponse {
        ok: true,
        worker: name.to_string(),
        deployment_id,
    }))
}

pub async fn invoke_worker(
    State(state): State<AppState>,
    Json(payload): Json<InvokeRequest>,
) -> ApiResult<Response<Body>> {
    let invocation = WorkerInvocation {
        method: "GET".to_string(),
        url: "http://worker/".to_string(),
        headers: Vec::new(),
        body: Vec::new(),
        request_id: Uuid::new_v4().to_string(),
    };

    let output = state
        .runtime
        .invoke(payload.worker_name, invocation)
        .await?;
    build_worker_response(output)
}

fn build_worker_response(worker_response: WorkerOutput) -> ApiResult<Response<Body>> {
    let mut response = Response::builder()
        .status(worker_response.status)
        .body(Body::from(worker_response.body))
        .map_err(|error| PlatformError::internal(error.to_string()))?;

    for (name, value) in worker_response.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().append(name, value);
        }
    }

    Ok(response)
}

impl From<PlatformError> for ApiError {
    fn from(value: PlatformError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        let status = match self.0.kind() {
            ErrorKind::BadRequest | ErrorKind::Runtime => StatusCode::BAD_REQUEST,
            ErrorKind::NotFound => StatusCode::NOT_FOUND,
            ErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, Json(ErrorBody::from_error(&self.0))).into_response()
    }
}
