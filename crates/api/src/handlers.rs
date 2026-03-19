use crate::state::AppState;
use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::header::{HeaderName, HeaderValue};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use common::{
    DeployBinding, DeployRequest, DeployResponse, ErrorBody, ErrorKind, PlatformError,
    WorkerInvocation,
};
use std::collections::HashSet;
use tokio_stream::StreamExt;
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

    validate_deploy_bindings(&payload.config.bindings)?;
    let deployment_id = state
        .runtime
        .deploy_with_config(name.to_string(), payload.source, payload.config)
        .await?;

    Ok(Json(DeployResponse {
        ok: true,
        worker: name.to_string(),
        deployment_id,
    }))
}

pub async fn invoke_worker(
    State(state): State<AppState>,
    request: Request,
) -> ApiResult<Response<Body>> {
    let (parts, body) = request.into_parts();
    let (worker_name, url) =
        parse_invoke_request_uri(parts.uri.path(), parts.uri.path_and_query())?;
    let body = to_bytes(body, usize::MAX).await.map_err(|error| {
        PlatformError::internal(format!("failed to read request body: {error}"))
    })?;

    let mut headers = Vec::with_capacity(parts.headers.len());
    for (name, value) in &parts.headers {
        let value = value.to_str().map_err(|error| {
            PlatformError::bad_request(format!("invalid header value for {name}: {error}"))
        })?;
        headers.push((name.as_str().to_string(), value.to_string()));
    }

    let invocation = WorkerInvocation {
        method: parts.method.as_str().to_string(),
        url,
        headers,
        body: body.to_vec(),
        request_id: Uuid::new_v4().to_string(),
    };

    let output = state.runtime.invoke_stream(worker_name, invocation).await?;
    build_worker_stream_response(output)
}

fn parse_invoke_request_uri(
    path: &str,
    path_and_query: Option<&axum::http::uri::PathAndQuery>,
) -> Result<(String, String), PlatformError> {
    let remainder = path
        .strip_prefix("/invoke")
        .ok_or_else(|| PlatformError::bad_request("invalid invoke route"))?;
    let remainder = remainder.strip_prefix('/').unwrap_or(remainder);
    if remainder.is_empty() {
        return Err(PlatformError::bad_request("worker name must not be empty"));
    }

    let mut segments = remainder.splitn(2, '/');
    let worker_name = segments.next().unwrap_or_default().trim();
    if worker_name.is_empty() {
        return Err(PlatformError::bad_request("worker name must not be empty"));
    }

    let path_suffix = segments.next().unwrap_or_default();
    let url_path = if path_suffix.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path_suffix)
    };
    let query_suffix = match path_and_query {
        Some(path_and_query) => path_and_query
            .as_str()
            .split_once('?')
            .map(|(_, query)| format!("?{query}"))
            .unwrap_or_default(),
        None => String::new(),
    };
    let url = format!("http://worker{}{}", url_path, query_suffix);

    Ok((worker_name.to_string(), url))
}

fn validate_deploy_bindings(bindings: &[DeployBinding]) -> Result<(), PlatformError> {
    let mut seen = HashSet::new();
    for binding in bindings {
        match binding {
            DeployBinding::Kv { binding } if binding.trim().is_empty() => {
                return Err(PlatformError::bad_request("binding name must not be empty"));
            }
            DeployBinding::Kv { binding } => {
                let normalized = binding.trim().to_string();
                if !seen.insert(normalized.clone()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {normalized}"
                    )));
                }
            }
        }
    }

    Ok(())
}

fn build_worker_stream_response(
    worker_response: runtime::WorkerStreamOutput,
) -> ApiResult<Response<Body>> {
    let mut response = Response::builder()
        .status(worker_response.status)
        .body(Body::from_stream(
            tokio_stream::wrappers::UnboundedReceiverStream::new(worker_response.body).map(
                |chunk| {
                    chunk
                        .map(axum::body::Bytes::from)
                        .map_err(|error| std::io::Error::other(error.to_string()))
                },
            ),
        ))
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

#[cfg(test)]
mod tests {
    use super::{parse_invoke_request_uri, validate_deploy_bindings};
    use common::DeployBinding;

    #[test]
    fn parse_invoke_path_and_query() {
        let uri: axum::http::Uri = "/invoke/hello/api/v1?x=1&y=2".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/api/v1?x=1&y=2");
    }

    #[test]
    fn parse_invoke_root_path() {
        let uri: axum::http::Uri = "/invoke/hello".parse().expect("uri");
        let (worker, url) = parse_invoke_request_uri(uri.path(), uri.path_and_query()).expect("ok");
        assert_eq!(worker, "hello");
        assert_eq!(url, "http://worker/");
    }

    #[test]
    fn duplicate_bindings_are_rejected() {
        let bindings = vec![
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
            DeployBinding::Kv {
                binding: "MY_KV".to_string(),
            },
        ];
        assert!(validate_deploy_bindings(&bindings).is_err());
    }
}
