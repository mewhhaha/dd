use serde::{Deserialize, Serialize};
use std::fmt;

pub type Result<T> = std::result::Result<T, PlatformError>;

pub const DEFAULT_PUBLIC_BIND_ADDR: &str = "0.0.0.0:8080";
pub const DEFAULT_PRIVATE_BIND_ADDR: &str = "[::]:8081";
pub const DEFAULT_PRIVATE_SERVER_URL: &str = "http://127.0.0.1:8081";

pub fn first_non_empty_trimmed<I, S>(values: I) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    values
        .into_iter()
        .map(|value| value.as_ref().trim().to_string())
        .find(|value| !value.is_empty())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Unauthorized,
    Forbidden,
    Conflict,
    BadRequest,
    NotFound,
    Overloaded,
    StorageUnavailable,
    Runtime,
    Internal,
}

#[derive(Debug, Clone)]
pub struct PlatformError {
    kind: ErrorKind,
    message: String,
}

impl PlatformError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::BadRequest, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unauthorized, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Forbidden, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    pub fn overloaded(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Overloaded, message)
    }

    pub fn storage_unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::StorageUnavailable, message)
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Runtime, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn code(&self) -> &'static str {
        match self.kind {
            ErrorKind::Unauthorized => "unauthorized",
            ErrorKind::Forbidden => "forbidden",
            ErrorKind::Conflict => "conflict",
            ErrorKind::BadRequest => "invalid_request",
            ErrorKind::NotFound => "not_found",
            ErrorKind::Overloaded => "overloaded",
            ErrorKind::StorageUnavailable => "storage_unavailable",
            ErrorKind::Runtime => "worker_runtime",
            ErrorKind::Internal => "internal",
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self.kind,
            ErrorKind::Overloaded | ErrorKind::StorageUnavailable | ErrorKind::Internal
        )
    }
}

impl fmt::Display for PlatformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PlatformError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub ok: bool,
    pub error: String,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub retryable: bool,
}

impl ErrorBody {
    pub fn from_error(error: &PlatformError) -> Self {
        Self {
            ok: false,
            error: error.message.clone(),
            code: error.code().to_string(),
            trace_id: None,
            retryable: error.retryable(),
        }
    }

    pub fn with_trace_id(mut self, trace_id: Option<String>) -> Self {
        self.trace_id = trace_id;
        self
    }
}

/// Deploy protocol between the `dd` CLI and `dd_server`. Workers arrive as
/// Perry-compiled wasm; the CLI compiles TypeScript with `perry` first.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployRequest {
    pub name: String,
    pub wasm_base64: String,
    #[serde(default)]
    pub config: WorkerConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    #[serde(default)]
    pub public: bool,
    /// Service binding name -> co-deployed worker name (dd_service_fetch).
    #[serde(default)]
    pub services: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployResponse {
    pub ok: bool,
    pub name: String,
    pub wasm_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSummary {
    pub name: String,
    pub public: bool,
    pub wasm_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerListResponse {
    pub workers: Vec<WorkerSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteWorkerResponse {
    pub ok: bool,
    pub name: String,
}

/// One worker request, as carried across the runtime boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInvocation {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerOutput {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::{DeployRequest, PlatformError};

    #[test]
    fn deploy_request_rejects_unknown_fields() {
        let error = serde_json::from_str::<DeployRequest>(
            r#"{"name":"a","wasm_base64":"AA==","entrypoint":"x.js"}"#,
        )
        .expect_err("legacy fields must be rejected");
        assert!(error.to_string().contains("entrypoint"));
    }

    #[test]
    fn platform_errors_carry_their_message() {
        let error = PlatformError::bad_request("missing name");
        assert_eq!(error.to_string(), "missing name");
        assert_eq!(error.code(), "invalid_request");
    }
}
