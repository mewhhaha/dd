use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

pub type Result<T> = std::result::Result<T, PlatformError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    BadRequest,
    NotFound,
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

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
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
}

impl ErrorBody {
    pub fn from_error(error: &PlatformError) -> Self {
        Self {
            ok: false,
            error: error.message.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployRequest {
    pub name: String,
    pub source: String,
    #[serde(default)]
    pub config: DeployConfig,
    #[serde(default)]
    pub assets: Vec<DeployAsset>,
    #[serde(default)]
    pub asset_headers: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployAsset {
    pub path: String,
    pub content_base64: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeployConfig {
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub bindings: Vec<DeployBinding>,
    #[serde(default)]
    pub internal: DeployInternalConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeployInternalConfig {
    #[serde(default)]
    pub trace: Option<DeployTraceDestination>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployTraceDestination {
    pub worker: String,
    #[serde(default = "default_trace_path")]
    pub path: String,
}

fn default_trace_path() -> String {
    "/ingest".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DeployBinding {
    Kv {
        binding: String,
    },
    #[serde(rename = "memory")]
    Actor {
        binding: String,
    },
    Dynamic {
        binding: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployResponse {
    pub ok: bool,
    pub worker: String,
    pub deployment_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicDeployRequest {
    pub source: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub egress_allow_hosts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicDeployResponse {
    pub ok: bool,
    pub worker: String,
    pub deployment_id: String,
    pub env_placeholders: HashMap<String, String>,
}

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
    use super::DeployRequest;

    #[test]
    fn deploy_binding_rejects_legacy_actor_json_type() {
        let result = serde_json::from_str::<DeployRequest>(
            r#"{
                "name": "worker",
                "source": "export default {}",
                "config": {
                    "bindings": [
                        { "type": "actor", "binding": "ROOMS" }
                    ]
                }
            }"#,
        );
        assert!(result.is_err());
    }
}
