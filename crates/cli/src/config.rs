//! `dd.json`: finding it, checking its schema, and the server it names.
use crate::args::Command;
use crate::parse_json_bytes;
use common::{
    DEFAULT_PRIVATE_SERVER_URL, DeployBinding, DeployCacheConfig, DeployConfig,
    DeployInternalConfig, DeployServerModuleKind, first_non_empty_trimmed,
};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_CONFIG_FILE: &str = "dd.json";

pub const CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployFileConfig {
    #[serde(rename = "$schema", default)]
    _schema: Option<String>,
    pub schema_version: u32,
    #[serde(rename = "base_url", default, alias = "baseUrl")]
    _base_url: Option<String>,
    pub name: String,
    pub entrypoint: String,
    #[serde(default)]
    pub assets_dir: Option<String>,
    #[serde(default)]
    pub temporary: bool,
    #[serde(default)]
    pub asset_excludes: Vec<String>,
    #[serde(default)]
    pub server_modules: Vec<DeployFileServerModule>,
    #[serde(default)]
    pub config: Option<DeployConfig>,
    #[serde(default)]
    pub public: Option<bool>,
    #[serde(default)]
    pub cache: Option<DeployCacheConfig>,
    #[serde(default)]
    pub bindings: Option<Vec<DeployBinding>>,
    #[serde(default)]
    pub internal: Option<DeployInternalConfig>,
    #[serde(default)]
    pub egress_allow_hosts: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployFileServerModule {
    pub path: String,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(rename = "type", alias = "kind")]
    pub kind: DeployServerModuleKind,
}

#[derive(Debug, Default, Deserialize)]
pub struct CliConfig {
    #[serde(rename = "schema_version", default)]
    _schema_version: u32,
    #[serde(default, alias = "baseUrl")]
    base_url: Option<String>,
}

pub fn load_cli_config(command: &Command) -> Result<CliConfig, String> {
    let mut config = load_workspace_cli_config()?;
    if let Some(path) = command_config_path(command) {
        let command_config = load_cli_config_file(Path::new(path))?;
        if command_config.base_url.is_some() {
            config.base_url = command_config.base_url;
        }
    }
    Ok(config)
}

fn load_workspace_cli_config() -> Result<CliConfig, String> {
    let cwd = env::current_dir().map_err(|error| error.to_string())?;
    load_workspace_cli_config_from(&cwd)
}

fn load_workspace_cli_config_from(start: &Path) -> Result<CliConfig, String> {
    let Some(path) = find_config_file(start) else {
        return Ok(CliConfig::default());
    };
    load_cli_config_file(&path)
}

fn command_config_path(command: &Command) -> Option<&str> {
    match command {
        Command::DeployConfigFile(command) | Command::PackageDeployConfigFile(command) => {
            Some(command.config_file.as_str())
        }
        _ => None,
    }
}

fn load_cli_config_file(path: &Path) -> Result<CliConfig, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    validate_config_document(&bytes)
        .map_err(|error| format!("failed to validate {}: {error}", path.display()))?;
    parse_json_bytes(&bytes).map_err(|error| format!("failed to parse {}: {error}", path.display()))
}

pub fn validate_config_document(bytes: &[u8]) -> Result<(), String> {
    const ALLOWED_FIELDS: &[&str] = &[
        "$schema",
        "schema_version",
        "name",
        "entrypoint",
        "base_url",
        "baseUrl",
        "assets_dir",
        "temporary",
        "asset_excludes",
        "server_modules",
        "config",
        "public",
        "cache",
        "bindings",
        "internal",
        "egress_allow_hosts",
    ];

    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| format!("invalid JSON: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "configuration must be a JSON object".to_string())?;
    let version = object
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            format!("schema_version is required and must equal {CONFIG_SCHEMA_VERSION}")
        })?;
    if version != u64::from(CONFIG_SCHEMA_VERSION) {
        return Err(format!(
            "unsupported schema_version {version}; expected {CONFIG_SCHEMA_VERSION}"
        ));
    }
    if let Some(field) = object
        .keys()
        .find(|field| !ALLOWED_FIELDS.contains(&field.as_str()))
    {
        return Err(format!("unknown configuration field `{field}`"));
    }
    Ok(())
}

fn find_config_file(start: &Path) -> Option<PathBuf> {
    let mut current = if start.is_dir() {
        start.to_path_buf()
    } else {
        start.parent()?.to_path_buf()
    };
    loop {
        let candidate = current.join(DEFAULT_CONFIG_FILE);
        if candidate.is_file() {
            return Some(candidate);
        }
        if !current.pop() {
            return None;
        }
    }
}

pub fn resolve_server(cli_server: Option<&str>, config: &CliConfig) -> Result<String, String> {
    let env_server = env::var("DD_SERVER").unwrap_or_default();
    resolve_server_from(cli_server, env_server.as_str(), config)
}

fn resolve_server_from(
    cli_server: Option<&str>,
    env_server: &str,
    config: &CliConfig,
) -> Result<String, String> {
    let config_server = config.base_url.clone().unwrap_or_default();
    let server = first_non_empty_trimmed([
        cli_server.unwrap_or("").to_string(),
        env_server.to_string(),
        config_server,
        DEFAULT_PRIVATE_SERVER_URL.to_string(),
    ])
    .expect("default server is non-empty");
    normalize_server_url(&server)
}

fn normalize_server_url(server: &str) -> Result<String, String> {
    let trimmed = server.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("server URL must not be empty".to_string());
    }
    let url =
        reqwest::Url::parse(trimmed).map_err(|error| format!("invalid server URL: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("server URL must use http or https".to_string());
    }
    if url.host_str().is_none() {
        return Err("server URL must include a host".to_string());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("server URL must not include query or fragment".to_string());
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        CliConfig, DEFAULT_PRIVATE_SERVER_URL, load_workspace_cli_config_from, resolve_server_from,
    };
    use crate::temp_dir;
    use std::fs;

    #[test]
    fn cli_config_loads_base_url_from_nearest_dd_json() {
        let root = temp_dir("dd-json");
        let nested = root.join("packages").join("worker");
        fs::create_dir_all(&nested).expect("create nested");
        fs::write(
            root.join("dd.json"),
            r#"{
              "schema_version": 1,
              "name": "chat",
              "entrypoint": "src/worker.ts",
              "base_url": "https://dd.example.com"
            }"#,
        )
        .expect("write dd.json");

        let config = load_workspace_cli_config_from(&nested).expect("load config");
        assert_eq!(config.base_url.as_deref(), Some("https://dd.example.com"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cli_config_rejects_missing_version_and_unknown_fields() {
        let root = temp_dir("dd-json-schema");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("dd.json"), r#"{"name":"chat"}"#).expect("write config");
        let missing = load_workspace_cli_config_from(&root).expect_err("version is required");
        assert!(missing.contains("schema_version"));

        fs::write(
            root.join("dd.json"),
            r#"{"schema_version":1,"name":"chat","publik":true}"#,
        )
        .expect("write config");
        let unknown = load_workspace_cli_config_from(&root).expect_err("unknown fields fail");
        assert!(unknown.contains("unknown configuration field `publik`"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn server_resolution_prefers_cli_env_config_then_default() {
        let config = CliConfig {
            base_url: Some("https://config.example.com/".to_string()),
            ..CliConfig::default()
        };
        assert_eq!(
            resolve_server_from(
                Some("https://cli.example.com/"),
                "https://env.example.com",
                &config
            )
            .expect("cli server"),
            "https://cli.example.com"
        );
        assert_eq!(
            resolve_server_from(None, "https://env.example.com", &config).expect("env server"),
            "https://env.example.com"
        );
        assert_eq!(
            resolve_server_from(None, "", &config).expect("config server"),
            "https://config.example.com"
        );
        assert_eq!(
            resolve_server_from(None, "", &CliConfig::default()).expect("default server"),
            DEFAULT_PRIVATE_SERVER_URL
        );
    }
}
