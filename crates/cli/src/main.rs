use base64::Engine;
use clap::{Args, Parser, Subcommand};
use common::{
    DeployAsset, DeployBinding, DeployConfig, DeployInternalConfig, DeployRequest, DeployResponse,
    DeployTraceDestination, DynamicDeployRequest, DynamicDeployResponse, ErrorBody,
};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

#[derive(Parser)]
#[command(name = "dd")]
#[command(about = "CLI for the dd worker platform")]
struct Cli {
    #[arg(long, global = true, default_value_t = default_server())]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Deploy(DeployCmd),
    #[command(hide = true)]
    PackageDeploy(DeployCmd),
    DynamicDeploy(DynamicDeployCmd),
    Invoke(InvokeCmd),
}

#[derive(Args)]
struct DeployCmd {
    name: String,

    file: String,

    #[arg(long)]
    public: bool,

    #[arg(long = "assets-dir")]
    assets_dir: Option<String>,

    #[arg(long = "kv-binding")]
    kv_bindings: Vec<String>,

    #[arg(long = "memory-binding")]
    memory_bindings: Vec<String>,

    #[arg(long = "dynamic-binding")]
    dynamic_bindings: Vec<String>,

    #[arg(long = "trace-worker")]
    trace_worker: Option<String>,

    #[arg(long = "trace-path", default_value = "/ingest")]
    trace_path: String,
}

#[derive(Args)]
struct InvokeCmd {
    name: String,

    #[arg(long, default_value = "GET")]
    method: String,

    #[arg(long, default_value = "/")]
    path: String,

    #[arg(long = "header")]
    headers: Vec<String>,

    #[arg(long = "body-file")]
    body_file: Option<String>,
}

#[derive(Args)]
struct DynamicDeployCmd {
    file: String,

    #[arg(long = "env")]
    env_vars: Vec<String>,

    #[arg(long = "allow-host")]
    allow_hosts: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let private_bearer_token = private_bearer_token();

    match cli.command {
        Command::Deploy(command) => {
            deploy(
                &client,
                &cli.server,
                private_bearer_token.as_deref(),
                command,
            )
            .await?
        }
        Command::PackageDeploy(command) => {
            let request = build_deploy_request(command).await?;
            println!("{}", to_json_string(&request)?);
        }
        Command::DynamicDeploy(command) => {
            dynamic_deploy(
                &client,
                &cli.server,
                private_bearer_token.as_deref(),
                command,
            )
            .await?
        }
        Command::Invoke(command) => {
            invoke(
                &client,
                &cli.server,
                private_bearer_token.as_deref(),
                command,
            )
            .await?
        }
    }

    Ok(())
}

async fn deploy(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: DeployCmd,
) -> Result<(), String> {
    let request = build_deploy_request(command).await?;
    let response = with_private_auth(
        client.post(format!("{server}/v1/deploy")),
        private_bearer_token,
    )
    .json(&request)
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let deployment: DeployResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deployment)?);
    Ok(())
}

async fn build_deploy_request(command: DeployCmd) -> Result<DeployRequest, String> {
    let source = tokio::fs::read_to_string(&command.file)
        .await
        .map_err(|error| format!("failed to read {}: {error}", command.file))?;
    let (assets, asset_headers) = package_assets_dir(command.assets_dir.as_deref())?;
    let mut bindings: Vec<DeployBinding> = command
        .kv_bindings
        .into_iter()
        .map(|binding| DeployBinding::Kv { binding })
        .collect();
    bindings.extend(
        command
            .memory_bindings
            .into_iter()
            .map(parse_memory_binding)
            .collect::<Result<Vec<_>, _>>()?,
    );
    bindings.extend(
        command
            .dynamic_bindings
            .into_iter()
            .map(|binding| DeployBinding::Dynamic {
                binding: binding.trim().to_string(),
            }),
    );
    let config = DeployConfig {
        public: command.public,
        bindings,
        internal: DeployInternalConfig {
            trace: command.trace_worker.map(|worker| DeployTraceDestination {
                worker,
                path: command.trace_path,
            }),
        },
    };
    Ok(DeployRequest {
        name: command.name,
        source,
        config,
        assets,
        asset_headers,
    })
}

async fn dynamic_deploy(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: DynamicDeployCmd,
) -> Result<(), String> {
    let source = tokio::fs::read_to_string(&command.file)
        .await
        .map_err(|error| format!("failed to read {}: {error}", command.file))?;
    let env = parse_env_vars(&command.env_vars)?;
    let response = with_private_auth(
        client.post(format!("{server}/v1/dynamic/deploy")),
        private_bearer_token,
    )
    .json(&DynamicDeployRequest {
        source,
        env,
        egress_allow_hosts: command.allow_hosts,
    })
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let deployed: DynamicDeployResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deployed)?);
    Ok(())
}

fn parse_memory_binding(value: String) -> Result<DeployBinding, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("memory binding must not be empty".to_string());
    }
    if trimmed.contains('=') {
        return Err(format!(
            "invalid memory binding {trimmed:?}, expected BINDING"
        ));
    }
    let binding = trimmed.trim();
    if binding.is_empty() {
        return Err(format!(
            "invalid memory binding {trimmed:?}, expected BINDING"
        ));
    }
    Ok(DeployBinding::Actor {
        binding: binding.to_string(),
    })
}

fn parse_env_vars(values: &[String]) -> Result<HashMap<String, String>, String> {
    let mut out = HashMap::new();
    for value in values {
        let trimmed = value.trim();
        let Some((name, secret)) = trimmed.split_once('=') else {
            return Err(format!("invalid env value {trimmed:?}, expected KEY=VALUE"));
        };
        let key = name.trim();
        if key.is_empty() {
            return Err("env key must not be empty".to_string());
        }
        out.insert(key.to_string(), secret.to_string());
    }
    Ok(out)
}

async fn invoke(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: InvokeCmd,
) -> Result<(), String> {
    let method = reqwest::Method::from_bytes(command.method.to_uppercase().as_bytes())
        .map_err(|error| format!("invalid HTTP method {}: {error}", command.method))?;
    let headers = parse_headers(&command.headers)?;
    let body = read_request_body(command.body_file.as_deref()).await?;
    let path = normalize_path(&command.path);
    let url = format!(
        "{}/v1/invoke/{}{}",
        server.trim_end_matches('/'),
        command.name,
        path
    );

    let response = with_private_auth(client.request(method, url), private_bearer_token)
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(|error| error.to_string())?;

    if !response.status().is_success() {
        return Err(decode_error_response(response).await?);
    }

    let mut response = response;
    let mut stdout = io::stdout();
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        stdout
            .write_all(&chunk)
            .await
            .map_err(|error| error.to_string())?;
    }
    stdout.flush().await.map_err(|error| error.to_string())?;
    Ok(())
}

async fn decode_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, String> {
    let status = response.status();
    let body = response.bytes().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        if let Ok(error) = parse_json_bytes::<ErrorBody>(&body) {
            return Err(format!("{} {}", status.as_u16(), error.error));
        }
        return Err(format!(
            "{} {}",
            status.as_u16(),
            String::from_utf8_lossy(&body)
        ));
    }

    parse_json_bytes(&body)
}

async fn decode_error_response(response: reqwest::Response) -> Result<String, String> {
    let status = response.status();
    let body = response.bytes().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        if let Ok(error) = parse_json_bytes::<ErrorBody>(&body) {
            return Err(format!("{} {}", status.as_u16(), error.error));
        }
        return Err(format!(
            "{} {}",
            status.as_u16(),
            String::from_utf8_lossy(&body)
        ));
    }

    Ok(format!(
        "{} {}",
        status.as_u16(),
        String::from_utf8_lossy(&body)
    ))
}

fn parse_headers(values: &[String]) -> Result<reqwest::header::HeaderMap, String> {
    let mut headers = reqwest::header::HeaderMap::new();
    for value in values {
        let (name, header_value) = value
            .split_once(':')
            .ok_or_else(|| format!("invalid header {value:?}, expected `Name: value`"))?;
        let name = reqwest::header::HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|error| format!("invalid header name {name:?}: {error}"))?;
        let header_value = reqwest::header::HeaderValue::from_str(header_value.trim())
            .map_err(|error| format!("invalid header value for {name}: {error}"))?;
        headers.append(name, header_value);
    }

    Ok(headers)
}

async fn read_request_body(path: Option<&str>) -> Result<Vec<u8>, String> {
    match path {
        None => Ok(Vec::new()),
        Some("-") => {
            let mut stdin = io::stdin();
            let mut body = Vec::new();
            stdin
                .read_to_end(&mut body)
                .await
                .map_err(|error| error.to_string())?;
            Ok(body)
        }
        Some(path) => tokio::fs::read(path)
            .await
            .map_err(|error| format!("failed to read {path}: {error}")),
    }
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        "/".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn default_server() -> String {
    env::var("DD_SERVER").unwrap_or_else(|_| "http://127.0.0.1:3001".to_string())
}

fn private_bearer_token() -> Option<String> {
    env::var("DD_PRIVATE_TOKEN")
        .ok()
        .or_else(|| env::var("PRIVATE_BEARER_TOKEN").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn with_private_auth(
    request: reqwest::RequestBuilder,
    private_bearer_token: Option<&str>,
) -> reqwest::RequestBuilder {
    match private_bearer_token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

fn package_assets_dir(dir: Option<&str>) -> Result<(Vec<DeployAsset>, Option<String>), String> {
    let Some(dir) = dir else {
        return Ok((Vec::new(), None));
    };

    let root = Path::new(dir);
    if !root.exists() {
        return Err(format!("assets dir not found: {}", root.display()));
    }
    if !root.is_dir() {
        return Err(format!("assets dir is not a directory: {}", root.display()));
    }

    let mut assets = Vec::new();
    let mut asset_headers = None;
    collect_assets_from_dir(root, root, &mut assets, &mut asset_headers)?;
    assets.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((assets, asset_headers))
}

fn collect_assets_from_dir(
    root: &Path,
    current: &Path,
    assets: &mut Vec<DeployAsset>,
    asset_headers: &mut Option<String>,
) -> Result<(), String> {
    let mut entries = fs::read_dir(current)
        .map_err(|error| format!("failed to read assets dir {}: {error}", current.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to read assets dir {}: {error}", current.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("failed to stat asset path {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "asset symlinks are not supported: {}",
                path.display()
            ));
        }
        if metadata.is_dir() {
            collect_assets_from_dir(root, &path, assets, asset_headers)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        let relative = path.strip_prefix(root).map_err(|error| {
            format!("failed to normalize asset path {}: {error}", path.display())
        })?;
        let relative_path = normalize_asset_relative_path(relative)?;
        if relative_path == "_headers" {
            *asset_headers = Some(
                fs::read_to_string(&path)
                    .map_err(|error| format!("failed to read {}: {error}", path.display()))?,
            );
            continue;
        }

        let normalized_path = format!("/{relative_path}");
        if assets.iter().any(|asset| asset.path == normalized_path) {
            return Err(format!("duplicate asset path: {normalized_path}"));
        }

        let bytes = fs::read(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        assets.push(DeployAsset {
            path: normalized_path,
            content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        });
    }

    Ok(())
}

fn normalize_asset_relative_path(path: &Path) -> Result<String, String> {
    let mut segments = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| format!("asset path is not valid UTF-8: {}", path.display()))?;
                if value.is_empty() || value == "." || value == ".." {
                    return Err(format!("invalid asset path segment: {value}"));
                }
                segments.push(value.to_string());
            }
            _ => {
                return Err(format!(
                    "asset path must stay within the assets dir: {}",
                    path.display()
                ));
            }
        }
    }

    if segments.is_empty() {
        return Err("asset path must not be empty".to_string());
    }

    Ok(segments.join("/"))
}

fn parse_json_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let mut input = bytes.to_vec();
    simd_json::serde::from_slice(&mut input).map_err(|error| error.to_string())
}

fn to_json_string<T: serde::Serialize + ?Sized>(value: &T) -> Result<String, String> {
    simd_json::serde::to_string(value).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{normalize_asset_relative_path, package_assets_dir, Cli};
    use clap::Parser;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-cli-{name}-{}", Uuid::new_v4()))
    }

    #[test]
    fn packages_nested_assets_and_extracts_root_headers() {
        let root = temp_dir("assets");
        fs::create_dir_all(root.join("nested")).expect("create nested");
        fs::write(root.join("a.js"), "console.log('a');").expect("write a.js");
        fs::write(root.join("nested").join("b.css"), "body{}").expect("write b.css");
        fs::write(
            root.join("_headers"),
            "/a.js\n  Cache-Control: public, max-age=60\n",
        )
        .expect("write headers");

        let (assets, asset_headers) = package_assets_dir(root.to_str()).expect("package assets");
        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].path, "/a.js");
        assert_eq!(assets[1].path, "/nested/b.css");
        assert!(asset_headers
            .expect("headers should be present")
            .contains("Cache-Control"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn normalize_asset_relative_path_rejects_non_normal_segments() {
        let error = normalize_asset_relative_path(PathBuf::from("../escape.js").as_path())
            .expect_err("path should fail");
        assert!(error.contains("within the assets dir"));
    }

    #[cfg(unix)]
    #[test]
    fn package_assets_dir_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("symlink");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("target.js"), "export{}").expect("write target");
        symlink(root.join("target.js"), root.join("link.js")).expect("create symlink");

        let error = package_assets_dir(root.to_str()).expect_err("symlink should fail");
        assert!(error.contains("symlinks"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cli_rejects_legacy_actor_binding_flag() {
        let result = Cli::try_parse_from([
            "dd",
            "deploy",
            "worker",
            "examples/hello.js",
            "--actor-binding",
            "ROOMS",
        ]);
        assert!(result.is_err());
    }
}
