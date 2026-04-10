use clap::{Args, Parser, Subcommand};
use common::{
    DeployBinding, DeployConfig, DeployInternalConfig, DeployRequest, DeployResponse,
    DeployTraceDestination, DynamicDeployRequest, DynamicDeployResponse, ErrorBody,
};
use std::collections::HashMap;
use std::env;
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
    DynamicDeploy(DynamicDeployCmd),
    Invoke(InvokeCmd),
}

#[derive(Args)]
struct DeployCmd {
    name: String,

    file: String,

    #[arg(long)]
    public: bool,

    #[arg(long = "kv-binding")]
    kv_bindings: Vec<String>,

    #[arg(long = "memory-binding", visible_alias = "actor-binding")]
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

    match cli.command {
        Command::Deploy(command) => deploy(&client, &cli.server, command).await?,
        Command::DynamicDeploy(command) => dynamic_deploy(&client, &cli.server, command).await?,
        Command::Invoke(command) => invoke(&client, &cli.server, command).await?,
    }

    Ok(())
}

async fn deploy(client: &reqwest::Client, server: &str, command: DeployCmd) -> Result<(), String> {
    let source = tokio::fs::read_to_string(&command.file)
        .await
        .map_err(|error| format!("failed to read {}: {error}", command.file))?;
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
    let response = client
        .post(format!("{server}/v1/deploy"))
        .json(&DeployRequest {
            name: command.name,
            source,
            config,
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;

    let deployment: DeployResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deployment)?);
    Ok(())
}

async fn dynamic_deploy(
    client: &reqwest::Client,
    server: &str,
    command: DynamicDeployCmd,
) -> Result<(), String> {
    let source = tokio::fs::read_to_string(&command.file)
        .await
        .map_err(|error| format!("failed to read {}: {error}", command.file))?;
    let env = parse_env_vars(&command.env_vars)?;
    let response = client
        .post(format!("{server}/v1/dynamic/deploy"))
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

async fn invoke(client: &reqwest::Client, server: &str, command: InvokeCmd) -> Result<(), String> {
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

    let response = client
        .request(method, url)
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

fn parse_json_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let mut input = bytes.to_vec();
    simd_json::serde::from_slice(&mut input).map_err(|error| error.to_string())
}

fn to_json_string<T: serde::Serialize + ?Sized>(value: &T) -> Result<String, String> {
    simd_json::serde::to_string(value).map_err(|error| error.to_string())
}
