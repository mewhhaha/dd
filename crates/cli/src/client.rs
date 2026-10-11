//! Control-plane and invoke calls, and the HTTP helpers every request shares.
use crate::args::{
    DeploymentIdCmd, InvokeCmd, ListDeploymentsCmd, MintDeployTokenCmd, RollbackCmd, TokenIdCmd,
    WorkerCmd,
};
use crate::deploy::{parse_memory_binding, parse_service_binding};
use crate::{parse_json_bytes, to_json_string};
use common::{
    DeployBinding, DeployTokenCapabilities, DeployTokenDeleteResponse, DeployTokenGetResponse,
    DeployTokenListResponse, DeployTokenMintRequest, DeployTokenMintResponse,
    DeploymentInspectResponse, DeploymentListResponse, ErrorBody, RollbackRequest,
    RollbackResponse, UndeployResponse, WorkerNameRequest,
};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

pub async fn mint_deploy_token(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: MintDeployTokenCmd,
) -> Result<(), String> {
    let request = build_deploy_token_mint_request(command)?;
    let response = with_bearer_auth(
        client.post(api_url(server, "/v1/admin/tokens")),
        private_bearer_token,
    )
    .json(&request)
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let minted: DeployTokenMintResponse = decode_json(response).await?;
    println!("{}", to_json_string(&minted)?);
    Ok(())
}

pub async fn list_tokens(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
) -> Result<(), String> {
    let response = with_bearer_auth(
        client.get(api_url(server, "/v1/admin/tokens")),
        private_bearer_token,
    )
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let tokens: DeployTokenListResponse = decode_json(response).await?;
    println!("{}", to_json_string(&tokens)?);
    Ok(())
}

pub async fn get_token(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: TokenIdCmd,
) -> Result<(), String> {
    let response = with_bearer_auth(
        client.get(api_url(server, &format!("/v1/admin/tokens/{}", command.id))),
        private_bearer_token,
    )
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let token: DeployTokenGetResponse = decode_json(response).await?;
    println!("{}", to_json_string(&token)?);
    Ok(())
}

pub async fn delete_token(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: TokenIdCmd,
) -> Result<(), String> {
    let response = with_bearer_auth(
        client.delete(api_url(server, &format!("/v1/admin/tokens/{}", command.id))),
        private_bearer_token,
    )
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let deleted: DeployTokenDeleteResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deleted)?);
    Ok(())
}

pub async fn list_deployments(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: ListDeploymentsCmd,
) -> Result<(), String> {
    let path = command
        .worker
        .as_deref()
        .map(|worker| {
            format!(
                "/v1/admin/deployments?worker={}",
                encode_query_value(worker)
            )
        })
        .unwrap_or_else(|| "/v1/admin/deployments".to_string());
    let request = client.get(api_url(server, &path));
    let response = with_bearer_auth(request, private_bearer_token)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let deployments: DeploymentListResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deployments)?);
    Ok(())
}

pub async fn inspect_deployment(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: DeploymentIdCmd,
) -> Result<(), String> {
    let response = with_bearer_auth(
        client.get(api_url(
            server,
            &format!(
                "/v1/admin/deployment?id={}",
                encode_query_value(&command.id)
            ),
        )),
        private_bearer_token,
    )
    .send()
    .await
    .map_err(|error| error.to_string())?;
    let deployment: DeploymentInspectResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deployment)?);
    Ok(())
}

pub async fn undeploy(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: WorkerCmd,
) -> Result<(), String> {
    let response = with_bearer_auth(
        client.post(api_url(server, "/v1/admin/undeploy")),
        private_bearer_token,
    )
    .json(&WorkerNameRequest {
        worker: command.worker,
    })
    .send()
    .await
    .map_err(|error| error.to_string())?;
    let undeployed: UndeployResponse = decode_json(response).await?;
    println!("{}", to_json_string(&undeployed)?);
    Ok(())
}

pub async fn rollback(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: RollbackCmd,
) -> Result<(), String> {
    let response = with_bearer_auth(
        client.post(api_url(server, "/v1/admin/rollback")),
        private_bearer_token,
    )
    .json(&RollbackRequest {
        worker: command.worker,
        deployment_id: command.deployment_id,
    })
    .send()
    .await
    .map_err(|error| error.to_string())?;
    let rolled_back: RollbackResponse = decode_json(response).await?;
    println!("{}", to_json_string(&rolled_back)?);
    Ok(())
}

fn build_deploy_token_mint_request(
    command: MintDeployTokenCmd,
) -> Result<DeployTokenMintRequest, String> {
    let mut bindings: Vec<DeployBinding> = command
        .kv_bindings
        .into_iter()
        .map(|binding| DeployBinding::Kv {
            binding: binding.trim().to_string(),
        })
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
            .service_bindings
            .into_iter()
            .map(parse_service_binding)
            .collect::<Result<Vec<_>, _>>()?,
    );

    Ok(DeployTokenMintRequest {
        id: command.id,
        name: command.name,
        expires_in_seconds: command.expires_in_seconds,
        expires_at_unix: command.expires_at_unix,
        max_uses: command.max_uses,
        capabilities: DeployTokenCapabilities {
            workers: command.workers,
            allow_any_worker: command.allow_any_worker,
            allow_public: command.allow_public,
            allow_private: command.allow_private,
            bindings,
            allow_any_bindings: command.allow_any_bindings,
            allow_internal_trace: command.allow_internal_trace,
            max_source_bytes: command.max_source_bytes,
            max_assets: command.max_assets,
            max_asset_bytes: command.max_asset_bytes,
        },
    })
}

pub async fn invoke(
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
    let url = api_url(server, &format!("/v1/invoke/{}{}", command.name, path));

    let response = with_bearer_auth(client.request(method, url), private_bearer_token)
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

pub async fn decode_json<T: serde::de::DeserializeOwned>(
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

pub fn with_bearer_auth(
    request: reqwest::RequestBuilder,
    bearer_token: Option<&str>,
) -> reqwest::RequestBuilder {
    match bearer_token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

pub fn api_url(server: &str, path: &str) -> String {
    format!("{}{}", server.trim_end_matches('/'), path)
}

fn encode_query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut encoded, "%{byte:02X}");
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::build_deploy_token_mint_request;
    use crate::args::MintDeployTokenCmd;

    #[test]
    fn mint_deploy_token_command_builds_capabilities() {
        let request = build_deploy_token_mint_request(MintDeployTokenCmd {
            id: None,
            name: Some("ci".to_string()),
            workers: vec!["chat".to_string()],
            allow_any_worker: false,
            allow_public: true,
            allow_private: false,
            kv_bindings: vec!["CACHE".to_string()],
            memory_bindings: vec!["ROOM".to_string()],
            service_bindings: vec!["AUTH=auth-worker".to_string()],
            allow_any_bindings: false,
            allow_internal_trace: false,
            max_source_bytes: Some(1024),
            max_assets: Some(4),
            max_asset_bytes: Some(4096),
            expires_in_seconds: Some(3600),
            expires_at_unix: None,
            max_uses: Some(1),
        })
        .expect("mint request");

        assert_eq!(request.name.as_deref(), Some("ci"));
        assert_eq!(request.id.as_deref(), None);
        assert_eq!(request.capabilities.workers, vec!["chat"]);
        assert!(request.capabilities.allow_public);
        assert_eq!(request.capabilities.bindings.len(), 3);
        assert_eq!(request.expires_in_seconds, Some(3600));
        assert_eq!(request.max_uses, Some(1));
    }
}
