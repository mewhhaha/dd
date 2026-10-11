mod args;
mod auth;
mod client;
mod config;
mod deploy;
mod package;
mod storage;

use args::{Cli, Command};
use auth::{deploy_bearer_token, private_bearer_token};
use clap::Parser;
use client::{
    delete_token, get_token, inspect_deployment, invoke, list_deployments, list_tokens,
    mint_deploy_token, rollback, undeploy,
};
use config::{load_cli_config, resolve_server};
use deploy::{
    build_deploy_request, build_deploy_request_from_config_file, deploy, deploy_config_file,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let cli = match cli.command {
        Command::Storage(command) => return storage::run(command).await,
        _ => cli,
    };
    let cli_config = load_cli_config(&cli.command)?;
    let server = resolve_server(cli.server.as_deref(), &cli_config)?;
    let client = reqwest::Client::new();
    let private_bearer_token = private_bearer_token();
    let deploy_bearer_token = deploy_bearer_token(&server).or_else(|| private_bearer_token.clone());

    match cli.command {
        Command::Storage(_) => unreachable!("storage commands return before network setup"),
        Command::Deploy(command) => {
            deploy(&client, &server, deploy_bearer_token.as_deref(), command).await?
        }
        Command::PackageDeploy(command) => {
            let request = build_deploy_request(command).await?;
            println!("{}", to_json_string(&request)?);
        }
        Command::DeployConfigFile(command) => {
            deploy_config_file(&client, &server, deploy_bearer_token.as_deref(), command).await?
        }
        Command::PackageDeployConfigFile(command) => {
            let request = build_deploy_request_from_config_file(command).await?;
            println!("{}", to_json_string(&request)?);
        }
        Command::Auth(command) => auth::run(command, &server)?,
        Command::MintDeployToken(command) => {
            mint_deploy_token(&client, &server, private_bearer_token.as_deref(), command).await?
        }
        Command::ListTokens => {
            list_tokens(&client, &server, private_bearer_token.as_deref()).await?
        }
        Command::GetToken(command) => {
            get_token(&client, &server, private_bearer_token.as_deref(), command).await?
        }
        Command::DeleteToken(command) => {
            delete_token(&client, &server, private_bearer_token.as_deref(), command).await?
        }
        Command::ListDeployments(command) => {
            list_deployments(&client, &server, private_bearer_token.as_deref(), command).await?
        }
        Command::InspectDeployment(command) => {
            inspect_deployment(&client, &server, private_bearer_token.as_deref(), command).await?
        }
        Command::Undeploy(command) => {
            undeploy(&client, &server, private_bearer_token.as_deref(), command).await?
        }
        Command::Rollback(command) => {
            rollback(&client, &server, private_bearer_token.as_deref(), command).await?
        }
        Command::Invoke(command) => {
            invoke(&client, &server, private_bearer_token.as_deref(), command).await?
        }
    }

    Ok(())
}

fn parse_json_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let mut input = bytes.to_vec();
    simd_json::serde::from_slice(&mut input).map_err(|error| error.to_string())
}

fn to_json_string<T: serde::Serialize + ?Sized>(value: &T) -> Result<String, String> {
    simd_json::serde::to_string(value).map_err(|error| error.to_string())
}

#[cfg(test)]
fn temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("dd-cli-{name}-{}", uuid::Uuid::new_v4()))
}
