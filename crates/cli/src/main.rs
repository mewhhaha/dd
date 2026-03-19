use clap::{Args, Parser, Subcommand};
use common::{DeployRequest, DeployResponse, ErrorBody, InvokeRequest};
use std::env;
use std::fs;
use std::io::{self, Write};

#[derive(Parser)]
#[command(name = "grugd")]
#[command(about = "CLI for the grugd worker platform")]
struct Cli {
    #[arg(long, global = true, default_value_t = default_server())]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Deploy(DeployCmd),
    Invoke(InvokeCmd),
}

#[derive(Args)]
struct DeployCmd {
    name: String,

    file: String,
}

#[derive(Args)]
struct InvokeCmd {
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();

    match cli.command {
        Command::Deploy(command) => deploy(&client, &cli.server, command).await?,
        Command::Invoke(command) => invoke(&client, &cli.server, command).await?,
    }

    Ok(())
}

async fn deploy(client: &reqwest::Client, server: &str, command: DeployCmd) -> Result<(), String> {
    let source = fs::read_to_string(&command.file)
        .map_err(|error| format!("failed to read {}: {error}", command.file))?;
    let response = client
        .post(format!("{server}/deploy"))
        .json(&DeployRequest {
            name: command.name,
            source,
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;

    let deployment: DeployResponse = decode_json(response).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&deployment).map_err(|error| error.to_string())?
    );
    Ok(())
}

async fn invoke(client: &reqwest::Client, server: &str, command: InvokeCmd) -> Result<(), String> {
    let response = client
        .post(format!("{server}/invoke"))
        .json(&InvokeRequest {
            worker_name: command.name,
        })
        .send()
        .await
        .map_err(|error| error.to_string())?;

    let body = decode_bytes(response).await?;
    io::stdout()
        .write_all(&body)
        .map_err(|error| error.to_string())?;
    if !body.ends_with(b"\n") {
        println!();
    }
    Ok(())
}

async fn decode_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, String> {
    let status = response.status();
    let body = response.bytes().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        if let Ok(error) = serde_json::from_slice::<ErrorBody>(&body) {
            return Err(format!("{} {}", status.as_u16(), error.error));
        }
        return Err(format!(
            "{} {}",
            status.as_u16(),
            String::from_utf8_lossy(&body)
        ));
    }

    serde_json::from_slice(&body).map_err(|error| error.to_string())
}

async fn decode_bytes(response: reqwest::Response) -> Result<Vec<u8>, String> {
    let status = response.status();
    let body = response.bytes().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        if let Ok(error) = serde_json::from_slice::<ErrorBody>(&body) {
            return Err(format!("{} {}", status.as_u16(), error.error));
        }
        return Err(format!(
            "{} {}",
            status.as_u16(),
            String::from_utf8_lossy(&body)
        ));
    }

    Ok(body.to_vec())
}

fn default_server() -> String {
    env::var("GRUGD_SERVER").unwrap_or_else(|_| "http://127.0.0.1:3000".to_string())
}
