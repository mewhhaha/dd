//! dd CLI: deploy and drive Perry-compiled wasm workers on dd_server.
//!
//! `dd deploy` accepts either a ready `.wasm` module or TypeScript/JavaScript
//! source, which it compiles with the Perry compiler (`perry compile
//! --target wasm`) before upload. Install Perry with
//! `npm install -g @perryts/perry`, or point the `PERRY` env var at the
//! binary.

use base64::Engine as _;
use clap::{Parser, Subcommand};
use common::{
    DEFAULT_PRIVATE_SERVER_URL, DeleteWorkerResponse, DeployRequest, DeployResponse, ErrorBody,
    WorkerConfig, WorkerListResponse,
};
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

#[derive(Parser)]
#[command(name = "dd", about = "Deploy Perry-compiled wasm workers to dd_server")]
struct Args {
    /// Private control-plane URL (or DD_SERVER)
    #[arg(long, env = "DD_SERVER", default_value = DEFAULT_PRIVATE_SERVER_URL)]
    server: String,
    /// Bearer token for the private API (or DD_PRIVATE_TOKEN)
    #[arg(long, env = "DD_PRIVATE_TOKEN")]
    token: Option<String>,
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Compile (if needed) and upload a worker
    Deploy {
        name: String,
        /// A .wasm module, or .ts/.js source compiled via Perry
        entrypoint: PathBuf,
        /// Route public traffic (worker name matched against the Host header)
        #[arg(long)]
        public: bool,
        /// Service bindings as BINDING=WORKER (repeatable)
        #[arg(long = "service")]
        services: Vec<String>,
    },
    /// List deployed workers
    List,
    /// Remove a deployed worker
    Delete { name: String },
    /// Send a request to a worker through the private API
    Invoke {
        name: String,
        #[arg(long, default_value = "GET")]
        method: String,
        #[arg(long, default_value = "/")]
        path: String,
        #[arg(long, default_value = "")]
        body: String,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    let client = reqwest::Client::new();
    let request = |method: reqwest::Method, path: &str| {
        let mut builder = client.request(method, format!("{}{path}", args.server));
        if let Some(token) = &args.token {
            builder = builder.bearer_auth(token);
        }
        builder
    };

    match args.command {
        CliCommand::Deploy {
            name,
            entrypoint,
            public,
            services,
        } => {
            let wasm = load_or_compile(&entrypoint)?;
            let mut config = WorkerConfig {
                public,
                ..WorkerConfig::default()
            };
            for entry in services {
                let (binding, worker) = entry
                    .split_once('=')
                    .ok_or_else(|| format!("--service expects BINDING=WORKER, got {entry:?}"))?;
                config
                    .services
                    .insert(binding.to_string(), worker.to_string());
            }
            let payload = DeployRequest {
                name: name.clone(),
                wasm_base64: base64::engine::general_purpose::STANDARD.encode(&wasm),
                config,
            };
            let response = request(reqwest::Method::POST, "/v1/deploy")
                .json(&payload)
                .send()
                .await
                .map_err(|e| format!("deploy request failed: {e}"))?;
            let deployed: DeployResponse = parse_response(response).await?;
            println!(
                "deployed {} ({} bytes of wasm)",
                deployed.name, deployed.wasm_bytes
            );
        }
        CliCommand::List => {
            let response = request(reqwest::Method::GET, "/v1/workers")
                .send()
                .await
                .map_err(|e| format!("list request failed: {e}"))?;
            let list: WorkerListResponse = parse_response(response).await?;
            if list.workers.is_empty() {
                println!("no workers deployed");
            }
            for worker in list.workers {
                println!(
                    "{}\t{}\t{} bytes",
                    worker.name,
                    if worker.public { "public" } else { "private" },
                    worker.wasm_bytes
                );
            }
        }
        CliCommand::Delete { name } => {
            let response = request(reqwest::Method::DELETE, &format!("/v1/workers/{name}"))
                .send()
                .await
                .map_err(|e| format!("delete request failed: {e}"))?;
            let deleted: DeleteWorkerResponse = parse_response(response).await?;
            println!("deleted {}", deleted.name);
        }
        CliCommand::Invoke {
            name,
            method,
            path,
            body,
        } => {
            let response = request(
                reqwest::Method::from_bytes(method.as_bytes())
                    .map_err(|e| format!("invalid method {method:?}: {e}"))?,
                &format!("/v1/invoke/{name}{path}"),
            )
            .body(body)
            .send()
            .await
            .map_err(|e| format!("invoke request failed: {e}"))?;
            let status = response.status();
            let text = response
                .text()
                .await
                .map_err(|e| format!("invoke response read failed: {e}"))?;
            println!("{status}");
            println!("{text}");
        }
    }
    Ok(())
}

/// Read a wasm module, compiling TypeScript/JavaScript through Perry first.
fn load_or_compile(entrypoint: &Path) -> Result<Vec<u8>, String> {
    let extension = entrypoint
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default();
    if extension == "wasm" {
        return std::fs::read(entrypoint)
            .map_err(|e| format!("cannot read {}: {e}", entrypoint.display()));
    }
    if !matches!(extension, "ts" | "js" | "tsx" | "jsx" | "mts" | "mjs") {
        return Err(format!(
            "unsupported entrypoint {} — expected .wasm or TypeScript/JavaScript source",
            entrypoint.display()
        ));
    }

    let perry = std::env::var("PERRY").unwrap_or_else(|_| "perry".to_string());
    let out = std::env::temp_dir().join(format!("dd-deploy-{}.wasm", uuid::Uuid::new_v4()));
    let output = ProcessCommand::new(&perry)
        .arg("compile")
        .arg(entrypoint)
        .arg("-o")
        .arg(&out)
        .arg("--target")
        .arg("wasm")
        .output()
        .map_err(|e| {
            format!(
                "cannot run the Perry compiler ({perry}): {e} — \
                 install it with `npm install -g @perryts/perry` or set PERRY"
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "perry compile failed for {}:\n{}{}",
            entrypoint.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ));
    }
    let wasm = std::fs::read(&out).map_err(|e| {
        format!(
            "perry reported success but {} is unreadable: {e}",
            out.display()
        )
    });
    let _ = std::fs::remove_file(&out);
    wasm
}

async fn parse_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, String> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|e| format!("response read failed: {e}"))?;
    if !status.is_success() {
        if let Ok(body) = serde_json::from_str::<ErrorBody>(&text) {
            return Err(format!("{status}: {} ({})", body.error, body.code));
        }
        return Err(format!("{status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|e| format!("unexpected response body: {e}\n{text}"))
}
