//! Minimal HTTP server that fronts one Perry-compiled wasm worker.
//!
//! ```bash
//! perry compile worker.ts -o worker.wasm --target wasm
//! cargo run -p wasm_host --bin dd_wasm_server -- --worker worker.wasm --port 8090
//! ```

use clap::Parser;
use common::WorkerInvocation;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::TcpListener;
use wasm_host::{InvokeOptions, WorkerModule};

#[derive(Parser)]
#[command(about = "Serve HTTP through a Perry-compiled wasm worker (experimental)")]
struct Args {
    /// Path to the worker .wasm produced by `perry compile --target wasm`
    #[arg(long)]
    worker: std::path::PathBuf,
    #[arg(long, default_value = "8090")]
    port: u16,
    /// Per-request time budget in milliseconds
    #[arg(long, default_value = "5000")]
    timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let bytes = std::fs::read(&args.worker)
        .map_err(|error| format!("cannot read {}: {error}", args.worker.display()))?;
    let module = Arc::new(WorkerModule::from_bytes(&bytes)?);
    let options = InvokeOptions {
        timeout: std::time::Duration::from_millis(args.timeout_ms),
    };

    let listener = TcpListener::bind(("127.0.0.1", args.port)).await?;
    tracing::info!(
        "serving {} on http://127.0.0.1:{}",
        args.worker.display(),
        args.port
    );

    loop {
        let (stream, _) = listener.accept().await?;
        let module = Arc::clone(&module);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let module = Arc::clone(&module);
                async move { handle(module, options, request).await }
            });
            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!("connection ended: {error}");
            }
        });
    }
}

async fn handle(
    module: Arc<WorkerModule>,
    options: InvokeOptions,
    request: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();
    let headers = parts
        .headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                value.to_str().unwrap_or("").to_string(),
            )
        })
        .collect();
    let host = parts
        .headers
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    let invocation = WorkerInvocation {
        method: parts.method.as_str().to_string(),
        url: format!("http://{host}{}", parts.uri),
        headers,
        body: body.to_vec(),
        request_id: uuid::Uuid::new_v4().to_string(),
    };

    let outcome = tokio::task::spawn_blocking(move || module.invoke(invocation, options)).await;
    let response = match outcome {
        Ok(Ok(output)) => {
            let mut builder = Response::builder()
                .status(StatusCode::from_u16(output.status).unwrap_or(StatusCode::OK));
            for (name, value) in &output.headers {
                builder = builder.header(name, value);
            }
            builder
                .body(Full::new(Bytes::from(output.body)))
                .unwrap_or_else(|error| plain_error(format!("bad response headers: {error}")))
        }
        Ok(Err(error)) => plain_error(error.to_string()),
        Err(join_error) => plain_error(format!("worker task panicked: {join_error}")),
    };
    Ok(response)
}

fn plain_error(message: String) -> Response<Full<Bytes>> {
    tracing::error!("{message}");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(message)))
        .expect("static error response cannot fail to build")
}
