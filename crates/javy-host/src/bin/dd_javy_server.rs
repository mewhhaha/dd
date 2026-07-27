use clap::Parser;
use common::WorkerInvocation;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use javy_host::{InvokeOptions, JavyWorker, WorkerOptions};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(about = "Serve HTTP through Javy-compiled Wasm workers (experimental)")]
struct Args {
    /// Wasm worker produced by `dd-javy-build`
    #[arg(long)]
    worker: PathBuf,
    /// Static files served before worker code runs
    #[arg(long)]
    assets_dir: Option<PathBuf>,
    /// String binding passed to the worker as KEY=VALUE
    #[arg(long)]
    env: Vec<String>,
    /// In-memory KV namespace exposed to the worker under this binding name
    #[arg(long)]
    kv: Vec<String>,
    /// In-memory transactional namespace exposed under this binding name
    #[arg(long)]
    memory: Vec<String>,
    #[arg(long, default_value = "8091")]
    port: u16,
    /// Per-request execution budget in milliseconds
    #[arg(long, default_value = "5000")]
    timeout_ms: u64,
}

struct Server {
    worker: Arc<JavyWorker>,
    assets_dir: Option<PathBuf>,
    invoke_options: InvokeOptions,
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
    let worker_bytes = std::fs::read(&args.worker)
        .map_err(|error| format!("cannot read {}: {error}", args.worker.display()))?;
    let worker = Arc::new(JavyWorker::new(
        &worker_bytes,
        WorkerOptions {
            env: parse_env(&args.env)?,
            kv_bindings: args.kv,
            memory_bindings: args.memory,
        },
    )?);
    let server = Arc::new(Server {
        worker,
        assets_dir: args.assets_dir,
        invoke_options: InvokeOptions {
            timeout: std::time::Duration::from_millis(args.timeout_ms),
        },
    });

    let listener = TcpListener::bind(("127.0.0.1", args.port)).await?;
    tracing::info!(
        "serving {} on http://127.0.0.1:{}",
        args.worker.display(),
        args.port
    );
    loop {
        let (stream, _) = listener.accept().await?;
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let server = Arc::clone(&server);
                async move { serve_request(server, request).await }
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

fn parse_env(entries: &[String]) -> Result<HashMap<String, String>, String> {
    let mut env = HashMap::new();
    for entry in entries {
        let (name, value) = entry
            .split_once('=')
            .ok_or_else(|| format!("--env expects KEY=VALUE, got {entry:?}"))?;
        if name.trim().is_empty() {
            return Err(format!("--env binding name must not be empty in {entry:?}"));
        }
        env.insert(name.to_string(), value.to_string());
    }
    Ok(env)
}

async fn serve_request(
    server: Arc<Server>,
    request: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, incoming_body) = request.into_parts();
    if parts.method == hyper::Method::GET
        && let Some(assets_dir) = &server.assets_dir
        && let Some(response) = serve_asset(assets_dir, parts.uri.path()).await
    {
        return Ok(response);
    }

    let body = incoming_body.collect().await?.to_bytes();
    let host = parts
        .headers
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    let invocation = WorkerInvocation {
        method: parts.method.as_str().to_string(),
        url: format!("http://{host}{}", parts.uri),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().unwrap_or("").to_string(),
                )
            })
            .collect(),
        body: body.to_vec(),
        request_id: uuid::Uuid::new_v4().to_string(),
    };
    let worker = Arc::clone(&server.worker);
    let invoke_options = server.invoke_options;
    let outcome =
        tokio::task::spawn_blocking(move || worker.invoke(invocation, invoke_options)).await;
    Ok(match outcome {
        Ok(Ok(output)) => {
            let mut builder = Response::builder()
                .status(StatusCode::from_u16(output.status).unwrap_or(StatusCode::OK));
            for (name, value) in output.headers {
                builder = builder.header(name, value);
            }
            builder
                .body(Full::new(Bytes::from(output.body)))
                .unwrap_or_else(|error| server_error(format!("invalid response headers: {error}")))
        }
        Ok(Err(error)) => server_error(error.to_string()),
        Err(error) => server_error(format!("Javy worker task panicked: {error}")),
    })
}

async fn serve_asset(assets_dir: &Path, request_path: &str) -> Option<Response<Full<Bytes>>> {
    let relative = request_path.trim_start_matches('/');
    if relative
        .split('/')
        .any(|segment| segment == ".." || segment.is_empty() && !relative.is_empty())
    {
        return None;
    }
    let asset_path = if relative.is_empty() {
        assets_dir.join("index.html")
    } else {
        assets_dir.join(relative)
    };
    let bytes = tokio::fs::read(&asset_path).await.ok()?;
    Response::builder()
        .status(StatusCode::OK)
        .header(
            "content-type",
            mime_guess::from_path(&asset_path)
                .first_or_octet_stream()
                .as_ref(),
        )
        .body(Full::new(Bytes::from(bytes)))
        .ok()
}

fn server_error(message: String) -> Response<Full<Bytes>> {
    tracing::error!("{message}");
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(message)))
        .expect("static error response cannot fail")
}

#[cfg(test)]
mod tests {
    use super::parse_env;

    #[test]
    fn env_bindings_require_a_non_empty_name_and_preserve_value_equals() {
        let env = parse_env(&["API_URL=https://example.com?a=b".to_string()]).expect("valid env");
        assert_eq!(env["API_URL"], "https://example.com?a=b");

        let error = parse_env(&["=missing".to_string()]).expect_err("empty name");
        assert!(error.contains("must not be empty"), "{error}");
    }
}
