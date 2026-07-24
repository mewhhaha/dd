//! HTTP server that fronts Perry-compiled wasm workers.
//!
//! ```bash
//! perry compile worker.ts -o worker.wasm --target wasm
//! cargo run -p wasm_host --bin dd_wasm_server -- --worker worker.wasm \
//!   --store-dir ./wasm-store --service auth=auth.wasm --assets-dir ./public
//! ```
//!
//! `--store-dir` attaches disk-backed KV, memory namespaces, and cache.
//! `--service name=path` loads additional workers reachable from the main
//! worker through `dd_service_fetch("name", ...)`. `--assets-dir` serves
//! static files before worker code runs, like dd's deploy-time assets.

use clap::Parser;
use common::WorkerInvocation;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use wasm_host::{
    InvokeOptions, ServiceRegistry, WorkerModule, WorkerOptions, WorkerStores, WsEvent, WsOutbound,
};

#[derive(Parser)]
#[command(about = "Serve HTTP through Perry-compiled wasm workers (experimental)")]
struct Args {
    /// Path to the public worker .wasm produced by `perry compile --target wasm`
    #[arg(long)]
    worker: PathBuf,
    /// Worker name; scopes KV keys, memory namespaces, and the cache
    #[arg(long, default_value = "worker")]
    name: String,
    /// Additional workers as `binding=path`, reachable via dd_service_fetch
    #[arg(long)]
    service: Vec<String>,
    /// Directory for disk-backed KV/memory/cache; omitting it disables storage
    #[arg(long)]
    store_dir: Option<PathBuf>,
    /// Static files served before worker code runs
    #[arg(long)]
    assets_dir: Option<PathBuf>,
    #[arg(long, default_value = "8090")]
    port: u16,
    /// Per-request time budget in milliseconds
    #[arg(long, default_value = "5000")]
    timeout_ms: u64,
}

struct Server {
    module: Arc<WorkerModule>,
    options: InvokeOptions,
    assets_dir: Option<PathBuf>,
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
    let stores = match &args.store_dir {
        Some(dir) => Some(WorkerStores::open(dir).await?),
        None => None,
    };
    let services: ServiceRegistry = Arc::new(RwLock::new(HashMap::new()));

    let module = load_worker(&args.worker, &args.name, &stores, &services)?;
    for entry in &args.service {
        let (binding, path) = entry
            .split_once('=')
            .ok_or_else(|| format!("--service expects binding=path, got {entry:?}"))?;
        let service = load_worker(&PathBuf::from(path), binding, &stores, &services)?;
        services
            .write()
            .expect("service registry is never poisoned")
            .insert(binding.to_string(), service);
        tracing::info!("service {binding} <- {path}");
    }

    let server = Arc::new(Server {
        module,
        options: InvokeOptions {
            timeout: std::time::Duration::from_millis(args.timeout_ms),
        },
        assets_dir: args.assets_dir,
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
                async move { handle(server, request).await }
            });
            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await
            {
                tracing::debug!("connection ended: {error}");
            }
        });
    }
}

fn load_worker(
    path: &std::path::Path,
    name: &str,
    stores: &Option<Arc<WorkerStores>>,
    services: &ServiceRegistry,
) -> Result<Arc<WorkerModule>, Box<dyn std::error::Error>> {
    let bytes =
        std::fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    Ok(Arc::new(WorkerModule::new(
        &bytes,
        WorkerOptions {
            name: Some(name.to_string()),
            stores: stores.clone(),
            services: Some(Arc::clone(services)),
        },
    )?))
}

/// Serve a static asset when one matches; normalized to reject traversal.
async fn try_assets(assets_dir: &std::path::Path, path: &str) -> Option<Response<Full<Bytes>>> {
    let relative = path.trim_start_matches('/');
    if relative
        .split('/')
        .any(|part| part == ".." || part.is_empty() && !relative.is_empty())
    {
        return None;
    }
    let candidate = if relative.is_empty() {
        assets_dir.join("index.html")
    } else {
        assets_dir.join(relative)
    };
    let bytes = tokio::fs::read(&candidate).await.ok()?;
    let mime = mime_guess::from_path(&candidate)
        .first_or_octet_stream()
        .to_string();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", mime)
        .body(Full::new(Bytes::from(bytes)))
        .ok()
}

async fn handle(
    server: Arc<Server>,
    request: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    if is_websocket_upgrade(&request) {
        return Ok(accept_websocket(server, request));
    }
    let (parts, body) = request.into_parts();

    if parts.method == hyper::Method::GET
        && let Some(assets_dir) = &server.assets_dir
        && let Some(response) = try_assets(assets_dir, parts.uri.path()).await
    {
        return Ok(response);
    }

    let module = Arc::clone(&server.module);
    let options = server.options;
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

fn is_websocket_upgrade(request: &Request<hyper::body::Incoming>) -> bool {
    let headers = request.headers();
    request.method() == hyper::Method::GET
        && headers
            .get(hyper::header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
        && headers.contains_key(hyper::header::SEC_WEBSOCKET_KEY)
}

/// Complete the websocket handshake and bridge frames to the worker's
/// dispatcher: inbound text becomes `WsEvent`s, outbound `WsOutbound`s from
/// any handler are written back to this client.
fn accept_websocket(
    server: Arc<Server>,
    mut request: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
    use tokio_tungstenite::tungstenite::protocol::Role;

    let key = request
        .headers()
        .get(hyper::header::SEC_WEBSOCKET_KEY)
        .expect("checked by is_websocket_upgrade")
        .clone();
    let accept = derive_accept_key(key.as_bytes());
    let host = request
        .headers()
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    let url = format!("http://{host}{}", request.uri());

    let module = Arc::clone(&server.module);
    tokio::spawn(async move {
        let upgraded = match hyper::upgrade::on(&mut request).await {
            Ok(upgraded) => upgraded,
            Err(error) => {
                tracing::debug!("websocket upgrade failed: {error}");
                return;
            }
        };
        let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
            TokioIo::new(upgraded),
            Role::Server,
            None,
        )
        .await;
        run_websocket_connection(module, url, ws).await;
    });

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(hyper::header::UPGRADE, "websocket")
        .header(hyper::header::CONNECTION, "Upgrade")
        .header(hyper::header::SEC_WEBSOCKET_ACCEPT, accept)
        .body(Full::new(Bytes::new()))
        .expect("static upgrade response cannot fail to build")
}

async fn run_websocket_connection<S>(
    module: Arc<WorkerModule>,
    url: String,
    ws: tokio_tungstenite::WebSocketStream<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (connection, mut outbound) = module.ws_connections().register();
    let events = module.websocket_events();
    let (mut sink, mut stream) = ws.split();

    if events.send(WsEvent::Open { connection, url }).is_err() {
        module.ws_connections().remove(connection);
        return;
    }

    let writer = tokio::spawn(async move {
        while let Some(frame) = outbound.recv().await {
            let message = match frame {
                WsOutbound::Text(text) => Message::Text(text.into()),
                WsOutbound::Close => Message::Close(None),
            };
            let is_close = matches!(message, Message::Close(_));
            if sink.send(message).await.is_err() || is_close {
                break;
            }
        }
        let _ = sink.close().await;
    });

    while let Some(frame) = stream.next().await {
        match frame {
            Ok(Message::Text(text)) => {
                if events
                    .send(WsEvent::Message {
                        connection,
                        text: text.to_string(),
                    })
                    .is_err()
                {
                    break;
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {} // ping/pong handled by tungstenite; binary unsupported
        }
    }

    module.ws_connections().remove(connection);
    let _ = events.send(WsEvent::Closed { connection });
    writer.abort();
}
