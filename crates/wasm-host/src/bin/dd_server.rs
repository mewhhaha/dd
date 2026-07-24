//! dd_server: the dd worker platform, serving Perry-compiled wasm workers.
//!
//! Two listeners, like dd has always had:
//! - public (default 0.0.0.0:8080): routes by the first Host label
//!   (`hello.example.com` -> worker `hello`), serves static assets, and
//!   upgrades websockets into worker handlers.
//! - private (default [::]:8081): the control plane. `POST /v1/deploy`
//!   accepts a Perry-compiled wasm module (the `dd` CLI compiles TypeScript
//!   before upload), `GET /v1/workers` lists, `DELETE /v1/workers/{name}`
//!   removes, and `/v1/invoke/{name}/...` forwards a request to a worker.
//!   Set DD_PRIVATE_TOKEN to require bearer auth.
//!
//! Deployed workers persist under `<store-dir>/workers/` and reload on start.

use base64::Engine as _;
use clap::Parser;
use common::{
    DeleteWorkerResponse, DeployRequest, DeployResponse, ErrorBody, PlatformError, WorkerConfig,
    WorkerInvocation, WorkerListResponse, WorkerSummary,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use wasm_host::{
    InvokeOptions, WorkerModule, WorkerOptions, WorkerRegistry, WorkerStores, WsEvent, WsOutbound,
};

#[derive(Parser)]
#[command(about = "Serve Perry-compiled wasm workers")]
struct Args {
    /// Public traffic bind address
    #[arg(long, env = "BIND_PUBLIC_ADDR", default_value = "0.0.0.0:8080")]
    public_addr: String,
    /// Private control-plane bind address
    #[arg(long, env = "BIND_PRIVATE_ADDR", default_value = "[::]:8081")]
    private_addr: String,
    /// Directory for worker persistence and disk-backed KV/memory/cache
    #[arg(long, env = "DD_STORE_DIR", default_value = "store")]
    store_dir: PathBuf,
    /// Static files served on the public listener before worker code runs
    #[arg(long, env = "DD_ASSETS_DIR")]
    assets_dir: Option<PathBuf>,
    /// Preload one worker from disk as `--name`, public (dev convenience)
    #[arg(long)]
    worker: Option<PathBuf>,
    #[arg(long, default_value = "worker")]
    name: String,
    /// Per-request time budget in milliseconds
    #[arg(long, default_value = "5000")]
    timeout_ms: u64,
    /// Bearer token required on the private listener (or DD_PRIVATE_TOKEN)
    #[arg(long, env = "DD_PRIVATE_TOKEN")]
    private_token: Option<String>,
}

struct WorkerRecord {
    config: WorkerConfig,
    wasm_bytes: u64,
}

struct Server {
    workers: WorkerRegistry,
    records: RwLock<HashMap<String, WorkerRecord>>,
    stores: Arc<WorkerStores>,
    workers_dir: PathBuf,
    options: InvokeOptions,
    assets_dir: Option<PathBuf>,
    private_token: Option<String>,
    /// While draining (snapshot in progress), public traffic gets 503.
    draining: std::sync::atomic::AtomicBool,
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
    let stores = WorkerStores::open(&args.store_dir).await?;
    let workers_dir = args.store_dir.join("workers");
    tokio::fs::create_dir_all(&workers_dir).await?;

    let server = Arc::new(Server {
        workers: Arc::new(RwLock::new(HashMap::new())),
        records: RwLock::new(HashMap::new()),
        stores,
        workers_dir,
        options: InvokeOptions {
            timeout: std::time::Duration::from_millis(args.timeout_ms),
        },
        assets_dir: args.assets_dir,
        private_token: args.private_token,
        draining: std::sync::atomic::AtomicBool::new(false),
    });

    if server.private_token.is_none() {
        tracing::warn!(
            "DD_PRIVATE_TOKEN is unset; the private API accepts unauthenticated requests"
        );
    }

    let loaded = load_persisted_workers(&server).await;
    tracing::info!("restored {loaded} persisted worker(s)");

    if let Some(path) = &args.worker {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        install_worker(
            &server,
            &args.name,
            &bytes,
            WorkerConfig {
                public: true,
                ..WorkerConfig::default()
            },
            false,
        )?;
        tracing::info!("preloaded worker {} from {}", args.name, path.display());
    }

    let public = TcpListener::bind(&args.public_addr).await?;
    let private = TcpListener::bind(&args.private_addr).await?;
    tracing::info!(
        "public on http://{} · private on http://{}",
        args.public_addr,
        args.private_addr
    );

    let public_server = Arc::clone(&server);
    let public_task = tokio::spawn(async move {
        serve_listener(public, public_server, handle_public).await;
    });
    let private_server = Arc::clone(&server);
    let private_task = tokio::spawn(async move {
        serve_listener(private, private_server, handle_private).await;
    });
    let _ = tokio::join!(public_task, private_task);
    Ok(())
}

type HandlerFuture =
    std::pin::Pin<Box<dyn Future<Output = Result<Response<Full<Bytes>>, hyper::Error>> + Send>>;

async fn serve_listener(
    listener: TcpListener,
    server: Arc<Server>,
    handler: fn(Arc<Server>, Request<hyper::body::Incoming>) -> HandlerFuture,
) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                tracing::error!("accept failed: {error}");
                continue;
            }
        };
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let service = service_fn(move |request| handler(Arc::clone(&server), request));
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

fn handle_public(server: Arc<Server>, request: Request<hyper::body::Incoming>) -> HandlerFuture {
    Box::pin(async move {
        if let Some(response) = public_response(server, request).await {
            Ok(response)
        } else {
            Ok(plain_status(StatusCode::NOT_FOUND, "no such worker"))
        }
    })
}

async fn public_response(
    server: Arc<Server>,
    request: Request<hyper::body::Incoming>,
) -> Option<Response<Full<Bytes>>> {
    // Platform-reserved readiness path (Fly health checks).
    if request.uri().path() == "/readyz" {
        return Some(plain_status(StatusCode::OK, "ok"));
    }
    if server.draining.load(std::sync::atomic::Ordering::Relaxed) {
        return Some(plain_status(
            StatusCode::SERVICE_UNAVAILABLE,
            "draining for snapshot",
        ));
    }
    if request.method() == hyper::Method::GET
        && let Some(assets_dir) = &server.assets_dir
        && let Some(response) = try_assets(assets_dir, request.uri().path()).await
    {
        return Some(response);
    }

    let host = request
        .headers()
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let module = resolve_public_worker(&server, host)?;

    if is_websocket_upgrade(&request) {
        return Some(accept_websocket(module, request));
    }

    let (parts, body) = request.into_parts();
    let body = body.collect().await.ok()?.to_bytes();
    let invocation = build_invocation(&parts, body.to_vec());
    Some(dispatch(server, module, invocation).await)
}

/// First Host label -> public worker; a single public worker also answers
/// unmatched hosts so `curl localhost:8080` works in dev.
fn resolve_public_worker(server: &Server, host: &str) -> Option<Arc<WorkerModule>> {
    let label = host.split([':', '.']).next().unwrap_or("").to_string();
    let records = server.records.read().expect("records lock");
    let workers = server.workers.read().expect("workers lock");
    if records
        .get(&label)
        .is_some_and(|record| record.config.public)
    {
        return workers.get(&label).cloned();
    }
    let mut public = records
        .iter()
        .filter(|(_, record)| record.config.public)
        .map(|(name, _)| name);
    match (public.next(), public.next()) {
        (Some(only), None) => workers.get(only).cloned(),
        _ => None,
    }
}

fn handle_private(server: Arc<Server>, request: Request<hyper::body::Incoming>) -> HandlerFuture {
    Box::pin(async move {
        if let Some(token) = &server.private_token {
            let authorized = request
                .headers()
                .get(hyper::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .is_some_and(|presented| presented == token);
            if !authorized {
                return Ok(error_response(&PlatformError::unauthorized(
                    "missing or invalid bearer token",
                )));
            }
        }
        Ok(private_response(server, request).await)
    })
}

async fn private_response(
    server: Arc<Server>,
    request: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => {
            let healthy = server.stores.kv.health_check().await.is_ok()
                && server.stores.cache.health_check().await.is_ok()
                && server.stores.memory.health_check().await.is_ok();
            if healthy {
                plain_status(StatusCode::OK, "ok")
            } else {
                plain_status(StatusCode::SERVICE_UNAVAILABLE, "storage unhealthy")
            }
        }
        ("POST", "/v1/admin/drain") => {
            server
                .draining
                .store(true, std::sync::atomic::Ordering::Relaxed);
            plain_status(StatusCode::OK, "draining")
        }
        ("POST", "/v1/admin/resume") => {
            server
                .draining
                .store(false, std::sync::atomic::Ordering::Relaxed);
            plain_status(StatusCode::OK, "resumed")
        }
        ("POST", "/v1/admin/checkpoint") => {
            let outcome = async {
                server.stores.kv.checkpoint().await?;
                server.stores.cache.checkpoint().await?;
                server.stores.memory.checkpoint_all_databases().await
            }
            .await;
            match outcome {
                Ok(databases) => plain_status(
                    StatusCode::OK,
                    &format!("checkpointed ({databases} memory databases)"),
                ),
                Err(error) => error_response(&error),
            }
        }
        ("POST", "/v1/deploy") => {
            let body = match request.into_body().collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(error) => {
                    return error_response(&PlatformError::bad_request(format!(
                        "unreadable body: {error}"
                    )));
                }
            };
            match deploy_from_json(&server, &body) {
                Ok(response) => json_response(StatusCode::OK, &response),
                Err(error) => error_response(&error),
            }
        }
        ("GET", "/v1/workers") => {
            let records = server.records.read().expect("records lock");
            let mut workers: Vec<WorkerSummary> = records
                .iter()
                .map(|(name, record)| WorkerSummary {
                    name: name.clone(),
                    public: record.config.public,
                    wasm_bytes: record.wasm_bytes,
                })
                .collect();
            workers.sort_by(|a, b| a.name.cmp(&b.name));
            json_response(StatusCode::OK, &WorkerListResponse { workers })
        }
        ("DELETE", _) if path.starts_with("/v1/workers/") => {
            let name = path.trim_start_matches("/v1/workers/").to_string();
            match remove_worker(&server, &name) {
                Ok(()) => json_response(StatusCode::OK, &DeleteWorkerResponse { ok: true, name }),
                Err(error) => error_response(&error),
            }
        }
        _ if path.starts_with("/v1/invoke/") => {
            let rest = path.trim_start_matches("/v1/invoke/");
            let (name, worker_path) = match rest.split_once('/') {
                Some((name, tail)) => (name.to_string(), format!("/{tail}")),
                None => (rest.to_string(), "/".to_string()),
            };
            let module = {
                let workers = server.workers.read().expect("workers lock");
                workers.get(&name).cloned()
            };
            let Some(module) = module else {
                return error_response(&PlatformError::not_found(format!(
                    "no worker named {name:?}"
                )));
            };
            let (parts, body) = request.into_parts();
            let body = match body.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(error) => {
                    return error_response(&PlatformError::bad_request(format!(
                        "unreadable body: {error}"
                    )));
                }
            };
            let query = parts
                .uri
                .query()
                .map(|q| format!("?{q}"))
                .unwrap_or_default();
            let mut invocation = build_invocation(&parts, body.to_vec());
            invocation.url = format!("http://{name}.internal{worker_path}{query}");
            dispatch(server, module, invocation).await
        }
        _ => error_response(&PlatformError::not_found(format!(
            "no route for {method} {path}"
        ))),
    }
}

fn deploy_from_json(server: &Arc<Server>, body: &[u8]) -> common::Result<DeployResponse> {
    let request: DeployRequest = serde_json::from_slice(body)
        .map_err(|error| PlatformError::bad_request(format!("invalid deploy request: {error}")))?;
    validate_worker_name(&request.name)?;
    let wasm = base64::engine::general_purpose::STANDARD
        .decode(&request.wasm_base64)
        .map_err(|error| PlatformError::bad_request(format!("invalid wasm_base64: {error}")))?;
    install_worker(server, &request.name, &wasm, request.config, true)?;
    Ok(DeployResponse {
        ok: true,
        name: request.name,
        wasm_bytes: wasm.len() as u64,
    })
}

/// Compile, register, and (optionally) persist one worker.
fn install_worker(
    server: &Arc<Server>,
    name: &str,
    wasm: &[u8],
    config: WorkerConfig,
    persist: bool,
) -> common::Result<()> {
    let module = WorkerModule::new(
        wasm,
        WorkerOptions {
            name: Some(name.to_string()),
            stores: Some(Arc::clone(&server.stores)),
            service_bindings: config.services.clone().into_iter().collect(),
            workers: Some(Arc::clone(&server.workers)),
        },
    )?;

    if persist {
        let wasm_path = server.workers_dir.join(format!("{name}.wasm"));
        let config_path = server.workers_dir.join(format!("{name}.json"));
        std::fs::write(&wasm_path, wasm).map_err(|error| {
            PlatformError::internal(format!("cannot persist {}: {error}", wasm_path.display()))
        })?;
        let config_json = serde_json::to_vec_pretty(&config)
            .map_err(|error| PlatformError::internal(format!("config serialization: {error}")))?;
        std::fs::write(&config_path, config_json).map_err(|error| {
            PlatformError::internal(format!("cannot persist {}: {error}", config_path.display()))
        })?;
    }

    server
        .workers
        .write()
        .expect("workers lock")
        .insert(name.to_string(), Arc::new(module));
    server.records.write().expect("records lock").insert(
        name.to_string(),
        WorkerRecord {
            config,
            wasm_bytes: wasm.len() as u64,
        },
    );
    Ok(())
}

fn remove_worker(server: &Arc<Server>, name: &str) -> common::Result<()> {
    let existed = server
        .workers
        .write()
        .expect("workers lock")
        .remove(name)
        .is_some();
    server.records.write().expect("records lock").remove(name);
    if !existed {
        return Err(PlatformError::not_found(format!(
            "no worker named {name:?}"
        )));
    }
    let _ = std::fs::remove_file(server.workers_dir.join(format!("{name}.wasm")));
    let _ = std::fs::remove_file(server.workers_dir.join(format!("{name}.json")));
    Ok(())
}

async fn load_persisted_workers(server: &Arc<Server>) -> usize {
    let mut loaded = 0;
    let Ok(entries) = std::fs::read_dir(&server.workers_dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|s| s.to_str()).map(String::from) else {
            continue;
        };
        let wasm_path = server.workers_dir.join(format!("{name}.wasm"));
        let outcome = std::fs::read(&path)
            .map_err(|e| PlatformError::internal(format!("read {}: {e}", path.display())))
            .and_then(|config_bytes| {
                serde_json::from_slice::<WorkerConfig>(&config_bytes)
                    .map_err(|e| PlatformError::internal(format!("parse {}: {e}", path.display())))
            })
            .and_then(|config| {
                let wasm = std::fs::read(&wasm_path).map_err(|e| {
                    PlatformError::internal(format!("read {}: {e}", wasm_path.display()))
                })?;
                install_worker(server, &name, &wasm, config, false)
            });
        match outcome {
            Ok(()) => loaded += 1,
            Err(error) => tracing::error!("skipping persisted worker {name}: {error}"),
        }
    }
    loaded
}

fn validate_worker_name(name: &str) -> common::Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if valid {
        Ok(())
    } else {
        Err(PlatformError::bad_request(format!(
            "worker name {name:?} must be lowercase alphanumeric with dashes, at most 64 chars"
        )))
    }
}

fn build_invocation(parts: &hyper::http::request::Parts, body: Vec<u8>) -> WorkerInvocation {
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
    WorkerInvocation {
        method: parts.method.as_str().to_string(),
        url: format!("http://{host}{}", parts.uri),
        headers,
        body,
        request_id: uuid::Uuid::new_v4().to_string(),
    }
}

async fn dispatch(
    server: Arc<Server>,
    module: Arc<WorkerModule>,
    invocation: WorkerInvocation,
) -> Response<Full<Bytes>> {
    let options = server.options;
    let outcome = tokio::task::spawn_blocking(move || module.invoke(invocation, options)).await;
    match outcome {
        Ok(Ok(output)) => {
            let mut builder = Response::builder()
                .status(StatusCode::from_u16(output.status).unwrap_or(StatusCode::OK));
            for (name, value) in &output.headers {
                builder = builder.header(name, value);
            }
            builder
                .body(Full::new(Bytes::from(output.body)))
                .unwrap_or_else(|error| {
                    error_response(&PlatformError::internal(format!(
                        "bad response headers: {error}"
                    )))
                })
        }
        Ok(Err(error)) => error_response(&error),
        Err(join_error) => error_response(&PlatformError::internal(format!(
            "worker task panicked: {join_error}"
        ))),
    }
}

fn json_response<T: serde::Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec(value).expect("response types always serialize");
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("static response cannot fail to build")
}

fn error_response(error: &PlatformError) -> Response<Full<Bytes>> {
    let status = match error.kind() {
        common::ErrorKind::Unauthorized => StatusCode::UNAUTHORIZED,
        common::ErrorKind::Forbidden => StatusCode::FORBIDDEN,
        common::ErrorKind::Conflict => StatusCode::CONFLICT,
        common::ErrorKind::BadRequest => StatusCode::BAD_REQUEST,
        common::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        common::ErrorKind::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
        common::ErrorKind::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        common::ErrorKind::Runtime | common::ErrorKind::Internal => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    if status.is_server_error() {
        tracing::error!("{error}");
    }
    json_response(status, &ErrorBody::from_error(error))
}

fn plain_status(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(message.to_string())))
        .expect("static response cannot fail to build")
}

/// Serve a static asset when one matches; normalized to reject traversal.
async fn try_assets(assets_dir: &Path, path: &str) -> Option<Response<Full<Bytes>>> {
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
    module: Arc<WorkerModule>,
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
