use common::{DeployConfig, ErrorKind, PlatformError};
use runtime::{
    InspectorMode, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
    WorkerConsoleLine, WorkerStats,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct RequestEnvelope {
    id: String,
    #[serde(flatten)]
    command: DevCommand,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum DevCommand {
    Deploy {
        name: String,
        source: String,
        #[serde(default)]
        config: DeployConfig,
    },
    Stats {
        name: String,
    },
    Shutdown,
}

#[derive(Debug, Serialize)]
struct ResponseEnvelope<T: Serialize> {
    id: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorEnvelope>,
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    kind: &'static str,
    message: String,
}

/// A worker console call, written as `{"event":"console", ...}` so the
/// client can tell it from command responses (which start with `{"id":`).
#[derive(Debug, Serialize)]
struct ConsoleEvent {
    event: &'static str,
    #[serde(flatten)]
    line: WorkerConsoleLine,
}

/// Where DevTools attaches, written as `{"event":"inspector", ...}`: once
/// for the listener, then once per worker isolate with its own URLs.
#[derive(Debug, Serialize)]
struct InspectorEvent {
    event: &'static str,
    address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    isolate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    devtools: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    websocket: Option<String>,
}

enum Outgoing {
    Response(ResponseEnvelope<CommandResult>),
    Console(ConsoleEvent),
    #[cfg_attr(not(feature = "websocket"), allow(dead_code))]
    Inspector(InspectorEvent),
}

/// A paused isolate holds its request; this stands in for "no limit".
const INSPECT_TIME_LIMIT: Duration = Duration::from_secs(24 * 60 * 60);

struct Inspect {
    address: SocketAddr,
    wait: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CommandResult {
    Deploy {
        worker: String,
        deployment_id: String,
        url: String,
    },
    Stats {
        stats: Option<Box<WorkerStats>>,
    },
    Shutdown,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut allow_code_generation = false;
    let mut inspect = None;
    for arg in std::env::args().skip(1) {
        let (flag, value) = match arg.split_once('=') {
            Some((flag, value)) => (flag, Some(value)),
            None => (arg.as_str(), None),
        };
        match (flag, value) {
            ("--allow-code-generation", None) => allow_code_generation = true,
            ("--stdio", None) => {}
            ("--inspect" | "--inspect-wait", value) => {
                inspect = Some(Inspect {
                    address: inspect_address(value.unwrap_or_default())?,
                    wait: flag == "--inspect-wait",
                });
            }
            ("--help" | "-h", None) => {
                eprintln!(
                    "Usage: dd_dev_runtime --stdio [--allow-code-generation] [--inspect[=[host:]port]] [--inspect-wait[=[host:]port]]\n\nReads deploy/control JSON commands on stdin. Deploy returns a loopback HTTP URL for streaming requests and WebSockets.\n\n--inspect serves Chrome DevTools for every worker isolate (default 127.0.0.1:9229; see chrome://inspect). --inspect-wait also holds each isolate's worker code until a debugger attaches and lets it run."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let inspector_listener = match &inspect {
        Some(inspect) => {
            if !inspect.address.ip().is_loopback() {
                eprintln!(
                    "dd: the inspector on {} can be reached from other machines, and whoever reaches it can run code in your workers",
                    inspect.address
                );
            }
            Some(
                tokio::net::TcpListener::bind(inspect.address)
                    .await
                    .map_err(|error| {
                        format!(
                            "failed to bind the inspector to {}: {error}",
                            inspect.address
                        )
                    })?,
            )
        }
        None => None,
    };

    let mut runtime = RuntimeConfig {
        min_isolates: 0,
        max_global_isolates: 4,
        max_isolates: 4,
        max_inflight_per_isolate: 4,
        idle_ttl: Duration::from_secs(10),
        scale_tick: Duration::from_millis(50),
        debug_code_generation: allow_code_generation,
        dev_unscoped_fetch: true,
        ..RuntimeConfig::default()
    };
    if let Some(inspect) = &inspect {
        runtime.inspector = if inspect.wait {
            InspectorMode::Wait
        } else {
            InspectorMode::On
        };
        // One isolate per worker, kept up, so breakpoints land in the isolate
        // DevTools is attached to; and no time limit kills an isolate paused
        // at one.
        runtime.min_isolates = 1;
        runtime.max_isolates = 1;
        runtime.request_wall_timeout = INSPECT_TIME_LIMIT;
        runtime.max_queue_wait = INSPECT_TIME_LIMIT;
        runtime.isolate_startup_timeout = INSPECT_TIME_LIMIT;
    }

    let store_dir = std::env::temp_dir().join(format!("dd-dev-runtime-{}", Uuid::new_v4()));
    let service = RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime,
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            memory_outbox_max_concurrent_shards: 8,
            memory_outbox_max_claimed_bytes: RuntimeStorageConfig::default()
                .memory_outbox_max_claimed_bytes,
            memory_snapshot_cache_max_entries: 4096,
            memory_snapshot_cache_max_bytes: 64 * 1024 * 1024,
            worker_store_enabled: false,
        },
    })
    .await
    .map_err(|error| error.to_string())?;

    let result = run_stdio(service.clone(), inspector_listener).await;
    let shutdown_result = service.shutdown().await.map_err(|error| error.to_string());
    let cleanup_result = tokio::fs::remove_dir_all(&store_dir)
        .await
        .map_err(|error| {
            format!(
                "failed to remove dev store {}: {error}",
                store_dir.display()
            )
        });
    result.and(shutdown_result).and(cleanup_result)
}

async fn forward_console(
    mut console: broadcast::Receiver<WorkerConsoleLine>,
    sender: mpsc::Sender<Outgoing>,
) {
    loop {
        let line = match console.recv().await {
            Ok(line) => line,
            Err(broadcast::error::RecvError::Lagged(skipped)) => WorkerConsoleLine {
                worker: String::new(),
                request_id: String::new(),
                level: runtime::WorkerConsoleLevel::Warn,
                message: format!("dd: {skipped} console lines dropped while the client lagged"),
            },
            Err(broadcast::error::RecvError::Closed) => return,
        };
        let event = ConsoleEvent {
            event: "console",
            line,
        };
        if sender.send(Outgoing::Console(event)).await.is_err() {
            return;
        }
    }
}

#[cfg(feature = "websocket")]
fn inspect_address(value: &str) -> Result<SocketAddr, String> {
    dd_server::inspector::inspect_address(value)
}

#[cfg(not(feature = "websocket"))]
fn inspect_address(_value: &str) -> Result<SocketAddr, String> {
    Err(NO_INSPECTOR.into())
}

#[cfg(not(feature = "websocket"))]
const NO_INSPECTOR: &str =
    "this dd_dev_runtime was built without the websocket feature, which --inspect needs";

/// Serves DevTools on `listener`, announcing it and then every worker
/// isolate as it starts.
#[cfg(feature = "websocket")]
fn spawn_inspector(
    listener: tokio::net::TcpListener,
    service: RuntimeService,
    sender: mpsc::Sender<Outgoing>,
    servers: &mut JoinSet<common::Result<()>>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    let address = listener
        .local_addr()
        .map_err(|error| format!("failed to read the inspector address: {error}"))?
        .to_string();
    servers.spawn(dd_server::inspector::serve_inspector(
        listener,
        service.clone(),
    ));
    Ok(tokio::spawn(async move {
        let announce = |worker: Option<String>, isolate: Option<u64>, id: Option<Uuid>| {
            let socket = id.map(|id| format!("{address}/{id}"));
            InspectorEvent {
                event: "inspector",
                address: address.clone(),
                worker,
                isolate,
                devtools: socket.as_ref().map(|socket| {
                    format!(
                        "devtools://devtools/bundled/js_app.html?experiments=true&v8only=true&ws={socket}"
                    )
                }),
                websocket: socket.map(|socket| format!("ws://{socket}")),
            }
        };
        if sender
            .send(Outgoing::Inspector(announce(None, None, None)))
            .await
            .is_err()
        {
            return;
        }
        // Isolates start and retire on their own threads; polling here, in
        // the development runtime only, keeps the runtime's API a listing.
        let mut announced = std::collections::HashSet::new();
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            tick.tick().await;
            let targets = service.inspector_targets();
            announced.retain(|id| targets.iter().any(|target| target.id == *id));
            for target in targets {
                if !announced.insert(target.id) {
                    continue;
                }
                let event = announce(
                    Some(target.worker),
                    Some(target.isolate_id),
                    Some(target.id),
                );
                if sender.send(Outgoing::Inspector(event)).await.is_err() {
                    return;
                }
            }
        }
    }))
}

#[cfg(not(feature = "websocket"))]
fn spawn_inspector(
    _listener: tokio::net::TcpListener,
    _service: RuntimeService,
    _sender: mpsc::Sender<Outgoing>,
    _servers: &mut JoinSet<common::Result<()>>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    Err(NO_INSPECTOR.into())
}

async fn run_stdio(
    service: RuntimeService,
    inspector_listener: Option<tokio::net::TcpListener>,
) -> Result<(), String> {
    let mut listeners = HashMap::<String, SocketAddr>::new();
    let mut servers = JoinSet::new();
    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    let (response_tx, mut response_rx) = mpsc::channel::<Outgoing>(128);
    let console_forwarder = tokio::spawn(forward_console(
        service.subscribe_console(),
        response_tx.clone(),
    ));
    let inspector_announcer = match inspector_listener {
        Some(listener) => Some(spawn_inspector(
            listener,
            service.clone(),
            response_tx.clone(),
            &mut servers,
        )?),
        None => None,
    };
    let writer = tokio::spawn(async move {
        let mut stdout = io::stdout();
        while let Some(outgoing) = response_rx.recv().await {
            let (line, is_shutdown) = match outgoing {
                Outgoing::Response(response) => (
                    serde_json::to_string(&response),
                    matches!(response.result, Some(CommandResult::Shutdown)),
                ),
                Outgoing::Console(event) => (serde_json::to_string(&event), false),
                Outgoing::Inspector(event) => (serde_json::to_string(&event), false),
            };
            let line = line.map_err(|error| error.to_string())?;
            stdout
                .write_all(line.as_bytes())
                .await
                .map_err(|error| error.to_string())?;
            stdout
                .write_all(b"\n")
                .await
                .map_err(|error| error.to_string())?;
            stdout.flush().await.map_err(|error| error.to_string())?;
            if is_shutdown {
                break;
            }
        }
        Ok::<(), String>(())
    });

    loop {
        let line = tokio::select! {
            line = lines.next_line() => line.map_err(|error| error.to_string())?,
            result = servers.join_next(), if !servers.is_empty() => {
                return Err(format!("dev HTTP listener stopped: {result:?}"));
            }
        };
        let Some(line) = line else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<RequestEnvelope>(&line) {
            Ok(request) => {
                let stop_reading = matches!(&request.command, DevCommand::Shutdown);
                let response =
                    handle_command(&service, &mut listeners, &mut servers, request).await;
                if response_tx
                    .send(Outgoing::Response(response))
                    .await
                    .is_err()
                {
                    break;
                }
                if stop_reading {
                    break;
                }
            }
            Err(error) => {
                let response = ResponseEnvelope {
                    id: serde_json::from_str::<serde_json::Value>(&line)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("id")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned)
                        })
                        .unwrap_or_default(),
                    ok: false,
                    result: None,
                    error: Some(ErrorEnvelope {
                        kind: "bad_request",
                        message: format!("invalid command JSON: {error}"),
                    }),
                };
                if response_tx
                    .send(Outgoing::Response(response))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    console_forwarder.abort();
    if let Some(announcer) = inspector_announcer {
        announcer.abort();
    }
    drop(response_tx);
    writer.await.map_err(|error| error.to_string())?
}

async fn handle_command(
    service: &RuntimeService,
    listeners: &mut HashMap<String, SocketAddr>,
    servers: &mut JoinSet<common::Result<()>>,
    request: RequestEnvelope,
) -> ResponseEnvelope<CommandResult> {
    let id = request.id;
    let result: common::Result<CommandResult> = match request.command {
        DevCommand::Deploy {
            name,
            source,
            config,
        } => {
            async {
                let listener =
                    if listeners.contains_key(&name) {
                        None
                    } else {
                        Some(tokio::net::TcpListener::bind("127.0.0.1:0").await.map_err(
                            |error| {
                                PlatformError::internal(format!(
                                    "failed to bind dev listener for {name}: {error}"
                                ))
                            },
                        )?)
                    };
                let deployment_id = service
                    .deploy_with_config(name.clone(), source, config)
                    .await?;
                if let Some(listener) = listener {
                    let address = listener.local_addr().map_err(|error| {
                        PlatformError::internal(format!(
                            "failed to read dev listener address for {name}: {error}"
                        ))
                    })?;
                    listeners.insert(name.clone(), address);
                    servers.spawn(dd_server::serve_worker_listener(
                        listener,
                        service.clone(),
                        name.clone(),
                    ));
                }
                Ok(CommandResult::Deploy {
                    url: format!("http://{}", listeners[&name]),
                    worker: name,
                    deployment_id,
                })
            }
            .await
        }
        DevCommand::Stats { name } => Ok(CommandResult::Stats {
            stats: service.stats(name).await.map(Box::new),
        }),
        DevCommand::Shutdown => Ok(CommandResult::Shutdown),
    };

    match result {
        Ok(result) => ResponseEnvelope {
            id,
            ok: true,
            result: Some(result),
            error: None,
        },
        Err(error) => ResponseEnvelope {
            id,
            ok: false,
            result: None,
            error: Some(ErrorEnvelope {
                kind: error_kind(error.kind()),
                message: error.to_string(),
            }),
        },
    }
}

fn error_kind(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Unauthorized => "unauthorized",
        ErrorKind::Forbidden => "forbidden",
        ErrorKind::Conflict => "conflict",
        ErrorKind::BadRequest => "bad_request",
        ErrorKind::NotFound => "not_found",
        ErrorKind::Overloaded => "overloaded",
        ErrorKind::StorageUnavailable => "storage_unavailable",
        ErrorKind::Runtime => "runtime",
        ErrorKind::Internal => "internal",
    }
}
