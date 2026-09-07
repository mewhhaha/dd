use common::{DeployConfig, ErrorKind, PlatformError};
use runtime::{
    RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig, WorkerStats,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
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

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CommandResult {
    Deploy {
        worker: String,
        deployment_id: String,
        url: String,
    },
    Stats {
        stats: Option<WorkerStatsEnvelope>,
    },
    Shutdown,
}

#[derive(Debug, Serialize)]
struct WorkerStatsEnvelope {
    generation: u64,
    public: bool,
    queued: usize,
    busy: usize,
    inflight_total: usize,
    wait_until_total: usize,
    isolates_total: usize,
    spawn_count: u64,
    reuse_count: u64,
    scale_down_count: u64,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let mut allow_code_generation = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--allow-code-generation" => allow_code_generation = true,
            "--stdio" => {}
            "--help" | "-h" => {
                eprintln!(
                    "Usage: dd_dev_runtime --stdio [--allow-code-generation]\n\nReads deploy/control JSON commands on stdin. Deploy returns a loopback HTTP URL for streaming requests and WebSockets."
                );
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let store_dir = std::env::temp_dir().join(format!("dd-dev-runtime-{}", Uuid::new_v4()));
    let service = RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: RuntimeConfig {
            min_isolates: 0,
            max_global_isolates: 4,
            max_isolates: 4,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(10),
            scale_tick: Duration::from_millis(50),
            debug_code_generation: allow_code_generation,
            ..RuntimeConfig::default()
        },
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            memory_outbox_max_concurrent_shards: 8,
            memory_snapshot_cache_max_entries: 4096,
            memory_snapshot_cache_max_bytes: 64 * 1024 * 1024,
            worker_store_enabled: false,
        },
    })
    .await
    .map_err(|error| error.to_string())?;

    let result = run_stdio(service.clone()).await;
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

async fn run_stdio(service: RuntimeService) -> Result<(), String> {
    let mut listeners = HashMap::<String, SocketAddr>::new();
    let mut servers = JoinSet::new();
    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    let (response_tx, mut response_rx) = mpsc::channel::<ResponseEnvelope<CommandResult>>(128);
    let writer = tokio::spawn(async move {
        let mut stdout = io::stdout();
        while let Some(response) = response_rx.recv().await {
            let is_shutdown = matches!(response.result, Some(CommandResult::Shutdown));
            let line = serde_json::to_string(&response).map_err(|error| error.to_string())?;
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
                if response_tx.send(response).await.is_err() {
                    break;
                }
                if stop_reading {
                    break;
                }
            }
            Err(error) => {
                let response = ResponseEnvelope {
                    id: String::new(),
                    ok: false,
                    result: None,
                    error: Some(ErrorEnvelope {
                        kind: "bad_request",
                        message: format!("invalid command JSON: {error}"),
                    }),
                };
                if response_tx.send(response).await.is_err() {
                    break;
                }
            }
        }
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
            stats: service.stats(name).await.map(WorkerStatsEnvelope::from),
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

impl From<WorkerStats> for WorkerStatsEnvelope {
    fn from(stats: WorkerStats) -> Self {
        Self {
            generation: stats.generation,
            public: stats.public,
            queued: stats.queued,
            busy: stats.busy,
            inflight_total: stats.inflight_total,
            wait_until_total: stats.wait_until_total,
            isolates_total: stats.isolates_total,
            spawn_count: stats.spawn_count,
            reuse_count: stats.reuse_count,
            scale_down_count: stats.scale_down_count,
        }
    }
}
