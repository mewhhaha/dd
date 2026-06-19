use base64::Engine;
use common::{DeployConfig, ErrorKind, PlatformError, WorkerInvocation, WorkerOutput};
use runtime::{
    BlobStoreConfig, RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig,
    WorkerStats,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
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
    Invoke {
        name: String,
        method: String,
        url: String,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        body_base64: String,
        #[serde(default)]
        request_id: Option<String>,
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
    },
    Invoke {
        status: u16,
        headers: Vec<(String, String)>,
        body_base64: String,
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
                    "Usage: dd_dev_runtime --stdio [--allow-code-generation]\n\nReads one JSON command per line from stdin and writes one JSON response per line to stdout."
                );
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let store_dir = std::env::temp_dir().join(format!("dd-dev-runtime-{}", Uuid::new_v4()));
    let database_url = format!("file:{}", store_dir.join("dd-kv.db").display());
    let service = RuntimeService::start_with_service_config(RuntimeServiceConfig {
        runtime: RuntimeConfig {
            min_isolates: 0,
            max_isolates: 4,
            max_inflight_per_isolate: 4,
            idle_ttl: Duration::from_secs(10),
            scale_tick: Duration::from_millis(50),
            debug_code_generation: allow_code_generation,
            ..RuntimeConfig::default()
        },
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            database_url,
            memory_namespace_shards: 16,
            memory_db_cache_max_open: 256,
            memory_db_idle_ttl: Duration::from_secs(30),
            worker_store_enabled: false,
            blob_store: BlobStoreConfig::local(store_dir.join("blobs")),
        },
    })
    .await
    .map_err(|error| error.to_string())?;

    let result = run_stdio(service).await;
    let _ = tokio::fs::remove_dir_all(store_dir).await;
    result
}

async fn run_stdio(service: RuntimeService) -> Result<(), String> {
    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = io::stdout();

    while let Some(line) = lines.next_line().await.map_err(|error| error.to_string())? {
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<RequestEnvelope>(&line) {
            Ok(request) => handle_command(&service, request).await,
            Err(error) => ResponseEnvelope {
                id: String::new(),
                ok: false,
                result: None,
                error: Some(ErrorEnvelope {
                    kind: "bad_request",
                    message: format!("invalid command JSON: {error}"),
                }),
            },
        };
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
        if matches!(response.result, Some(CommandResult::Shutdown)) {
            break;
        }
    }

    Ok(())
}

async fn handle_command(
    service: &RuntimeService,
    request: RequestEnvelope,
) -> ResponseEnvelope<CommandResult> {
    let id = request.id;
    let result = match request.command {
        DevCommand::Deploy {
            name,
            source,
            config,
        } => service
            .deploy_with_config(name.clone(), source, config)
            .await
            .map(|deployment_id| CommandResult::Deploy {
                worker: name,
                deployment_id,
            }),
        DevCommand::Invoke {
            name,
            method,
            url,
            headers,
            body_base64,
            request_id,
        } => invoke(service, name, method, url, headers, body_base64, request_id)
            .await
            .map(|output| CommandResult::Invoke {
                status: output.status,
                headers: output.headers,
                body_base64: base64::engine::general_purpose::STANDARD.encode(output.body),
            }),
        DevCommand::Stats { name } => Ok(CommandResult::Stats {
            stats: service.stats(name).await.map(WorkerStatsEnvelope::from),
        }),
        DevCommand::Shutdown => service.shutdown().await.map(|()| CommandResult::Shutdown),
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

async fn invoke(
    service: &RuntimeService,
    name: String,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body_base64: String,
    request_id: Option<String>,
) -> common::Result<WorkerOutput> {
    let body = if body_base64.trim().is_empty() {
        Vec::new()
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(body_base64)
            .map_err(|error| PlatformError::bad_request(format!("invalid base64 body: {error}")))?
    };
    service
        .invoke(
            name,
            WorkerInvocation {
                method,
                url,
                headers,
                body,
                request_id: request_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            },
        )
        .await
}

fn error_kind(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Unauthorized => "unauthorized",
        ErrorKind::Forbidden => "forbidden",
        ErrorKind::Conflict => "conflict",
        ErrorKind::BadRequest => "bad_request",
        ErrorKind::NotFound => "not_found",
        ErrorKind::Overloaded => "overloaded",
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
