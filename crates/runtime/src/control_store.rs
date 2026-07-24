use crate::turso_util::{
    checkpoint_database, configure_turso_connection, is_retryable_turso_error,
};
use common::{
    DeployAsset, DeployConfig, DeployServerModule, DeployTokenCapabilities, DeploymentDetails,
    DeploymentSummary, PlatformError, Result,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use turso::{Builder, Connection, Database, Row, transaction::TransactionBehavior};

const CONTROL_SCHEMA_VERSION: i64 = 1;
const MAX_DEPLOYMENTS_PER_WORKER: i64 = 5;
const MAX_BUSY_RETRY_ATTEMPTS: usize = 8;

const CONTROL_V1_TABLE_COLUMNS: &[(&str, &[&str])] = &[
    ("control_migrations", &["version", "applied_at_ms"]),
    (
        "deployments",
        &[
            "deployment_id",
            "worker_name",
            "source",
            "config_json",
            "assets_json",
            "server_modules_json",
            "asset_headers",
            "created_at_ms",
            "expires_at_ms",
        ],
    ),
    ("active_deployments", &["worker_name", "deployment_id"]),
    (
        "deploy_tokens",
        &[
            "id",
            "name",
            "token_hash",
            "created_at_unix",
            "expires_at_unix",
            "max_uses",
            "uses",
            "last_used_at_unix",
            "capabilities_json",
        ],
    ),
    ("migration_state", &["name", "completed_at_ms", "details"]),
    (
        "restore_diagnostics",
        &[
            "worker_name",
            "deployment_id",
            "ok",
            "error",
            "updated_at_ms",
        ],
    ),
];

#[derive(Clone)]
pub struct ControlStore {
    database: Arc<Database>,
    path: Arc<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ControlDeployment {
    pub worker: String,
    pub deployment_id: String,
    pub source: String,
    pub config: DeployConfig,
    pub assets: Vec<DeployAsset>,
    pub server_modules: Vec<DeployServerModule>,
    pub asset_headers: Option<String>,
    pub created_at_ms: i64,
    pub expires_at_ms: Option<i64>,
    pub active: bool,
}

impl ControlDeployment {
    pub fn summary(&self) -> DeploymentSummary {
        DeploymentSummary {
            worker: self.worker.clone(),
            deployment_id: self.deployment_id.clone(),
            created_at_ms: self.created_at_ms,
            active: self.active,
            temporary: self.expires_at_ms.is_some(),
            expires_at_ms: self.expires_at_ms,
        }
    }

    pub fn details(&self) -> DeploymentDetails {
        DeploymentDetails {
            summary: self.summary(),
            source: self.source.clone(),
            config: self.config.clone(),
            assets: self.assets.clone(),
            server_modules: self.server_modules.clone(),
            asset_headers: self.asset_headers.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlDeployToken {
    pub id: String,
    pub name: Option<String>,
    pub token_hash: String,
    pub created_at_unix: u64,
    pub expires_at_unix: Option<u64>,
    pub max_uses: Option<u64>,
    pub uses: u64,
    pub last_used_at_unix: Option<u64>,
    pub capabilities: DeployTokenCapabilities,
}

#[derive(Debug, Clone)]
pub struct ControlRestoreFailure {
    pub worker: String,
    pub deployment_id: Option<String>,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyWorkerDeployment {
    name: String,
    source: String,
    config: DeployConfig,
    #[serde(default)]
    assets: Vec<DeployAsset>,
    #[serde(default)]
    server_modules: Vec<DeployServerModule>,
    #[serde(default)]
    asset_headers: Option<String>,
    deployment_id: String,
    updated_at_ms: i64,
    #[serde(default)]
    expires_at_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyDeployTokenFile {
    version: u32,
    tokens: Vec<ControlDeployToken>,
}

impl ControlStore {
    pub async fn open(store_dir: impl AsRef<Path>) -> Result<Self> {
        let store_dir = store_dir.as_ref();
        tokio::fs::create_dir_all(store_dir)
            .await
            .map_err(|error| {
                PlatformError::internal(format!(
                    "failed to create control store directory {}: {error}",
                    store_dir.display()
                ))
            })?;
        let path = store_dir.join("control.db");
        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(control_error)?;
        let store = Self {
            database: Arc::new(database),
            path: Arc::new(path),
        };
        store.migrate().await?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        self.path.as_ref()
    }

    async fn connect(&self) -> Result<Connection> {
        let conn = self.database.connect().map_err(control_error)?;
        configure_turso_connection(&conn, control_error)?;
        conn.execute("PRAGMA foreign_keys = ON", ())
            .await
            .map_err(control_error)?;
        conn.execute("PRAGMA synchronous = FULL", ())
            .await
            .map_err(control_error)?;
        Ok(conn)
    }

    async fn migrate(&self) -> Result<()> {
        self.reject_unrecognized_existing_schema().await?;
        self.with_write_transaction(|tx| {
            Box::pin(async move {
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS control_migrations (
                       version INTEGER PRIMARY KEY,
                       applied_at_ms INTEGER NOT NULL
                     )",
                    (),
                )
                .await?;
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS deployments (
                       deployment_id TEXT PRIMARY KEY,
                       worker_name TEXT NOT NULL,
                       source TEXT NOT NULL,
                       config_json TEXT NOT NULL,
                       assets_json TEXT NOT NULL,
                       server_modules_json TEXT NOT NULL,
                       asset_headers TEXT,
                       created_at_ms INTEGER NOT NULL,
                       expires_at_ms INTEGER,
                       UNIQUE(worker_name, deployment_id)
                     )",
                    (),
                )
                .await?;
                tx.execute(
                    "CREATE INDEX IF NOT EXISTS deployments_worker_created
                     ON deployments(worker_name, created_at_ms DESC, deployment_id DESC)",
                    (),
                )
                .await?;
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS active_deployments (
                       worker_name TEXT PRIMARY KEY,
                       deployment_id TEXT NOT NULL,
                       FOREIGN KEY(worker_name, deployment_id)
                         REFERENCES deployments(worker_name, deployment_id)
                     )",
                    (),
                )
                .await?;
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS deploy_tokens (
                       id TEXT PRIMARY KEY,
                       name TEXT,
                       token_hash TEXT NOT NULL UNIQUE,
                       created_at_unix INTEGER NOT NULL,
                       expires_at_unix INTEGER,
                       max_uses INTEGER,
                       uses INTEGER NOT NULL,
                       last_used_at_unix INTEGER,
                       capabilities_json TEXT NOT NULL
                     )",
                    (),
                )
                .await?;
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS migration_state (
                       name TEXT PRIMARY KEY,
                       completed_at_ms INTEGER NOT NULL,
                       details TEXT
                     )",
                    (),
                )
                .await?;
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS restore_diagnostics (
                       worker_name TEXT PRIMARY KEY,
                       deployment_id TEXT,
                       ok INTEGER NOT NULL,
                       error TEXT,
                       updated_at_ms INTEGER NOT NULL
                     )",
                    (),
                )
                .await?;
                tx.execute(
                    "INSERT OR IGNORE INTO control_migrations(version, applied_at_ms)
                     VALUES (?1, ?2)",
                    (
                        CONTROL_SCHEMA_VERSION,
                        epoch_ms_i64().map_err(to_turso_error)?,
                    ),
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        self.validate_v1_schema().await
    }

    async fn reject_unrecognized_existing_schema(&self) -> Result<()> {
        let conn = self.connect().await?;
        let mut migrations = conn
            .query(
                "SELECT 1 FROM sqlite_schema
                 WHERE type = 'table' AND name = 'control_migrations'",
                (),
            )
            .await
            .map_err(control_error)?;
        if migrations.next().await.map_err(control_error)?.is_some() {
            let mut versions = conn
                .query(
                    "SELECT COALESCE(MAX(version), 0) FROM control_migrations",
                    (),
                )
                .await
                .map_err(control_error)?;
            let version = versions
                .next()
                .await
                .map_err(control_error)?
                .ok_or_else(|| PlatformError::internal("control schema version is unavailable"))?
                .get::<i64>(0)
                .map_err(control_error)?;
            if version > CONTROL_SCHEMA_VERSION {
                return Err(PlatformError::internal(format!(
                    "unsupported future control database schema version {version}"
                )));
            }
            return Ok(());
        }

        let mut tables = conn
            .query(
                "SELECT name FROM sqlite_schema
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
                (),
            )
            .await
            .map_err(control_error)?;
        if let Some(row) = tables.next().await.map_err(control_error)? {
            let name = row.get::<String>(0).map_err(control_error)?;
            return Err(PlatformError::internal(format!(
                "unrecognized control database format: table {name} exists without migration state"
            )));
        }
        Ok(())
    }

    async fn validate_v1_schema(&self) -> Result<()> {
        let conn = self.connect().await?;
        for &(table, expected_columns) in CONTROL_V1_TABLE_COLUMNS {
            let mut rows = conn
                .query(&format!("PRAGMA table_info({table})"), ())
                .await
                .map_err(control_error)?;
            let mut actual_columns = Vec::new();
            while let Some(row) = rows.next().await.map_err(control_error)? {
                actual_columns.push(row.get::<String>(1).map_err(control_error)?);
            }
            if actual_columns
                .iter()
                .map(String::as_str)
                .ne(expected_columns.iter().copied())
            {
                return Err(PlatformError::internal(format!(
                    "unrecognized control database schema: table {table} has columns [{}], expected [{}]",
                    actual_columns.join(", "),
                    expected_columns.join(", "),
                )));
            }
        }
        Ok(())
    }

    pub async fn import_legacy_workers(&self, workers_dir: &Path) -> Result<usize> {
        if self.migration_completed("legacy_workers_v1").await? {
            return Ok(0);
        }
        let mut deployments = Vec::new();
        let mut read_dir = match tokio::fs::read_dir(workers_dir).await {
            Ok(read_dir) => read_dir,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.record_migration("legacy_workers_v1", "no legacy directory")
                    .await?;
                return Ok(0);
            }
            Err(error) => {
                return Err(PlatformError::internal(format!(
                    "failed to read legacy worker store {}: {error}",
                    workers_dir.display()
                )));
            }
        };

        while let Some(entry) = read_dir.next_entry().await.map_err(|error| {
            PlatformError::internal(format!(
                "failed to read legacy worker entry in {}: {error}",
                workers_dir.display()
            ))
        })? {
            let path = entry.path();
            let file_type = entry.file_type().await.map_err(|error| {
                PlatformError::internal(format!(
                    "failed to inspect legacy worker entry {}: {error}",
                    path.display()
                ))
            })?;
            let extension = path.extension().and_then(|value| value.to_str());
            if file_type.is_file() && matches!(extension, Some("tmp" | "imported")) {
                continue;
            }
            if !file_type.is_file() || extension != Some("json") {
                return Err(PlatformError::internal(format!(
                    "unrecognized persisted worker artifact {}",
                    path.display()
                )));
            }
            let bytes = tokio::fs::read(&path).await.map_err(|error| {
                PlatformError::internal(format!(
                    "failed to read legacy worker file {}: {error}",
                    path.display()
                ))
            })?;
            let mut value: JsonValue = serde_json::from_slice(&bytes).map_err(|error| {
                PlatformError::internal(format!(
                    "unrecognized persisted worker format in {}: {error}",
                    path.display()
                ))
            })?;
            migrate_actor_bindings(&mut value);
            let stored: LegacyWorkerDeployment =
                serde_json::from_value(value).map_err(|error| {
                    PlatformError::internal(format!(
                        "unrecognized persisted worker format in {}: {error}",
                        path.display()
                    ))
                })?;
            deployments.push(ControlDeployment {
                worker: stored.name,
                deployment_id: stored.deployment_id,
                source: stored.source,
                config: stored.config,
                assets: stored.assets,
                server_modules: stored.server_modules,
                asset_headers: stored.asset_headers,
                created_at_ms: stored.updated_at_ms,
                expires_at_ms: stored.expires_at_ms,
                active: true,
            });
        }

        let imported = deployments.len();
        self.import_deployments_transactional(deployments).await?;
        Ok(imported)
    }

    async fn import_deployments_transactional(
        &self,
        mut deployments: Vec<ControlDeployment>,
    ) -> Result<()> {
        deployments.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.deployment_id.cmp(&right.deployment_id))
        });
        self.with_write_transaction(move |tx| {
            let deployments = deployments.clone();
            Box::pin(async move {
                let mut workers = HashSet::new();
                for deployment in &deployments {
                    insert_deployment_tx(tx, deployment).await?;
                    tx.execute(
                        "INSERT INTO active_deployments(worker_name, deployment_id)
                         VALUES (?1, ?2)
                         ON CONFLICT(worker_name) DO UPDATE SET deployment_id = excluded.deployment_id",
                        (deployment.worker.as_str(), deployment.deployment_id.as_str()),
                    )
                    .await?;
                    workers.insert(deployment.worker.clone());
                }
                for worker in workers {
                    prune_deployments_tx(tx, &worker).await?;
                }
                tx.execute(
                    "INSERT INTO migration_state(name, completed_at_ms, details)
                     VALUES ('legacy_workers_v1', ?1, ?2)
                     ON CONFLICT(name) DO NOTHING",
                    (
                        epoch_ms_i64().map_err(to_turso_error)?,
                        format!("imported {} deployment(s)", deployments.len()),
                    ),
                )
                .await?;
                Ok(())
            })
        })
        .await
    }

    pub async fn import_legacy_tokens(&self, path: &Path) -> Result<usize> {
        let migration_name = "legacy_deploy_tokens_v1";
        if self.migration_completed(migration_name).await? {
            return Ok(0);
        }
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.record_migration(migration_name, "no legacy token file")
                    .await?;
                return Ok(0);
            }
            Err(error) => {
                return Err(PlatformError::internal(format!(
                    "failed to read legacy token store {}: {error}",
                    path.display()
                )));
            }
        };
        let legacy: LegacyDeployTokenFile = serde_json::from_slice(&bytes).map_err(|error| {
            PlatformError::internal(format!(
                "unrecognized persisted token format in {}: {error}",
                path.display()
            ))
        })?;
        if legacy.version != 1 {
            return Err(PlatformError::internal(format!(
                "unrecognized persisted token format version {} in {}",
                legacy.version,
                path.display()
            )));
        }
        let imported = legacy.tokens.len();
        let tokens = legacy.tokens;
        self.with_write_transaction(move |tx| {
            let tokens = tokens.clone();
            Box::pin(async move {
                for token in &tokens {
                    let _ = insert_token_tx(tx, token).await?;
                }
                tx.execute(
                    "INSERT INTO migration_state(name, completed_at_ms, details)
                     VALUES (?1, ?2, ?3) ON CONFLICT(name) DO NOTHING",
                    (
                        migration_name,
                        epoch_ms_i64().map_err(to_turso_error)?,
                        format!("imported {imported} token(s)"),
                    ),
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        Ok(imported)
    }

    pub async fn insert_deployment(&self, deployment: &ControlDeployment) -> Result<()> {
        let deployment = deployment.clone();
        self.with_write_transaction(move |tx| {
            let deployment = deployment.clone();
            Box::pin(async move {
                insert_deployment_tx(tx, &deployment).await?;
                tx.execute(
                    "INSERT INTO active_deployments(worker_name, deployment_id)
                     VALUES (?1, ?2)
                     ON CONFLICT(worker_name) DO UPDATE SET deployment_id = excluded.deployment_id",
                    (
                        deployment.worker.as_str(),
                        deployment.deployment_id.as_str(),
                    ),
                )
                .await?;
                prune_deployments_tx(tx, &deployment.worker).await?;
                tx.execute(
                    "DELETE FROM restore_diagnostics WHERE worker_name = ?1",
                    (deployment.worker.as_str(),),
                )
                .await?;
                Ok(())
            })
        })
        .await
    }

    pub async fn list_deployments(&self, worker: Option<&str>) -> Result<Vec<ControlDeployment>> {
        let conn = self.connect().await?;
        let sql_all = "SELECT d.deployment_id, d.worker_name, d.source, d.config_json,
                              d.assets_json, d.server_modules_json, d.asset_headers,
                              d.created_at_ms, d.expires_at_ms,
                              CASE WHEN a.deployment_id = d.deployment_id THEN 1 ELSE 0 END
                       FROM deployments d
                       LEFT JOIN active_deployments a ON a.worker_name = d.worker_name
                       ORDER BY d.worker_name, d.created_at_ms DESC, d.deployment_id DESC";
        let sql_worker = "SELECT d.deployment_id, d.worker_name, d.source, d.config_json,
                                 d.assets_json, d.server_modules_json, d.asset_headers,
                                 d.created_at_ms, d.expires_at_ms,
                                 CASE WHEN a.deployment_id = d.deployment_id THEN 1 ELSE 0 END
                          FROM deployments d
                          LEFT JOIN active_deployments a ON a.worker_name = d.worker_name
                          WHERE d.worker_name = ?1
                          ORDER BY d.created_at_ms DESC, d.deployment_id DESC";
        let mut rows = match worker {
            Some(worker) => conn.query(sql_worker, (worker,)).await,
            None => conn.query(sql_all, ()).await,
        }
        .map_err(control_error)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(control_error)? {
            out.push(deployment_from_row(&row)?);
        }
        Ok(out)
    }

    pub async fn get_deployment(&self, deployment_id: &str) -> Result<ControlDeployment> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT d.deployment_id, d.worker_name, d.source, d.config_json,
                        d.assets_json, d.server_modules_json, d.asset_headers,
                        d.created_at_ms, d.expires_at_ms,
                        CASE WHEN a.deployment_id = d.deployment_id THEN 1 ELSE 0 END
                 FROM deployments d
                 LEFT JOIN active_deployments a ON a.worker_name = d.worker_name
                 WHERE d.deployment_id = ?1",
                (deployment_id,),
            )
            .await
            .map_err(control_error)?;
        rows.next()
            .await
            .map_err(control_error)?
            .map(|row| deployment_from_row(&row))
            .transpose()?
            .ok_or_else(|| PlatformError::not_found("deployment not found"))
    }

    pub async fn active_deployments(&self) -> Result<Vec<ControlDeployment>> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT d.deployment_id, d.worker_name, d.source, d.config_json,
                        d.assets_json, d.server_modules_json, d.asset_headers,
                        d.created_at_ms, d.expires_at_ms, 1
                 FROM active_deployments a
                 JOIN deployments d ON d.deployment_id = a.deployment_id
                 ORDER BY d.worker_name",
                (),
            )
            .await
            .map_err(control_error)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(control_error)? {
            out.push(deployment_from_row(&row)?);
        }
        Ok(out)
    }

    pub async fn deactivate_worker(&self, worker: &str) -> Result<bool> {
        let worker = worker.to_string();
        let changed = self
            .with_write_transaction(move |tx| {
                let worker = worker.clone();
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "DELETE FROM active_deployments WHERE worker_name = ?1",
                            (worker.as_str(),),
                        )
                        .await?;
                    tx.execute(
                        "DELETE FROM restore_diagnostics WHERE worker_name = ?1",
                        (worker.as_str(),),
                    )
                    .await?;
                    Ok(changed)
                })
            })
            .await?;
        Ok(changed > 0)
    }

    pub async fn activate_deployment(
        &self,
        worker: &str,
        deployment_id: &str,
    ) -> Result<ControlDeployment> {
        let deployment = self.get_deployment(deployment_id).await?;
        if deployment.worker != worker {
            return Err(PlatformError::bad_request(
                "deployment does not belong to requested worker",
            ));
        }
        let worker_owned = worker.to_string();
        let deployment_id_owned = deployment_id.to_string();
        self.with_write_transaction(move |tx| {
            let worker = worker_owned.clone();
            let deployment_id = deployment_id_owned.clone();
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO active_deployments(worker_name, deployment_id)
                     VALUES (?1, ?2)
                     ON CONFLICT(worker_name) DO UPDATE SET deployment_id = excluded.deployment_id",
                    (worker.as_str(), deployment_id.as_str()),
                )
                .await?;
                tx.execute(
                    "DELETE FROM restore_diagnostics WHERE worker_name = ?1",
                    (worker.as_str(),),
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        Ok(ControlDeployment {
            active: true,
            ..deployment
        })
    }

    pub async fn record_restore_result(
        &self,
        worker: &str,
        deployment_id: Option<&str>,
        result: &Result<()>,
    ) -> Result<()> {
        let (ok, error) = match result {
            Ok(()) => (1_i64, None),
            Err(error) => (0_i64, Some(error.to_string())),
        };
        let conn = self.connect().await?;
        retry_execute(|| {
            let conn = &conn;
            let error = error.clone();
            async move {
                conn.execute(
                    "INSERT INTO restore_diagnostics(worker_name, deployment_id, ok, error, updated_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(worker_name) DO UPDATE SET
                       deployment_id = excluded.deployment_id,
                       ok = excluded.ok,
                       error = excluded.error,
                       updated_at_ms = excluded.updated_at_ms",
                    (worker, deployment_id, ok, error, epoch_ms_i64().map_err(to_turso_error)?),
                )
                .await
            }
        })
        .await?;
        Ok(())
    }

    pub async fn restore_failures(&self) -> Result<Vec<ControlRestoreFailure>> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT worker_name, deployment_id, error
                 FROM restore_diagnostics WHERE ok = 0 ORDER BY worker_name",
                (),
            )
            .await
            .map_err(control_error)?;
        let mut failures = Vec::new();
        while let Some(row) = rows.next().await.map_err(control_error)? {
            failures.push(ControlRestoreFailure {
                worker: row.get::<String>(0).map_err(control_error)?,
                deployment_id: row.get::<Option<String>>(1).map_err(control_error)?,
                error: row
                    .get::<Option<String>>(2)
                    .map_err(control_error)?
                    .unwrap_or_else(|| "unknown restoration failure".to_string()),
            });
        }
        Ok(failures)
    }

    pub async fn insert_token(&self, token: &ControlDeployToken) -> Result<bool> {
        let conn = self.connect().await?;
        let changed = retry_execute(|| {
            let conn = &conn;
            async move { insert_token_tx(conn, token).await }
        })
        .await?;
        Ok(changed == 1)
    }

    pub async fn token_count(&self) -> Result<usize> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query("SELECT COUNT(*) FROM deploy_tokens", ())
            .await
            .map_err(control_error)?;
        let count = rows
            .next()
            .await
            .map_err(control_error)?
            .expect("token count row")
            .get::<i64>(0)
            .map_err(control_error)?;
        Ok(count.max(0) as usize)
    }

    pub async fn list_tokens(&self, now: u64) -> Result<Vec<ControlDeployToken>> {
        self.delete_expired_tokens(now).await?;
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT id, name, token_hash, created_at_unix, expires_at_unix,
                        max_uses, uses, last_used_at_unix, capabilities_json
                 FROM deploy_tokens ORDER BY created_at_unix, id",
                (),
            )
            .await
            .map_err(control_error)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(control_error)? {
            out.push(token_from_row(&row)?);
        }
        Ok(out)
    }

    pub async fn get_token(&self, id: &str, now: u64) -> Result<ControlDeployToken> {
        self.delete_expired_tokens(now).await?;
        self.get_token_where("id", id).await
    }

    pub async fn get_token_by_hash(&self, hash: &str) -> Result<Option<ControlDeployToken>> {
        match self.get_token_where("token_hash", hash).await {
            Ok(token) => Ok(Some(token)),
            Err(error) if error.kind() == common::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn get_token_where(&self, field: &str, value: &str) -> Result<ControlDeployToken> {
        let conn = self.connect().await?;
        let sql = match field {
            "id" => {
                "SELECT id, name, token_hash, created_at_unix, expires_at_unix,
                        max_uses, uses, last_used_at_unix, capabilities_json
                 FROM deploy_tokens WHERE id = ?1"
            }
            "token_hash" => {
                "SELECT id, name, token_hash, created_at_unix, expires_at_unix,
                        max_uses, uses, last_used_at_unix, capabilities_json
                 FROM deploy_tokens WHERE token_hash = ?1"
            }
            _ => unreachable!("fixed token selector"),
        };
        let mut rows = conn.query(sql, (value,)).await.map_err(control_error)?;
        rows.next()
            .await
            .map_err(control_error)?
            .map(|row| token_from_row(&row))
            .transpose()?
            .ok_or_else(|| PlatformError::not_found("token not found"))
    }

    pub async fn consume_token(&self, id: &str, now: u64) -> Result<bool> {
        let conn = self.connect().await?;
        let changed = retry_execute(|| {
            let conn = &conn;
            async move {
                conn.execute(
                    "UPDATE deploy_tokens
                     SET uses = uses + 1, last_used_at_unix = ?2
                     WHERE id = ?1
                       AND (expires_at_unix IS NULL OR expires_at_unix > ?2)
                       AND (max_uses IS NULL OR uses < max_uses)",
                    (id, u64_to_i64(now)?),
                )
                .await
            }
        })
        .await?;
        Ok(changed == 1)
    }

    pub async fn delete_token(&self, id: &str, now: u64) -> Result<bool> {
        self.delete_expired_tokens(now).await?;
        let conn = self.connect().await?;
        let changed = retry_execute(|| {
            let conn = &conn;
            async move {
                conn.execute("DELETE FROM deploy_tokens WHERE id = ?1", (id,))
                    .await
            }
        })
        .await?;
        Ok(changed > 0)
    }

    async fn delete_expired_tokens(&self, now: u64) -> Result<()> {
        let conn = self.connect().await?;
        retry_execute(|| {
            let conn = &conn;
            async move {
                conn.execute(
                    "DELETE FROM deploy_tokens WHERE expires_at_unix IS NOT NULL AND expires_at_unix <= ?1",
                    (u64_to_i64(now)?,),
                )
                .await
            }
        })
        .await?;
        Ok(())
    }

    pub async fn checkpoint(&self) -> Result<()> {
        checkpoint_database(&self.database)
            .await
            .map_err(control_error)
    }

    pub async fn health_check(&self) -> Result<()> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query(
                "SELECT COALESCE(MAX(version), 0) FROM control_migrations",
                (),
            )
            .await
            .map_err(control_error)?;
        let version = rows
            .next()
            .await
            .map_err(control_error)?
            .ok_or_else(|| PlatformError::internal("control migration state is unavailable"))?
            .get::<i64>(0)
            .map_err(control_error)?;
        if version != CONTROL_SCHEMA_VERSION {
            return Err(PlatformError::internal(format!(
                "control schema version mismatch: expected {CONTROL_SCHEMA_VERSION}, found {version}"
            )));
        }
        Ok(())
    }

    async fn migration_completed(&self, name: &str) -> Result<bool> {
        let conn = self.connect().await?;
        let mut rows = conn
            .query("SELECT 1 FROM migration_state WHERE name = ?1", (name,))
            .await
            .map_err(control_error)?;
        Ok(rows.next().await.map_err(control_error)?.is_some())
    }

    async fn record_migration(&self, name: &str, details: &str) -> Result<()> {
        let conn = self.connect().await?;
        retry_execute(|| {
            let conn = &conn;
            async move {
                conn.execute(
                    "INSERT INTO migration_state(name, completed_at_ms, details)
                     VALUES (?1, ?2, ?3) ON CONFLICT(name) DO NOTHING",
                    (name, epoch_ms_i64().map_err(to_turso_error)?, details),
                )
                .await
            }
        })
        .await?;
        Ok(())
    }

    async fn with_write_transaction<T>(
        &self,
        mut operation: impl for<'a> FnMut(
            &'a Connection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = turso::Result<T>> + Send + 'a>,
        >,
    ) -> Result<T> {
        let mut conn = self.connect().await?;
        for attempt in 0..MAX_BUSY_RETRY_ATTEMPTS {
            let tx = match conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
            {
                Ok(tx) => tx,
                Err(error)
                    if is_retryable_turso_error(&error)
                        && attempt + 1 < MAX_BUSY_RETRY_ATTEMPTS =>
                {
                    retry_sleep(attempt).await;
                    continue;
                }
                Err(error) => return Err(control_error_after_retry(error)),
            };
            match operation(&tx).await {
                Ok(value) => match tx.commit().await {
                    Ok(()) => return Ok(value),
                    Err(error)
                        if is_retryable_turso_error(&error)
                            && attempt + 1 < MAX_BUSY_RETRY_ATTEMPTS =>
                    {
                        retry_sleep(attempt).await;
                    }
                    Err(error) => return Err(control_error_after_retry(error)),
                },
                Err(error)
                    if is_retryable_turso_error(&error)
                        && attempt + 1 < MAX_BUSY_RETRY_ATTEMPTS =>
                {
                    let _ = tx.rollback().await;
                    retry_sleep(attempt).await;
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(control_error_after_retry(error));
                }
            }
        }
        Err(PlatformError::storage_unavailable(
            "control database remained busy",
        ))
    }
}

async fn insert_deployment_tx(
    conn: &Connection,
    deployment: &ControlDeployment,
) -> turso::Result<()> {
    let mut rows = conn
        .query(
            "SELECT COALESCE(MAX(created_at_ms), -1)
             FROM deployments WHERE worker_name = ?1",
            (deployment.worker.as_str(),),
        )
        .await?;
    let persisted_max = rows
        .next()
        .await?
        .ok_or_else(|| turso::Error::Error("deployment timestamp query returned no row".into()))?
        .get::<i64>(0)?;
    let created_at_ms = deployment
        .created_at_ms
        .max(persisted_max.saturating_add(1));
    conn.execute(
        "INSERT INTO deployments(
           deployment_id, worker_name, source, config_json, assets_json,
           server_modules_json, asset_headers, created_at_ms, expires_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(deployment_id) DO NOTHING",
        (
            deployment.deployment_id.as_str(),
            deployment.worker.as_str(),
            deployment.source.as_str(),
            json_string(&deployment.config)?,
            json_string(&deployment.assets)?,
            json_string(&deployment.server_modules)?,
            deployment.asset_headers.as_deref(),
            created_at_ms,
            deployment.expires_at_ms,
        ),
    )
    .await?;
    Ok(())
}

async fn prune_deployments_tx(conn: &Connection, worker: &str) -> turso::Result<()> {
    conn.execute(
        "DELETE FROM deployments
         WHERE worker_name = ?1
           AND deployment_id NOT IN (
             SELECT deployment_id FROM deployments
             WHERE worker_name = ?1
             ORDER BY created_at_ms DESC, deployment_id DESC
             LIMIT ?2
           )",
        (worker, MAX_DEPLOYMENTS_PER_WORKER),
    )
    .await?;
    Ok(())
}

async fn insert_token_tx(conn: &Connection, token: &ControlDeployToken) -> turso::Result<u64> {
    conn.execute(
        "INSERT INTO deploy_tokens(
           id, name, token_hash, created_at_unix, expires_at_unix,
           max_uses, uses, last_used_at_unix, capabilities_json
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(id) DO NOTHING",
        (
            token.id.as_str(),
            token.name.as_deref(),
            token.token_hash.as_str(),
            u64_to_i64(token.created_at_unix)?,
            option_u64_to_i64(token.expires_at_unix)?,
            option_u64_to_i64(token.max_uses)?,
            u64_to_i64(token.uses)?,
            option_u64_to_i64(token.last_used_at_unix)?,
            json_string(&token.capabilities)?,
        ),
    )
    .await
}

fn deployment_from_row(row: &Row) -> Result<ControlDeployment> {
    let config_json = row.get::<String>(3).map_err(control_error)?;
    let assets_json = row.get::<String>(4).map_err(control_error)?;
    let modules_json = row.get::<String>(5).map_err(control_error)?;
    Ok(ControlDeployment {
        deployment_id: row.get::<String>(0).map_err(control_error)?,
        worker: row.get::<String>(1).map_err(control_error)?,
        source: row.get::<String>(2).map_err(control_error)?,
        config: serde_json::from_str(&config_json).map_err(json_control_error)?,
        assets: serde_json::from_str(&assets_json).map_err(json_control_error)?,
        server_modules: serde_json::from_str(&modules_json).map_err(json_control_error)?,
        asset_headers: row.get::<Option<String>>(6).map_err(control_error)?,
        created_at_ms: row.get::<i64>(7).map_err(control_error)?,
        expires_at_ms: row.get::<Option<i64>>(8).map_err(control_error)?,
        active: row.get::<i64>(9).map_err(control_error)? != 0,
    })
}

fn token_from_row(row: &Row) -> Result<ControlDeployToken> {
    let capabilities_json = row.get::<String>(8).map_err(control_error)?;
    Ok(ControlDeployToken {
        id: row.get::<String>(0).map_err(control_error)?,
        name: row.get::<Option<String>>(1).map_err(control_error)?,
        token_hash: row.get::<String>(2).map_err(control_error)?,
        created_at_unix: i64_to_u64(row.get::<i64>(3).map_err(control_error)?)?,
        expires_at_unix: option_i64_to_u64(row.get::<Option<i64>>(4).map_err(control_error)?)?,
        max_uses: option_i64_to_u64(row.get::<Option<i64>>(5).map_err(control_error)?)?,
        uses: i64_to_u64(row.get::<i64>(6).map_err(control_error)?)?,
        last_used_at_unix: option_i64_to_u64(row.get::<Option<i64>>(7).map_err(control_error)?)?,
        capabilities: serde_json::from_str(&capabilities_json).map_err(json_control_error)?,
    })
}

fn migrate_actor_bindings(value: &mut JsonValue) {
    let Some(bindings) = value
        .get_mut("config")
        .and_then(|config| config.get_mut("bindings"))
        .and_then(JsonValue::as_array_mut)
    else {
        return;
    };
    for binding in bindings {
        if binding.get("type").and_then(JsonValue::as_str) == Some("actor") {
            binding["type"] = JsonValue::String("memory".to_string());
        }
    }
}

async fn retry_execute<F, Fut>(mut operation: F) -> Result<u64>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = turso::Result<u64>>,
{
    for attempt in 0..MAX_BUSY_RETRY_ATTEMPTS {
        match operation().await {
            Ok(changed) => return Ok(changed),
            Err(error)
                if is_retryable_turso_error(&error) && attempt + 1 < MAX_BUSY_RETRY_ATTEMPTS =>
            {
                retry_sleep(attempt).await;
            }
            Err(error) => return Err(control_error_after_retry(error)),
        }
    }
    Err(PlatformError::storage_unavailable(
        "control database remained busy",
    ))
}

async fn retry_sleep(attempt: usize) {
    crate::turso_util::record_storage_retry();
    tokio::time::sleep(Duration::from_millis(5 * (attempt + 1) as u64)).await;
}

fn json_string(value: &impl Serialize) -> turso::Result<String> {
    serde_json::to_string(value).map_err(to_turso_error)
}

fn u64_to_i64(value: u64) -> turso::Result<i64> {
    i64::try_from(value).map_err(to_turso_error)
}

fn option_u64_to_i64(value: Option<u64>) -> turso::Result<Option<i64>> {
    value.map(u64_to_i64).transpose()
}

fn i64_to_u64(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| PlatformError::internal("negative control database counter"))
}

fn option_i64_to_u64(value: Option<i64>) -> Result<Option<u64>> {
    value.map(i64_to_u64).transpose()
}

fn to_turso_error(error: impl std::fmt::Display) -> turso::Error {
    turso::Error::Error(error.to_string())
}

fn control_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("control database error: {error}"))
}

fn control_error_after_retry(error: turso::Error) -> PlatformError {
    if is_retryable_turso_error(&error) {
        PlatformError::storage_unavailable(format!("control database error: {error}"))
    } else {
        control_error(error)
    }
}

fn json_control_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::internal(format!("invalid control database payload: {error}"))
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| PlatformError::internal("system clock overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{DeployBinding, DeployCacheConfig, DeployInternalConfig};
    use uuid::Uuid;

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-control-{label}-{}", Uuid::new_v4()))
    }

    fn deployment(worker: &str, index: usize) -> ControlDeployment {
        ControlDeployment {
            worker: worker.to_string(),
            deployment_id: format!("deployment-{index}"),
            source: format!("export default {{ value: {index} }}"),
            config: DeployConfig::default(),
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            created_at_ms: index as i64,
            expires_at_ms: None,
            active: true,
        }
    }

    #[tokio::test]
    async fn deployment_history_retains_five_and_rolls_back_pointer() -> Result<()> {
        let root = temp_dir("history");
        let store = ControlStore::open(&root).await?;
        for index in 0..7 {
            store
                .insert_deployment(&deployment("worker", index))
                .await?;
        }
        let list = store.list_deployments(Some("worker")).await?;
        assert_eq!(list.len(), 5);
        assert_eq!(list[0].deployment_id, "deployment-6");
        assert!(list[0].active);
        store.activate_deployment("worker", "deployment-3").await?;
        let list = store.list_deployments(Some("worker")).await?;
        assert!(
            list.iter()
                .find(|item| item.deployment_id == "deployment-3")
                .is_some_and(|item| item.active)
        );
        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }

    #[tokio::test]
    async fn same_millisecond_deployments_keep_the_new_active_record() -> Result<()> {
        let root = temp_dir("same-millisecond");
        let store = ControlStore::open(&root).await?;
        for index in 0..7 {
            let mut deployment = deployment("worker", index);
            deployment.created_at_ms = 1;
            store.insert_deployment(&deployment).await?;
        }
        let list = store.list_deployments(Some("worker")).await?;
        assert_eq!(list.len(), 5);
        assert_eq!(list[0].deployment_id, "deployment-6");
        assert!(list[0].active);
        assert!(
            list.windows(2)
                .all(|pair| { pair[0].created_at_ms > pair[1].created_at_ms })
        );
        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }

    #[tokio::test]
    async fn imports_actor_binding_as_memory() -> Result<()> {
        let root = temp_dir("legacy");
        let workers = root.join("workers");
        tokio::fs::create_dir_all(&workers)
            .await
            .map_err(control_error)?;
        let legacy = serde_json::json!({
            "name": "legacy",
            "source": "export default {}",
            "config": {
                "bindings": [{"type": "actor", "binding": "ROOMS"}],
                "public": false,
                "cache": DeployCacheConfig::default(),
                "internal": DeployInternalConfig::default()
            },
            "assets": [],
            "server_modules": [],
            "asset_headers": null,
            "deployment_id": "legacy-id",
            "updated_at_ms": 1,
            "expires_at_ms": null
        });
        tokio::fs::write(
            workers.join("legacy.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .await
        .map_err(control_error)?;
        let store = ControlStore::open(&root).await?;
        assert_eq!(store.import_legacy_workers(&workers).await?, 1);
        let restored = store.get_deployment("legacy-id").await?;
        assert!(matches!(
            restored.config.bindings.as_slice(),
            [DeployBinding::Memory { binding }] if binding == "ROOMS"
        ));
        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_future_schema_and_unknown_legacy_artifacts() -> Result<()> {
        let root = temp_dir("future-schema");
        let store = ControlStore::open(&root).await?;
        let conn = store.connect().await?;
        conn.execute("DELETE FROM control_migrations", ())
            .await
            .map_err(control_error)?;
        conn.execute(
            "INSERT INTO control_migrations(version, applied_at_ms) VALUES (99, 1)",
            (),
        )
        .await
        .map_err(control_error)?;
        drop(conn);
        drop(store);
        let error = match ControlStore::open(&root).await {
            Ok(_) => panic!("future control schema should fail startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("future control database schema"));
        let _ = tokio::fs::remove_dir_all(&root).await;

        let root = temp_dir("unknown-artifact");
        let workers = root.join("workers");
        tokio::fs::create_dir_all(&workers)
            .await
            .map_err(control_error)?;
        tokio::fs::write(workers.join("worker.bin"), b"not a known store format")
            .await
            .map_err(control_error)?;
        let store = ControlStore::open(&root).await?;
        let error = store
            .import_legacy_workers(&workers)
            .await
            .expect_err("unknown worker artifact should fail startup");
        assert!(
            error
                .to_string()
                .contains("unrecognized persisted worker artifact")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_v1_migration_with_malformed_control_table() -> Result<()> {
        let root = temp_dir("malformed-v1-schema");
        let store = ControlStore::open(&root).await?;
        let conn = store.connect().await?;
        conn.execute("DELETE FROM control_migrations", ())
            .await
            .map_err(control_error)?;
        conn.execute(
            "INSERT INTO control_migrations(version, applied_at_ms) VALUES (1, 1)",
            (),
        )
        .await
        .map_err(control_error)?;
        conn.execute("DROP TABLE deploy_tokens", ())
            .await
            .map_err(control_error)?;
        conn.execute(
            "CREATE TABLE deploy_tokens (id TEXT PRIMARY KEY, unexpected TEXT NOT NULL)",
            (),
        )
        .await
        .map_err(control_error)?;
        drop(conn);
        drop(store);

        let error = match ControlStore::open(&root).await {
            Ok(_) => panic!("malformed v1 control schema should fail startup"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("unrecognized control database schema: table deploy_tokens")
        );

        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }

    #[tokio::test]
    async fn control_connections_use_full_durability() -> Result<()> {
        let root = temp_dir("durability");
        let store = ControlStore::open(&root).await?;
        let conn = store.connect().await?;
        let mut rows = conn
            .query("PRAGMA synchronous", ())
            .await
            .map_err(control_error)?;
        let value = rows
            .next()
            .await
            .map_err(control_error)?
            .expect("synchronous row")
            .get::<i64>(0)
            .map_err(control_error)?;
        assert_eq!(value, 2);
        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn active_restore_failure_fails_startup_and_records_diagnostic() -> Result<()> {
        let root = temp_dir("restore-failure");
        let control = ControlStore::open(&root).await?;
        let mut invalid = deployment("broken", 1);
        invalid.source = "export default { this is not valid JavaScript".to_string();
        control.insert_deployment(&invalid).await?;
        drop(control);

        let service =
            crate::RuntimeService::start_with_service_config(crate::RuntimeServiceConfig {
                runtime: crate::RuntimeConfig::default(),
                storage: crate::RuntimeStorageConfig {
                    store_dir: root.clone(),
                    database_url: format!("file:{}/dd-test.db", root.display()),
                    worker_store_enabled: true,
                    ..crate::RuntimeStorageConfig::default()
                },
            })
            .await;
        let error = match service {
            Ok(service) => {
                let _ = service.shutdown().await;
                panic!("invalid active deployment should fail startup")
            }
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("failed to restore worker broken")
        );

        let control = ControlStore::open(&root).await?;
        let failures = control.restore_failures().await?;
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].worker, "broken");
        let _ = tokio::fs::remove_dir_all(root).await;
        Ok(())
    }
}
