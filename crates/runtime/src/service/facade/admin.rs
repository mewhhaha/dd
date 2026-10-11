//! Runtime administration, asset resolution and cache access.

use super::*;
use futures_util::future::join_all;

impl RuntimeService {
    /// Every worker `console` call from now on. Output is also logged as
    /// `dd::worker` tracing events; a receiver that falls more than 1024
    /// lines behind skips the oldest.
    /// The worker isolates a debugger can attach to now; always empty unless
    /// the service runs with an [`InspectorMode`] other than `Off`.
    pub fn inspector_targets(&self) -> Vec<InspectorTarget> {
        self.inspector
            .as_ref()
            .map(InspectorRegistry::targets)
            .unwrap_or_default()
    }

    pub fn subscribe_console(&self) -> tokio::sync::broadcast::Receiver<crate::WorkerConsoleLine> {
        self.console.0.subscribe()
    }

    pub async fn stats(&self, worker_name: String) -> Option<WorkerStats> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::Stats {
                worker_name,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return None;
        }
        reply_rx.await.ok().flatten()
    }

    pub async fn admin_snapshot(&self) -> RuntimeAdminSnapshot {
        let worker_names = self.asset_catalog.worker_names();
        let active_deployments = worker_names.len();
        let workers = self.worker_statuses(worker_names).await;
        let restore_failures = match self.control_store.restore_failures().await {
            Ok(failures) => failures
                .into_iter()
                .map(|failure| RuntimeRestoreFailure {
                    worker: Some(failure.worker),
                    source: failure
                        .deployment_id
                        .map(|id| format!("control.db deployment {id}"))
                        .unwrap_or_else(|| "control.db".to_string()),
                    error: failure.error,
                })
                .collect(),
            Err(error) => vec![RuntimeRestoreFailure {
                worker: None,
                source: "control.db".to_string(),
                error: error.to_string(),
            }],
        };
        let readiness = self.readiness().await;
        let memory_cache = self.memory_store.cache_performance_snapshot();
        RuntimeAdminSnapshot {
            worker_schedulers: self.sender.worker_schedulers(),
            active_deployments,
            workers,
            restore_failures,
            readiness,
            storage_retry_count: crate::turso_util::storage_retry_count(),
            state_storage: self.state_store.performance_snapshot(),
            memory_snapshot_cache_hits: memory_cache.snapshot_hits,
            memory_snapshot_cache_misses: memory_cache.snapshot_misses,
            memory_snapshot_cache_evictions: memory_cache.snapshot_evictions,
            memory_outbox_claimed_bytes: self.memory_store.outbox_claimed_bytes(),
            memory_outbox_max_claimed_bytes: self.storage.memory_outbox_max_claimed_bytes,
        }
    }

    async fn worker_statuses(&self, worker_names: Vec<String>) -> Vec<RuntimeWorkerStatus> {
        join_all(worker_names.into_iter().map(|name| async move {
            self.stats(name.clone())
                .await
                .map(|stats| RuntimeWorkerStatus {
                    name,
                    outbox_lag_shards: stats
                        .pending_memory_outbox_shards
                        .max(stats.memory_outbox_worker_pending_shards)
                        .saturating_add(stats.memory_outbox_worker_in_flight_shards),
                    stats,
                })
        }))
        .await
        .into_iter()
        .flatten()
        .collect()
    }

    pub async fn is_quiescent(&self) -> bool {
        if self.deployment_admission.available_permits() != MAX_ACCEPTED_DEPLOYMENTS {
            return false;
        }
        let worker_names = self.asset_catalog.worker_names();
        let expected = worker_names.len();
        let workers = self.worker_statuses(worker_names).await;
        self.deployment_admission.available_permits() == MAX_ACCEPTED_DEPLOYMENTS
            && workers.len() == expected
            && self
                .memory_store
                .state_performance_snapshot()
                .pending_commands
                == 0
            && workers.iter().all(|worker| {
                let stats = &worker.stats;
                stats.queued == 0
                    && stats.busy == 0
                    && stats.inflight_total == 0
                    && stats.wait_until_total == 0
                    && stats.pending_memory_outbox_shards == 0
                    && stats.memory_outbox_worker_pending_shards == 0
                    && stats.memory_outbox_worker_in_flight_shards == 0
            })
    }

    pub async fn wait_for_quiescence(&self, timeout: Duration) -> bool {
        if self.is_quiescent().await {
            return true;
        }
        if timeout.is_zero() {
            return false;
        }
        let wait = async {
            loop {
                if self.is_quiescent().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(timeout, wait).await.is_ok()
    }

    pub async fn readiness(&self) -> RuntimeReadiness {
        let (control, state, cache, restore_failures) = tokio::join!(
            self.control_store.health_check(),
            self.state_store.health_check(),
            self.cache_store.health_check(),
            self.control_store.restore_failures(),
        );
        let mut failed_components = Vec::new();
        if control.is_err() {
            failed_components.push("control".to_string());
        }
        if state.is_err() {
            failed_components.push("state".to_string());
        }
        if cache.is_err() {
            failed_components.push("cache".to_string());
        }
        if restore_failures.is_err() {
            failed_components.push("restoration_diagnostics".to_string());
        }
        let runtime_ready = !self.sender.is_closed();
        if !runtime_ready {
            failed_components.push("runtime".to_string());
        }
        let restore_failure_count = restore_failures
            .as_ref()
            .map_or(0, |failures| failures.len());
        let storage_ready = control.is_ok() && state.is_ok() && cache.is_ok();
        // Opening stores verifies their schema. Readiness also requires all
        // physical stores to remain accessible after startup.
        let migrations_ready = storage_ready;
        let worker_restoration_ready = restore_failures.is_ok() && restore_failure_count == 0;
        RuntimeReadiness {
            ready: runtime_ready && migrations_ready && storage_ready && worker_restoration_ready,
            runtime_ready,
            migrations_ready,
            storage_ready,
            worker_restoration_ready,
            restore_failure_count,
            failed_components,
        }
    }

    pub async fn checkpoint(&self) -> Result<RuntimeCheckpointResult> {
        self.control_store.checkpoint().await?;
        self.state_store.checkpoint().await?;
        self.cache_store.checkpoint().await?;
        Ok(RuntimeCheckpointResult {
            state_shards: ::storage::state::STATE_SHARDS,
            cache: true,
            control: true,
        })
    }

    pub fn worker_is_public(&self, worker_name: &str) -> bool {
        self.asset_catalog
            .get(worker_name)
            .is_some_and(|entry| entry.worker_name == worker_name && entry.public)
    }

    pub fn front_cache_namespace(&self, worker_name: &str) -> Option<String> {
        self.front_cache_deployment(worker_name)
            .map(|deployment| deployment.namespace())
    }

    pub fn front_cache_deployment(&self, worker_name: &str) -> Option<FrontCacheDeployment> {
        let entry = self.asset_catalog.get(worker_name)?;
        entry.cache_enabled.then_some(FrontCacheDeployment(entry))
    }

    pub fn front_cache_deployment_is_current(&self, deployment: &FrontCacheDeployment) -> bool {
        self.asset_catalog
            .get(&deployment.0.worker_name)
            .is_some_and(|entry| Arc::ptr_eq(&entry, &deployment.0))
    }

    pub fn resolve_asset(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<Option<AssetResponse>> {
        self.resolve_asset_from_catalog(worker_name, method, host, path, headers, false)
    }

    pub fn resolve_public_asset(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<Option<AssetResponse>> {
        self.resolve_asset_from_catalog(worker_name, method, host, path, headers, true)
    }

    pub fn resolve_public_route_asset(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
    ) -> Result<PublicRouteAssetResolution> {
        let Some(entry) = self.asset_catalog.get(worker_name) else {
            return Ok(PublicRouteAssetResolution {
                public_worker: false,
                generation: None,
                asset: None,
            });
        };
        if entry.worker_name != worker_name {
            return Ok(PublicRouteAssetResolution {
                public_worker: false,
                generation: None,
                asset: None,
            });
        }
        if !entry.public {
            return Ok(PublicRouteAssetResolution {
                public_worker: false,
                generation: Some(entry.generation),
                asset: None,
            });
        }
        Ok(PublicRouteAssetResolution {
            public_worker: true,
            generation: Some(entry.generation),
            asset: resolve_asset(
                &entry.assets,
                AssetRequest {
                    method,
                    host,
                    path,
                    headers,
                },
            ),
        })
    }

    fn resolve_asset_from_catalog(
        &self,
        worker_name: &str,
        method: &str,
        host: Option<&str>,
        path: &str,
        headers: &[(String, String)],
        public_only: bool,
    ) -> Result<Option<AssetResponse>> {
        let Some(entry) = self.asset_catalog.get(worker_name) else {
            return Ok(None);
        };
        if entry.worker_name != worker_name {
            return Ok(None);
        }
        if public_only && !entry.public {
            return Ok(None);
        }
        Ok(resolve_asset(
            &entry.assets,
            AssetRequest {
                method,
                host,
                path,
                headers,
            },
        ))
    }

    pub async fn debug_dump(&self, worker_name: String) -> Option<WorkerDebugDump> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::DebugDump {
                worker_name,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return None;
        }
        reply_rx.await.ok().flatten()
    }

    #[cfg(test)]
    pub async fn force_fail_isolate_for_test(
        &self,
        worker_name: String,
        generation: u64,
        isolate_id: u64,
    ) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .sender
            .send(RuntimeCommand::ForceFailIsolate {
                worker_name,
                generation,
                isolate_id,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return false;
        }
        reply_rx.await.unwrap_or(false)
    }

    pub async fn cache_match(&self, request: CacheRequest) -> Result<CacheLookup> {
        let span = tracing::debug_span!(
            "runtime.cache.match",
            cache.name = %request.cache_name,
            http.method = %request.method,
            http.url = %request.url
        );
        self.cache_store.get(&request).instrument(span).await
    }

    pub async fn cache_put(&self, request: CacheRequest, response: CacheResponse) -> Result<bool> {
        let span = tracing::debug_span!(
            "runtime.cache.put",
            cache.name = %request.cache_name,
            http.method = %request.method,
            http.url = %request.url,
            response.status = response.status as u64,
            response.body_size = response.body.len() as u64
        );
        self.cache_store
            .put(&request, response)
            .instrument(span)
            .await
    }
}
