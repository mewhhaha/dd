//! Worker deployment and source registration.

use super::*;

impl WorkerManager {
    pub(crate) fn deploy(
        &mut self,
        prepared: PreparedWorkerDeployment,
        deployment_id: String,
        expires_at_ms: Option<i64>,
    ) -> Result<String> {
        let PreparedWorkerDeployment {
            worker_name,
            source,
            config,
            server_modules,
            compiled_assets,
            bindings,
            ..
        } = prepared;
        let worker_source =
            deployed_worker_source(&self.module_registry, &source, &server_modules)?;
        let (snapshot, snapshot_preloaded) = (self.bootstrap_snapshot, false);
        let generation = self.next_generation;
        self.next_generation += 1;
        let service_bindings = bindings
            .service
            .iter()
            .map(|binding| crate::ops::WorkerServiceBindingPayload {
                binding: binding.binding.clone(),
                service: binding.service.clone(),
            })
            .collect::<Vec<_>>();
        let deployment_config = Arc::new(crate::ops::WorkerDeploymentPayload {
            kv_bindings: bindings.kv.clone(),
            memory_bindings: bindings.memory.clone(),
            service_bindings: service_bindings.clone(),
        });
        let request_context = RequestExecutionContext::new(RequestExecutionContextInit {
            worker_name: worker_name.clone(),
            generation,
            service_bindings,
            egress_allow_hosts: config.egress_allow_hosts.clone(),
        });
        let asset_catalog_entry = AssetCatalogEntry {
            worker_name: worker_name.clone(),
            deployment_id: deployment_id.clone(),
            generation,
            assets: compiled_assets.clone(),
            public: config.public,
            cache_enabled: config.cache.enabled,
        };
        let pool =
            WorkerPool {
                worker_name: worker_name.clone(),
                generation,
                deployment_id: deployment_id.clone(),
                internal_trace: config.internal.trace.as_ref().map(|trace| {
                    InternalTraceDestination {
                        worker: trace.worker.trim().to_string(),
                        path: normalize_trace_path(&trace.path),
                    }
                }),
                is_public: config.public,
                expires_at_ms,
                retired_at: None,
                snapshot,
                snapshot_preloaded,
                source: worker_source,
                memory_bindings: bindings.memory,
                deployment_config,
                request_context,
                strict_request_isolation: false,
                memory_entity_leases: HashMap::new(),
                memory_shard_affinity: HashMap::new(),
                queue: PendingInvokeQueue::new(),
                isolates: Vec::new(),
                isolate_indices: HashMap::new(),
                stats: PoolStats::default(),
                queue_warn_level: 0,
            };

        let entry = self
            .workers
            .entry(worker_name.clone())
            .or_insert_with(|| WorkerEntry {
                current_generation: generation,
                pools: HashMap::new(),
            });
        if let Some(previous) = entry.pools.get_mut(&entry.current_generation) {
            previous.retired_at = Some(Instant::now());
        }
        entry.current_generation = generation;
        entry.pools.insert(generation, pool);
        self.asset_catalog
            .insert(worker_name.clone(), asset_catalog_entry);
        let retiring_sessions = self
            .websocket_sessions
            .iter()
            .filter(|(_, session)| {
                session.worker_name == worker_name && session.generation != generation
            })
            .map(|(session_id, _)| session_id.clone())
            .collect::<Vec<_>>();
        for session_id in retiring_sessions {
            self.websocket_close_signals.insert(
                session_id.clone(),
                SocketCloseEvent {
                    code: 1012,
                    reason: "worker redeployed".to_string(),
                },
            );
            self.flush_pending_websocket_frame_replies(&session_id);
            self.notify_websocket_frame_waiters(&session_id);
        }
        self.cleanup_drained_generations_for(&worker_name);
        info!(
            worker = %worker_name,
            generation,
            deployment_id = %deployment_id,
            temporary = expires_at_ms.is_some(),
            expires_at_ms,
            "deployed worker"
        );
        Ok(deployment_id)
    }

    pub(in crate::service) fn current_worker_is_permanent(&self, worker_name: &str) -> bool {
        self.workers
            .get(worker_name)
            .and_then(|entry| entry.pools.get(&entry.current_generation))
            .is_some_and(|pool| pool.expires_at_ms.is_none())
    }

    pub(crate) fn expire_temporary_workers(&mut self) {
        let now_ms = match epoch_ms_i64() {
            Ok(now_ms) => now_ms,
            Err(error) => {
                warn!(error = %error, "failed to read clock while expiring temporary workers");
                return;
            }
        };
        let expired = self
            .workers
            .iter()
            .filter_map(|(worker_name, entry)| {
                let pool = entry.pools.get(&entry.current_generation)?;
                let expires_at_ms = pool.expires_at_ms?;
                (expires_at_ms <= now_ms).then(|| (worker_name.clone(), pool.deployment_id.clone()))
            })
            .collect::<Vec<_>>();

        for (worker_name, deployment_id) in expired {
            self.retire_worker_completely_with_error(
                &worker_name,
                PlatformError::not_found("temporary worker expired"),
            );
            info!(worker = %worker_name, "expired temporary worker");
            let control_store = self.control_store.clone();
            tokio::spawn(async move {
                if let Err(error) = control_store
                    .deactivate_deployment(&worker_name, &deployment_id)
                    .await
                {
                    warn!(worker = %worker_name, deployment_id, error = %error,
                        "failed to remove expired deployment from local store");
                }
            });
        }
    }
}

pub(in crate::service) fn deployed_worker_source(
    module_registry: &crate::module_registry::ModuleRegistry,
    source: &str,
    server_modules: &[DeployServerModule],
) -> Result<crate::ops::WorkerSource> {
    if server_modules.is_empty() {
        return Ok(crate::ops::WorkerSource::inline(source.to_string()));
    }
    let (graph_id, entrypoint) = module_registry.register_server_module_graph(
        "worker.js",
        source.to_string(),
        server_modules.to_vec(),
    )?;
    Ok(crate::ops::WorkerSource::Module {
        graph_id,
        entrypoint,
    })
}

pub(super) fn release_worker_source_modules(
    module_registry: &crate::module_registry::ModuleRegistry,
    source: &crate::ops::WorkerSource,
) {
    if let crate::ops::WorkerSource::Module { graph_id, .. } = source {
        module_registry.release(graph_id);
    }
}
