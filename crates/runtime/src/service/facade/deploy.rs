//! Deployment publication, activation and admission.

use super::*;

impl RuntimeService {
    pub fn control_store(&self) -> ControlStore {
        self.control_store.clone()
    }

    pub async fn deployments(
        &self,
        worker: Option<&str>,
    ) -> Result<Vec<common::DeploymentSummary>> {
        self.control_store.list_deployments(worker).await
    }

    pub async fn deployment(&self, deployment_id: &str) -> Result<common::DeploymentDetails> {
        Ok(self
            .control_store
            .get_deployment(deployment_id)
            .await?
            .details())
    }

    pub async fn undeploy(&self, worker_name: String) -> Result<()> {
        let admission = self.admit_deployment()?;
        let service = self.clone();
        tokio::spawn(async move {
            let _admission = admission;
            let _order = service
                .deployment_phase(async { Ok(service.deployment_order.lock().await) })
                .await?;
            if service.storage.worker_store_enabled
                && !service
                    .control_store
                    .deactivate_worker(&worker_name)
                    .await?
            {
                return Err(PlatformError::not_found("worker not found"));
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            service
                .sender
                .send(RuntimeCommand::Undeploy {
                    worker_name,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
            reply_rx
                .await
                .map_err(|_| PlatformError::internal("runtime undeploy channel closed"))?
        })
        .await
        .map_err(|error| PlatformError::internal(format!("undeploy task failed: {error}")))?
    }

    pub async fn rollback(&self, worker_name: String, deployment_id: String) -> Result<String> {
        let stored = self.control_store.get_deployment(&deployment_id).await?;
        if stored.worker != worker_name {
            return Err(PlatformError::bad_request(
                "deployment does not belong to requested worker",
            ));
        }
        let now_ms = epoch_ms_i64()?;
        if stored
            .expires_at_ms
            .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
        {
            return Err(PlatformError::conflict(
                "expired temporary deployment cannot be rolled back",
            ));
        }
        self.deploy_with_config_internal(DeployWithConfigRequest {
            worker_name: stored.worker.clone(),
            source: stored.source,
            config: stored.config,
            assets: stored.assets,
            server_modules: stored.server_modules,
            asset_headers: stored.asset_headers,
            deployment_id: Some(stored.deployment_id.clone()),
            persistence: DeploymentPersistence::Activate,
            temporary: stored.expires_at_ms.is_some(),
            expires_at_ms: stored.expires_at_ms,
            enforce_temporary_transition: false,
        })
        .await
    }

    pub async fn deploy(&self, worker_name: String, source: String) -> Result<String> {
        self.deploy_with_bundle_config(
            worker_name,
            source,
            DeployConfig::default(),
            Vec::new(),
            None,
        )
        .await
    }

    pub async fn deploy_with_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
    ) -> Result<String> {
        self.deploy_with_bundle_config(worker_name, source, config, Vec::new(), None)
            .await
    }

    pub async fn deploy_with_bundle_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
    ) -> Result<String> {
        self.deploy_with_bundle_config_lifecycle(
            worker_name,
            source,
            config,
            assets,
            asset_headers,
            false,
        )
        .await
    }

    pub async fn deploy_temporary_with_bundle_config(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
    ) -> Result<String> {
        self.deploy_with_bundle_config_lifecycle(
            worker_name,
            source,
            config,
            assets,
            asset_headers,
            true,
        )
        .await
    }

    async fn deploy_with_bundle_config_lifecycle(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        asset_headers: Option<String>,
        temporary: bool,
    ) -> Result<String> {
        self.deploy_with_bundle_config_lifecycle_and_server_modules(
            worker_name,
            source,
            config,
            assets,
            Vec::new(),
            asset_headers,
            temporary,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn deploy_with_bundle_config_lifecycle_and_server_modules(
        &self,
        worker_name: String,
        source: String,
        config: DeployConfig,
        assets: Vec<DeployAsset>,
        server_modules: Vec<DeployServerModule>,
        asset_headers: Option<String>,
        temporary: bool,
    ) -> Result<String> {
        self.deploy_with_config_internal(DeployWithConfigRequest {
            worker_name,
            source,
            config,
            assets,
            server_modules,
            asset_headers,
            deployment_id: None,
            persistence: DeploymentPersistence::Insert,
            temporary,
            expires_at_ms: None,
            enforce_temporary_transition: true,
        })
        .await
    }

    pub(super) async fn deploy_with_config_internal(
        &self,
        request: DeployWithConfigRequest,
    ) -> Result<String> {
        let admission = self.admit_deployment()?;
        let service = self.clone();
        // Once accepted, finish persistence and publication even if the client disconnects.
        tokio::spawn(async move {
            let _admission = admission;
            let _order = service
                .deployment_phase(async { Ok(service.deployment_order.lock().await) })
                .await?;
            let DeployWithConfigRequest {
                worker_name,
                source,
                config,
                assets,
                server_modules,
                asset_headers,
                deployment_id,
                persistence,
                temporary,
                expires_at_ms,
                enforce_temporary_transition,
            } = request;
            let prepared = prepare_worker_deployment(
                worker_name,
                source,
                config,
                assets,
                server_modules,
                asset_headers,
            )?;
            if enforce_temporary_transition {
                let (reply, receive) = oneshot::channel();
                service
                    .sender
                    .send(RuntimeCommand::CheckDeployment {
                        worker_name: prepared.worker_name.clone(),
                        temporary,
                        reply,
                    })
                    .await
                    .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
                receive
                    .await
                    .map_err(|_| PlatformError::internal("deployment check channel closed"))??;
            }
            let modules = crate::module_registry::ModuleRegistry::default();
            let source = crate::service::lifecycle::deployed_worker_source(
                &modules,
                &prepared.source,
                &prepared.server_modules,
            )?;
            service
                .deployment_phase(service.deployment_validator.validate(source, modules))
                .await?;
            let expires_at_ms = if temporary {
                let ttl_ms = i64::try_from(service.temporary_worker_ttl.as_millis())
                    .map_err(|_| PlatformError::internal("temporary worker ttl is too large"))?;
                Some(match expires_at_ms {
                    Some(expires) => expires,
                    None => epoch_ms_i64()?.checked_add(ttl_ms).ok_or_else(|| {
                        PlatformError::internal("temporary worker expiration overflow")
                    })?,
                })
            } else {
                None
            };
            let deployment_id = deployment_id.unwrap_or_else(|| Uuid::new_v4().to_string());
            if matches!(persistence, DeploymentPersistence::Insert)
                && service.storage.worker_store_enabled
            {
                service
                    .control_store
                    .insert_deployment(&ControlDeployment {
                        worker: prepared.worker_name.clone(),
                        deployment_id: deployment_id.clone(),
                        source: prepared.source.clone(),
                        config: prepared.config.clone(),
                        assets: prepared.assets.clone(),
                        server_modules: prepared.server_modules.clone(),
                        asset_headers: prepared.asset_headers.clone(),
                        created_at_ms: epoch_ms_i64()?,
                        expires_at_ms,
                        active: true,
                    })
                    .await?;
            }
            if matches!(persistence, DeploymentPersistence::Activate) {
                service
                    .control_store
                    .activate_deployment(&prepared.worker_name, &deployment_id)
                    .await?;
            }
            let (reply, receive) = oneshot::channel();
            service
                .sender
                .send(RuntimeCommand::Deploy {
                    prepared,
                    deployment_id,
                    expires_at_ms,
                    reply,
                })
                .await
                .map_err(|_| PlatformError::internal("runtime thread is not available"))?;
            receive
                .await
                .map_err(|_| PlatformError::internal("runtime deploy channel closed"))?
        })
        .await
        .map_err(|error| PlatformError::internal(format!("deployment task failed: {error}")))?
    }

    #[cfg(test)]
    pub(in crate::service) fn deployment_validation_active(&self) -> bool {
        self.deployment_validator.slots.available_permits() == 0
    }

    fn admit_deployment(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.deployment_admission)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => {
                    PlatformError::overloaded("runtime is shutting down")
                }
                tokio::sync::TryAcquireError::NoPermits => {
                    PlatformError::overloaded("deployment queue is full (64 requests)")
                }
            })
    }

    async fn deployment_phase<T>(
        &self,
        phase: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let mut stopping = self.deployment_stopping.subscribe();
        tokio::select! {
            biased;
            _ = async {
                while !*stopping.borrow_and_update() {
                    if stopping.changed().await.is_err() {
                        break;
                    }
                }
            } => Err(PlatformError::overloaded("runtime is shutting down")),
            result = phase => result,
        }
    }
}
