use super::*;

pub(super) async fn persist_worker_deployment(
    storage: &RuntimeStorageConfig,
    worker_name: &str,
    source: &str,
    config: &DeployConfig,
    assets: &[DeployAsset],
    asset_headers: Option<&str>,
    deployment_id: &str,
) -> Result<()> {
    if !storage.worker_store_enabled {
        return Ok(());
    }

    let workers_dir = storage.store_dir.join("workers");
    tokio::fs::create_dir_all(&workers_dir)
        .await
        .map_err(|error| {
            PlatformError::internal(format!(
                "failed to create worker store directory {}: {error}",
                workers_dir.display()
            ))
        })?;
    let final_path = workers_dir.join(format!("{}.json", encoded_worker_name(worker_name)));
    let temp_path = workers_dir.join(format!("{}.tmp", encoded_worker_name(worker_name)));
    let payload = StoredWorkerDeployment {
        name: worker_name.to_string(),
        source: source.to_string(),
        config: config.clone(),
        assets: assets.to_vec(),
        asset_headers: asset_headers.map(str::to_string),
        deployment_id: deployment_id.to_string(),
        updated_at_ms: epoch_ms_i64()?,
    };
    let body = crate::json::to_vec(&payload).map_err(|error| {
        PlatformError::internal(format!("failed to serialize worker deployment: {error}"))
    })?;
    tokio::fs::write(&temp_path, body).await.map_err(|error| {
        PlatformError::internal(format!(
            "failed to write worker store file {}: {error}",
            temp_path.display()
        ))
    })?;
    tokio::fs::rename(&temp_path, &final_path)
        .await
        .map_err(|error| {
            PlatformError::internal(format!(
                "failed to commit worker store file {}: {error}",
                final_path.display()
            ))
        })?;
    Ok(())
}

pub(super) fn encoded_worker_name(worker_name: &str) -> String {
    let mut out = String::with_capacity(worker_name.len().saturating_mul(2).max(2));
    for byte in worker_name.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    if out.is_empty() {
        "00".to_string()
    } else {
        out
    }
}

pub(super) fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}
