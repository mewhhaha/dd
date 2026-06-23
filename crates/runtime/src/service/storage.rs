use super::*;

pub(super) struct PersistWorkerDeployment<'a> {
    pub(super) storage: &'a RuntimeStorageConfig,
    pub(super) worker_name: &'a str,
    pub(super) source: &'a str,
    pub(super) config: &'a DeployConfig,
    pub(super) assets: &'a [DeployAsset],
    pub(super) asset_headers: Option<&'a str>,
    pub(super) deployment_id: &'a str,
    pub(super) expires_at_ms: Option<i64>,
}

pub(super) async fn persist_worker_deployment(request: PersistWorkerDeployment<'_>) -> Result<()> {
    let PersistWorkerDeployment {
        storage,
        worker_name,
        source,
        config,
        assets,
        asset_headers,
        deployment_id,
        expires_at_ms,
    } = request;
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
        expires_at_ms,
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

pub(super) async fn delete_worker_deployment(
    storage: &RuntimeStorageConfig,
    worker_name: &str,
) -> Result<()> {
    if !storage.worker_store_enabled {
        return Ok(());
    }

    let workers_dir = storage.store_dir.join("workers");
    let mut read_dir = match tokio::fs::read_dir(&workers_dir).await {
        Ok(read_dir) => read_dir,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(PlatformError::internal(format!(
                "failed to read worker store {}: {error}",
                workers_dir.display()
            )))
        }
    };

    let encoded_name = encoded_worker_name(worker_name);
    let exact_name = format!("{encoded_name}.json");
    let timestamped_prefix = format!("{encoded_name}.");
    while let Some(entry) = read_dir.next_entry().await.map_err(|error| {
        PlatformError::internal(format!(
            "failed to read worker store entry in {}: {error}",
            workers_dir.display()
        ))
    })? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if file_name != exact_name && !file_name.starts_with(&timestamped_prefix) {
            continue;
        }

        match tokio::fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(PlatformError::internal(format!(
                    "failed to remove worker store file {}: {error}",
                    path.display()
                )));
            }
        }
    }

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
