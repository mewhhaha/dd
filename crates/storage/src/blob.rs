use common::{PlatformError, Result};
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

#[derive(Clone)]
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    pub async fn for_legacy_root(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root).await.map_err(blob_error)?;
        Ok(Self { root })
    }

    pub async fn read_legacy(&self, blob_ref: &str) -> Result<Vec<u8>> {
        let id = parse_local_ref(blob_ref)?;
        fs::read(self.root.join(format!("{id}.blob")))
            .await
            .map_err(blob_error)
    }

    pub async fn delete_legacy(&self, blob_ref: &str) -> Result<()> {
        let id = parse_local_ref(blob_ref)?;
        let path = self.root.join(format!("{id}.blob"));
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(blob_error(error)),
        }
    }

    #[cfg(test)]
    pub async fn put(&self, bytes: &[u8]) -> Result<String> {
        let id = Uuid::new_v4().to_string();
        let final_path = self.root.join(format!("{id}.blob"));
        let temp_path = self.root.join(format!("{id}.tmp"));
        fs::write(&temp_path, bytes).await.map_err(blob_error)?;
        fs::rename(&temp_path, &final_path)
            .await
            .map_err(blob_error)?;
        Ok(format!("local:{id}"))
    }
}

fn parse_local_ref(blob_ref: &str) -> Result<&str> {
    let Some(id) = blob_ref.strip_prefix("local:") else {
        return Err(PlatformError::runtime(format!(
            "blob error: invalid legacy local blob ref `{blob_ref}`"
        )));
    };
    if Uuid::parse_str(id).is_err() {
        return Err(PlatformError::runtime(format!(
            "blob error: invalid legacy local blob id `{id}`"
        )));
    }
    Ok(id)
}

fn blob_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("blob error: {error}"))
}

#[cfg(test)]
pub async fn local_blob_store_for_tests(root: impl AsRef<Path>) -> Result<BlobStore> {
    BlobStore::for_legacy_root(root.as_ref().to_path_buf()).await
}
