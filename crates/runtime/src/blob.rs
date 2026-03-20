use common::{PlatformError, Result};
use std::env;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

#[derive(Clone)]
pub enum BlobStore {
    Local(LocalBlobStore),
    S3(S3BlobStore),
}

#[derive(Clone)]
pub struct LocalBlobStore {
    root: PathBuf,
}

#[derive(Clone)]
pub struct S3BlobStore {
    bucket: String,
    endpoint: Option<String>,
    prefix: String,
}

impl BlobStore {
    pub async fn from_env() -> Result<Self> {
        let backend = env::var("DD_BLOB_BACKEND").unwrap_or_else(|_| "local".to_string());
        match backend.trim().to_ascii_lowercase().as_str() {
            "" | "local" => {
                let store_dir = env::var("DD_STORE_DIR").unwrap_or_else(|_| "./store".to_string());
                let root = env::var("DD_BLOB_DIR").unwrap_or_else(|_| format!("{store_dir}/blobs"));
                Ok(Self::Local(LocalBlobStore::new(root.into()).await?))
            }
            "s3" => Ok(Self::S3(S3BlobStore::from_env())),
            other => Err(PlatformError::runtime(format!(
                "blob error: unsupported DD_BLOB_BACKEND value `{other}`"
            ))),
        }
    }

    pub async fn put(&self, bytes: &[u8]) -> Result<String> {
        match self {
            Self::Local(store) => store.put(bytes).await,
            Self::S3(store) => store.put(bytes).await,
        }
    }

    pub async fn get(&self, blob_ref: &str) -> Result<Vec<u8>> {
        match self {
            Self::Local(store) => store.get(blob_ref).await,
            Self::S3(store) => store.get(blob_ref).await,
        }
    }

    pub async fn delete(&self, blob_ref: &str) -> Result<()> {
        match self {
            Self::Local(store) => store.delete(blob_ref).await,
            Self::S3(store) => store.delete(blob_ref).await,
        }
    }
}

impl LocalBlobStore {
    pub async fn new(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root).await.map_err(blob_error)?;
        Ok(Self { root })
    }

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

    pub async fn get(&self, blob_ref: &str) -> Result<Vec<u8>> {
        let id = parse_local_ref(blob_ref)?;
        let path = self.root.join(format!("{id}.blob"));
        fs::read(path).await.map_err(blob_error)
    }

    pub async fn delete(&self, blob_ref: &str) -> Result<()> {
        let id = parse_local_ref(blob_ref)?;
        let path = self.root.join(format!("{id}.blob"));
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(blob_error(error)),
        }
    }
}

impl S3BlobStore {
    fn from_env() -> Self {
        let bucket = env::var("DD_BLOB_S3_BUCKET").unwrap_or_default();
        let endpoint = env::var("DD_BLOB_S3_ENDPOINT").ok();
        let prefix = env::var("DD_BLOB_S3_PREFIX").unwrap_or_else(|_| "cache/".to_string());
        Self {
            bucket,
            endpoint,
            prefix,
        }
    }

    async fn put(&self, _bytes: &[u8]) -> Result<String> {
        Err(PlatformError::runtime(format!(
            "blob error: s3 backend is not implemented yet (bucket=`{}`, endpoint={:?}, prefix=`{}`)",
            self.bucket, self.endpoint, self.prefix
        )))
    }

    async fn get(&self, _blob_ref: &str) -> Result<Vec<u8>> {
        Err(PlatformError::runtime(
            "blob error: s3 backend is not implemented yet",
        ))
    }

    async fn delete(&self, _blob_ref: &str) -> Result<()> {
        Err(PlatformError::runtime(
            "blob error: s3 backend is not implemented yet",
        ))
    }
}

fn parse_local_ref(blob_ref: &str) -> Result<&str> {
    let Some(id) = blob_ref.strip_prefix("local:") else {
        return Err(PlatformError::runtime(format!(
            "blob error: invalid local blob ref `{blob_ref}`"
        )));
    };
    if Uuid::parse_str(id).is_err() {
        return Err(PlatformError::runtime(format!(
            "blob error: invalid local blob id `{id}`"
        )));
    }
    Ok(id)
}

fn blob_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("blob error: {error}"))
}

#[cfg(test)]
pub async fn local_blob_store_for_tests(root: impl AsRef<Path>) -> Result<BlobStore> {
    Ok(BlobStore::Local(
        LocalBlobStore::new(root.as_ref().to_path_buf()).await?,
    ))
}
