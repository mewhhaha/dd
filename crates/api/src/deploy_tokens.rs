use base64::Engine;
use common::{
    DeployBinding, DeployRequest, DeployTokenCapabilities, DeployTokenDeleteResponse,
    DeployTokenGetResponse, DeployTokenListResponse, DeployTokenMetadata, DeployTokenMintRequest,
    DeployTokenMintResponse, PlatformError, Result,
};
use runtime::{ControlDeployToken, ControlStore};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct DeployTokenStore {
    control: ControlStore,
    mutation: Arc<Mutex<()>>,
}

impl DeployTokenStore {
    pub async fn load(path: PathBuf) -> Result<Self> {
        let file_name = path.file_name().and_then(|value| value.to_str());
        let store_dir = if matches!(file_name, Some("tokens.json" | "deploy-tokens.json")) {
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        } else {
            path.with_extension("control")
        };
        let control = ControlStore::open(store_dir).await?;
        Self::from_control_store(control, Some(&path)).await
    }

    pub async fn from_control_store(
        control: ControlStore,
        legacy_path: Option<&Path>,
    ) -> Result<Self> {
        if let Some(path) = legacy_path {
            control.import_legacy_tokens(path).await?;
        }
        Ok(Self {
            control,
            mutation: Arc::new(Mutex::new(())),
        })
    }

    pub async fn mint(&self, request: DeployTokenMintRequest) -> Result<DeployTokenMintResponse> {
        let now = unix_now()?;
        let expires_at_unix =
            resolve_expiry(now, request.expires_in_seconds, request.expires_at_unix)?;
        validate_max_uses(request.max_uses)?;
        validate_capabilities(&request.capabilities)?;

        let id = resolve_token_id(request.id.as_deref(), request.name.as_deref())?;
        let named = request.id.is_some() || request.name.is_some();
        let token = generate_token();
        let stored = ControlDeployToken {
            id: id.clone(),
            name: named.then(|| id.clone()),
            token_hash: token_hash(&token),
            created_at_unix: now,
            expires_at_unix,
            max_uses: request.max_uses,
            uses: 0,
            last_used_at_unix: None,
            capabilities: request.capabilities.clone(),
        };

        let _guard = self.mutation.lock().await;
        if self.control.list_tokens(now).await?.len() >= 1_024 {
            return Err(PlatformError::conflict("token store limit reached"));
        }
        match self.control.get_token(&id, now).await {
            Ok(_) => return Err(PlatformError::conflict("token id already exists")),
            Err(error) if error.kind() == common::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if !self.control.insert_token(&stored).await? {
            return Err(PlatformError::conflict("token id already exists"));
        }

        Ok(DeployTokenMintResponse {
            ok: true,
            id,
            name: stored.name,
            token,
            expires_at_unix,
            max_uses: request.max_uses,
            capabilities: request.capabilities,
        })
    }

    pub async fn authorize_deploy(
        &self,
        bearer_token: &str,
        request: &DeployRequest,
    ) -> Result<()> {
        let hash = token_hash(bearer_token.trim());
        let now = unix_now()?;
        let _guard = self.mutation.lock().await;
        let Some(token) = self.control.get_token_by_hash(&hash).await? else {
            return Err(PlatformError::unauthorized("invalid token"));
        };

        if token.is_expired(now) {
            return Err(PlatformError::unauthorized("token expired"));
        }
        if token
            .max_uses
            .is_some_and(|max_uses| token.uses >= max_uses)
        {
            return Err(PlatformError::unauthorized("token exhausted"));
        }

        enforce_capabilities(&token.capabilities, request)?;
        if !self.control.consume_token(&token.id, now).await? {
            return Err(PlatformError::unauthorized("token exhausted"));
        }
        Ok(())
    }

    pub async fn preflight(&self, bearer_token: &str) -> Result<()> {
        let hash = token_hash(bearer_token.trim());
        let now = unix_now()?;
        let Some(token) = self.control.get_token_by_hash(&hash).await? else {
            return Err(PlatformError::unauthorized("invalid token"));
        };
        if token.is_expired(now) {
            return Err(PlatformError::unauthorized("token expired"));
        }
        if token
            .max_uses
            .is_some_and(|max_uses| token.uses >= max_uses)
        {
            return Err(PlatformError::unauthorized("token exhausted"));
        }
        Ok(())
    }

    pub async fn list(&self) -> Result<DeployTokenListResponse> {
        let now = unix_now()?;
        Ok(DeployTokenListResponse {
            ok: true,
            tokens: self
                .control
                .list_tokens(now)
                .await?
                .iter()
                .map(ControlDeployToken::metadata)
                .collect(),
        })
    }

    pub async fn get(&self, id: &str) -> Result<DeployTokenGetResponse> {
        let token = self.control.get_token(id, unix_now()?).await?;
        Ok(DeployTokenGetResponse {
            ok: true,
            token: token.metadata(),
        })
    }

    pub async fn delete(&self, id: &str) -> Result<DeployTokenDeleteResponse> {
        let _guard = self.mutation.lock().await;
        if !self.control.delete_token(id, unix_now()?).await? {
            return Err(PlatformError::not_found("token not found"));
        }
        Ok(DeployTokenDeleteResponse {
            ok: true,
            id: id.to_string(),
        })
    }
}

trait ControlDeployTokenExt {
    fn is_expired(&self, now: u64) -> bool;
    fn metadata(&self) -> DeployTokenMetadata;
}

impl ControlDeployTokenExt for ControlDeployToken {
    fn is_expired(&self, now: u64) -> bool {
        self.expires_at_unix
            .is_some_and(|expires_at_unix| now >= expires_at_unix)
    }

    fn metadata(&self) -> DeployTokenMetadata {
        DeployTokenMetadata {
            id: self.id.clone(),
            name: self.name.clone(),
            created_at_unix: self.created_at_unix,
            expires_at_unix: self.expires_at_unix,
            max_uses: self.max_uses,
            uses: self.uses,
            last_used_at_unix: self.last_used_at_unix,
            capabilities: self.capabilities.clone(),
        }
    }
}

fn resolve_expiry(
    now: u64,
    expires_in_seconds: Option<u64>,
    expires_at_unix: Option<u64>,
) -> Result<Option<u64>> {
    match (expires_in_seconds, expires_at_unix) {
        (Some(_), Some(_)) => Err(PlatformError::bad_request(
            "set either expires_in_seconds or expires_at_unix, not both",
        )),
        (Some(0), _) => Err(PlatformError::bad_request(
            "expires_in_seconds must be greater than 0",
        )),
        (Some(seconds), _) => now
            .checked_add(seconds)
            .map(Some)
            .ok_or_else(|| PlatformError::bad_request("expires_in_seconds is too large")),
        (_, Some(expires_at)) if expires_at <= now => Err(PlatformError::bad_request(
            "expires_at_unix must be in the future",
        )),
        (_, Some(expires_at)) => Ok(Some(expires_at)),
        (None, None) => Ok(None),
    }
}

fn validate_max_uses(max_uses: Option<u64>) -> Result<()> {
    if max_uses == Some(0) {
        return Err(PlatformError::bad_request(
            "max_uses must be greater than 0",
        ));
    }
    Ok(())
}

fn resolve_token_id(id: Option<&str>, name: Option<&str>) -> Result<String> {
    let id = id.map(|id| normalize_token_slug(id, "id")).transpose()?;
    let name = name
        .map(|name| normalize_token_slug(name, "name"))
        .transpose()?;

    match (id, name) {
        (Some(id), Some(name)) if id != name => Err(PlatformError::bad_request(
            "token id and name must match when both are set",
        )),
        (Some(id), _) | (_, Some(id)) => Ok(id),
        (None, None) => Ok(Uuid::new_v4().to_string()),
    }
}

fn normalize_token_slug(value: &str, field: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(PlatformError::bad_request(format!(
            "token {field} must not be empty"
        )));
    }
    if trimmed.len() > 128 {
        return Err(PlatformError::bad_request(format!(
            "token {field} must be at most 128 bytes"
        )));
    }

    let mut normalized = String::with_capacity(trimmed.len());
    for byte in trimmed.bytes() {
        match byte {
            b'A'..=b'Z' => normalized.push((byte + 32) as char),
            b'a'..=b'z' | b'0'..=b'9' | b'-' => normalized.push(byte as char),
            _ => {
                return Err(PlatformError::bad_request(format!(
                    "token {field} must contain only ASCII letters, numbers, and '-'"
                )));
            }
        }
    }

    if normalized.starts_with('-')
        || normalized.ends_with('-')
        || normalized.as_bytes().windows(2).any(|pair| pair == b"--")
    {
        return Err(PlatformError::bad_request(format!(
            "token {field} must be dash-delimited words"
        )));
    }

    Ok(normalized)
}

fn validate_capabilities(capabilities: &DeployTokenCapabilities) -> Result<()> {
    if !capabilities.allow_any_worker && normalized_workers(capabilities).is_empty() {
        return Err(PlatformError::bad_request(
            "token must allow at least one worker or set allow_any_worker",
        ));
    }
    if !capabilities.allow_public && !capabilities.allow_private {
        return Err(PlatformError::bad_request(
            "token must allow public or private deploys",
        ));
    }
    for binding in &capabilities.bindings {
        if binding_name(binding).trim().is_empty() {
            return Err(PlatformError::bad_request(
                "token binding names must not be empty",
            ));
        }
        if service_target(binding).is_some_and(|target| target.trim().is_empty()) {
            return Err(PlatformError::bad_request(
                "token service binding targets must not be empty",
            ));
        }
    }
    Ok(())
}

fn enforce_capabilities(
    capabilities: &DeployTokenCapabilities,
    request: &DeployRequest,
) -> Result<()> {
    let worker = request.name.trim();
    if worker.is_empty() {
        return Err(PlatformError::bad_request("Worker name must not be empty"));
    }
    if !capabilities.allow_any_worker
        && !normalized_workers(capabilities)
            .iter()
            .any(|allowed| allowed == worker)
    {
        return Err(PlatformError::forbidden(
            "token is not allowed to deploy this worker",
        ));
    }

    if request.config.public && !capabilities.allow_public {
        return Err(PlatformError::forbidden(
            "token is not allowed to deploy public workers",
        ));
    }
    if !request.config.public && !capabilities.allow_private {
        return Err(PlatformError::forbidden(
            "token is not allowed to deploy private workers",
        ));
    }
    if request.config.internal.trace.is_some() && !capabilities.allow_internal_trace {
        return Err(PlatformError::forbidden(
            "token is not allowed to configure internal tracing",
        ));
    }
    if let Some(max_source_bytes) = capabilities.max_source_bytes
        && request.source.len() as u64 > max_source_bytes
    {
        return Err(PlatformError::forbidden("token source byte limit exceeded"));
    }
    if let Some(max_assets) = capabilities.max_assets {
        let asset_count = request
            .assets
            .len()
            .saturating_add(request.server_modules.len()) as u64;
        if asset_count > max_assets {
            return Err(PlatformError::forbidden("token asset count limit exceeded"));
        }
    }
    if let Some(max_asset_bytes) = capabilities.max_asset_bytes {
        let asset_bytes = total_upload_asset_bytes(request)?;
        if asset_bytes > max_asset_bytes {
            return Err(PlatformError::forbidden("token asset byte limit exceeded"));
        }
    }
    if !capabilities.allow_any_bindings {
        for binding in &request.config.bindings {
            if !capabilities
                .bindings
                .iter()
                .any(|allowed| bindings_match(allowed, binding))
            {
                return Err(PlatformError::forbidden(
                    "token is not allowed to use one or more bindings",
                ));
            }
        }
    }
    Ok(())
}

fn total_upload_asset_bytes(request: &DeployRequest) -> Result<u64> {
    let mut total = 0u64;
    for asset in &request.assets {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(asset.content_base64.as_bytes())
            .map_err(|error| {
                PlatformError::bad_request(format!(
                    "asset {} has invalid base64 content: {error}",
                    asset.path
                ))
            })?;
        total = total
            .checked_add(decoded.len() as u64)
            .ok_or_else(|| PlatformError::bad_request("asset payload is too large"))?;
    }
    for module in &request.server_modules {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(module.content_base64.as_bytes())
            .map_err(|error| {
                PlatformError::bad_request(format!(
                    "server module {} has invalid base64 content: {error}",
                    module.path
                ))
            })?;
        total = total
            .checked_add(decoded.len() as u64)
            .ok_or_else(|| PlatformError::bad_request("asset payload is too large"))?;
    }
    Ok(total)
}

fn normalized_workers(capabilities: &DeployTokenCapabilities) -> Vec<String> {
    capabilities
        .workers
        .iter()
        .map(|worker| worker.trim().to_string())
        .filter(|worker| !worker.is_empty())
        .collect()
}

fn bindings_match(left: &DeployBinding, right: &DeployBinding) -> bool {
    std::mem::discriminant(left) == std::mem::discriminant(right)
        && binding_name(left).trim() == binding_name(right).trim()
        && service_target(left).map(str::trim) == service_target(right).map(str::trim)
}

fn binding_name(binding: &DeployBinding) -> &str {
    match binding {
        DeployBinding::Kv { binding }
        | DeployBinding::Memory { binding }
        | DeployBinding::Dynamic { binding }
        | DeployBinding::Service { binding, .. } => binding,
    }
}

fn service_target(binding: &DeployBinding) -> Option<&str> {
    match binding {
        DeployBinding::Service { service, .. } => Some(service),
        _ => None,
    }
}

fn token_hash(token: &str) -> String {
    hex_digest(&Sha256::digest(token.as_bytes()))
}

fn generate_token() -> String {
    format!(
        "dddt_{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

fn unix_now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            PlatformError::internal(format!("system clock before unix epoch: {error}"))
        })
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use common::{DeployAsset, DeployConfig, DeployServerModule, DeployServerModuleKind};

    fn caps_for_worker(worker: &str) -> DeployTokenCapabilities {
        DeployTokenCapabilities {
            workers: vec![worker.to_string()],
            allow_public: true,
            bindings: vec![DeployBinding::Memory {
                binding: "ROOM".to_string(),
            }],
            max_source_bytes: Some(1024),
            max_assets: Some(1),
            max_asset_bytes: Some(16),
            ..DeployTokenCapabilities::default()
        }
    }

    #[test]
    fn capabilities_allow_scoped_worker_and_binding() {
        let request = DeployRequest {
            name: "chat".to_string(),
            source: "export default {}".to_string(),
            config: DeployConfig {
                public: true,
                cache: Default::default(),
                bindings: vec![DeployBinding::Memory {
                    binding: "ROOM".to_string(),
                }],
                ..DeployConfig::default()
            },
            assets: vec![DeployAsset {
                path: "/a.txt".to_string(),
                content_base64: "b2s=".to_string(),
            }],
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        };
        enforce_capabilities(&caps_for_worker("chat"), &request).expect("allowed");
    }

    #[test]
    fn capabilities_count_server_modules_as_assets() {
        let mut request = DeployRequest {
            name: "chat".to_string(),
            source: "export default {}".to_string(),
            config: DeployConfig {
                public: true,
                cache: Default::default(),
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: vec![DeployServerModule {
                path: "config.json".to_string(),
                kind: DeployServerModuleKind::Json,
                content_base64: "e30=".to_string(),
            }],
            asset_headers: None,
            temporary: false,
        };
        let caps = DeployTokenCapabilities {
            workers: vec!["chat".to_string()],
            allow_public: true,
            max_assets: Some(1),
            max_asset_bytes: Some(16),
            ..DeployTokenCapabilities::default()
        };
        enforce_capabilities(&caps, &request).expect("single server module should fit limits");

        request.assets.push(DeployAsset {
            path: "/public.txt".to_string(),
            content_base64: "b2s=".to_string(),
        });
        let error = enforce_capabilities(&caps, &request).expect_err("asset count should fail");
        assert_eq!(error.kind(), common::ErrorKind::Forbidden);

        request.assets.clear();
        request.server_modules[0].content_base64 =
            base64::engine::general_purpose::STANDARD.encode([0u8; 17]);
        let error = enforce_capabilities(&caps, &request).expect_err("asset bytes should fail");
        assert_eq!(error.kind(), common::ErrorKind::Forbidden);
    }

    #[test]
    fn capabilities_scope_service_binding_target() {
        let capabilities = DeployTokenCapabilities {
            workers: vec!["app".to_string()],
            allow_private: true,
            bindings: vec![DeployBinding::Service {
                binding: "AUTH".to_string(),
                service: "auth-worker".to_string(),
            }],
            ..DeployTokenCapabilities::default()
        };
        let allowed = DeployRequest {
            name: "app".to_string(),
            source: "export default {}".to_string(),
            config: DeployConfig {
                public: false,
                cache: Default::default(),
                bindings: vec![DeployBinding::Service {
                    binding: "AUTH".to_string(),
                    service: "auth-worker".to_string(),
                }],
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        };
        enforce_capabilities(&capabilities, &allowed).expect("target should match");

        let rejected = DeployRequest {
            config: DeployConfig {
                bindings: vec![DeployBinding::Service {
                    binding: "AUTH".to_string(),
                    service: "other-worker".to_string(),
                }],
                ..allowed.config.clone()
            },
            ..allowed
        };
        let error = enforce_capabilities(&capabilities, &rejected).expect_err("reject");
        assert_eq!(error.kind(), common::ErrorKind::Forbidden);
    }

    #[test]
    fn capabilities_reject_unscoped_worker() {
        let request = DeployRequest {
            name: "other".to_string(),
            source: "export default {}".to_string(),
            config: DeployConfig {
                public: true,
                cache: Default::default(),
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        };
        let error = enforce_capabilities(&caps_for_worker("chat"), &request).expect_err("reject");
        assert_eq!(error.kind(), common::ErrorKind::Forbidden);
    }

    #[test]
    fn token_slug_rejects_non_dash_delimited_words() {
        let error = normalize_token_slug("my--token", "name").expect_err("invalid");
        assert_eq!(error.kind(), common::ErrorKind::BadRequest);

        let error = normalize_token_slug("my token", "name").expect_err("invalid");
        assert_eq!(error.kind(), common::ErrorKind::BadRequest);

        let slug = normalize_token_slug("My-Token-At-Home", "name").expect("slug");
        assert_eq!(slug, "my-token-at-home");
    }

    #[tokio::test]
    async fn mint_uses_unique_slug_name_as_id() {
        let path = std::env::temp_dir().join(format!(
            "dd-token-store-test-{}.json",
            Uuid::new_v4().simple()
        ));
        let control_dir = path.with_extension("control");
        let store = DeployTokenStore::load(path.clone()).await.expect("store");
        let request = DeployTokenMintRequest {
            name: Some("My-Token-At-Home".to_string()),
            capabilities: caps_for_worker("chat"),
            ..DeployTokenMintRequest::default()
        };

        let minted = store.mint(request.clone()).await.expect("mint");
        assert_eq!(minted.id, "my-token-at-home");
        assert_eq!(minted.name.as_deref(), Some("my-token-at-home"));

        let listed = store.list().await.expect("list");
        assert_eq!(listed.tokens.len(), 1);
        assert_eq!(listed.tokens[0].id, "my-token-at-home");
        assert_eq!(listed.tokens[0].name.as_deref(), Some("my-token-at-home"));

        let fetched = store.get("my-token-at-home").await.expect("get");
        assert_eq!(fetched.token.id, "my-token-at-home");

        let error = store.mint(request).await.expect_err("duplicate");
        assert_eq!(error.kind(), common::ErrorKind::Conflict);

        store
            .delete("my-token-at-home")
            .await
            .expect("delete by slug");
        let _ = tokio::fs::remove_dir_all(control_dir).await;
    }

    #[tokio::test]
    async fn imports_legacy_tokens_once_and_persists_usage_in_control_db() {
        let root =
            std::env::temp_dir().join(format!("dd-token-import-test-{}", Uuid::new_v4().simple()));
        tokio::fs::create_dir_all(&root).await.expect("test dir");
        let path = root.join("tokens.json");
        let raw_token = "legacy-secret";
        let capabilities = caps_for_worker("chat");
        let legacy = serde_json::json!({
            "version": 1,
            "tokens": [{
                "id": "legacy",
                "name": "legacy",
                "token_hash": token_hash(raw_token),
                "created_at_unix": 1,
                "expires_at_unix": null,
                "max_uses": 2,
                "uses": 0,
                "last_used_at_unix": null,
                "capabilities": capabilities,
            }]
        });
        tokio::fs::write(&path, serde_json::to_vec(&legacy).expect("legacy json"))
            .await
            .expect("legacy token file");

        let store = DeployTokenStore::load(path.clone()).await.expect("import");
        let deploy = DeployRequest {
            name: "chat".to_string(),
            source: "export default {}".to_string(),
            config: DeployConfig {
                public: true,
                bindings: vec![DeployBinding::Memory {
                    binding: "ROOM".to_string(),
                }],
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        };
        store
            .authorize_deploy(raw_token, &deploy)
            .await
            .expect("consume legacy token");
        drop(store);

        let restored = DeployTokenStore::load(path)
            .await
            .expect("restore control db");
        let metadata = restored.get("legacy").await.expect("legacy metadata");
        assert_eq!(metadata.token.uses, 1);
        assert!(metadata.token.last_used_at_unix.is_some());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn rejects_unrecognized_legacy_token_version() {
        let root =
            std::env::temp_dir().join(format!("dd-token-version-test-{}", Uuid::new_v4().simple()));
        tokio::fs::create_dir_all(&root).await.expect("test dir");
        let path = root.join("tokens.json");
        tokio::fs::write(&path, br#"{"version":99,"tokens":[]}"#)
            .await
            .expect("legacy token file");
        let error = match DeployTokenStore::load(path).await {
            Ok(_) => panic!("future token format should fail startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("format version 99"));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[test]
    fn token_id_and_name_must_match_when_both_are_set() {
        let error = resolve_token_id(Some("deploy-home"), Some("deploy-work"))
            .expect_err("mismatched token identity");
        assert_eq!(error.kind(), common::ErrorKind::BadRequest);

        let id = resolve_token_id(Some("Deploy-Home"), Some("deploy-home")).expect("match");
        assert_eq!(id, "deploy-home");
    }
}
