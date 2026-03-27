use crate::blob::BlobStore;
use common::{PlatformError, Result};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use turso::{Builder, Connection, Database};
use uuid::Uuid;

const DEFAULT_TTL: Duration = Duration::from_secs(60);
const MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_entries: usize,
    pub max_bytes: usize,
    pub default_ttl: Duration,
    pub inline_body_limit_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 2048,
            max_bytes: 64 * 1024 * 1024,
            default_ttl: DEFAULT_TTL,
            inline_body_limit_bytes: 64 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CacheRequest {
    pub cache_name: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub bypass_stale: bool,
}

#[derive(Clone, Debug)]
pub struct CacheResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug)]
pub enum CacheLookup {
    Fresh(CacheResponse),
    StaleWhileRevalidate(CacheResponse),
    StaleIfError(CacheResponse),
    Miss,
}

#[derive(Clone)]
pub struct CacheStore {
    database: Arc<Database>,
    blob_store: BlobStore,
    config: CacheConfig,
    access_seq: Arc<AtomicU64>,
}

struct CacheRecord {
    id: String,
    vary_headers_json: String,
    vary_values_json: String,
    status: i64,
    headers_json: String,
    body_storage: String,
    body_inline_hex: String,
    body_ref: String,
    expires_at_ms: i64,
}

struct CacheDeleteCandidate {
    id: String,
    body_storage: String,
    body_ref: String,
}

#[derive(Default)]
struct CacheControl {
    no_store: bool,
    no_cache: bool,
    private: bool,
    max_age: Option<u64>,
    s_maxage: Option<u64>,
    stale_while_revalidate: Option<u64>,
    stale_if_error: Option<u64>,
}

impl CacheStore {
    pub async fn from_config(
        config: CacheConfig,
        database_url: &str,
        blob_store: BlobStore,
    ) -> Result<Self> {
        let local_path = database_url
            .strip_prefix("file:")
            .unwrap_or(database_url)
            .to_string();
        ensure_parent_dir(&local_path)?;
        Self::from_local_path(config, local_path, blob_store).await
    }

    pub async fn get(&self, request: &CacheRequest) -> Result<CacheLookup> {
        let Some(method) = normalize_cache_method(&request.method) else {
            return Ok(CacheLookup::Miss);
        };
        let request_headers = to_header_map(&request.headers);
        let request_control = parse_cache_control(header_value(&request_headers, "cache-control"));
        if request_control.no_store || request_control.no_cache {
            return Ok(CacheLookup::Miss);
        }

        let cache_name = normalize_cache_name(&request.cache_name);
        let now_ms = epoch_ms_i64()?;
        let conn = self.connect()?;
        let mut rows = conn
            .query(
                "SELECT id, vary_headers_json, vary_values_json, status, headers_json, body_storage, body_inline_hex, body_ref, expires_at_ms
                 FROM worker_cache_entries
                 WHERE cache_name = ?1 AND method = ?2 AND url = ?3
                 ORDER BY last_access_seq DESC",
                (cache_name.as_str(), method.as_str(), request.url.as_str()),
            )
            .await
            .map_err(cache_error)?;

        let mut expired = Vec::new();
        let mut stale = Vec::new();
        let mut selected_id = String::new();
        let mut lookup = CacheLookup::Miss;

        while let Some(row) = rows.next().await.map_err(cache_error)? {
            let mut record = row_to_record(&row)?;
            let vary_headers: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_headers_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate {
                            id: record.id,
                            body_storage: record.body_storage,
                            body_ref: record.body_ref,
                        });
                        continue;
                    }
                };
            let stored_vary_values: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_values_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate {
                            id: record.id,
                            body_storage: record.body_storage,
                            body_ref: record.body_ref,
                        });
                        continue;
                    }
                };
            let request_vary_values = derive_vary_values(&vary_headers, &request_headers);
            if request_vary_values != stored_vary_values {
                continue;
            }

            let headers: Vec<(String, String)> =
                match crate::json::from_string(std::mem::take(&mut record.headers_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate {
                            id: record.id,
                            body_storage: record.body_storage,
                            body_ref: record.body_ref,
                        });
                        continue;
                    }
                };

            let body = if record.body_storage == "inline" {
                match hex_to_bytes(&record.body_inline_hex) {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate {
                            id: record.id,
                            body_storage: record.body_storage,
                            body_ref: record.body_ref,
                        });
                        continue;
                    }
                }
            } else if record.body_storage == "blob" {
                match self.blob_store.get(&record.body_ref).await {
                    Ok(value) => value,
                    Err(_) => {
                        stale.push(CacheDeleteCandidate {
                            id: record.id,
                            body_storage: record.body_storage,
                            body_ref: record.body_ref,
                        });
                        continue;
                    }
                }
            } else {
                stale.push(CacheDeleteCandidate {
                    id: record.id,
                    body_storage: record.body_storage,
                    body_ref: record.body_ref,
                });
                continue;
            };

            let response = CacheResponse {
                status: record.status as u16,
                headers,
                body,
            };
            let response_headers = to_header_map(&response.headers);
            let response_control =
                parse_cache_control(header_value(&response_headers, "cache-control"));
            let swr_until_ms = record.expires_at_ms.saturating_add(
                (response_control.stale_while_revalidate.unwrap_or(0) as i64) * 1000,
            );
            let sie_until_ms = record
                .expires_at_ms
                .saturating_add((response_control.stale_if_error.unwrap_or(0) as i64) * 1000);
            let max_stale_until_ms = swr_until_ms.max(sie_until_ms);

            if now_ms <= record.expires_at_ms {
                selected_id = record.id;
                lookup = CacheLookup::Fresh(response);
                break;
            }

            if now_ms > max_stale_until_ms {
                expired.push(CacheDeleteCandidate {
                    id: record.id,
                    body_storage: record.body_storage,
                    body_ref: record.body_ref,
                });
                continue;
            }

            if request.bypass_stale {
                continue;
            }

            selected_id = record.id;
            if now_ms <= swr_until_ms && response_control.stale_while_revalidate.unwrap_or(0) > 0 {
                lookup = CacheLookup::StaleWhileRevalidate(response);
            } else if now_ms <= sie_until_ms && response_control.stale_if_error.unwrap_or(0) > 0 {
                lookup = CacheLookup::StaleIfError(response);
            } else {
                continue;
            }
            break;
        }
        drop(rows);

        if !expired.is_empty() {
            self.remove_records(&conn, &expired).await?;
        }
        if !stale.is_empty() {
            self.remove_records(&conn, &stale).await?;
        }

        if !selected_id.is_empty() {
            conn.execute(
                "UPDATE worker_cache_entries
                 SET last_access_seq = ?1
                 WHERE id = ?3",
                (self.next_access_seq(), selected_id),
            )
            .await
            .map_err(cache_error)?;
        }

        Ok(lookup)
    }

    pub async fn put(&self, request: &CacheRequest, response: CacheResponse) -> Result<bool> {
        let Some(method) = normalize_cache_method(&request.method) else {
            return Ok(false);
        };
        let request_headers = to_header_map(&request.headers);
        if request_headers.contains_key("authorization") || request_headers.contains_key("cookie") {
            return Ok(false);
        }
        let request_control = parse_cache_control(header_value(&request_headers, "cache-control"));
        if request_control.no_store {
            return Ok(false);
        }

        if !(200..300).contains(&response.status) {
            return Ok(false);
        }

        let response_headers = to_header_map(&response.headers);
        let response_control =
            parse_cache_control(header_value(&response_headers, "cache-control"));
        if response_control.no_store || response_control.no_cache || response_control.private {
            return Ok(false);
        }
        if response_headers.contains_key("set-cookie") {
            return Ok(false);
        }

        let Some(vary_headers) = parse_vary_headers(&response.headers) else {
            return Ok(false);
        };
        let vary_values = derive_vary_values(&vary_headers, &request_headers);

        let ttl = choose_ttl(response_control, self.config.default_ttl);
        if ttl.is_zero() {
            return Ok(false);
        }

        let cache_name = normalize_cache_name(&request.cache_name);
        let size_bytes =
            estimate_entry_size(&cache_name, &method, &request.url, &response, &vary_values);
        if size_bytes > self.config.max_bytes {
            return Ok(false);
        }

        let vary_headers_json = crate::json::to_string(&vary_headers)
            .map_err(|error| PlatformError::runtime(format!("cache error: {error}")))?;
        let vary_values_json = crate::json::to_string(&vary_values)
            .map_err(|error| PlatformError::runtime(format!("cache error: {error}")))?;
        let headers_json = crate::json::to_string(&response.headers)
            .map_err(|error| PlatformError::runtime(format!("cache error: {error}")))?;

        let mut body_storage = "inline".to_string();
        let mut body_inline_hex = bytes_to_hex(&response.body);
        let mut body_ref = String::new();
        if response.body.len() > self.config.inline_body_limit_bytes {
            body_storage = "blob".to_string();
            body_inline_hex.clear();
            body_ref = self.blob_store.put(&response.body).await?;
        }

        let conn = self.connect()?;
        let now_ms = epoch_ms_i64()?;
        let expires_at_ms = now_ms + ttl.as_millis() as i64;
        let existing_variant = self
            .load_variant_candidates(
                &conn,
                &cache_name,
                &method,
                &request.url,
                &vary_headers_json,
                &vary_values_json,
            )
            .await?;
        let access_seq = self.next_access_seq();
        if let Some(existing) = existing_variant.first() {
            let old_body_storage = existing.body_storage.clone();
            let old_body_ref = existing.body_ref.clone();
            let update_result = conn
                .execute(
                    "UPDATE worker_cache_entries
                     SET status = ?2,
                         headers_json = ?3,
                         body_storage = ?4,
                         body_inline_hex = ?5,
                         body_ref = ?6,
                         body_size = ?7,
                         expires_at_ms = ?8,
                         last_access_seq = ?9,
                         updated_at_ms = ?10
                     WHERE id = ?1",
                    (
                        existing.id.as_str(),
                        response.status as i64,
                        headers_json.as_str(),
                        body_storage.as_str(),
                        body_inline_hex.as_str(),
                        body_ref.as_str(),
                        size_bytes as i64,
                        expires_at_ms,
                        access_seq,
                        now_ms,
                    ),
                )
                .await;
            if let Err(error) = update_result {
                if body_storage == "blob" && !body_ref.is_empty() {
                    let _ = self.blob_store.delete(&body_ref).await;
                }
                return Err(cache_error(error));
            }
            if old_body_storage == "blob" && !old_body_ref.is_empty() && old_body_ref != body_ref {
                self.blob_store.delete(&old_body_ref).await?;
            }
            if existing_variant.len() > 1 {
                self.remove_records(&conn, &existing_variant[1..]).await?;
            }
        } else {
            let entry_id = Uuid::new_v4().to_string();
            let insert_result = conn
                .execute(
                    "INSERT INTO worker_cache_entries (
                       id, cache_name, method, url, vary_headers_json, vary_values_json,
                       status, headers_json, body_storage, body_inline_hex, body_ref,
                       body_size, expires_at_ms, last_access_seq, updated_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                    (
                        entry_id.as_str(),
                        cache_name.as_str(),
                        method.as_str(),
                        request.url.as_str(),
                        vary_headers_json.as_str(),
                        vary_values_json.as_str(),
                        response.status as i64,
                        headers_json.as_str(),
                        body_storage.as_str(),
                        body_inline_hex.as_str(),
                        body_ref.as_str(),
                        size_bytes as i64,
                        expires_at_ms,
                        access_seq,
                        now_ms,
                    ),
                )
                .await;
            if let Err(error) = insert_result {
                if body_storage == "blob" && !body_ref.is_empty() {
                    let _ = self.blob_store.delete(&body_ref).await;
                }
                return Err(cache_error(error));
            }
        }
        self.cleanup_expired(&conn, now_ms).await?;
        self.evict_if_needed(&conn).await?;
        Ok(true)
    }

    pub async fn delete(&self, request: &CacheRequest) -> Result<bool> {
        let Some(method) = normalize_cache_method(&request.method) else {
            return Ok(false);
        };
        let request_headers = to_header_map(&request.headers);
        let cache_name = normalize_cache_name(&request.cache_name);
        let conn = self.connect()?;
        let records = self
            .load_candidates(&conn, &cache_name, &method, &request.url)
            .await?;

        let mut to_delete = Vec::new();
        for mut record in records {
            let vary_headers: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_headers_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        to_delete.push(CacheDeleteCandidate {
                            id: record.id,
                            body_storage: record.body_storage,
                            body_ref: record.body_ref,
                        });
                        continue;
                    }
                };
            let stored_vary_values: Vec<String> =
                match crate::json::from_string(std::mem::take(&mut record.vary_values_json)) {
                    Ok(value) => value,
                    Err(_) => {
                        to_delete.push(CacheDeleteCandidate {
                            id: record.id,
                            body_storage: record.body_storage,
                            body_ref: record.body_ref,
                        });
                        continue;
                    }
                };
            if derive_vary_values(&vary_headers, &request_headers) == stored_vary_values {
                to_delete.push(CacheDeleteCandidate {
                    id: record.id,
                    body_storage: record.body_storage,
                    body_ref: record.body_ref,
                });
            }
        }

        if to_delete.is_empty() {
            return Ok(false);
        }

        self.remove_records(&conn, &to_delete).await?;
        Ok(true)
    }

    async fn from_local_path(
        config: CacheConfig,
        local_path: String,
        blob_store: BlobStore,
    ) -> Result<Self> {
        let database = Builder::new_local(&local_path)
            .build()
            .await
            .map_err(cache_error)?;
        let store = Self {
            database: Arc::new(database),
            blob_store,
            config,
            access_seq: Arc::new(AtomicU64::new(1)),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn ensure_schema(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS worker_cache_entries (
               id TEXT PRIMARY KEY,
               cache_name TEXT NOT NULL,
               method TEXT NOT NULL,
               url TEXT NOT NULL,
               vary_headers_json TEXT NOT NULL,
               vary_values_json TEXT NOT NULL,
               status INTEGER NOT NULL,
               headers_json TEXT NOT NULL,
               body_storage TEXT NOT NULL,
               body_inline_hex TEXT NOT NULL,
               body_ref TEXT NOT NULL,
               body_size INTEGER NOT NULL,
               expires_at_ms INTEGER NOT NULL,
               last_access_seq INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             )",
            (),
        )
        .await
        .map_err(cache_error)?;
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_cache_variant
             ON worker_cache_entries(cache_name, method, url, vary_headers_json, vary_values_json)",
            (),
        )
        .await
        .map_err(cache_error)?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_worker_cache_lookup
             ON worker_cache_entries(cache_name, method, url)",
            (),
        )
        .await
        .map_err(cache_error)?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_worker_cache_expiry
             ON worker_cache_entries(expires_at_ms)",
            (),
        )
        .await
        .map_err(cache_error)?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_worker_cache_lru
             ON worker_cache_entries(last_access_seq, updated_at_ms)",
            (),
        )
        .await
        .map_err(cache_error)?;
        Ok(())
    }

    async fn load_candidates(
        &self,
        conn: &Connection,
        cache_name: &str,
        method: &str,
        url: &str,
    ) -> Result<Vec<CacheRecord>> {
        let mut rows = conn
            .query(
                "SELECT id, vary_headers_json, vary_values_json, status, headers_json, body_storage, body_inline_hex, body_ref, expires_at_ms
                 FROM worker_cache_entries
                 WHERE cache_name = ?1 AND method = ?2 AND url = ?3",
                (cache_name, method, url),
            )
            .await
            .map_err(cache_error)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(cache_error)? {
            out.push(row_to_record(&row)?);
        }
        Ok(out)
    }

    async fn load_variant_candidates(
        &self,
        conn: &Connection,
        cache_name: &str,
        method: &str,
        url: &str,
        vary_headers_json: &str,
        vary_values_json: &str,
    ) -> Result<Vec<CacheDeleteCandidate>> {
        let mut rows = conn
            .query(
                "SELECT id, body_storage, body_ref
                 FROM worker_cache_entries
                 WHERE cache_name = ?1 AND method = ?2 AND url = ?3
                   AND vary_headers_json = ?4 AND vary_values_json = ?5",
                (cache_name, method, url, vary_headers_json, vary_values_json),
            )
            .await
            .map_err(cache_error)?;

        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(cache_error)? {
            out.push(CacheDeleteCandidate {
                id: row.get::<String>(0).map_err(cache_error)?,
                body_storage: row.get::<String>(1).map_err(cache_error)?,
                body_ref: row.get::<String>(2).map_err(cache_error)?,
            });
        }
        Ok(out)
    }

    async fn cleanup_expired(&self, conn: &Connection, now_ms: i64) -> Result<()> {
        loop {
            let mut rows = conn
                .query(
                    "SELECT id, body_storage, body_ref
                     FROM worker_cache_entries
                     WHERE expires_at_ms <= ?1
                     LIMIT 64",
                    (now_ms,),
                )
                .await
                .map_err(cache_error)?;
            let mut expired = Vec::new();
            while let Some(row) = rows.next().await.map_err(cache_error)? {
                expired.push(CacheDeleteCandidate {
                    id: row.get::<String>(0).map_err(cache_error)?,
                    body_storage: row.get::<String>(1).map_err(cache_error)?,
                    body_ref: row.get::<String>(2).map_err(cache_error)?,
                });
            }
            drop(rows);
            if expired.is_empty() {
                break;
            }
            self.remove_records(conn, &expired).await?;
        }
        Ok(())
    }

    async fn evict_if_needed(&self, conn: &Connection) -> Result<()> {
        loop {
            let (count, total_bytes) = self.cache_usage(conn).await?;
            if count <= self.config.max_entries && total_bytes <= self.config.max_bytes {
                break;
            }
            let mut rows = conn
                .query(
                    "SELECT id, body_storage, body_ref
                     FROM worker_cache_entries
                     ORDER BY last_access_seq ASC, updated_at_ms ASC
                     LIMIT 1",
                    (),
                )
                .await
                .map_err(cache_error)?;
            let Some(row) = rows.next().await.map_err(cache_error)? else {
                break;
            };
            let victim = CacheDeleteCandidate {
                id: row.get::<String>(0).map_err(cache_error)?,
                body_storage: row.get::<String>(1).map_err(cache_error)?,
                body_ref: row.get::<String>(2).map_err(cache_error)?,
            };
            drop(rows);
            self.remove_records(conn, &[victim]).await?;
        }
        Ok(())
    }

    async fn cache_usage(&self, conn: &Connection) -> Result<(usize, usize)> {
        let mut rows = conn
            .query(
                "SELECT COUNT(*), COALESCE(SUM(body_size), 0)
                 FROM worker_cache_entries",
                (),
            )
            .await
            .map_err(cache_error)?;
        if let Some(row) = rows.next().await.map_err(cache_error)? {
            let count = row.get::<i64>(0).map_err(cache_error)?.max(0) as usize;
            let bytes = row.get::<i64>(1).map_err(cache_error)?.max(0) as usize;
            return Ok((count, bytes));
        }
        Ok((0, 0))
    }

    async fn remove_records(
        &self,
        conn: &Connection,
        records: &[CacheDeleteCandidate],
    ) -> Result<()> {
        for record in records {
            conn.execute(
                "DELETE FROM worker_cache_entries WHERE id = ?1",
                (record.id.as_str(),),
            )
            .await
            .map_err(cache_error)?;
            if record.body_storage == "blob" && !record.body_ref.is_empty() {
                self.blob_store.delete(&record.body_ref).await?;
            }
        }
        Ok(())
    }

    fn connect(&self) -> Result<Connection> {
        self.database.connect().map_err(cache_error)
    }

    fn next_access_seq(&self) -> i64 {
        self.access_seq.fetch_add(1, Ordering::SeqCst) as i64
    }
}

fn row_to_record(row: &turso::Row) -> Result<CacheRecord> {
    Ok(CacheRecord {
        id: row.get::<String>(0).map_err(cache_error)?,
        vary_headers_json: row.get::<String>(1).map_err(cache_error)?,
        vary_values_json: row.get::<String>(2).map_err(cache_error)?,
        status: row.get::<i64>(3).map_err(cache_error)?,
        headers_json: row.get::<String>(4).map_err(cache_error)?,
        body_storage: row.get::<String>(5).map_err(cache_error)?,
        body_inline_hex: row.get::<String>(6).map_err(cache_error)?,
        body_ref: row.get::<String>(7).map_err(cache_error)?,
        expires_at_ms: row.get::<i64>(8).map_err(cache_error)?,
    })
}

fn normalize_cache_method(method: &str) -> Option<String> {
    let method = method.trim().to_ascii_uppercase();
    match method.as_str() {
        "GET" => Some(method),
        "HEAD" => Some("GET".to_string()),
        _ => None,
    }
}

fn normalize_cache_name(value: &str) -> String {
    let normalized = value.trim();
    if normalized.is_empty() {
        "default".to_string()
    } else {
        normalized.to_string()
    }
}

fn to_header_map(headers: &[(String, String)]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (name, value) in headers {
        let normalized = name.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        let trimmed = value.trim();
        map.entry(normalized)
            .and_modify(|existing: &mut String| {
                if !existing.is_empty() {
                    existing.push_str(", ");
                }
                existing.push_str(trimmed);
            })
            .or_insert_with(|| trimmed.to_string());
    }
    map
}

fn header_value<'a>(headers: &'a HashMap<String, String>, name: &str) -> &'a str {
    headers
        .get(&name.trim().to_ascii_lowercase())
        .map(|value| value.as_str())
        .unwrap_or("")
}

fn parse_cache_control(value: &str) -> CacheControl {
    let mut control = CacheControl::default();
    for raw_directive in value.split(',') {
        let directive = raw_directive.trim().to_ascii_lowercase();
        if directive.is_empty() {
            continue;
        }

        match directive.as_str() {
            "no-store" => {
                control.no_store = true;
                continue;
            }
            "no-cache" => {
                control.no_cache = true;
                continue;
            }
            "private" => {
                control.private = true;
                continue;
            }
            _ => {}
        }

        if let Some(value) = directive.strip_prefix("s-maxage=") {
            control.s_maxage = parse_u64_token(value);
            continue;
        }
        if let Some(value) = directive.strip_prefix("max-age=") {
            control.max_age = parse_u64_token(value);
            continue;
        }
        if let Some(value) = directive.strip_prefix("stale-while-revalidate=") {
            control.stale_while_revalidate = parse_u64_token(value);
            continue;
        }
        if let Some(value) = directive.strip_prefix("stale-if-error=") {
            control.stale_if_error = parse_u64_token(value);
        }
    }

    control
}

fn parse_u64_token(value: &str) -> Option<u64> {
    value.trim().trim_matches('"').parse::<u64>().ok()
}

fn parse_vary_headers(response_headers: &[(String, String)]) -> Option<Vec<String>> {
    let mut seen = HashSet::new();
    let mut vary = Vec::new();
    for (name, value) in response_headers {
        if !name.eq_ignore_ascii_case("vary") {
            continue;
        }
        for token in value.split(',') {
            let normalized = token.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                continue;
            }
            if normalized == "*" {
                return None;
            }
            if seen.insert(normalized.clone()) {
                vary.push(normalized);
            }
        }
    }
    Some(vary)
}

fn derive_vary_values(
    vary_headers: &[String],
    request_headers: &HashMap<String, String>,
) -> Vec<String> {
    vary_headers
        .iter()
        .map(|name| header_value(request_headers, name).to_string())
        .collect()
}

fn choose_ttl(control: CacheControl, fallback: Duration) -> Duration {
    let secs = control
        .s_maxage
        .or(control.max_age)
        .unwrap_or(fallback.as_secs());
    Duration::from_secs(secs)
        .min(MAX_TTL)
        .max(Duration::from_secs(0))
}

fn estimate_entry_size(
    cache_name: &str,
    method: &str,
    url: &str,
    response: &CacheResponse,
    vary_values: &[String],
) -> usize {
    let headers_bytes = response
        .headers
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum::<usize>();
    let vary_bytes = vary_values.iter().map(String::len).sum::<usize>();
    cache_name.len() + method.len() + url.len() + headers_bytes + vary_bytes + response.body.len()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_to_bytes(input: &str) -> Result<Vec<u8>> {
    if input.len() % 2 != 0 {
        return Err(PlatformError::runtime(
            "cache error: invalid inline body hex",
        ));
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        let hi = from_hex_nibble(bytes[idx])?;
        let lo = from_hex_nibble(bytes[idx + 1])?;
        out.push((hi << 4) | lo);
        idx += 2;
    }
    Ok(out)
}

fn from_hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(PlatformError::runtime(
            "cache error: invalid inline body hex",
        )),
    }
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}

fn cache_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("cache error: {error}"))
}

fn ensure_parent_dir(path: &str) -> Result<()> {
    let Some(parent) = Path::new(path).parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(cache_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CacheConfig, CacheLookup, CacheRequest, CacheResponse, CacheStore};
    use crate::blob::local_blob_store_for_tests;
    use common::Result;
    use std::time::Duration;
    use uuid::Uuid;

    async fn test_store(config: CacheConfig) -> CacheStore {
        let database_path = format!("/tmp/dd-cache-test-{}.db", Uuid::new_v4());
        let blob_dir = format!("/tmp/dd-cache-blob-test-{}", Uuid::new_v4());
        let blob_store = local_blob_store_for_tests(&blob_dir)
            .await
            .expect("blob store");
        CacheStore::from_local_path(config, database_path, blob_store)
            .await
            .expect("cache store")
    }

    fn request(path: &str) -> CacheRequest {
        CacheRequest {
            cache_name: "default".to_string(),
            method: "GET".to_string(),
            url: format!("http://worker{path}"),
            headers: Vec::new(),
            bypass_stale: false,
        }
    }

    fn response(body: &str) -> CacheResponse {
        CacheResponse {
            status: 200,
            headers: vec![("cache-control".to_string(), "max-age=60".to_string())],
            body: body.as_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn put_then_get_hits() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(5),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/x");
        assert!(store.put(&req, response("one")).await?);
        let hit = store.get(&req).await?;
        match hit {
            CacheLookup::Fresh(value) => assert_eq!(value.body, b"one"),
            other => panic!("expected fresh hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn vary_header_selects_variant() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(5),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let mut req_a = request("/vary");
        req_a
            .headers
            .push(("accept-language".to_string(), "en".to_string()));
        let mut req_b = request("/vary");
        req_b
            .headers
            .push(("accept-language".to_string(), "fr".to_string()));

        let mut res_a = response("hello");
        res_a
            .headers
            .push(("vary".to_string(), "accept-language".to_string()));
        let mut res_b = response("salut");
        res_b
            .headers
            .push(("vary".to_string(), "accept-language".to_string()));

        assert!(store.put(&req_a, res_a).await?);
        assert!(store.put(&req_b, res_b).await?);
        match store.get(&req_a).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body, b"hello"),
            other => panic!("expected fresh en hit, got {:?}", other),
        }
        match store.get(&req_b).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body, b"salut"),
            other => panic!("expected fresh fr hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn lru_eviction_respects_size_limits() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 2,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req_a = request("/a");
        let req_b = request("/b");
        let req_c = request("/c");

        assert!(store.put(&req_a, response("a")).await?);
        assert!(store.put(&req_b, response("b")).await?);
        let _ = store.get(&req_b).await?;
        assert!(store.put(&req_c, response("c")).await?);

        assert!(matches!(store.get(&req_a).await?, CacheLookup::Miss));
        match store.get(&req_b).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body, b"b"),
            other => panic!("expected fresh b hit, got {:?}", other),
        }
        match store.get(&req_c).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body, b"c"),
            other => panic!("expected fresh c hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn large_body_uses_blob_storage() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 16,
        })
        .await;
        let req = request("/blob");
        let body = "x".repeat(1024);
        let response = CacheResponse {
            status: 200,
            headers: vec![("cache-control".to_string(), "max-age=60".to_string())],
            body: body.as_bytes().to_vec(),
        };

        assert!(store.put(&req, response).await?);
        match store.get(&req).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body.len(), 1024),
            other => panic!("expected blob fresh hit, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn put_overwrites_existing_variant_without_unique_conflict() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/overwrite");

        assert!(store.put(&req, response("one")).await?);
        assert!(store.put(&req, response("two")).await?);

        match store.get(&req).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body, b"two"),
            other => panic!("expected overwritten cache entry, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_recently_overwritten_variant() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/delete");

        assert!(store.put(&req, response("temp")).await?);
        assert!(store.put(&req, response("temp-2")).await?);
        assert!(store.delete(&req).await?);
        assert!(matches!(store.get(&req).await?, CacheLookup::Miss));
        Ok(())
    }

    #[tokio::test]
    async fn stale_while_revalidate_lookup_serves_stale() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/swr");
        let response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-while-revalidate=30".to_string(),
            )],
            body: b"stale".to_vec(),
        };

        assert!(store.put(&req, response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(matches!(
            store.get(&req).await?,
            CacheLookup::StaleWhileRevalidate(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stale_if_error_lookup_serves_stale() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/sie");
        let response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-if-error=30".to_string(),
            )],
            body: b"stale".to_vec(),
        };

        assert!(store.put(&req, response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(matches!(
            store.get(&req).await?,
            CacheLookup::StaleIfError(_)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn bypass_stale_skips_stale_windows() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let mut req = request("/bypass");
        let response = CacheResponse {
            status: 200,
            headers: vec![(
                "cache-control".to_string(),
                "max-age=1, stale-while-revalidate=30, stale-if-error=30".to_string(),
            )],
            body: b"stale".to_vec(),
        };

        assert!(store.put(&req, response).await?);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        req.bypass_stale = true;
        assert!(matches!(store.get(&req).await?, CacheLookup::Miss));
        Ok(())
    }

    #[tokio::test]
    async fn put_same_variant_replaces_previous_entry() -> Result<()> {
        let store = test_store(CacheConfig {
            max_entries: 8,
            max_bytes: 1024 * 1024,
            default_ttl: Duration::from_secs(60),
            inline_body_limit_bytes: 64 * 1024,
        })
        .await;
        let req = request("/replace");

        assert!(store.put(&req, response("first")).await?);
        assert!(store.put(&req, response("second")).await?);

        match store.get(&req).await? {
            CacheLookup::Fresh(value) => assert_eq!(value.body, b"second"),
            other => panic!("expected fresh second value, got {:?}", other),
        }
        assert!(store.delete(&req).await?);
        assert!(matches!(store.get(&req).await?, CacheLookup::Miss));

        Ok(())
    }
}
