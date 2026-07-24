async fn read_single_i64(conn: &Connection, sql: &str) -> Result<i64> {
    let mut rows = match conn.query(sql, ()).await {
        Ok(rows) => rows,
        Err(error) => {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("no such table") || message.contains("no such column") {
                return Ok(0);
            }
            return Err(memory_error(error));
        }
    };
    let Some(row) = rows.next().await.map_err(memory_error)? else {
        return Ok(0);
    };
    let value = row
        .get::<Option<i64>>(0)
        .map_err(memory_error)?
        .unwrap_or(0)
        .max(0);
    let _ = rows.next().await.map_err(memory_error)?;
    Ok(value)
}

async fn configure_connection(conn: &Connection) -> Result<()> {
    configure_turso_connection(conn, memory_error)?;
    conn.pragma_update("synchronous", "'FULL'")
        .await
        .map_err(memory_error)?;
    Ok(())
}

async fn ensure_mvcc_mode(conn: &Connection) -> Result<()> {
    conn.pragma_update("journal_mode", "'mvcc'")
        .await
        .map_err(memory_error)?;
    let mut rows = conn
        .query("PRAGMA journal_mode", ())
        .await
        .map_err(memory_error)?;
    let Some(row) = rows.next().await.map_err(memory_error)? else {
        return Err(PlatformError::runtime(
            "memory store error: failed to read journal_mode",
        ));
    };
    let mode = row.get::<String>(0).map_err(memory_error)?;
    let _ = rows.next().await.map_err(memory_error)?;
    if !mode.eq_ignore_ascii_case("mvcc") {
        return Err(PlatformError::runtime(format!(
            "memory store error: expected mvcc journal mode, got {mode}",
        )));
    }
    Ok(())
}

async fn ensure_compat_columns(conn: &Connection) -> turso::Result<()> {
    let mut rows = conn.query("PRAGMA table_info(memory_state)", ()).await?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await? {
        let name: String = row.get::<String>(1)?;
        columns.insert(name);
    }

    if !columns.contains("value_blob") {
        conn.execute("ALTER TABLE memory_state ADD COLUMN value_blob BLOB", ())
            .await?;
    }
    if !columns.contains("encoding") {
        conn.execute(
            "ALTER TABLE memory_state ADD COLUMN encoding TEXT NOT NULL DEFAULT 'utf8'",
            (),
        )
        .await?;
    }

    let mut rows = conn.query("PRAGMA table_info(memory_meta)", ()).await?;
    let mut columns = HashSet::new();
    while let Some(row) = rows.next().await? {
        let name: String = row.get::<String>(1)?;
        columns.insert(name);
    }
    if !columns.contains("owner_epoch") {
        conn.execute(
            "ALTER TABLE memory_meta ADD COLUMN owner_epoch INTEGER NOT NULL DEFAULT 0",
            (),
        )
        .await?;
    }
    Ok(())
}

fn is_retryable_memory_error(error: &turso::Error) -> bool {
    is_retryable_turso_error(error)
}

fn epoch_ms_i64() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| PlatformError::internal(format!("system clock error: {error}")))?;
    Ok(duration.as_millis() as i64)
}

fn memory_error(error: impl std::fmt::Display) -> PlatformError {
    PlatformError::runtime(format!("memory store error: {error}"))
}

fn memory_error_after_retry(error: turso::Error) -> PlatformError {
    if is_retryable_turso_error(&error) {
        PlatformError::storage_unavailable(format!("memory store error: {error}"))
    } else {
        memory_error(error)
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(memory_error)?;
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2).max(2));
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    if out.is_empty() {
        "00".to_string()
    } else {
        out
    }
}

fn normalize_encoding(raw: &str) -> String {
    match raw {
        ENCODING_UTF8 => ENCODING_UTF8.to_string(),
        ENCODING_V8SC => ENCODING_V8SC.to_string(),
        _ => ENCODING_UTF8.to_string(),
    }
}

fn snapshot_entries_from_records(
    records: &HashMap<String, MemorySnapshotEntry>,
) -> Vec<MemorySnapshotEntry> {
    let mut entries = records.values().cloned().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

fn partitioned_budget(total: usize, shards: usize, shard_index: usize) -> usize {
    let shards = shards.max(1);
    let base = total / shards;
    let remainder = total % shards;
    base + usize::from(shard_index % shards < remainder)
}

fn approximate_snapshot_cache_bytes(
    cache_key: &str,
    records: &HashMap<String, MemorySnapshotEntry>,
    loaded_keys: &HashSet<String>,
) -> usize {
    // This intentionally approximates allocator overhead while staying cheap
    // and deterministic. It accounts for cache key bytes, loaded-key strings,
    // record keys, value buffers, encodings, and a small per-entry overhead.
    let record_bytes = records
        .values()
        .map(|entry| {
            96usize
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.len())
                .saturating_add(entry.encoding.len())
        })
        .sum::<usize>();
    let loaded_key_bytes = loaded_keys
        .iter()
        .map(|key| 48usize.saturating_add(key.len()))
        .sum::<usize>();
    128usize
        .saturating_add(cache_key.len())
        .saturating_add(record_bytes)
        .saturating_add(loaded_key_bytes)
}

fn hex_decode_to_utf8(input: &str) -> Option<String> {
    if !input.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    let chars = input.as_bytes();
    let mut index = 0usize;
    while index < chars.len() {
        let hi = char::from(chars[index]).to_digit(16)?;
        let lo = char::from(chars[index + 1]).to_digit(16)?;
        bytes.push(((hi << 4) | lo) as u8);
        index += 2;
    }
    String::from_utf8(bytes).ok()
}

/// Stable storage-format hash for persisted memory shard routing.
pub fn stable_memory_shard_index(memory_key: &str, namespace_shards: usize) -> usize {
    if namespace_shards <= 1 {
        return 0;
    }
    let mut hasher = Sha256::new();
    hasher.update(MEMORY_SHARD_HASH_DOMAIN);
    hasher.update(memory_key.as_bytes());
    let digest = hasher.finalize();
    let mut shard_bytes = [0u8; 8];
    shard_bytes.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(shard_bytes) as usize) % namespace_shards
}

fn legacy_default_hasher_memory_shard_index(memory_key: &str, namespace_shards: usize) -> usize {
    if namespace_shards <= 1 {
        return 0;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    memory_key.hash(&mut hasher);
    (hasher.finish() as usize) % namespace_shards
}

fn memory_shard_index_for_hash_version(
    memory_key: &str,
    namespace_shards: usize,
    shard_hash_version: u32,
) -> Result<usize> {
    match shard_hash_version {
        STABLE_SHA256_MEMORY_SHARD_HASH_VERSION => {
            Ok(stable_memory_shard_index(memory_key, namespace_shards))
        }
        LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION => Ok(
            legacy_default_hasher_memory_shard_index(memory_key, namespace_shards),
        ),
        version => Err(PlatformError::runtime(format!(
            "unsupported memory shard_hash_version {version}; supported shard_hash_versions are {}",
            supported_memory_shard_hash_versions_label()
        ))),
    }
}

fn is_supported_memory_shard_hash_version(version: u32) -> bool {
    matches!(
        version,
        STABLE_SHA256_MEMORY_SHARD_HASH_VERSION | LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION
    )
}

fn supported_memory_shard_hash_versions_label() -> &'static str {
    "0 (legacy DefaultHasher), 1 (dd-memory-shard-v1)"
}

const ENCODING_UTF8: &str = "utf8";
const ENCODING_V8SC: &str = "v8sc";
