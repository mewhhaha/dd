fn validate_owner_epoch(current_owner_epoch: Option<i64>, owner_epoch: Option<i64>) -> Result<()> {
    let Some(owner_epoch) = owner_epoch else {
        return Ok(());
    };
    let owner_epoch = owner_epoch.max(0);
    if owner_epoch == 0 {
        return Ok(());
    }
    let current_owner_epoch = current_owner_epoch.unwrap_or(0).max(0);
    if current_owner_epoch > owner_epoch {
        return Err(PlatformError::runtime(format!(
            "stale memory entity owner epoch {owner_epoch}; current owner epoch is {current_owner_epoch}"
        )));
    }
    Ok(())
}

#[cfg(test)]
struct MemoryEntityCacheDebugSnapshot {
    snapshot_keys: HashSet<String>,
    snapshot_entries: usize,
    snapshot_approximate_bytes: usize,
    version_entries: usize,
}

async fn upsert_memory_state_row(
    conn: &Connection,
    memory_key: &str,
    item_key: &str,
    value: &[u8],
    encoding: &str,
    deleted: bool,
    version: i64,
) -> MemoryTransactionResult<()> {
    let now_ms = epoch_ms_i64()?;
    if deleted {
        let empty_blob: &[u8] = &[];
        execute_cached(
            conn,
            "INSERT INTO memory_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
             VALUES (?1, ?2, '', ?3, ?4, 1, ?5, ?6)
             ON CONFLICT(entity_key, item_key) DO UPDATE SET
               value = excluded.value,
               value_blob = excluded.value_blob,
               encoding = excluded.encoding,
               deleted = 1,
               version = excluded.version,
               updated_at_ms = excluded.updated_at_ms",
            (
                memory_key,
                item_key,
                empty_blob,
                ENCODING_UTF8,
                version,
                now_ms,
            ),
        )
        .await?;
        return Ok(());
    }

    let value_text = if encoding == ENCODING_UTF8 {
        std::str::from_utf8(value)
            .map_err(|error| PlatformError::bad_request(format!("invalid utf8 value: {error}")))?
    } else {
        ""
    };
    execute_cached(
        conn,
        "INSERT INTO memory_state (entity_key, item_key, value, value_blob, encoding, deleted, version, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)
         ON CONFLICT(entity_key, item_key) DO UPDATE SET
           value = excluded.value,
           value_blob = excluded.value_blob,
           encoding = excluded.encoding,
           deleted = 0,
           version = excluded.version,
           updated_at_ms = excluded.updated_at_ms",
        (
            memory_key, item_key, value_text, value, encoding, version, now_ms,
        ),
    )
    .await?;
    Ok(())
}

async fn upsert_memory_meta_row(
    conn: &Connection,
    memory_key: &str,
    max_version: i64,
    owner_epoch: Option<i64>,
) -> MemoryTransactionResult<()> {
    let now_ms = epoch_ms_i64()?;
    let owner_epoch = owner_epoch.unwrap_or(0).max(0);
    execute_cached(
        conn,
        "INSERT INTO memory_meta (entity_key, max_version, owner_epoch, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(entity_key) DO UPDATE SET
           max_version = excluded.max_version,
           owner_epoch = CASE WHEN excluded.owner_epoch > 0 THEN excluded.owner_epoch ELSE memory_meta.owner_epoch END,
           updated_at_ms = excluded.updated_at_ms",
        (memory_key, max_version, owner_epoch, now_ms),
    )
    .await?;
    Ok(())
}

async fn insert_memory_command_result_row(
    conn: &Connection,
    memory_key: &str,
    idempotency_key: &str,
    result: &[u8],
    revision: i64,
) -> MemoryTransactionResult<()> {
    let now_ms = epoch_ms_i64()?;
    execute_cached(
        conn,
        "INSERT INTO memory_commands (entity_key, idempotency_key, result_blob, revision, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        (memory_key, idempotency_key, result, revision, now_ms),
    )
    .await?;
    Ok(())
}

async fn insert_memory_outbox_row(
    conn: &Connection,
    memory_key: &str,
    effect: &MemoryOutboxEffectWrite,
    revision: i64,
    ordinal: usize,
) -> MemoryTransactionResult<()> {
    let now_ms = epoch_ms_i64()?;
    let effect_id = memory_outbox_effect_id(memory_key, revision, ordinal, effect);
    execute_cached(
        conn,
        "INSERT INTO memory_outbox (
           effect_id,
           entity_key,
           revision,
           kind,
           payload_blob,
           status,
           attempt_count,
           next_attempt_at_ms,
           created_at_ms,
           updated_at_ms
         )
         VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?6, ?6)",
        (
            effect_id,
            memory_key,
            revision,
            effect.kind.trim(),
            effect.payload.as_slice(),
            now_ms,
        ),
    )
    .await?;
    Ok(())
}

fn memory_outbox_effect_id(
    memory_key: &str,
    revision: i64,
    ordinal: usize,
    effect: &MemoryOutboxEffectWrite,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(memory_key.as_bytes());
    hasher.update([0]);
    hasher.update(revision.to_be_bytes());
    hasher.update((ordinal as u64).to_be_bytes());
    hasher.update(effect.kind.trim().as_bytes());
    hasher.update([0]);
    hasher.update(effect.payload.as_slice());
    let digest = hasher.finalize();
    let mut id = String::with_capacity("memfx_".len() + 64);
    id.push_str("memfx_");
    for byte in digest {
        use std::fmt::Write;
        write!(&mut id, "{byte:02x}").expect("writing to String cannot fail");
    }
    id
}
