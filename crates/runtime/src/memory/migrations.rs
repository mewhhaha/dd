async fn ensure_schema(database: &Database) -> Result<()> {
    const MAX_ATTEMPTS: usize = 8;

    let mut conn = database.connect().map_err(memory_error)?;
    configure_connection(&conn).await?;
    ensure_mvcc_mode(&conn).await?;
    let applied_at_ms = epoch_ms_i64()?;

    for attempt in 0..MAX_ATTEMPTS {
        let tx = match conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
        {
            Ok(tx) => tx,
            Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                sleep_memory_schema_retry(attempt).await;
                continue;
            }
            Err(error) => return Err(memory_error_after_retry(error)),
        };

        match migrate_memory_schema_transaction(&tx, applied_at_ms).await {
            Ok(()) => match tx.commit().await {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_turso_error(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    sleep_memory_schema_retry(attempt).await;
                }
                Err(error) => return Err(memory_error_after_retry(error)),
            },
            Err(error) => {
                let retryable = is_retryable_turso_error(&error);
                let _ = tx.rollback().await;
                if retryable && attempt + 1 < MAX_ATTEMPTS {
                    sleep_memory_schema_retry(attempt).await;
                    continue;
                }
                return Err(memory_error_after_retry(error));
            }
        }
    }

    Err(PlatformError::runtime(
        "memory store error: schema migration failed after retries",
    ))
}

async fn migrate_memory_schema_transaction(
    conn: &Connection,
    applied_at_ms: i64,
) -> turso::Result<()> {
    ensure_storage_migration_table(conn).await?;
    let applied_version = storage_schema_version(conn, "memory").await?;
    if applied_version > MEMORY_SCHEMA_VERSION {
        return Err(turso::Error::Error(format!(
            "unsupported memory schema version {applied_version}; maximum supported version is {MEMORY_SCHEMA_VERSION}"
        )));
    }

    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_state (
          entity_key TEXT NOT NULL,
          item_key TEXT NOT NULL,
          value TEXT NOT NULL,
          value_blob BLOB,
          encoding TEXT NOT NULL DEFAULT 'utf8',
          deleted INTEGER NOT NULL DEFAULT 0,
          version INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          PRIMARY KEY (entity_key, item_key)
        )",
        (),
    )
    .await?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_meta (
          entity_key TEXT NOT NULL PRIMARY KEY,
          max_version INTEGER NOT NULL,
          owner_epoch INTEGER NOT NULL DEFAULT 0,
          updated_at_ms INTEGER NOT NULL
        )",
        (),
    )
    .await?;
    ensure_compat_columns(conn).await?;
    conn.execute("DROP INDEX IF EXISTS idx_memory_state_lookup", ())
        .await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_state_list
         ON memory_state(entity_key, deleted, item_key)",
        (),
    )
    .await?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_commands (
          entity_key TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          result_blob BLOB NOT NULL,
          revision INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          PRIMARY KEY (entity_key, idempotency_key)
        )",
        (),
    )
    .await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_commands_entity
         ON memory_commands(entity_key, updated_at_ms)",
        (),
    )
    .await?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS memory_outbox (
          effect_id TEXT PRIMARY KEY,
          entity_key TEXT NOT NULL,
          revision INTEGER NOT NULL,
          kind TEXT NOT NULL,
          payload_blob BLOB NOT NULL,
          status TEXT NOT NULL,
          attempt_count INTEGER NOT NULL,
          next_attempt_at_ms INTEGER NOT NULL,
          created_at_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL
        )",
        (),
    )
    .await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_outbox_entity_revision
         ON memory_outbox(entity_key, revision, effect_id)",
        (),
    )
    .await?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_outbox_due
         ON memory_outbox(entity_key, status, next_attempt_at_ms, revision, effect_id)",
        (),
    )
    .await?;
    record_storage_schema_version(conn, "memory", MEMORY_SCHEMA_VERSION, applied_at_ms).await?;
    Ok(())
}

async fn sleep_memory_schema_retry(attempt: usize) {
    record_storage_retry();
    tokio::time::sleep(Duration::from_millis(5 * (attempt + 1) as u64)).await;
}

async fn migrate_existing_memory_databases(root_dir: &Path) -> Result<()> {
    for shard_file in discover_legacy_memory_shard_files(root_dir)? {
        let path = shard_file.path.to_string_lossy().to_string();
        let database = Builder::new_local(&path)
            .build()
            .await
            .map_err(memory_error)?;
        ensure_schema(&database).await?;
    }
    Ok(())
}

struct MemoryDurabilityFloors {
    version_floors: Vec<u64>,
    owner_epoch_floor: u64,
}

async fn detect_memory_floors(
    root_dir: &Path,
    namespace_shards: usize,
) -> Result<MemoryDurabilityFloors> {
    let mut max_versions = vec![0u64; namespace_shards];
    let mut max_owner_epoch = 0u64;
    let entries = match std::fs::read_dir(root_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MemoryDurabilityFloors {
                version_floors: vec![1; namespace_shards],
                owner_epoch_floor: 1,
            });
        }
        Err(error) => return Err(memory_error(error)),
    };
    for entry in entries {
        let entry = entry.map_err(memory_error)?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let shard_entries = std::fs::read_dir(&path).map_err(memory_error)?;
        for shard_entry in shard_entries {
            let shard_entry = shard_entry.map_err(memory_error)?;
            let shard_path = shard_entry.path();
            if shard_path.extension().and_then(|ext| ext.to_str()) != Some("db") {
                continue;
            }
            let Some(shard_index) = shard_index_from_db_path(&shard_path) else {
                continue;
            };
            if shard_index >= namespace_shards {
                continue;
            }
            let path_str = shard_path.to_string_lossy().to_string();
            let database = Builder::new_local(&path_str)
                .build()
                .await
                .map_err(memory_error)?;
            let conn = database.connect().map_err(memory_error)?;
            configure_connection(&conn).await?;
            max_versions[shard_index] = max_versions[shard_index].max(
                read_single_i64(&conn, "SELECT MAX(max_version) FROM memory_meta").await? as u64,
            );
            max_owner_epoch = max_owner_epoch.max(
                read_single_i64(&conn, "SELECT MAX(owner_epoch) FROM memory_meta").await? as u64,
            );
        }
    }
    Ok(MemoryDurabilityFloors {
        version_floors: max_versions
            .into_iter()
            .map(|version| version.saturating_add(1).max(1))
            .collect(),
        owner_epoch_floor: max_owner_epoch.saturating_add(1).max(1),
    })
}
