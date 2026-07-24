impl MemoryStore {
    pub async fn new(
        root_dir: PathBuf,
        namespace_shards: usize,
        db_cache_max_open: usize,
        db_idle_ttl: Duration,
    ) -> Result<Self> {
        Self::new_with_connection_limits(
            root_dir,
            namespace_shards,
            db_cache_max_open,
            db_idle_ttl,
            4,
            db_cache_max_open.saturating_mul(5).max(1),
        )
        .await
    }

    pub async fn new_with_connection_limits(
        root_dir: PathBuf,
        namespace_shards: usize,
        db_cache_max_open: usize,
        db_idle_ttl: Duration,
        db_read_connections_per_database: usize,
        db_max_total_connections: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(&root_dir).map_err(memory_error)?;
        if namespace_shards == 0 {
            return Err(PlatformError::internal(
                "memory_namespace_shards must be greater than 0",
            ));
        }
        if db_cache_max_open == 0 {
            return Err(PlatformError::internal(
                "memory_db_cache_max_open must be greater than 0",
            ));
        }
        if db_idle_ttl.is_zero() {
            return Err(PlatformError::internal(
                "memory_db_idle_ttl must be greater than 0",
            ));
        }
        if db_read_connections_per_database == 0 {
            return Err(PlatformError::internal(
                "memory_db_read_connections_per_database must be greater than 0",
            ));
        }
        if db_max_total_connections == 0 {
            return Err(PlatformError::internal(
                "memory_db_max_total_connections must be greater than 0",
            ));
        }
        let layout = load_or_adopt_memory_layout(&root_dir, namespace_shards).await?;
        if layout.namespace_shards != namespace_shards {
            return Err(memory_layout_mismatch_error(
                &root_dir,
                namespace_shards,
                layout.namespace_shards,
            ));
        }
        migrate_existing_memory_databases(&root_dir).await?;
        let version_floors = detect_memory_version_floors(&root_dir, namespace_shards).await?;
        let shards = version_floors
            .iter()
            .copied()
            .map(MemoryShard::new)
            .collect::<Vec<_>>();
        let store = Self {
            root_dir: Arc::new(root_dir),
            shards: Arc::from(shards),
            db_cache_max_open,
            db_idle_ttl,
            db_read_connections_per_database,
            db_connection_permits: Arc::new(Semaphore::new(db_max_total_connections)),
            db_live_connections: Arc::new(AtomicUsize::new(0)),
            namespace_shards,
            shard_hash_version: layout.shard_hash_version,
            namespace_shard_hash_versions: Arc::new(layout.namespace_shard_hash_versions),
            namespace_key_shard_overrides: Arc::new(layout.namespace_key_shard_overrides),
            snapshot_cache_max_entries: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES
                .max(namespace_shards.saturating_mul(MEMORY_ENTITY_CACHE_STRIPES)),
            snapshot_cache_max_bytes: DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_BYTES,
        };
        Ok(store)
    }

    pub async fn checkpoint_all_databases(&self) -> Result<usize> {
        let shard_files = discover_legacy_memory_shard_files(self.root_dir.as_ref())?;
        for shard_file in &shard_files {
            let path = shard_file.path.to_string_lossy().to_string();
            let database = Builder::new_local(&path)
                .build()
                .await
                .map_err(memory_error)?;
            checkpoint_database(&database).await.map_err(memory_error)?;
        }
        Ok(shard_files.len())
    }

    pub async fn health_check(&self) -> Result<()> {
        if !self.root_dir.is_dir() {
            return Err(PlatformError::internal(
                "memory storage directory is unavailable",
            ));
        }
        let mut databases = Vec::new();
        for shard in self.shards.iter() {
            let entries = shard.databases.lock().await;
            databases.extend(entries.entries.values().filter_map(|entry| {
                entry
                    .slot
                    .handle
                    .get()
                    .map(|handle| Arc::clone(&handle.database))
            }));
        }
        for database in &databases {
            health_check_database(database)
                .await
                .map_err(memory_error)?;
            let conn = database.connect().map_err(memory_error)?;
            configure_connection(&conn).await?;
            let version = storage_schema_version(&conn, "memory")
                .await
                .map_err(memory_error)?;
            if version != MEMORY_SCHEMA_VERSION {
                return Err(PlatformError::runtime(format!(
                    "memory store error: schema version {version} is not ready; expected {MEMORY_SCHEMA_VERSION}"
                )));
            }
        }
        Ok(())
    }

    pub async fn snapshot(&self, namespace: &str, memory_key: &str) -> Result<MemorySnapshot> {
        if let Some(snapshot) = self.cached_full_snapshot(namespace, memory_key).await {
            self.observe_version(namespace, memory_key, snapshot.max_version);
            self.observe_memory_version(namespace, memory_key, snapshot.max_version)
                .await;
            return Ok(snapshot);
        }
        let conn = self.connect(namespace, memory_key).await?;
        let mut rows = query_cached(
            &conn,
            "SELECT item_key, value_blob, encoding, value, version, deleted
                 FROM memory_state
                 WHERE entity_key = ?1
                 ORDER BY item_key ASC",
            (memory_key,),
        )
        .await
        .map_err(memory_error)?;

        let mut entries = Vec::new();
        let mut max_version = -1i64;
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            let key: String = row.get::<String>(0).map_err(memory_error)?;
            let value_blob: Option<Vec<u8>> =
                row.get::<Option<Vec<u8>>>(1).map_err(memory_error)?;
            let encoding: String = row.get::<String>(2).map_err(memory_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(memory_error)?;
            let version: i64 = row.get::<i64>(4).map_err(memory_error)?;
            let deleted: i64 = row.get::<i64>(5).map_err(memory_error)?;
            max_version = max_version.max(version);
            entries.push(MemorySnapshotEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
                version,
                deleted: deleted != 0,
            });
        }
        self.observe_version(namespace, memory_key, max_version);
        self.observe_memory_version(namespace, memory_key, max_version)
            .await;
        let snapshot = MemorySnapshot {
            entries,
            max_version,
        };
        self.put_full_snapshot(namespace, memory_key, &snapshot)
            .await;
        Ok(snapshot)
    }

    pub async fn apply_batch(
        &self,
        namespace: &str,
        memory_key: &str,
        mutations: &[MemoryBatchMutation],
        command_result: Option<&MemoryCommandResultWrite>,
        outbox_effects: &[MemoryOutboxEffectWrite],
        owner_epoch: Option<i64>,
    ) -> Result<MemoryBatchApplyResult> {
        if mutations.is_empty() && command_result.is_none() && outbox_effects.is_empty() {
            let conn = self.connect(namespace, memory_key).await?;
            let max_version = self
                .max_version_for_memory(&conn, memory_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(namespace, memory_key, max_version);
            return Ok(MemoryBatchApplyResult { max_version });
        }

        for mutation in mutations {
            if mutation.key.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory batch mutation key must not be empty",
                ));
            }
            if !mutation.deleted
                && mutation.encoding != ENCODING_UTF8
                && mutation.encoding != ENCODING_V8SC
            {
                return Err(PlatformError::bad_request(format!(
                    "unsupported memory storage encoding: {}",
                    mutation.encoding
                )));
            }
        }

        if let Some(command_result) = command_result {
            if command_result.idempotency_key.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory command idempotency key must not be empty",
                ));
            }
            if command_result.idempotency_key.len() > 512 {
                return Err(PlatformError::bad_request(
                    "memory command idempotency key must be at most 512 characters",
                ));
            }
        }
        for effect in outbox_effects {
            if effect.kind.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory outbox effect kind must not be empty",
                ));
            }
        }

        let mut conn = self.writer_connection(namespace, memory_key).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                    record_storage_retry();
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => {
                    conn.discard();
                    return Err(memory_error_after_retry(error));
                }
            }

            let outcome: MemoryTransactionResult<MemoryBatchCommitOutcome> = async {
                let (current, current_owner_epoch) =
                    self.memory_meta_for_commit(&conn, memory_key).await?;
                let current = current.unwrap_or(-1);
                validate_owner_epoch(current_owner_epoch, owner_epoch)?;

                let commit_version = if !mutations.is_empty() || !outbox_effects.is_empty() {
                    Some(self.reserve_version_after(namespace, memory_key, current))
                } else {
                    None
                };

                for mutation in mutations {
                    let version =
                        commit_version.expect("mutation commits must reserve a canonical version");
                    upsert_memory_state_row(
                        &conn,
                        memory_key,
                        mutation.key.as_str(),
                        mutation.value.as_slice(),
                        mutation.encoding.as_str(),
                        mutation.deleted,
                        version,
                    )
                    .await?;
                }

                let max_version = if let Some(version) = commit_version {
                    version
                } else {
                    current
                };
                if !mutations.is_empty() || !outbox_effects.is_empty() {
                    upsert_memory_meta_row(&conn, memory_key, max_version, owner_epoch).await?;
                }
                for (effect_ordinal, effect) in outbox_effects.iter().enumerate() {
                    insert_memory_outbox_row(
                        &conn,
                        memory_key,
                        effect,
                        max_version,
                        effect_ordinal,
                    )
                    .await?;
                }
                if let Some(command_result) = command_result {
                    insert_memory_command_result_row(
                        &conn,
                        memory_key,
                        command_result.idempotency_key.trim(),
                        &command_result.result,
                        max_version,
                    )
                    .await?;
                }
                let cache_mutations = mutations.to_vec();
                Ok(MemoryBatchCommitOutcome {
                    result: MemoryBatchApplyResult { max_version },
                    cache_mutations,
                })
            }
            .await;

            match outcome {
                Ok(outcome) => {
                    let result = outcome.result;
                    match conn.execute("COMMIT", ()).await {
                        Ok(_) => {}
                        Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            record_storage_retry();
                            tokio::time::sleep(std::time::Duration::from_millis(
                                5 * attempt as u64,
                            ))
                            .await;
                            continue;
                        }
                        Err(error) => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            conn.discard();
                            return Err(memory_error_after_retry(error));
                        }
                    }
                    self.observe_version(namespace, memory_key, result.max_version);
                    self.observe_memory_version(namespace, memory_key, result.max_version)
                        .await;
                    if !outcome.cache_mutations.is_empty() {
                        self.update_cached_snapshot_after_commit(
                            namespace,
                            memory_key,
                            result.max_version,
                            &outcome.cache_mutations,
                        )
                        .await;
                    }
                    return Ok(result);
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if error.is_retryable() && attempt < 8 {
                        record_storage_retry();
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    conn.discard();
                    return Err(error.into());
                }
            }
        }
    }
}
