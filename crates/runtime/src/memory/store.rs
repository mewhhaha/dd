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
        let floors = detect_memory_floors(&root_dir, namespace_shards).await?;
        let shards = floors
            .version_floors
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
            db_peak_connections: Arc::new(AtomicUsize::new(0)),
            namespace_shards,
            shard_hash_version: layout.shard_hash_version,
            namespace_shard_hash_versions: Arc::new(layout.namespace_shard_hash_versions),
            namespace_key_shard_overrides: Arc::new(layout.namespace_key_shard_overrides),
            snapshot_cache_max_entries: db_cache_max_open.max(64),
            snapshot_cache_max_bytes: db_cache_max_open.max(64).saturating_mul(64 * 1024),
            owner_epoch_floor: Arc::new(AtomicU64::new(floors.owner_epoch_floor.max(1))),
            profile: Arc::new(MemoryProfile::default()),
        };
        Ok(store)
    }

    pub fn owner_epoch_floor(&self) -> u64 {
        self.owner_epoch_floor.load(Ordering::Relaxed)
    }

    pub fn set_profile_enabled(&self, enabled: bool) {
        self.profile.set_enabled(enabled);
    }

    pub fn record_profile(&self, metric: MemoryProfileMetricKind, duration_us: u64, items: u64) {
        if metric == MemoryProfileMetricKind::StoreWriterBusyRetry {
            record_storage_retry();
        }
        self.profile.record(metric, duration_us, items);
    }

    pub fn take_profile_snapshot_and_reset(&self) -> MemoryProfileSnapshot {
        self.profile.take_snapshot_and_reset()
    }

    pub fn reset_profile(&self) {
        self.profile.reset();
    }

    pub fn namespace_shards(&self) -> usize {
        self.namespace_shards
    }

    pub(crate) async fn checkpoint_all_databases(&self) -> Result<usize> {
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

    pub(crate) async fn health_check(&self) -> Result<()> {
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

    pub fn shard_index_for_key(&self, namespace: &str, memory_key: &str) -> usize {
        self.shard_index(namespace, memory_key)
    }

    pub async fn snapshot(&self, namespace: &str, memory_key: &str) -> Result<MemorySnapshot> {
        let started = Instant::now();
        if let Some(snapshot) = self.cached_full_snapshot(namespace, memory_key).await {
            self.observe_version(namespace, memory_key, snapshot.max_version);
            self.observe_memory_version(namespace, memory_key, snapshot.max_version)
                .await;
            self.record_profile(
                MemoryProfileMetricKind::StoreSnapshot,
                started.elapsed().as_micros() as u64,
                snapshot.entries.len() as u64,
            );
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
        self.record_profile(
            MemoryProfileMetricKind::StoreSnapshot,
            started.elapsed().as_micros() as u64,
            snapshot.entries.len() as u64,
        );
        Ok(snapshot)
    }

    pub async fn point_read(
        &self,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
    ) -> Result<MemoryPointRead> {
        let started = Instant::now();
        let item_key = item_key.trim();
        if item_key.is_empty() {
            return Err(PlatformError::runtime("memory item key must not be empty"));
        }
        if let Some(point) = self
            .cached_point_read(namespace, memory_key, item_key)
            .await
        {
            self.observe_version(namespace, memory_key, point.max_version);
            self.observe_memory_version(namespace, memory_key, point.max_version)
                .await;
            self.record_profile(
                MemoryProfileMetricKind::StoreRead,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok(point);
        }

        let conn = self.connect(namespace, memory_key).await?;
        let record = self.record_for_key(&conn, memory_key, item_key).await?;
        let max_version = self
            .max_version_for_memory(&conn, memory_key)
            .await?
            .unwrap_or(-1);
        self.observe_version(namespace, memory_key, max_version);
        self.observe_memory_version(namespace, memory_key, max_version)
            .await;
        self.put_partial_snapshot(
            namespace,
            memory_key,
            max_version,
            record.clone().into_iter().collect::<Vec<_>>(),
            std::iter::once(item_key.to_string()),
            false,
        )
        .await;
        self.record_profile(
            MemoryProfileMetricKind::StoreRead,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok(MemoryPointRead {
            record,
            max_version,
        })
    }

    pub async fn snapshot_keys(
        &self,
        namespace: &str,
        memory_key: &str,
        keys: &[String],
    ) -> Result<MemorySnapshot> {
        if keys.is_empty() {
            return self.snapshot(namespace, memory_key).await;
        }
        let started = Instant::now();
        let filtered_keys = keys
            .iter()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty())
            .collect::<Vec<_>>();
        if filtered_keys.is_empty() {
            let conn = self.connect(namespace, memory_key).await?;
            let max_version = self
                .max_version_for_memory(&conn, memory_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(namespace, memory_key, max_version);
            return Ok(MemorySnapshot {
                entries: Vec::new(),
                max_version,
            });
        }
        if let Some(snapshot) = self
            .cached_keys_snapshot(namespace, memory_key, &filtered_keys)
            .await
        {
            self.record_profile(
                MemoryProfileMetricKind::StoreSnapshotKeys,
                started.elapsed().as_micros() as u64,
                filtered_keys.len() as u64,
            );
            return Ok(snapshot);
        }
        let conn = self.connect(namespace, memory_key).await?;
        let placeholders = (0..filtered_keys.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT item_key, value_blob, encoding, value, version, deleted
             FROM memory_state
             WHERE entity_key = ?1 AND item_key IN ({placeholders})
             ORDER BY item_key ASC"
        );
        let mut params = Vec::with_capacity(filtered_keys.len() + 1);
        params.push(Value::Text(memory_key.to_string()));
        params.extend(
            filtered_keys
                .iter()
                .map(|key| Value::Text((*key).to_string())),
        );
        let mut entries = Vec::new();
        let mut rows = conn.query(&sql, params).await.map_err(memory_error)?;
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            let key: String = row.get::<String>(0).map_err(memory_error)?;
            let value_blob: Option<Vec<u8>> =
                row.get::<Option<Vec<u8>>>(1).map_err(memory_error)?;
            let encoding: String = row.get::<String>(2).map_err(memory_error)?;
            let legacy_value: String = row.get::<String>(3).map_err(memory_error)?;
            let version: i64 = row.get::<i64>(4).map_err(memory_error)?;
            let deleted: i64 = row.get::<i64>(5).map_err(memory_error)?;
            entries.push(MemorySnapshotEntry {
                key,
                value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
                encoding: normalize_encoding(&encoding),
                version,
                deleted: deleted != 0,
            });
        }
        let max_version = self
            .max_version_for_memory(&conn, memory_key)
            .await?
            .unwrap_or(-1);
        self.observe_version(namespace, memory_key, max_version);
        self.observe_memory_version(namespace, memory_key, max_version)
            .await;
        self.put_partial_snapshot(
            namespace,
            memory_key,
            max_version,
            entries.clone(),
            filtered_keys.iter().cloned(),
            false,
        )
        .await;
        self.record_profile(
            MemoryProfileMetricKind::StoreSnapshotKeys,
            started.elapsed().as_micros() as u64,
            filtered_keys.len() as u64,
        );
        Ok(MemorySnapshot {
            entries,
            max_version,
        })
    }

    pub async fn version_if_newer(
        &self,
        namespace: &str,
        memory_key: &str,
        known_version: i64,
    ) -> Result<Option<i64>> {
        let started = Instant::now();
        let memory_key = memory_key.trim();
        if memory_key.is_empty() {
            return Err(PlatformError::runtime("memory key must not be empty"));
        }
        let version_key = Self::memory_version_key(namespace, memory_key);
        let cached_version = {
            let (_, state) = self.lock_entity_cache_stripe(namespace, memory_key).await;
            state.memory_versions.get(&version_key).copied()
        };
        if let Some(current) = cached_version {
            self.record_profile(
                MemoryProfileMetricKind::StoreVersionIfNewer,
                started.elapsed().as_micros() as u64,
                1,
            );
            return Ok((current > known_version).then_some(current));
        }
        let conn = self.connect(namespace, memory_key).await?;
        let current = self
            .max_version_for_memory(&conn, memory_key)
            .await?
            .unwrap_or(-1);
        self.observe_memory_version(namespace, memory_key, current)
            .await;
        self.record_profile(
            MemoryProfileMetricKind::StoreVersionIfNewer,
            started.elapsed().as_micros() as u64,
            1,
        );
        Ok((current > known_version).then_some(current))
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
        let started = Instant::now();
        if mutations.is_empty() && command_result.is_none() && outbox_effects.is_empty() {
            let conn = self.connect(namespace, memory_key).await?;
            let max_version = self
                .max_version_for_memory(&conn, memory_key)
                .await?
                .unwrap_or(-1);
            self.observe_version(namespace, memory_key, max_version);
            self.record_profile(
                MemoryProfileMetricKind::StoreApplyBatch,
                started.elapsed().as_micros() as u64,
                1,
            );
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
                    self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => {
                    conn.discard();
                    return Err(memory_error_after_retry(error));
                }
            }

            let outcome: MemoryTransactionResult<MemoryBatchCommitOutcome> = async {
                let validate_started = Instant::now();
                let (current, current_owner_epoch) =
                    self.memory_meta_for_commit(&conn, memory_key).await?;
                let current = current.unwrap_or(-1);
                validate_owner_epoch(current_owner_epoch, owner_epoch)?;
                self.record_profile(
                    MemoryProfileMetricKind::StoreApplyBatchValidate,
                    validate_started.elapsed().as_micros() as u64,
                    1,
                );

                let write_started = Instant::now();
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
                self.record_profile(
                    MemoryProfileMetricKind::StoreApplyBatchWrite,
                    write_started.elapsed().as_micros() as u64,
                    mutations.len() as u64 + 1,
                );
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
                            self.record_profile(
                                MemoryProfileMetricKind::StoreWriterBusyRetry,
                                0,
                                1,
                            );
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
                    self.record_profile(
                        MemoryProfileMetricKind::StoreApplyBatch,
                        started.elapsed().as_micros() as u64,
                        mutations.len() as u64 + 1,
                    );
                    return Ok(result);
                }
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if error.is_retryable() && attempt < 8 {
                        self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
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

    pub async fn command_result(
        &self,
        namespace: &str,
        memory_key: &str,
        idempotency_key: &str,
    ) -> Result<Option<MemoryCommandResult>> {
        let key = idempotency_key.trim();
        if key.is_empty() {
            return Ok(None);
        }
        let conn = self.connect(namespace, memory_key).await?;
        let mut rows = query_cached(
            &conn,
            "SELECT result_blob, revision
                 FROM memory_commands
                 WHERE entity_key = ?1 AND idempotency_key = ?2
                 LIMIT 1",
            (memory_key, key),
        )
        .await
        .map_err(memory_error)?;
        let Some(row) = rows.next().await.map_err(memory_error)? else {
            return Ok(None);
        };
        let result = row.get::<Vec<u8>>(0).map_err(memory_error)?;
        let revision = row.get::<i64>(1).map_err(memory_error)?;
        let _ = rows.next().await.map_err(memory_error)?;
        Ok(Some(MemoryCommandResult { result, revision }))
    }

    #[allow(dead_code)]
    pub async fn outbox_records(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Result<Vec<MemoryOutboxRecord>> {
        let conn = self.connect(namespace, memory_key).await?;
        let mut rows = query_cached(
            &conn,
                "SELECT effect_id, kind, payload_blob, revision, status, attempt_count, next_attempt_at_ms
                 FROM memory_outbox
                 WHERE entity_key = ?1
                 ORDER BY revision, effect_id",
                (memory_key,),
            )
            .await
            .map_err(memory_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            records.push(MemoryOutboxRecord {
                effect_id: row.get::<String>(0).map_err(memory_error)?,
                kind: row.get::<String>(1).map_err(memory_error)?,
                payload: row.get::<Vec<u8>>(2).map_err(memory_error)?,
                revision: row.get::<i64>(3).map_err(memory_error)?,
                status: row.get::<String>(4).map_err(memory_error)?,
                attempt_count: row.get::<i64>(5).map_err(memory_error)?,
                next_attempt_at_ms: row.get::<i64>(6).map_err(memory_error)?,
            });
        }
        Ok(records)
    }

    #[allow(dead_code)]
    pub async fn claim_outbox_records(
        &self,
        namespace: &str,
        memory_key: &str,
        limit: usize,
        lease_for: Duration,
    ) -> Result<Vec<MemoryOutboxRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now_ms = epoch_ms_i64()?;
        let lease_until_ms = now_ms.saturating_add(duration_ms_i64(lease_for)?);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut conn = self.writer_connection(namespace, memory_key).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                    self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => {
                    conn.discard();
                    return Err(memory_error_after_retry(error));
                }
            }

            let outcome = async {
                let mut rows = query_cached(
                    &conn,
                        "SELECT effect_id, kind, payload_blob, revision, status, attempt_count, next_attempt_at_ms
                         FROM memory_outbox
                         WHERE entity_key = ?1
                           AND status IN ('pending', 'inflight')
                           AND next_attempt_at_ms <= ?2
                         ORDER BY revision, effect_id
                         LIMIT ?3",
                        (memory_key, now_ms, limit),
                    )
                    .await?;
                let mut records = Vec::new();
                while let Some(row) = rows.next().await? {
                    records.push(MemoryOutboxRecord {
                        effect_id: row.get::<String>(0)?,
                        kind: row.get::<String>(1)?,
                        payload: row.get::<Vec<u8>>(2)?,
                        revision: row.get::<i64>(3)?,
                        status: "inflight".to_string(),
                        attempt_count: row.get::<i64>(5)?.saturating_add(1),
                        next_attempt_at_ms: lease_until_ms,
                    });
                }
                for record in &records {
                    execute_cached(
                        &conn,
                        "UPDATE memory_outbox
                         SET status = 'inflight',
                             attempt_count = ?1,
                             next_attempt_at_ms = ?2,
                             updated_at_ms = ?3
                         WHERE effect_id = ?4
                           AND entity_key = ?5
                           AND status IN ('pending', 'inflight')
                           AND next_attempt_at_ms <= ?3",
                        (
                            record.attempt_count,
                            lease_until_ms,
                            now_ms,
                            record.effect_id.as_str(),
                            memory_key,
                        ),
                    )
                    .await?;
                }
                Ok::<_, MemoryTransactionError>(records)
            }
            .await;

            match outcome {
                Ok(records) => match conn.execute("COMMIT", ()).await {
                    Ok(_) => return Ok(records),
                    Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        conn.discard();
                        return Err(memory_error_after_retry(error));
                    }
                },
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if error.is_retryable() && attempt < 8 {
                        self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
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

    pub async fn claim_due_outbox_records(
        &self,
        limit: usize,
        lease_for: Duration,
        kinds: &[&str],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || kinds.is_empty() {
            return Ok(Vec::new());
        }
        let (exact_kinds, kind_prefixes) = normalize_outbox_kind_selectors(kinds);
        if exact_kinds.is_empty() && kind_prefixes.is_empty() {
            return Ok(Vec::new());
        }
        let mut claims = Vec::new();
        for shard_index in 0..self.namespace_shards {
            if claims.len() >= limit {
                break;
            }
            let remaining = limit.saturating_sub(claims.len());
            let mut shard_claims = self
                .claim_due_outbox_records_for_shard_index_with_selectors(
                    shard_index,
                    remaining,
                    lease_for,
                    &exact_kinds,
                    &kind_prefixes,
                )
                .await?;
            claims.append(&mut shard_claims);
        }
        Ok(claims)
    }

    pub async fn claim_due_outbox_records_for_shard_index(
        &self,
        shard_index: usize,
        limit: usize,
        lease_for: Duration,
        kinds: &[&str],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || kinds.is_empty() {
            return Ok(Vec::new());
        }
        let (exact_kinds, kind_prefixes) = normalize_outbox_kind_selectors(kinds);
        if exact_kinds.is_empty() && kind_prefixes.is_empty() {
            return Ok(Vec::new());
        }
        self.claim_due_outbox_records_for_shard_index_with_selectors(
            shard_index,
            limit,
            lease_for,
            &exact_kinds,
            &kind_prefixes,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn mark_outbox_delivered(
        &self,
        namespace: &str,
        memory_key: &str,
        effect_id: &str,
    ) -> Result<()> {
        let effect_id = effect_id.trim();
        if effect_id.is_empty() {
            return Err(PlatformError::bad_request(
                "memory outbox effect_id is required",
            ));
        }
        let now_ms = epoch_ms_i64()?;
        let conn = self.writer_connection(namespace, memory_key).await?;
        execute_cached(
            &conn,
            "UPDATE memory_outbox
             SET status = 'delivered',
                 updated_at_ms = ?1
             WHERE entity_key = ?2
               AND effect_id = ?3",
            (now_ms, memory_key, effect_id),
        )
        .await
        .map_err(memory_error)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn retry_outbox_record(
        &self,
        namespace: &str,
        memory_key: &str,
        effect_id: &str,
        retry_after: Duration,
    ) -> Result<()> {
        let effect_id = effect_id.trim();
        if effect_id.is_empty() {
            return Err(PlatformError::bad_request(
                "memory outbox effect_id is required",
            ));
        }
        let now_ms = epoch_ms_i64()?;
        let next_attempt_at_ms = now_ms.saturating_add(duration_ms_i64(retry_after)?);
        let conn = self.writer_connection(namespace, memory_key).await?;
        execute_cached(
            &conn,
            "UPDATE memory_outbox
             SET status = 'pending',
                 next_attempt_at_ms = ?1,
                 updated_at_ms = ?2
             WHERE entity_key = ?3
               AND effect_id = ?4",
            (next_attempt_at_ms, now_ms, memory_key, effect_id),
        )
        .await
        .map_err(memory_error)?;
        Ok(())
    }

    pub async fn apply_outbox_delivery_outcomes(
        &self,
        outcomes: &[MemoryOutboxDeliveryOutcome],
    ) -> Result<()> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let mut grouped: BTreeMap<(String, String), Vec<&MemoryOutboxDeliveryOutcome>> =
            BTreeMap::new();
        for outcome in outcomes {
            if outcome.effect_id.trim().is_empty() {
                return Err(PlatformError::bad_request(
                    "memory outbox effect_id is required",
                ));
            }
            grouped
                .entry((outcome.namespace.clone(), outcome.memory_key.clone()))
                .or_default()
                .push(outcome);
        }

        for ((namespace, memory_key), entity_outcomes) in grouped {
            let mut conn = self.writer_connection(&namespace, &memory_key).await?;
            let mut attempt = 0usize;
            loop {
                attempt += 1;
                match conn.execute("BEGIN IMMEDIATE", ()).await {
                    Ok(_) => {}
                    Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                        self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        conn.discard();
                        return Err(memory_error_after_retry(error));
                    }
                }

                let outcome = async {
                    let now_ms = epoch_ms_i64()?;
                    for delivery in &entity_outcomes {
                        match delivery.action {
                            MemoryOutboxDeliveryAction::Delivered
                            | MemoryOutboxDeliveryAction::DroppedTerminal => {
                                execute_cached(
                                    &conn,
                                    "UPDATE memory_outbox
                                     SET status = 'delivered',
                                         updated_at_ms = ?1
                                     WHERE entity_key = ?2
                                       AND effect_id = ?3",
                                    (now_ms, memory_key.as_str(), delivery.effect_id.as_str()),
                                )
                                .await?;
                            }
                            MemoryOutboxDeliveryAction::Retry { retry_after } => {
                                let next_attempt_at_ms =
                                    now_ms.saturating_add(duration_ms_i64(retry_after)?);
                                execute_cached(
                                    &conn,
                                    "UPDATE memory_outbox
                                     SET status = 'pending',
                                         next_attempt_at_ms = ?1,
                                         updated_at_ms = ?2
                                     WHERE entity_key = ?3
                                       AND effect_id = ?4",
                                    (
                                        next_attempt_at_ms,
                                        now_ms,
                                        memory_key.as_str(),
                                        delivery.effect_id.as_str(),
                                    ),
                                )
                                .await?;
                            }
                        }
                    }
                    Ok::<_, MemoryTransactionError>(())
                }
                .await;

                match outcome {
                    Ok(()) => match conn.execute("COMMIT", ()).await {
                        Ok(_) => break,
                        Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                            let _ = conn.execute("ROLLBACK", ()).await;
                            self.record_profile(
                                MemoryProfileMetricKind::StoreWriterBusyRetry,
                                0,
                                1,
                            );
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
                    },
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        if error.is_retryable() && attempt < 8 {
                            self.record_profile(
                                MemoryProfileMetricKind::StoreWriterBusyRetry,
                                0,
                                1,
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                5 * attempt as u64,
                            ))
                            .await;
                            continue;
                        }
                        conn.discard();
                        return Err(error.into());
                    }
                }
            }
        }
        Ok(())
    }

    async fn claim_due_outbox_records_for_shard_index_with_selectors(
        &self,
        shard_index: usize,
        limit: usize,
        lease_for: Duration,
        exact_kinds: &[String],
        kind_prefixes: &[String],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || (exact_kinds.is_empty() && kind_prefixes.is_empty()) {
            return Ok(Vec::new());
        }
        let shard_index = shard_index % self.namespace_shards.max(1);
        let mut claims = Vec::new();
        let namespaces = self.discover_namespaces_for_shard(shard_index).await?;
        for namespace in namespaces {
            if claims.len() >= limit {
                break;
            }
            let remaining = limit.saturating_sub(claims.len());
            let mut namespace_claims = self
                .claim_due_outbox_records_for_namespace_shard(
                    &namespace,
                    shard_index,
                    remaining,
                    lease_for,
                    exact_kinds,
                    kind_prefixes,
                )
                .await?;
            claims.append(&mut namespace_claims);
        }
        Ok(claims)
    }

    async fn claim_due_outbox_records_for_namespace_shard(
        &self,
        namespace: &str,
        shard_index: usize,
        limit: usize,
        lease_for: Duration,
        exact_kinds: &[String],
        kind_prefixes: &[String],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if limit == 0 || (exact_kinds.is_empty() && kind_prefixes.is_empty()) {
            return Ok(Vec::new());
        }
        let now_ms = epoch_ms_i64()?;
        let lease_until_ms = now_ms.saturating_add(duration_ms_i64(lease_for)?);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut kind_selectors = Vec::with_capacity(exact_kinds.len() + kind_prefixes.len());
        let mut next_placeholder = 2usize;
        for _ in exact_kinds {
            kind_selectors.push(format!("kind = ?{next_placeholder}"));
            next_placeholder += 1;
        }
        for _ in kind_prefixes {
            kind_selectors.push(format!("kind LIKE ?{next_placeholder} ESCAPE '\\'"));
            next_placeholder += 1;
        }
        let kind_filter = kind_selectors.join(" OR ");
        let limit_placeholder = next_placeholder;
        let select_sql = format!(
            "SELECT entity_key, effect_id, kind, payload_blob, revision, status, attempt_count, next_attempt_at_ms
             FROM memory_outbox
             WHERE status IN ('pending', 'inflight')
               AND next_attempt_at_ms <= ?1
               AND ({kind_filter})
             ORDER BY revision, effect_id
             LIMIT ?{limit_placeholder}"
        );
        let mut select_params = Vec::with_capacity(exact_kinds.len() + kind_prefixes.len() + 2);
        select_params.push(Value::Integer(now_ms));
        select_params.extend(exact_kinds.iter().map(|kind| Value::Text(kind.clone())));
        select_params.extend(
            kind_prefixes
                .iter()
                .map(|prefix| Value::Text(format!("{}%", escape_sql_like_prefix(prefix)))),
        );
        select_params.push(Value::Integer(limit));
        let mut conn = self.writer_connection_shard(namespace, shard_index).await?;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match conn.execute("BEGIN IMMEDIATE", ()).await {
                Ok(_) => {}
                Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                    self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64)).await;
                    continue;
                }
                Err(error) => {
                    conn.discard();
                    return Err(memory_error_after_retry(error));
                }
            }

            let outcome = async {
                let mut rows = conn.query(&select_sql, select_params.clone()).await?;
                let mut claims = Vec::new();
                while let Some(row) = rows.next().await? {
                    let memory_key = row.get::<String>(0)?;
                    claims.push(MemoryOutboxClaim {
                        namespace: namespace.to_string(),
                        memory_key,
                        record: MemoryOutboxRecord {
                            effect_id: row.get::<String>(1)?,
                            kind: row.get::<String>(2)?,
                            payload: row.get::<Vec<u8>>(3)?,
                            revision: row.get::<i64>(4)?,
                            status: "inflight".to_string(),
                            attempt_count: row.get::<i64>(6)?.saturating_add(1),
                            next_attempt_at_ms: lease_until_ms,
                        },
                    });
                }
                for claim in &claims {
                    execute_cached(
                        &conn,
                        "UPDATE memory_outbox
                         SET status = 'inflight',
                             attempt_count = ?1,
                             next_attempt_at_ms = ?2,
                             updated_at_ms = ?3
                         WHERE effect_id = ?4
                           AND entity_key = ?5
                           AND kind = ?6
                           AND status IN ('pending', 'inflight')
                           AND next_attempt_at_ms <= ?3",
                        (
                            claim.record.attempt_count,
                            lease_until_ms,
                            now_ms,
                            claim.record.effect_id.as_str(),
                            claim.memory_key.as_str(),
                            claim.record.kind.as_str(),
                        ),
                    )
                    .await?;
                }
                Ok::<_, MemoryTransactionError>(claims)
            }
            .await;

            match outcome {
                Ok(claims) => match conn.execute("COMMIT", ()).await {
                    Ok(_) => return Ok(claims),
                    Err(error) if is_retryable_memory_error(&error) && attempt < 8 => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempt as u64))
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = conn.execute("ROLLBACK", ()).await;
                        conn.discard();
                        return Err(memory_error_after_retry(error));
                    }
                },
                Err(error) => {
                    let _ = conn.execute("ROLLBACK", ()).await;
                    if error.is_retryable() && attempt < 8 {
                        self.record_profile(MemoryProfileMetricKind::StoreWriterBusyRetry, 0, 1);
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
