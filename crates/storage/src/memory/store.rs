pub(crate) fn namespace_owner(namespace: &str) -> Result<(&str, &str)> {
    let invalid =
        || PlatformError::bad_request(format!("invalid qualified memory namespace {namespace:?}"));
    let (length, qualified) = namespace.split_once(':').ok_or_else(invalid)?;
    let length = length.parse::<usize>().map_err(|_| invalid())?;
    if length == 0 || length >= qualified.len() || !qualified.is_char_boundary(length) {
        return Err(invalid());
    }
    Ok(qualified.split_at(length))
}

pub(crate) fn validate_value(value: &[u8], encoding: &str) -> Result<()> {
    match encoding {
        "utf8" => {
            std::str::from_utf8(value).map_err(|error| {
                PlatformError::bad_request(format!("invalid utf8 state value: {error}"))
            })?;
            Ok(())
        }
        "v8sc" => Ok(()),
        _ => Err(PlatformError::bad_request(format!(
            "unsupported state encoding {encoding:?}"
        ))),
    }
}

fn epoch_ms() -> Result<i64> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(storage_error)?
            .as_millis(),
    )
    .map_err(storage_error)
}

impl MemoryStore {
    pub fn from_state(state: Arc<StateStore>) -> Self {
        Self {
            snapshots: Arc::clone(&state.memory_snapshots),
            state,
            profile: Arc::new(MemoryProfile::default()),
        }
    }
    pub fn state_performance_snapshot(&self) -> crate::state::StatePerformanceSnapshot {
        self.state.performance_snapshot()
    }
    pub fn owner_epoch_floor(&self) -> u64 {
        self.state.owner_epoch_floor()
    }
    pub fn next_owner_epoch(&self) -> Result<i64> {
        self.state.next_owner_epoch()
    }
    pub async fn acquire_lease(
        &self,
        namespace: &str,
        entity: &str,
    ) -> Result<Arc<crate::state::MemoryLease>> {
        namespace_owner(namespace)?;
        self.state.acquire_lease(namespace, entity).await
    }
    pub fn set_profile_enabled(&self, enabled: bool) {
        self.profile.set_enabled(enabled);
    }
    pub fn set_snapshot_cache_limits(&mut self, max_entries: usize, max_bytes: usize) {
        let previous = {
            let mut cache = self
                .snapshots
                .lock()
                .expect("memory snapshots lock poisoned");
            std::mem::replace(
                &mut *cache,
                SnapshotCache {
                    max_entries,
                    max_bytes,
                    ..Default::default()
                },
            )
        };
        drop(previous);
    }
    pub fn cache_performance_snapshot(&self) -> MemoryCachePerformanceSnapshot {
        self.profile.cache_performance_snapshot()
    }
    pub fn record_profile(&self, metric: MemoryProfileMetricKind, duration_us: u64, items: u64) {
        self.profile.record(metric, duration_us, items);
    }
    pub fn take_profile_snapshot_and_reset(&self) -> MemoryProfileSnapshot {
        self.profile.take_snapshot_and_reset()
    }
    pub fn reset_profile(&self) {
        self.profile.reset();
    }
    pub fn namespace_shards(&self) -> usize {
        STATE_SHARDS
    }
    pub async fn checkpoint_all_databases(&self) -> Result<usize> {
        self.state.checkpoint().await?;
        Ok(STATE_SHARDS)
    }
    pub async fn health_check(&self) -> Result<()> {
        self.state.health_check().await
    }
    pub fn shard_index_for_key(&self, namespace: &str, memory_key: &str) -> usize {
        let (worker, binding) = namespace_owner(namespace).expect("qualified memory namespace");
        StateStore::shard_index(worker, binding, memory_key)
    }

    pub async fn snapshot(&self, namespace: &str, memory_key: &str) -> Result<MemorySnapshot> {
        let (worker, binding) = namespace_owner(namespace)?;
        let shard = StateStore::shard_index(worker, binding, memory_key);
        let epoch = self.state.epoch(shard);
        let cache_key = (namespace.to_owned(), memory_key.to_owned());
        let cached = {
            let mut cache = self
                .snapshots
                .lock()
                .expect("memory snapshots lock poisoned");
            if let Some(cached) = cache.entries.get(&cache_key) {
                let snapshot = Arc::clone(&cached.snapshot);
                let previous_ordinal = cached.ordinal;
                let ordinal = cache.next_ordinal;
                cache.next_ordinal += 1;
                cache.order.remove(&previous_ordinal);
                cache.order.insert(ordinal, cache_key.clone());
                cache
                    .entries
                    .get_mut(&cache_key)
                    .expect("cached snapshot")
                    .ordinal = ordinal;
                Some(snapshot)
            } else {
                None
            }
        };
        if let Some(snapshot) = cached {
            self.profile
                .record(MemoryProfileMetricKind::StoreSnapshotCacheHit, 0, 1);
            return Ok((*snapshot).clone());
        }
        self.profile
            .record(MemoryProfileMetricKind::StoreSnapshotCacheMiss, 0, 1);
        let conn = self.state.read(shard).await?;
        let mut rows = query_cached(&conn, "SELECT m.max_version, s.item_key, s.value, s.encoding, s.version, s.deleted
                 FROM memory_meta m LEFT JOIN memory_state s ON s.worker=m.worker AND s.binding=m.binding AND s.entity_key=m.entity_key
                 WHERE m.worker=?1 AND m.binding=?2 AND m.entity_key=?3 ORDER BY s.item_key", (worker, binding, memory_key)).await.map_err(storage_error)?;
        let mut snapshot = MemorySnapshot {
            entries: Vec::new(),
            max_version: -1,
        };
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            snapshot.max_version = row.get(0).map_err(storage_error)?;
            if let Some(key) = row.get::<Option<String>>(1).map_err(storage_error)? {
                snapshot.entries.push(MemorySnapshotEntry {
                    key,
                    value: row.get(2).map_err(storage_error)?,
                    encoding: row.get(3).map_err(storage_error)?,
                    version: row.get(4).map_err(storage_error)?,
                    deleted: row.get::<i64>(5).map_err(storage_error)? != 0,
                });
            }
        }
        drop(rows);
        drop(conn);
        let bytes = namespace.len()
            + memory_key.len()
            + 128
            + snapshot
                .entries
                .iter()
                .map(|entry| entry.key.len() + entry.value.len() + entry.encoding.len() + 96)
                .sum::<usize>();
        let snapshot = Arc::new(snapshot);
        let mut evicted_snapshots = Vec::new();
        {
            let mut cache = self
                .snapshots
                .lock()
                .expect("memory snapshots lock poisoned");
            if self.state.epoch(shard) == epoch && bytes <= cache.max_bytes && cache.max_entries > 0
            {
                if let Some(previous) = cache.remove(&cache_key) {
                    evicted_snapshots.push(previous);
                }
                while cache.entries.len() >= cache.max_entries
                    || cache.bytes + bytes > cache.max_bytes
                {
                    let (_, oldest) = cache.order.pop_first().expect("nonempty snapshot cache");
                    evicted_snapshots.push(cache.remove(&oldest).expect("snapshot eviction key"));
                    self.profile
                        .record(MemoryProfileMetricKind::StoreSnapshotCacheEviction, 0, 1);
                }
                cache.bytes += bytes;
                let ordinal = cache.next_ordinal;
                cache.next_ordinal += 1;
                cache.order.insert(ordinal, cache_key.clone());
                cache.entries.insert(
                    cache_key,
                    CachedSnapshot {
                        snapshot: Arc::clone(&snapshot),
                        bytes,
                        ordinal,
                    },
                );
            }
        }
        drop(evicted_snapshots);
        Ok(Arc::unwrap_or_clone(snapshot))
    }

    pub async fn point_read(
        &self,
        namespace: &str,
        memory_key: &str,
        key: &str,
    ) -> Result<MemoryPointRead> {
        let (worker, binding) = namespace_owner(namespace)?;
        let conn = self
            .state
            .read(StateStore::shard_index(worker, binding, memory_key))
            .await?;
        let mut rows = query_cached(&conn, "SELECT m.max_version, s.item_key, s.value, s.encoding, s.version, s.deleted
                 FROM memory_meta m LEFT JOIN memory_state s ON s.worker=m.worker AND s.binding=m.binding AND s.entity_key=m.entity_key AND s.item_key=?4
                 WHERE m.worker=?1 AND m.binding=?2 AND m.entity_key=?3", (worker, binding, memory_key, key)).await.map_err(storage_error)?;
        let Some(row) = rows.next().await.map_err(storage_error)? else {
            return Ok(MemoryPointRead {
                record: None,
                max_version: -1,
            });
        };
        let max_version = row.get(0).map_err(storage_error)?;
        let record = row
            .get::<Option<String>>(1)
            .map_err(storage_error)?
            .map(|key| {
                Ok::<_, turso::Error>(MemorySnapshotEntry {
                    key,
                    value: row.get(2)?,
                    encoding: row.get(3)?,
                    version: row.get(4)?,
                    deleted: row.get::<i64>(5)? != 0,
                })
            })
            .transpose()
            .map_err(storage_error)?;
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
        let mut snapshot = self.snapshot(namespace, memory_key).await?;
        if !keys.is_empty() {
            snapshot.entries.retain(|entry| keys.contains(&entry.key));
        }
        Ok(snapshot)
    }
    pub async fn version_if_newer(
        &self,
        namespace: &str,
        memory_key: &str,
        known_version: i64,
    ) -> Result<Option<i64>> {
        let (worker, binding) = namespace_owner(namespace)?;
        let conn = self
            .state
            .read(StateStore::shard_index(worker, binding, memory_key))
            .await?;
        let mut rows = query_cached(
            &conn,
            "SELECT max_version FROM memory_meta WHERE worker=?1 AND binding=?2 AND entity_key=?3",
            (worker, binding, memory_key),
        )
        .await
        .map_err(storage_error)?;
        let current = rows
            .next()
            .await
            .map_err(storage_error)?
            .map(|row| row.get::<i64>(0))
            .transpose()
            .map_err(storage_error)?
            .unwrap_or(-1);
        Ok((current > known_version).then_some(current))
    }

    pub async fn apply_batch(
        &self,
        namespace: &str,
        memory_key: &str,
        commit: MemoryCommit<'_>,
    ) -> Result<MemoryBatchApplyResult> {
        let MemoryCommit {
            mutations,
            command_result,
            outbox_effects,
            owner_epoch,
            lease,
        } = commit;
        let (worker, binding) = namespace_owner(namespace)?;
        if memory_key.is_empty() {
            return Err(PlatformError::bad_request("memory entity key is empty"));
        }
        for mutation in mutations {
            if mutation.key.is_empty() {
                return Err(PlatformError::bad_request("memory mutation key is empty"));
            }
            validate_value(&mutation.value, &mutation.encoding)?;
        }
        if let Some(command) = command_result
            && (command.idempotency_key.is_empty() || command.idempotency_key.len() > 512)
        {
            return Err(PlatformError::bad_request(format!(
                "memory idempotency key length {} is outside 1..=512",
                command.idempotency_key.len()
            )));
        }
        for effect in outbox_effects {
            if effect.kind.is_empty() {
                return Err(PlatformError::bad_request(
                    "memory outbox effect kind is empty",
                ));
            }
        }
        if mutations.is_empty() && command_result.is_none() && outbox_effects.is_empty() {
            return Ok(MemoryBatchApplyResult {
                max_version: self
                    .version_if_newer(namespace, memory_key, -1)
                    .await?
                    .unwrap_or(-1),
            });
        }
        let bytes = namespace.len()
            + memory_key.len()
            + 256
            + mutations
                .iter()
                .map(|mutation| {
                    mutation.key.len() + mutation.value.len() + mutation.encoding.len() + 96
                })
                .sum::<usize>()
            + command_result.map_or(0, |command| {
                command.idempotency_key.len() + command.result.len() + 96
            })
            + outbox_effects
                .iter()
                .map(|effect| effect.kind.len() + effect.payload.len() + 96)
                .sum::<usize>();
        let shard = StateStore::shard_index(worker, binding, memory_key);
        let worker = worker.to_owned();
        let binding = binding.to_owned();
        let entity = memory_key.to_owned();
        let mutations = mutations.to_vec();
        let command_result = command_result.cloned();
        let effects = outbox_effects.to_vec();
        let now = epoch_ms()?;
        self.state
            .write(shard, bytes, WriteOptions {
                invalidate_memory: Some((namespace.to_owned(), memory_key.to_owned())),
            }, move |conn, version| {
                let worker = worker.clone();
                let binding = binding.clone();
                let entity = entity.clone();
                let mutations = mutations.clone();
                let command_result = command_result.clone();
                let effects = effects.clone();
                let lease = lease.clone();
                Box::pin(async move {
                    let _lease = lease;
                    let mut rows = query_cached(
                        conn,
                        "SELECT max_version, owner_epoch FROM memory_meta
                         WHERE worker=?1 AND binding=?2 AND entity_key=?3",
                        (worker.as_str(), binding.as_str(), entity.as_str()),
                    ).await?;
                    let (current, previous_owner) = rows.next().await?
                        .map(|row| Ok::<_, turso::Error>((row.get::<i64>(0)?, row.get::<i64>(1)?)))
                        .transpose()?.unwrap_or((-1, 0));
                    drop(rows);
                    if let Some(owner) = owner_epoch
                        && owner < previous_owner {
                            return Err(PlatformError::runtime(format!(
                                "stale memory owner epoch {owner}; current owner epoch {previous_owner} for {worker}/{binding}/{entity}"
                            )).into());
                        }
                    let revision = if mutations.is_empty() && effects.is_empty() {
                        current
                    } else {
                        version
                    };
                    for mutation in mutations {
                        execute_cached(
                            conn,
                            "INSERT INTO memory_state(worker,binding,entity_key,item_key,value,encoding,deleted,version)
                             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                             ON CONFLICT(worker,binding,entity_key,item_key) DO UPDATE SET
                             value=excluded.value,encoding=excluded.encoding,
                             deleted=excluded.deleted,version=excluded.version",
                            (worker.as_str(), binding.as_str(), entity.as_str(), mutation.key,
                             mutation.value, mutation.encoding, i64::from(mutation.deleted), revision),
                        ).await?;
                    }
                    execute_cached(
                        conn,
                        "INSERT INTO memory_meta(worker,binding,entity_key,max_version,owner_epoch)
                         VALUES (?1,?2,?3,?4,?5)
                         ON CONFLICT(worker,binding,entity_key) DO UPDATE SET
                         max_version=excluded.max_version,owner_epoch=excluded.owner_epoch",
                        (worker.as_str(), binding.as_str(), entity.as_str(), revision,
                         owner_epoch.unwrap_or(previous_owner)),
                    ).await?;
                    for (ordinal, effect) in effects.into_iter().enumerate() {
                        use sha2::{Digest, Sha256};
                        let mut hash = Sha256::new();
                        for component in [&worker, &binding, &entity] {
                            hash.update((component.len() as u64).to_be_bytes());
                            hash.update(component.as_bytes());
                        }
                        hash.update(version.to_be_bytes());
                        hash.update((ordinal as u64).to_be_bytes());
                        let digest = hash.finalize().iter()
                            .map(|byte| format!("{byte:02x}")).collect::<String>();
                        let effect_id = format!("memfx_{digest}");
                        execute_cached(
                            conn,
                            "INSERT INTO memory_outbox(worker,binding,entity_key,effect_id,revision,
                             kind,payload_blob,status,attempt_count,next_attempt_at_ms)
                             VALUES (?1,?2,?3,?4,?5,?6,?7,'pending',0,?8)",
                            (worker.as_str(), binding.as_str(), entity.as_str(), effect_id,
                             revision, effect.kind, effect.payload, now),
                        ).await?;
                    }
                    if let Some(command) = command_result {
                        execute_cached(
                            conn,
                            "INSERT INTO memory_commands(worker,binding,entity_key,idempotency_key,result_blob,revision)
                             VALUES (?1,?2,?3,?4,?5,?6)",
                            (worker, binding, entity, command.idempotency_key, command.result, revision),
                        ).await?;
                    }
                    Ok(MemoryBatchApplyResult { max_version: revision })
                })
            }).await
    }

    pub async fn command_result(
        &self,
        namespace: &str,
        memory_key: &str,
        idempotency_key: &str,
    ) -> Result<Option<MemoryCommandResult>> {
        let (worker, binding) = namespace_owner(namespace)?;
        let conn = self
            .state
            .read(StateStore::shard_index(worker, binding, memory_key))
            .await?;
        let mut rows=query_cached(&conn, "SELECT result_blob,revision FROM memory_commands WHERE worker=?1 AND binding=?2 AND entity_key=?3 AND idempotency_key=?4", (worker,binding,memory_key,idempotency_key)).await.map_err(storage_error)?;
        rows.next()
            .await
            .map_err(storage_error)?
            .map(|row| {
                Ok::<_, turso::Error>(MemoryCommandResult {
                    result: row.get(0)?,
                    revision: row.get(1)?,
                })
            })
            .transpose()
            .map_err(storage_error)
    }
    pub async fn outbox_records(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Result<Vec<MemoryOutboxRecord>> {
        let (worker, binding) = namespace_owner(namespace)?;
        let conn = self
            .state
            .read(StateStore::shard_index(worker, binding, memory_key))
            .await?;
        let mut rows = query_cached(
            &conn,
            "SELECT effect_id,kind,payload_blob,revision,status,attempt_count,next_attempt_at_ms
                 FROM memory_outbox
                 WHERE worker=?1 AND binding=?2 AND entity_key=?3 ORDER BY revision,effect_id",
            (worker, binding, memory_key),
        )
        .await
        .map_err(storage_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            records.push(outbox_record(&row, 0).map_err(storage_error)?);
        }
        Ok(records)
    }
    pub async fn claim_outbox_records(
        &self,
        namespace: &str,
        memory_key: &str,
        limit: usize,
        lease_for: Duration,
    ) -> Result<Vec<MemoryOutboxRecord>> {
        let (worker, binding) = namespace_owner(namespace)?;
        let claims = self
            .claim(
                StateStore::shard_index(worker, binding, memory_key),
                limit,
                lease_for,
                &[],
                Some((worker.to_owned(), binding.to_owned(), memory_key.to_owned())),
            )
            .await?;
        Ok(claims.into_iter().map(|claim| claim.record).collect())
    }
    pub async fn claim_due_outbox_records(
        &self,
        limit: usize,
        lease_for: Duration,
        kinds: &[&str],
    ) -> Result<Vec<MemoryOutboxClaim>> {
        let mut claims = Vec::new();
        for shard in 0..STATE_SHARDS {
            if claims.len() >= limit {
                break;
            }
            claims.extend(
                self.claim_due_outbox_records_for_shard_index(
                    shard,
                    limit - claims.len(),
                    lease_for,
                    kinds,
                )
                .await?,
            );
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
        if kinds.is_empty() {
            return Ok(Vec::new());
        }
        self.claim(shard_index, limit, lease_for, kinds, None).await
    }
    async fn claim(
        &self,
        shard: usize,
        limit: usize,
        lease_for: Duration,
        kinds: &[&str],
        entity: Option<(String, String, String)>,
    ) -> Result<Vec<MemoryOutboxClaim>> {
        if shard >= STATE_SHARDS {
            return Err(PlatformError::bad_request(format!(
                "state shard {shard} outside 0..{STATE_SHARDS}"
            )));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = epoch_ms()?;
        let until =
            now.saturating_add(i64::try_from(lease_for.as_millis()).map_err(storage_error)?);
        let mut sql = String::from(
            "SELECT worker,binding,entity_key,effect_id,kind,payload_blob,revision,status,attempt_count,next_attempt_at_ms
                 FROM memory_outbox
                 WHERE status IN ('pending','inflight') AND next_attempt_at_ms<=?1",
        );
        let mut params = vec![Value::Integer(now)];
        if let Some((worker, binding, entity)) = entity {
            sql.push_str(" AND worker=?2 AND binding=?3 AND entity_key=?4");
            params.extend([
                Value::Text(worker),
                Value::Text(binding),
                Value::Text(entity),
            ]);
        }
        if !kinds.is_empty() {
            sql.push_str(" AND (");
            for (index, kind) in kinds.iter().enumerate() {
                if index > 0 {
                    sql.push_str(" OR ");
                }
                let parameter = params.len() + 1;
                if let Some(prefix) = kind.strip_suffix('*') {
                    sql.push_str(&format!("kind LIKE ?{parameter} ESCAPE '\\'"));
                    params.push(Value::Text(format!(
                        "{}%",
                        prefix
                            .replace('\\', "\\\\")
                            .replace('%', "\\%")
                            .replace('_', "\\_")
                    )));
                } else {
                    sql.push_str(&format!("kind=?{parameter}"));
                    params.push(Value::Text((*kind).into()));
                }
            }
            sql.push(')');
        }
        sql.push_str(&format!(
            " ORDER BY revision,effect_id LIMIT ?{}",
            params.len() + 1
        ));
        params.push(Value::Integer(
            i64::try_from(limit.min(4096)).map_err(storage_error)?,
        ));
        {
            let conn = self.state.read(shard).await?;
            let mut rows = conn
                .query(&sql, params.clone())
                .await
                .map_err(storage_error)?;
            if rows.next().await.map_err(storage_error)?.is_none() {
                return Ok(Vec::new());
            }
        }
        self.state
            .write(
                shard,
                sql.len() + 4096,
                WriteOptions::default(),
                move |conn, _| {
                    let sql = sql.clone();
                    let params = params.clone();
                    Box::pin(async move {
                        let mut rows = conn.query(&sql, params).await?;
                        let mut claims = Vec::new();
                        while let Some(row) = rows.next().await? {
                            let worker = row.get::<String>(0)?;
                            let binding = row.get::<String>(1)?;
                            let entity = row.get::<String>(2)?;
                            let mut record = outbox_record(&row, 3)?;
                            record.status = "inflight".into();
                            record.attempt_count += 1;
                            record.next_attempt_at_ms = until;
                            claims.push(MemoryOutboxClaim {
                                namespace: worker_namespace(&worker, &binding),
                                memory_key: entity,
                                record,
                            });
                        }
                        drop(rows);
                        for claim in &claims {
                            let (worker, binding) = namespace_owner(&claim.namespace)?;
                            execute_cached(
                                conn,
                                "UPDATE memory_outbox
                         SET status='inflight',attempt_count=?1,next_attempt_at_ms=?2
                         WHERE worker=?3 AND binding=?4 AND entity_key=?5 AND effect_id=?6",
                                (
                                    claim.record.attempt_count,
                                    until,
                                    worker,
                                    binding,
                                    claim.memory_key.as_str(),
                                    claim.record.effect_id.as_str(),
                                ),
                            )
                            .await?;
                        }
                        Ok(claims)
                    })
                },
            )
            .await
    }
    pub async fn mark_outbox_delivered(
        &self,
        namespace: &str,
        memory_key: &str,
        effect_id: &str,
    ) -> Result<()> {
        self.apply_outbox_delivery_outcomes(&[MemoryOutboxDeliveryOutcome {
            namespace: namespace.into(),
            memory_key: memory_key.into(),
            effect_id: effect_id.into(),
            action: MemoryOutboxDeliveryAction::Delivered,
        }])
        .await
    }
    pub async fn retry_outbox_record(
        &self,
        namespace: &str,
        memory_key: &str,
        effect_id: &str,
        retry_after: Duration,
    ) -> Result<()> {
        self.apply_outbox_delivery_outcomes(&[MemoryOutboxDeliveryOutcome {
            namespace: namespace.into(),
            memory_key: memory_key.into(),
            effect_id: effect_id.into(),
            action: MemoryOutboxDeliveryAction::Retry { retry_after },
        }])
        .await
    }
    pub async fn apply_outbox_delivery_outcomes(
        &self,
        outcomes: &[MemoryOutboxDeliveryOutcome],
    ) -> Result<()> {
        let now = epoch_ms()?;
        for outcome in outcomes {
            let (worker, binding) = namespace_owner(&outcome.namespace)?;
            let shard = StateStore::shard_index(worker, binding, &outcome.memory_key);
            let worker = worker.to_owned();
            let binding = binding.to_owned();
            let outcome = outcome.clone();
            let bytes =
                outcome.namespace.len() + outcome.memory_key.len() + outcome.effect_id.len() + 256;
            self.state
                .write(shard, bytes, WriteOptions::default(), move |conn, _| {
                    let worker = worker.clone();
                    let binding = binding.clone();
                    let outcome = outcome.clone();
                    Box::pin(async move {
                        let (status, next) = match outcome.action {
                            MemoryOutboxDeliveryAction::Delivered => ("delivered", now),
                            MemoryOutboxDeliveryAction::DroppedTerminal => ("dropped", now),
                            MemoryOutboxDeliveryAction::Retry { retry_after } => (
                                "pending",
                                now.saturating_add(
                                    i64::try_from(retry_after.as_millis())
                                        .map_err(storage_error)?,
                                ),
                            ),
                        };
                        execute_cached(
                            conn,
                            "UPDATE memory_outbox SET status=?1,next_attempt_at_ms=?2
                         WHERE worker=?3 AND binding=?4 AND entity_key=?5 AND effect_id=?6",
                            (
                                status,
                                next,
                                worker,
                                binding,
                                outcome.memory_key,
                                outcome.effect_id,
                            ),
                        )
                        .await?;
                        Ok(())
                    })
                })
                .await?;
        }
        Ok(())
    }
}

fn outbox_record(row: &turso::Row, offset: usize) -> turso::Result<MemoryOutboxRecord> {
    Ok(MemoryOutboxRecord {
        effect_id: row.get(offset)?,
        kind: row.get(offset + 1)?,
        payload: row.get(offset + 2)?,
        revision: row.get(offset + 3)?,
        status: row.get(offset + 4)?,
        attempt_count: row.get(offset + 5)?,
        next_attempt_at_ms: row.get(offset + 6)?,
    })
}
