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
        let started = self
            .profile
            .enabled
            .load(Ordering::Relaxed)
            .then(std::time::Instant::now);
        let result = self.state.acquire_lease(namespace, entity).await;
        if let Some(started) = started {
            self.profile.record(
                MemoryProfileMetricKind::StoreLease,
                started.elapsed().as_micros() as u64,
                1,
            );
        }
        result
    }
    pub fn set_profile_enabled(&self, enabled: bool) {
        self.profile.set_enabled(enabled);
        self.state.set_profile_enabled(enabled);
    }
    pub fn set_outbox_claim_byte_limit(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 || bytes > u32::MAX as usize {
            return Err(PlatformError::bad_request(
                "outbox claim byte limit must be within 1..=u32::MAX",
            ));
        }
        let mut budget = self
            .state
            .outbox_claim_budget
            .lock()
            .expect("outbox budget lock poisoned");
        if Arc::strong_count(&budget) != 1 {
            return Err(PlatformError::conflict(
                "outbox byte limit cannot change while claims or effect commits are active",
            ));
        }
        *budget = Arc::new(OutboxClaimBudget::new(bytes));
        Ok(())
    }
    pub fn outbox_claimed_bytes(&self) -> usize {
        let budget = self
            .state
            .outbox_claim_budget
            .lock()
            .expect("outbox budget lock poisoned");
        budget.max_bytes - budget.permits.available_permits()
    }
    pub fn set_snapshot_cache_limits(&mut self, max_entries: usize, max_bytes: usize) {
        let previous = {
            let mut cache = self
                .snapshots
                .lock()
                .expect("memory snapshots lock poisoned");
            let loads = std::mem::take(&mut cache.loads);
            #[cfg(test)]
            let before_fill = cache.before_fill.take();
            std::mem::replace(
                &mut *cache,
                SnapshotCache {
                    max_entries,
                    max_bytes,
                    loads,
                    #[cfg(test)]
                    before_fill,
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
    pub fn shard_count(&self) -> usize {
        STATE_SHARDS
    }
    pub fn shard_index_for_key(&self, namespace: &str, memory_key: &str) -> usize {
        let (worker, binding) = namespace_owner(namespace).expect("qualified memory namespace");
        StateStore::shard_index(worker, binding, memory_key)
    }

    fn cached_snapshot(&self, cache_key: &MemorySnapshotKey) -> Option<Arc<MemorySnapshot>> {
        let cached = {
            let mut cache = self
                .snapshots
                .lock()
                .expect("memory snapshots lock poisoned");
            if let Some(cached) = cache.entries.get(cache_key) {
                let snapshot = Arc::clone(&cached.snapshot);
                let previous_ordinal = cached.ordinal;
                let ordinal = cache.next_ordinal;
                cache.next_ordinal += 1;
                cache.order.remove(&previous_ordinal);
                cache.order.insert(ordinal, cache_key.clone());
                cache
                    .entries
                    .get_mut(cache_key)
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
            return Some(snapshot);
        }
        None
    }

    pub async fn snapshot(&self, namespace: &str, memory_key: &str) -> Result<Arc<MemorySnapshot>> {
        let (worker, binding) = namespace_owner(namespace)?;
        let cache_key = (namespace.to_owned(), memory_key.to_owned());
        if let Some(snapshot) = self.cached_snapshot(&cache_key) {
            return Ok(snapshot);
        }
        let loading = {
            let mut cache = self
                .snapshots
                .lock()
                .expect("memory snapshots lock poisoned");
            if cache.loads.len() >= DEFAULT_MEMORY_SNAPSHOT_CACHE_MAX_ENTRIES {
                cache.loads.retain(|_, loading| loading.strong_count() > 0);
            }
            if let Some(loading) = cache.loads.get(&cache_key).and_then(Weak::upgrade) {
                loading
            } else {
                let loading = Arc::new(tokio::sync::Mutex::new(()));
                cache
                    .loads
                    .insert(cache_key.clone(), Arc::downgrade(&loading));
                loading
            }
        };
        // Cold fills must stay ordered even between SQL COMMIT and cache publication.
        // This lock is independent of the entity's write lease; warm readers bypass it.
        let _load = loading.lock().await;
        if let Some(snapshot) = self.cached_snapshot(&cache_key) {
            return Ok(snapshot);
        }
        let shard = StateStore::shard_index(worker, binding, memory_key);
        let epoch = self.state.epoch(shard);
        self.profile
            .record(MemoryProfileMetricKind::StoreSnapshotCacheMiss, 0, 1);
        let conn = self.state.read(shard).await?;
        let mut rows = query_cached(&conn, "SELECT m.max_version, s.item_key, s.value, s.encoding, s.version, s.deleted
                 FROM memory_meta m LEFT JOIN memory_state s ON s.worker=m.worker AND s.binding=m.binding AND s.entity_key=m.entity_key AND s.deleted=0
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
                    value: row.get::<Vec<u8>>(2).map_err(storage_error)?.into(),
                    encoding: row.get(3).map_err(storage_error)?,
                    version: row.get(4).map_err(storage_error)?,
                    deleted: row.get::<i64>(5).map_err(storage_error)? != 0,
                });
            }
        }
        drop(rows);
        drop(conn);
        #[cfg(test)]
        {
            let pause = self.snapshots.lock().unwrap().before_fill.take();
            if let Some(pause) = pause {
                pause.loaded.notify_one();
                pause.resume.notified().await;
            }
        }
        let bytes = snapshot.cache_bytes(&cache_key);
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
        Ok(snapshot)
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
                 FROM memory_meta m LEFT JOIN memory_state s ON s.worker=m.worker AND s.binding=m.binding AND s.entity_key=m.entity_key AND s.item_key=?4 AND s.deleted=0
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
                    value: row.get::<Vec<u8>>(2)?.into(),
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
        mut commit: MemoryCommit,
    ) -> Result<MemoryBatchApplyResult> {
        for mutation in &mut commit.mutations {
            if mutation.deleted {
                mutation.value = Bytes::new();
                mutation.encoding = "utf8".into();
            }
        }
        let MemoryCommit {
            mutations,
            command_result,
            outbox_effects,
            owner_epoch,
            ..
        } = &commit;
        let owner_epoch = *owner_epoch;
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
        let outbox_budget = (!outbox_effects.is_empty()).then(|| {
            self.state
                .outbox_claim_budget
                .lock()
                .expect("outbox budget lock poisoned")
                .clone()
        });
        for effect in outbox_effects {
            if effect.kind.is_empty() {
                return Err(PlatformError::bad_request(
                    "memory outbox effect kind is empty",
                ));
            }
            let budget = outbox_budget
                .as_ref()
                .expect("effects retain their admission policy");
            let charge = outbox_payload_charge(
                effect.payload.len(),
                worker,
                binding,
                memory_key,
                70,
                &effect.kind,
            );
            if charge > budget.max_bytes {
                return Err(PlatformError::bad_request(format!(
                    "outbox effect needs {charge} claimed bytes, exceeding configured limit {}",
                    budget.max_bytes
                )));
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
            + command_result.as_ref().map_or(0, |command| {
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
        let commit = Arc::new(commit);
        let now = epoch_ms()?;
        self.state
            .write(shard, bytes, WriteOptions {
                invalidate_memory: Some((namespace.to_owned(), memory_key.to_owned())),
            }, move |conn, version| {
                let worker = worker.clone();
                let binding = binding.clone();
                let entity = entity.clone();
                let commit = Arc::clone(&commit);
                let outbox_budget = outbox_budget.clone();
                Box::pin(async move {
                    // Configuration cannot replace the shared budget while an
                    // accepted effect commit is still queued or executing.
                    let _outbox_budget = outbox_budget;
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
                    let revision = if commit.mutations.is_empty() && commit.outbox_effects.is_empty() {
                        current
                    } else {
                        version
                    };
                    let previous_size = if commit.mutations.is_empty() {
                        None
                    } else {
                        Some(entity_size(conn, &worker, &binding, &entity).await?)
                    };
                    for mutation in &commit.mutations {
                        if mutation.deleted {
                            execute_cached(conn, "DELETE FROM memory_state WHERE worker=?1 AND binding=?2 AND entity_key=?3 AND item_key=?4",
                                (worker.as_str(), binding.as_str(), entity.as_str(), mutation.key.as_str())).await?;
                            continue;
                        }
                        execute_cached(
                            conn,
                            "INSERT INTO memory_state(worker,binding,entity_key,item_key,value,encoding,deleted,version)
                             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                             ON CONFLICT(worker,binding,entity_key,item_key) DO UPDATE SET
                             value=excluded.value,encoding=excluded.encoding,
                             deleted=excluded.deleted,version=excluded.version",
                            (worker.as_str(), binding.as_str(), entity.as_str(), mutation.key.as_str(),
                             mutation.value.as_ref(), mutation.encoding.as_str(), i64::from(mutation.deleted), revision),
                        ).await?;
                    }
                    if let Some(previous_size) = previous_size {
                        entity_size(conn, &worker, &binding, &entity).await?.validate(Some(previous_size))?;
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
                    for (ordinal, effect) in commit.outbox_effects.iter().enumerate() {
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
                            "INSERT INTO memory_outbox(worker,binding,entity_key,effect_id,revision,ordinal,
                             kind,payload_blob,status,attempt_count,next_attempt_at_ms)
                             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,'pending',0,?9)",
                            (worker.as_str(), binding.as_str(), entity.as_str(), effect_id,
                             revision, ordinal as i64, effect.kind.as_str(), effect.payload.as_slice(), now),
                        ).await?;
                    }
                    if let Some(command) = &commit.command_result {
                        execute_cached(
                            conn,
                            "INSERT INTO memory_commands(worker,binding,entity_key,idempotency_key,result_blob,revision)
                             VALUES (?1,?2,?3,?4,?5,?6)",
                            (worker, binding, entity, command.idempotency_key.as_str(), command.result.as_slice(), revision),
                        ).await?;
                    }
                    Ok(WriteOutcome {
                        value: MemoryBatchApplyResult { max_version: revision },
                        snapshot: Some(MemorySnapshotChange {
                            previous_version: current, version: revision, commit,
                        }),
                    })
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
            "SELECT effect_id,kind,payload_blob,revision,status,attempt_count,next_attempt_at_ms,ordinal
                 FROM memory_outbox
                 WHERE worker=?1 AND binding=?2 AND entity_key=?3 ORDER BY revision,ordinal,effect_id",
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
        let mut params = vec![Value::Integer(now)];
        let mut restriction = String::new();
        if let Some((worker, binding, entity)) = entity {
            restriction.push_str(" AND m.worker=?2 AND m.binding=?3 AND m.entity_key=?4");
            params.extend([
                Value::Text(worker),
                Value::Text(binding),
                Value::Text(entity),
            ]);
        }
        let mut kind_filter = Vec::new();
        for kind in kinds {
            let parameter = params.len() + 1;
            if let Some(prefix) = kind.strip_suffix('*') {
                kind_filter.push(format!("kind LIKE ?{parameter} ESCAPE '\\'"));
                params.push(Value::Text(format!(
                    "{}%",
                    prefix
                        .replace('\\', "\\\\")
                        .replace('%', "\\%")
                        .replace('_', "\\_")
                )));
            } else {
                kind_filter.push(format!("kind=?{parameter}"));
                params.push(Value::Text((*kind).into()));
            }
        }
        let filter = |alias: &str| {
            if kind_filter.is_empty() {
                String::new()
            } else {
                format!(
                    " AND ({})",
                    kind_filter
                        .iter()
                        .map(|condition| format!("{alias}.{condition}"))
                        .collect::<Vec<_>>()
                        .join(" OR ")
                )
            }
        };
        // Select an ordered prefix for each entity. An earlier effect whose
        // retry/claim lease is deferred blocks its successors across batches.
        let sql = format!(
            "SELECT m.worker,m.binding,m.entity_key,m.effect_id,m.kind,length(m.payload_blob),m.revision,m.ordinal,m.attempt_count
             FROM memory_outbox m
             WHERE m.status IN ('pending','inflight') AND m.next_attempt_at_ms<=?1{restriction}{}
             AND NOT EXISTS (
                 SELECT 1 FROM memory_outbox p
                 WHERE p.worker=m.worker AND p.binding=m.binding AND p.entity_key=m.entity_key
                 AND p.status IN ('pending','inflight') AND p.next_attempt_at_ms>?1{}
                 AND (p.revision<m.revision OR (p.revision=m.revision AND
                     (p.ordinal<m.ordinal OR (p.ordinal=m.ordinal AND p.effect_id<m.effect_id)))))
             ORDER BY m.revision,m.ordinal,m.effect_id LIMIT ?{}",
            filter("m"), filter("p"), params.len()+1,
        );
        params.push(Value::Integer(
            i64::try_from(limit.min(4096)).map_err(storage_error)?,
        ));
        let budget = self
            .state
            .outbox_claim_budget
            .lock()
            .expect("outbox budget lock poisoned")
            .clone();
        if budget.permits.available_permits() == 0 {
            return Ok(Vec::new());
        }
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
        self.state.write(shard, sql.len()+4096, WriteOptions::default(), move |conn, _| {
            let sql = sql.clone();
            let params = params.clone();
            let budget = Arc::clone(&budget);
            Box::pin(async move {
                let mut rows = conn.query(&sql, params).await?;
                let mut selected = Vec::new();
                let mut blocked = std::collections::HashSet::new();
                while let Some(row) = rows.next().await? {
                    let worker = row.get::<String>(0)?;
                    let binding = row.get::<String>(1)?;
                    let entity = row.get::<String>(2)?;
                    if blocked.contains(&(worker.clone(),binding.clone(),entity.clone())) { continue; }
                    let effect_id = row.get::<String>(3)?;
                    let kind = row.get::<String>(4)?;
                    let payload_bytes = usize::try_from(row.get::<i64>(5)?).map_err(storage_error)?;
                    let charge = outbox_payload_charge(payload_bytes, &worker, &binding, &entity, effect_id.len(), &kind);
                    if charge > budget.max_bytes {
                        return Err(PlatformError::bad_request(format!("outbox effect needs {charge} claimed bytes, exceeding configured limit {}", budget.max_bytes)).into());
                    }
                    let Ok(permit) = Arc::clone(&budget.permits).try_acquire_many_owned(charge as u32) else {
                        blocked.insert((worker,binding,entity));
                        continue;
                    };
                    let lease = Arc::new(MemoryOutboxPayloadLease { _permit: permit, _budget: Arc::clone(&budget) });
                    selected.push((worker,binding,entity,effect_id,kind,row.get::<i64>(6)?,row.get::<i64>(7)?,row.get::<i64>(8)?+1,lease));
                }
                drop(rows);
                let mut claims = Vec::with_capacity(selected.len());
                for (worker,binding,entity,effect_id,kind,revision,ordinal,attempt_count,lease) in selected {
                    // Admission precedes materializing any BLOB. Each lease also
                    // covers the parsed socket payload copy until delivery releases it.
                    let mut rows = query_cached(conn,
                        "SELECT payload_blob FROM memory_outbox WHERE worker=?1 AND binding=?2 AND entity_key=?3 AND effect_id=?4",
                        (worker.as_str(),binding.as_str(),entity.as_str(),effect_id.as_str())).await?;
                    let payload = rows.next().await?.expect("selected outbox effect").get::<Vec<u8>>(0)?;
                    drop(rows);
                    execute_cached(conn,
                        "UPDATE memory_outbox SET status='inflight',attempt_count=?1,next_attempt_at_ms=?2 WHERE worker=?3 AND binding=?4 AND entity_key=?5 AND effect_id=?6",
                        (attempt_count,until,worker.as_str(),binding.as_str(),entity.as_str(),effect_id.as_str())).await?;
                    claims.push(MemoryOutboxClaim {
                        namespace: worker_namespace(&worker,&binding), memory_key: entity,
                        record: MemoryOutboxRecord { effect_id,kind,payload,revision,ordinal,status:"inflight".into(),attempt_count,next_attempt_at_ms:until,payload_lease:Some(lease) },
                    });
                }
                Ok(claims.into())
            })
        }).await
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
                        Ok(().into())
                    })
                })
                .await?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct EntitySize {
    values: i64,
    metadata: i64,
    entries: i64,
}

impl EntitySize {
    fn validate(self, previous: Option<Self>) -> Result<()> {
        let dimensions = [
            (
                "values",
                self.values,
                MEMORY_ENTITY_MAX_VALUE_BYTES as i64,
                previous.map(|size| size.values),
            ),
            (
                "metadata",
                self.metadata,
                MEMORY_ENTITY_MAX_METADATA_BYTES as i64,
                previous.map(|size| size.metadata),
            ),
            (
                "entries",
                self.entries,
                MEMORY_ENTITY_MAX_ENTRIES as i64,
                previous.map(|size| size.entries),
            ),
        ];
        let mut oversized = None;
        let shrank = dimensions.iter().any(|(_, value, maximum, before)| {
            before.is_some_and(|before| before > *maximum && *value < before)
        });
        for (name, value, maximum, before) in dimensions {
            if value <= maximum {
                continue;
            }
            oversized = Some((name, maximum));
            if before.is_none_or(|before| value > before) {
                return Err(PlatformError::bad_request(format!(
                    "memory entity {name} exceeded {maximum}; oversized entities must shrink"
                )));
            }
        }
        if let Some((name, maximum)) = oversized
            && !shrank
        {
            return Err(PlatformError::bad_request(format!(
                "memory entity {name} exceeded {maximum}; oversized entities must shrink"
            )));
        }
        Ok(())
    }
}

async fn entity_size(
    conn: &turso::Connection,
    worker: &str,
    binding: &str,
    entity: &str,
) -> turso::Result<EntitySize> {
    let mut rows = query_cached(
        conn,
        "SELECT COALESCE(SUM(length(value)), 0), COALESCE(SUM(length(CAST(item_key AS BLOB))+length(CAST(encoding AS BLOB))+96),0), COUNT(*) FROM memory_state
         WHERE worker=?1 AND binding=?2 AND entity_key=?3 AND deleted=0",
        (worker, binding, entity),
    )
    .await?;
    let row = rows.next().await?.expect("memory entity size");
    Ok(EntitySize {
        values: row.get(0)?,
        metadata: row.get(1)?,
        entries: row.get(2)?,
    })
}

fn outbox_record(row: &turso::Row, offset: usize) -> turso::Result<MemoryOutboxRecord> {
    Ok(MemoryOutboxRecord {
        effect_id: row.get(offset)?,
        kind: row.get(offset + 1)?,
        payload: row.get(offset + 2)?,
        revision: row.get(offset + 3)?,
        ordinal: row.get(offset + 7)?,
        status: row.get(offset + 4)?,
        attempt_count: row.get(offset + 5)?,
        next_attempt_at_ms: row.get(offset + 6)?,
        payload_lease: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value_commit(key: &str, bytes: usize) -> MemoryCommit {
        MemoryCommit {
            mutations: vec![MemoryBatchMutation {
                key: key.into(),
                value: Bytes::from(vec![b'x'; bytes]),
                encoding: "utf8".into(),
                deleted: false,
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn aggregate_value_limit_rolls_back_growth_and_serializes_concurrent_writes() {
        let root = std::env::temp_dir().join(format!("dd-memory-budget-{}", uuid::Uuid::new_v4()));
        let memory = MemoryStore::from_state(StateStore::open(&root).await.unwrap());
        let namespace = worker_namespace("worker", "MEMORY");
        let half = MEMORY_ENTITY_MAX_VALUE_BYTES / 2;
        memory
            .apply_batch(&namespace, "entity", value_commit("a", half))
            .await
            .unwrap();
        let revision = memory
            .apply_batch(&namespace, "entity", value_commit("b", half))
            .await
            .unwrap()
            .max_version;
        let mut excess = value_commit("b", half + 1);
        excess.command_result = Some(MemoryCommandResultWrite {
            idempotency_key: "rejected".into(),
            result: b"must not persist".to_vec(),
        });
        excess.outbox_effects.push(MemoryOutboxEffectWrite {
            kind: "audit".into(),
            payload: b"must not deliver".to_vec(),
        });
        let error = memory
            .apply_batch(&namespace, "entity", excess)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), common::ErrorKind::BadRequest);
        let snapshot = memory.snapshot(&namespace, "entity").await.unwrap();
        assert_eq!(snapshot.value_bytes(), MEMORY_ENTITY_MAX_VALUE_BYTES);
        assert_eq!(snapshot.max_version, revision);
        assert!(
            memory
                .command_result(&namespace, "entity", "rejected")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            memory
                .outbox_records(&namespace, "entity")
                .await
                .unwrap()
                .is_empty()
        );

        memory
            .apply_batch(
                &namespace,
                "concurrent",
                value_commit("existing", 9 * 1024 * 1024),
            )
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            memory.apply_batch(&namespace, "concurrent", value_commit("a", 4 * 1024 * 1024)),
            memory.apply_batch(&namespace, "concurrent", value_commit("b", 4 * 1024 * 1024)),
        );
        assert_ne!(first.is_ok(), second.is_ok());
        let error = first.err().or_else(|| second.err()).unwrap();
        assert_eq!(error.kind(), common::ErrorKind::BadRequest);
        assert_eq!(
            memory
                .snapshot(&namespace, "concurrent")
                .await
                .unwrap()
                .value_bytes(),
            13 * 1024 * 1024
        );
        drop(memory);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn oversized_legacy_entities_can_shrink_and_delete_without_growing() {
        let root =
            std::env::temp_dir().join(format!("dd-memory-recovery-{}", uuid::Uuid::new_v4()));
        let state = StateStore::open(&root).await.unwrap();
        let namespace = worker_namespace("worker", "MEMORY");
        let conn = state
            .read(StateStore::shard_index("worker", "MEMORY", "entity"))
            .await
            .unwrap();
        for key in ["a", "b", "c"] {
            conn.execute("INSERT INTO memory_state VALUES ('worker','MEMORY','entity',?1,zeroblob(?2),'utf8',0,0)", (key, 9 * 1024 * 1024)).await.unwrap();
        }
        conn.execute(
            "INSERT INTO memory_meta VALUES ('worker','MEMORY','entity',0,0)",
            (),
        )
        .await
        .unwrap();
        drop(conn);
        let memory = MemoryStore::from_state(state);
        assert_eq!(
            memory
                .snapshot(&namespace, "entity")
                .await
                .unwrap()
                .value_bytes(),
            27 * 1024 * 1024
        );
        assert!(
            memory
                .apply_batch(&namespace, "entity", value_commit("a", 9 * 1024 * 1024))
                .await
                .is_err()
        );
        memory
            .apply_batch(&namespace, "entity", value_commit("a", 8 * 1024 * 1024))
            .await
            .unwrap();
        assert_eq!(
            memory
                .snapshot(&namespace, "entity")
                .await
                .unwrap()
                .value_bytes(),
            26 * 1024 * 1024
        );
        for key in ["b", "c"] {
            let mut deletion = value_commit(key, 1);
            deletion.mutations[0].deleted = true;
            memory
                .apply_batch(&namespace, "entity", deletion)
                .await
                .unwrap();
        }
        let snapshot = memory.snapshot(&namespace, "entity").await.unwrap();
        assert_eq!(snapshot.value_bytes(), 8 * 1024 * 1024);
        assert!(
            snapshot
                .entries
                .iter()
                .filter(|entry| entry.deleted)
                .all(|entry| entry.value.is_empty())
        );
        drop(memory);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn cold_loads_stay_ordered_across_cache_resizes_and_concurrent_commits() {
        let root = std::env::temp_dir().join(format!("dd-cold-load-{}", uuid::Uuid::new_v4()));
        let state = StateStore::open(&root).await.unwrap();
        let mut memory = MemoryStore::from_state(state);
        let namespace = worker_namespace("worker", "MEMORY");
        let commit = |value: &'static [u8]| MemoryCommit {
            mutations: vec![MemoryBatchMutation {
                key: "key".into(),
                value: Bytes::from_static(value),
                encoding: "utf8".into(),
                deleted: false,
            }],
            ..Default::default()
        };
        memory
            .apply_batch(&namespace, "entity", commit(b"before"))
            .await
            .unwrap();
        memory.set_profile_enabled(true);
        let pause = Arc::new(SnapshotLoadPause::default());
        memory.snapshots.lock().unwrap().before_fill = Some(Arc::clone(&pause));
        let first_memory = memory.clone();
        let first_namespace = namespace.clone();
        let first = tokio::spawn(async move {
            first_memory
                .snapshot(&first_namespace, "entity")
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(2), pause.loaded.notified())
            .await
            .unwrap();
        memory.set_snapshot_cache_limits(1, 1024);
        memory
            .apply_batch(&namespace, "entity", commit(b"after"))
            .await
            .unwrap();
        let misses = memory.cache_performance_snapshot().snapshot_misses;
        let mut second = Box::pin(memory.snapshot(&namespace, "entity"));
        std::future::poll_fn(|context| {
            assert!(
                second.as_mut().poll(context).is_pending(),
                "a second cold load must await the first fill"
            );
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(
            memory.cache_performance_snapshot().snapshot_misses,
            misses,
            "duplicate SQL loads can fill the cache out of order during publication"
        );
        tokio::time::timeout(Duration::from_secs(2), memory.snapshot(&namespace, "other"))
            .await
            .unwrap()
            .unwrap();
        pause.resume.notify_one();
        assert_eq!(first.await.unwrap().entries[0].value.as_ref(), b"before");
        assert_eq!(second.await.unwrap().entries[0].value.as_ref(), b"after");
        assert_eq!(
            memory.snapshot(&namespace, "entity").await.unwrap().entries[0]
                .value
                .as_ref(),
            b"after"
        );
        let canceled_pause = Arc::new(SnapshotLoadPause::default());
        memory.snapshots.lock().unwrap().before_fill = Some(Arc::clone(&canceled_pause));
        let canceled_memory = memory.clone();
        let canceled_namespace = namespace.clone();
        let canceled = tokio::spawn(async move {
            canceled_memory
                .snapshot(&canceled_namespace, "canceled")
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), canceled_pause.loaded.notified())
            .await
            .unwrap();
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        tokio::time::timeout(
            Duration::from_secs(2),
            memory.snapshot(&namespace, "canceled"),
        )
        .await
        .unwrap()
        .unwrap();
        drop(memory);
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn outbox_payload_charge(
    payload: usize,
    worker: &str,
    binding: &str,
    entity: &str,
    id_bytes: usize,
    kind: &str,
) -> usize {
    payload
        .saturating_mul(2)
        .saturating_add(worker.len())
        .saturating_add(binding.len())
        .saturating_add(entity.len())
        .saturating_add(id_bytes)
        .saturating_add(kind.len())
        .saturating_add(256)
}

#[cfg(test)]
mod bounded_state_tests {
    use super::*;

    fn mutation(key: String, deleted: bool) -> MemoryBatchMutation {
        MemoryBatchMutation {
            key,
            value: Bytes::new(),
            encoding: "utf8".into(),
            deleted,
        }
    }

    fn long_key(index: usize) -> String {
        let prefix = format!("key-{index:05}-");
        format!("{prefix}{}", "x".repeat(4096 - prefix.len()))
    }

    #[tokio::test]
    async fn metadata_growth_rolls_back_and_delete_compaction_preserves_fencing_after_restart() {
        let root =
            std::env::temp_dir().join(format!("dd-memory-metadata-{}", uuid::Uuid::new_v4()));
        let state = StateStore::open(&root).await.unwrap();
        let memory = MemoryStore::from_state(Arc::clone(&state));
        let namespace = worker_namespace("worker", "MEMORY");
        let lease = memory.acquire_lease(&namespace, "entity").await.unwrap();
        let initial = memory
            .apply_batch(
                &namespace,
                "entity",
                MemoryCommit {
                    mutations: (0..999)
                        .map(|index| mutation(long_key(index), false))
                        .collect(),
                    owner_epoch: Some(lease.owner_epoch()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .max_version;
        let snapshot = memory.snapshot(&namespace, "entity").await.unwrap();
        snapshot.validate_limits().unwrap();
        assert!(snapshot.metadata_bytes() > 4_000_000);
        let rejected = MemoryCommit {
            mutations: vec![mutation(long_key(999), false)],
            command_result: Some(MemoryCommandResultWrite {
                idempotency_key: "rejected".into(),
                result: b"must rollback".to_vec(),
            }),
            outbox_effects: vec![MemoryOutboxEffectWrite {
                kind: "audit.rejected".into(),
                payload: vec![],
            }],
            ..Default::default()
        };
        assert!(
            memory
                .apply_batch(&namespace, "entity", rejected)
                .await
                .unwrap_err()
                .to_string()
                .contains("metadata")
        );
        assert_eq!(
            memory
                .snapshot(&namespace, "entity")
                .await
                .unwrap()
                .max_version,
            initial
        );
        assert!(
            memory
                .command_result(&namespace, "entity", "rejected")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            memory
                .outbox_records(&namespace, "entity")
                .await
                .unwrap()
                .is_empty()
        );
        memory
            .apply_batch(
                &namespace,
                "entity",
                MemoryCommit {
                    mutations: vec![mutation(long_key(0), true), mutation(long_key(999), false)],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let current = memory.snapshot(&namespace, "entity").await.unwrap();
        assert_eq!(current.entries.len(), 999);
        assert!(current.entries.iter().all(|entry| !entry.deleted));
        let deleted = memory
            .apply_batch(
                &namespace,
                "entity",
                MemoryCommit {
                    mutations: current
                        .entries
                        .iter()
                        .map(|entry| mutation(entry.key.clone(), true))
                        .collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .max_version;
        assert!(
            memory
                .snapshot(&namespace, "entity")
                .await
                .unwrap()
                .entries
                .is_empty()
        );
        let conn = state
            .read(StateStore::shard_index("worker", "MEMORY", "entity"))
            .await
            .unwrap();
        let mut rows=conn.query("SELECT COUNT(*) FROM memory_state WHERE worker='worker' AND binding='MEMORY' AND entity_key='entity'",()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0
        );
        drop(rows);
        drop(conn);
        drop(current);
        drop(snapshot);
        let owner = lease.owner_epoch();
        drop(lease);
        drop(memory);
        drop(state);
        let memory = MemoryStore::from_state(StateStore::open(&root).await.unwrap());
        let snapshot = memory.snapshot(&namespace, "entity").await.unwrap();
        assert!(snapshot.entries.is_empty());
        assert_eq!(snapshot.max_version, deleted);
        assert!(
            memory
                .apply_batch(
                    &namespace,
                    "entity",
                    MemoryCommit {
                        mutations: vec![mutation("stale".into(), false)],
                        owner_epoch: Some(owner - 1),
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        let lease = memory.acquire_lease(&namespace, "entity").await.unwrap();
        assert!(lease.owner_epoch() > owner);
        let restored = memory
            .apply_batch(
                &namespace,
                "entity",
                MemoryCommit {
                    mutations: vec![mutation("restored".into(), false)],
                    owner_epoch: Some(lease.owner_epoch()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(restored.max_version > deleted);
        drop(lease);
        drop(memory);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn entry_count_is_enforced_independently_of_value_and_metadata_bytes() {
        let root = std::env::temp_dir().join(format!("dd-memory-entries-{}", uuid::Uuid::new_v4()));
        let memory = MemoryStore::from_state(StateStore::open(&root).await.unwrap());
        let namespace = worker_namespace("worker", "MEMORY");
        memory
            .apply_batch(
                &namespace,
                "entity",
                MemoryCommit {
                    mutations: (0..MEMORY_ENTITY_MAX_ENTRIES)
                        .map(|index| mutation(format!("key-{index:05}"), false))
                        .collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            memory
                .apply_batch(
                    &namespace,
                    "entity",
                    MemoryCommit {
                        mutations: vec![mutation("overflow".into(), false)],
                        ..Default::default()
                    }
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("entries")
        );
        memory
            .apply_batch(
                &namespace,
                "entity",
                MemoryCommit {
                    mutations: vec![
                        mutation("key-00000".into(), true),
                        mutation("overflow".into(), false),
                    ],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            memory
                .snapshot(&namespace, "entity")
                .await
                .unwrap()
                .entries
                .len(),
            MEMORY_ENTITY_MAX_ENTRIES
        );
        drop(memory);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_size_recovery_must_reduce_a_violated_dimension_without_growing_another() {
        let before = EntitySize {
            values: MEMORY_ENTITY_MAX_VALUE_BYTES as i64 + 1,
            metadata: MEMORY_ENTITY_MAX_METADATA_BYTES as i64 + 1,
            entries: 1,
        };
        assert!(before.validate(Some(before)).is_err());
        assert!(
            EntitySize {
                values: 0,
                ..before
            }
            .validate(Some(before))
            .is_ok()
        );
        assert!(
            EntitySize {
                values: 0,
                metadata: before.metadata + 1,
                ..before
            }
            .validate(Some(before))
            .is_err()
        );
    }
}
