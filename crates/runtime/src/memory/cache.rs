impl MemoryStore {
    fn memory_version_key(namespace: &str, memory_key: &str) -> String {
        format!("{namespace}\u{1f}{memory_key}")
    }

    fn memory_snapshot_key(namespace: &str, memory_key: &str) -> String {
        format!("{namespace}\u{1f}{memory_key}")
    }

    fn db_path(&self, namespace: &str, shard_index: usize) -> PathBuf {
        let encoded_namespace = hex_encode(namespace.as_bytes());
        self.root_dir
            .join(encoded_namespace)
            .join(format!("shard-{shard_index:04}.db"))
    }

    fn shard_hash_version_for_namespace(&self, namespace: &str) -> u32 {
        self.namespace_shard_hash_versions
            .get(namespace)
            .copied()
            .unwrap_or(self.shard_hash_version)
    }

    fn shard_index(&self, namespace: &str, memory_key: &str) -> usize {
        if let Some(shard_index) = self
            .namespace_key_shard_overrides
            .get(namespace)
            .and_then(|keys| keys.get(memory_key))
        {
            return *shard_index;
        }
        memory_shard_index_for_hash_version(
            memory_key,
            self.namespace_shards,
            self.shard_hash_version_for_namespace(namespace),
        )
        .expect("memory store must have a supported shard hash version")
    }

    fn shard_for_key(&self, namespace: &str, memory_key: &str) -> &MemoryShard {
        self.shard_for_index(self.shard_index(namespace, memory_key))
    }

    fn shard_for_index(&self, shard_index: usize) -> &MemoryShard {
        &self.shards[shard_index % self.shards.len()]
    }

    fn entity_cache_stripe<'a>(
        &'a self,
        namespace: &str,
        memory_key: &str,
    ) -> (usize, &'a MemoryEntityCacheStripe) {
        let shard = self.shard_for_key(namespace, memory_key);
        let stripe_index = Self::entity_cache_stripe_index(namespace, memory_key);
        (stripe_index, &shard.cache_stripes[stripe_index])
    }

    fn entity_cache_stripe_index(namespace: &str, memory_key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        namespace.hash(&mut hasher);
        0xffu8.hash(&mut hasher);
        memory_key.hash(&mut hasher);
        (hasher.finish() as usize) % MEMORY_ENTITY_CACHE_STRIPES
    }

    async fn lock_entity_cache_stripe<'a>(
        &'a self,
        namespace: &str,
        memory_key: &str,
    ) -> (usize, tokio::sync::MutexGuard<'a, MemoryEntityCacheState>) {
        let (stripe_index, stripe) = self.entity_cache_stripe(namespace, memory_key);
        let started_at = Instant::now();
        let state = stripe.state.lock().await;
        self.record_profile(
            MemoryProfileMetricKind::EntityCacheLockWait,
            started_at.elapsed().as_micros() as u64,
            1,
        );
        (stripe_index, state)
    }

    fn observe_version(&self, namespace: &str, memory_key: &str, version: i64) {
        self.shard_for_key(namespace, memory_key)
            .observe_version(version);
    }

    async fn observe_memory_version(&self, namespace: &str, memory_key: &str, version: i64) {
        if version < 0 {
            return;
        }
        let (_, mut state) = self.lock_entity_cache_stripe(namespace, memory_key).await;
        state
            .memory_versions
            .insert(Self::memory_version_key(namespace, memory_key), version);
    }

    async fn cached_full_snapshot(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Option<MemorySnapshot> {
        let entry = self.cached_snapshot_entry(namespace, memory_key).await?;
        if !entry.complete {
            return None;
        }
        Some(MemorySnapshot {
            entries: snapshot_entries_from_records(entry.records.as_ref()),
            max_version: entry.max_version,
        })
    }

    async fn cached_point_read(
        &self,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
    ) -> Option<MemoryPointRead> {
        let entry = self.cached_snapshot_entry(namespace, memory_key).await?;
        if !entry.complete && !entry.loaded_keys.contains(item_key) {
            return None;
        }
        Some(MemoryPointRead {
            record: entry.records.get(item_key).cloned(),
            max_version: entry.max_version,
        })
    }

    async fn cached_keys_snapshot(
        &self,
        namespace: &str,
        memory_key: &str,
        keys: &[String],
    ) -> Option<MemorySnapshot> {
        let entry = self.cached_snapshot_entry(namespace, memory_key).await?;
        if !entry.complete
            && keys
                .iter()
                .any(|key| !entry.loaded_keys.contains(key.as_str()))
        {
            return None;
        }
        let mut entries = keys
            .iter()
            .filter_map(|key| entry.records.get(key).cloned())
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        Some(MemorySnapshot {
            entries,
            max_version: entry.max_version,
        })
    }

    async fn cached_snapshot_entry(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Option<MemorySharedSnapshotEntry> {
        let key = Self::memory_snapshot_key(namespace, memory_key);
        let now = Instant::now();
        let shard_index = self.shard_index(namespace, memory_key);
        let (stripe_index, mut state) = self.lock_entity_cache_stripe(namespace, memory_key).await;
        let current_version = state
            .memory_versions
            .get(&Self::memory_version_key(namespace, memory_key))
            .copied();
        self.prune_snapshots_locked(shard_index, stripe_index, &mut state.snapshots, now);
        let Some(entry) = state.snapshots.get_cloned(&key, now) else {
            self.record_profile(MemoryProfileMetricKind::StoreSnapshotCacheMiss, 0, 1);
            return None;
        };
        if current_version.is_some_and(|current| current > entry.max_version) {
            state.snapshots.remove(&key);
            self.record_profile(MemoryProfileMetricKind::StoreSnapshotCacheMiss, 0, 1);
            return None;
        }
        self.record_profile(MemoryProfileMetricKind::StoreSnapshotCacheHit, 0, 1);
        Some(entry)
    }

    async fn put_full_snapshot(
        &self,
        namespace: &str,
        memory_key: &str,
        snapshot: &MemorySnapshot,
    ) {
        self.put_partial_snapshot(
            namespace,
            memory_key,
            snapshot.max_version,
            snapshot.entries.clone(),
            snapshot.entries.iter().map(|entry| entry.key.clone()),
            true,
        )
        .await;
    }

    async fn put_partial_snapshot<I>(
        &self,
        namespace: &str,
        memory_key: &str,
        max_version: i64,
        entries: Vec<MemorySnapshotEntry>,
        loaded_keys: I,
        complete: bool,
    ) where
        I: IntoIterator<Item = String>,
    {
        let key = Self::memory_snapshot_key(namespace, memory_key);
        let now = Instant::now();
        let shard_index = self.shard_index(namespace, memory_key);
        let (stripe_index, mut state) = self.lock_entity_cache_stripe(namespace, memory_key).await;
        self.prune_snapshots_locked(shard_index, stripe_index, &mut state.snapshots, now);
        let existing = state.snapshots.entries.remove(&key);
        let existing = if let Some(existing) = existing {
            state.snapshots.approximate_bytes = state
                .snapshots
                .approximate_bytes
                .saturating_sub(existing.approximate_bytes);
            state
                .snapshots
                .recency
                .remove(&(existing.last_used_at, existing.recency_id));
            Some(existing)
        } else {
            None
        };

        if existing
            .as_ref()
            .is_some_and(|entry| entry.max_version > max_version)
        {
            let mut entry = existing.expect("checked above");
            entry.last_used_at = now;
            state.snapshots.insert_or_replace(key, entry);
            return;
        }

        let mut next_records = if complete
            || existing
                .as_ref()
                .is_some_and(|entry| entry.max_version < max_version)
        {
            HashMap::new()
        } else {
            existing
                .as_ref()
                .map(|entry| entry.records.as_ref().clone())
                .unwrap_or_default()
        };
        let mut next_loaded_keys = if complete
            || existing
                .as_ref()
                .is_some_and(|entry| entry.max_version < max_version)
        {
            HashSet::new()
        } else {
            existing
                .as_ref()
                .map(|entry| entry.loaded_keys.as_ref().clone())
                .unwrap_or_default()
        };

        for loaded_key in loaded_keys {
            let normalized_key = loaded_key.trim();
            if !normalized_key.is_empty() {
                next_loaded_keys.insert(normalized_key.to_string());
            }
        }
        for snapshot_entry in entries {
            let normalized_key = snapshot_entry.key.trim();
            if normalized_key.is_empty() {
                continue;
            }
            next_loaded_keys.insert(normalized_key.to_string());
            next_records.insert(normalized_key.to_string(), snapshot_entry);
        }

        let approximate_bytes =
            approximate_snapshot_cache_bytes(&key, &next_records, &next_loaded_keys);
        if approximate_bytes > self.snapshot_cache_byte_budget_for_stripe(shard_index, stripe_index)
        {
            return;
        }
        state.snapshots.insert_or_replace(
            key,
            MemorySharedSnapshotEntry {
                records: Arc::new(next_records),
                loaded_keys: Arc::new(next_loaded_keys),
                complete,
                max_version,
                last_used_at: now,
                recency_id: 0,
                approximate_bytes,
            },
        );
        self.prune_snapshots_locked(shard_index, stripe_index, &mut state.snapshots, now);
    }

    async fn update_cached_snapshot_after_commit(
        &self,
        namespace: &str,
        memory_key: &str,
        max_version: i64,
        mutations: &[MemoryBatchMutation],
    ) {
        let key = Self::memory_snapshot_key(namespace, memory_key);
        let now = Instant::now();
        let shard_index = self.shard_index(namespace, memory_key);
        let (stripe_index, mut state) = self.lock_entity_cache_stripe(namespace, memory_key).await;
        self.prune_snapshots_locked(shard_index, stripe_index, &mut state.snapshots, now);
        let Some(entry) = state.snapshots.remove(&key) else {
            return;
        };
        let mut next_records = entry.records.as_ref().clone();
        let mut next_loaded_keys = entry.loaded_keys.as_ref().clone();
        for mutation in mutations {
            let normalized_key = mutation.key.trim();
            if normalized_key.is_empty() {
                continue;
            }
            let record = MemorySnapshotEntry {
                key: normalized_key.to_string(),
                value: mutation.value.clone(),
                encoding: mutation.encoding.clone(),
                version: max_version,
                deleted: mutation.deleted,
            };
            next_loaded_keys.insert(normalized_key.to_string());
            next_records.insert(normalized_key.to_string(), record);
        }
        let approximate_bytes =
            approximate_snapshot_cache_bytes(&key, &next_records, &next_loaded_keys);
        if approximate_bytes > self.snapshot_cache_byte_budget_for_stripe(shard_index, stripe_index)
        {
            return;
        }
        state.snapshots.insert_or_replace(
            key,
            MemorySharedSnapshotEntry {
                records: Arc::new(next_records),
                loaded_keys: Arc::new(next_loaded_keys),
                complete: entry.complete,
                max_version,
                last_used_at: now,
                recency_id: 0,
                approximate_bytes,
            },
        );
    }

    fn prune_databases_locked(
        &self,
        shard_index: usize,
        databases: &mut MemoryDatabaseCache,
        now: Instant,
    ) {
        let evicted = databases.prune(
            now,
            self.db_idle_ttl,
            self.database_cache_budget_for_shard(shard_index),
        );
        if evicted > 0 {
            self.record_profile(
                MemoryProfileMetricKind::StoreDatabaseCacheEviction,
                0,
                evicted as u64,
            );
        }
    }

    fn prune_snapshots_locked(
        &self,
        shard_index: usize,
        stripe_index: usize,
        snapshots: &mut MemorySharedSnapshotCache,
        now: Instant,
    ) {
        let evicted = snapshots.prune(
            now,
            self.db_idle_ttl,
            self.snapshot_cache_entry_budget_for_stripe(shard_index, stripe_index),
            self.snapshot_cache_byte_budget_for_stripe(shard_index, stripe_index),
        );
        if evicted > 0 {
            self.record_profile(
                MemoryProfileMetricKind::StoreSnapshotCacheEviction,
                0,
                evicted as u64,
            );
        }
        self.record_profile(
            MemoryProfileMetricKind::EntityCacheStripeCount,
            0,
            MEMORY_ENTITY_CACHE_STRIPES as u64,
        );
        self.record_profile(
            MemoryProfileMetricKind::EntityCachePeakEntriesPerStripe,
            snapshots.entries.len() as u64,
            1,
        );
        self.record_profile(
            MemoryProfileMetricKind::EntityCacheMaxEntriesPerStripe,
            self.snapshot_cache_entry_budget_for_stripe(shard_index, stripe_index) as u64,
            1,
        );
    }

    fn database_cache_budget_for_shard(&self, shard_index: usize) -> usize {
        partitioned_budget(self.db_cache_max_open, self.namespace_shards, shard_index)
    }

    fn snapshot_cache_entry_budget_for_shard(&self, shard_index: usize) -> usize {
        partitioned_budget(
            self.snapshot_cache_max_entries,
            self.namespace_shards,
            shard_index,
        )
    }

    fn snapshot_cache_byte_budget_for_shard(&self, shard_index: usize) -> usize {
        partitioned_budget(
            self.snapshot_cache_max_bytes,
            self.namespace_shards,
            shard_index,
        )
    }

    fn snapshot_cache_entry_budget_for_stripe(
        &self,
        shard_index: usize,
        stripe_index: usize,
    ) -> usize {
        partitioned_budget(
            self.snapshot_cache_entry_budget_for_shard(shard_index),
            MEMORY_ENTITY_CACHE_STRIPES,
            stripe_index,
        )
    }

    fn snapshot_cache_byte_budget_for_stripe(
        &self,
        shard_index: usize,
        stripe_index: usize,
    ) -> usize {
        partitioned_budget(
            self.snapshot_cache_byte_budget_for_shard(shard_index),
            MEMORY_ENTITY_CACHE_STRIPES,
            stripe_index,
        )
    }

    #[cfg(test)]
    async fn test_entity_cache_snapshot(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> MemoryEntityCacheDebugSnapshot {
        let (_, state) = self.lock_entity_cache_stripe(namespace, memory_key).await;
        MemoryEntityCacheDebugSnapshot {
            snapshot_keys: state.snapshots.entries.keys().cloned().collect(),
            snapshot_entries: state.snapshots.entries.len(),
            snapshot_approximate_bytes: state.snapshots.approximate_bytes,
        }
    }

    #[cfg(test)]
    fn test_entity_cache_stripe_index(&self, namespace: &str, memory_key: &str) -> usize {
        Self::entity_cache_stripe_index(namespace, memory_key)
    }
}
