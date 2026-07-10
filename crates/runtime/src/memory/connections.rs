impl MemoryStore {
    fn reserve_version_after(&self, namespace: &str, memory_key: &str, floor: i64) -> i64 {
        let minimum = floor.saturating_add(1).max(1) as u64;
        let shard = self.shard_for_key(namespace, memory_key);
        let mut current = shard.version.load(Ordering::Relaxed);
        loop {
            let next = current.max(minimum);
            match shard.version.compare_exchange(
                current,
                next.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next as i64,
                Err(actual) => current = actual,
            }
        }
    }

    async fn connect(&self, namespace: &str, memory_key: &str) -> Result<MemoryReadConnection> {
        let namespace = namespace.trim();
        if namespace.is_empty() {
            return Err(PlatformError::runtime("memory namespace must not be empty"));
        }
        let memory_key = memory_key.trim();
        if memory_key.is_empty() {
            return Err(PlatformError::runtime("memory key must not be empty"));
        }

        self.connect_shard(namespace, self.shard_index(namespace, memory_key))
            .await
    }

    async fn connect_shard(
        &self,
        namespace: &str,
        shard_index: usize,
    ) -> Result<MemoryReadConnection> {
        let handle = self.database_handle_shard(namespace, shard_index).await?;
        self.checkout_reader(handle).await
    }

    async fn writer_connection(
        &self,
        namespace: &str,
        memory_key: &str,
    ) -> Result<MemoryWriterConnection> {
        let namespace = namespace.trim();
        if namespace.is_empty() {
            return Err(PlatformError::runtime("memory namespace must not be empty"));
        }
        let memory_key = memory_key.trim();
        if memory_key.is_empty() {
            return Err(PlatformError::runtime("memory key must not be empty"));
        }
        self.writer_connection_shard(namespace, self.shard_index(namespace, memory_key))
            .await
    }

    async fn writer_connection_shard(
        &self,
        namespace: &str,
        shard_index: usize,
    ) -> Result<MemoryWriterConnection> {
        let handle = self.database_handle_shard(namespace, shard_index).await?;
        self.checkout_writer(handle).await
    }

    async fn database_handle_shard(
        &self,
        namespace: &str,
        shard_index: usize,
    ) -> Result<Arc<MemoryDatabaseHandle>> {
        let shard = self.shard_for_index(shard_index);
        let db_key = self.database_key(namespace, shard_index);
        let now = Instant::now();
        let slot = {
            let mut databases = shard.databases.lock().await;
            self.prune_databases_locked(shard_index, &mut databases, now);
            if let Some(slot) = databases.touch(&db_key, now) {
                self.record_profile(MemoryProfileMetricKind::StoreDatabaseCacheHit, 0, 1);
                slot
            } else {
                self.record_profile(MemoryProfileMetricKind::StoreDatabaseCacheMiss, 0, 1);
                let slot = Arc::new(MemoryDatabaseSlot::new());
                databases.insert_slot(db_key.clone(), Arc::clone(&slot), now);
                slot
            }
        };

        let path = self.db_path(namespace, shard_index);
        let handle = match slot
            .handle
            .get_or_try_init(|| async {
                ensure_parent_dir(&path)?;
                let path_str = path.to_string_lossy().to_string();
                let database = Builder::new_local(&path_str)
                    .build()
                    .await
                    .map_err(memory_error)?;
                let database = Arc::new(database);
                ensure_schema(&database).await?;
                Ok::<_, PlatformError>(Arc::new(MemoryDatabaseHandle {
                    database,
                    readers: Arc::new(MemoryReadPool {
                        idle: StdMutex::new(Vec::new()),
                        permits: Arc::new(Semaphore::new(self.db_read_connections_per_database)),
                        global_permits: Arc::clone(&self.db_connection_permits),
                        max_idle: self.db_read_connections_per_database,
                    }),
                    writer: Arc::new(Mutex::new(None)),
                }))
            })
            .await
        {
            Ok(handle) => Arc::clone(handle),
            Err(error) => {
                let mut databases = shard.databases.lock().await;
                let remove_failed_slot = databases
                    .entries
                    .get(&db_key)
                    .map(|entry| {
                        Arc::ptr_eq(&entry.slot, &slot) && entry.slot.handle.get().is_none()
                    })
                    .unwrap_or(false);
                if remove_failed_slot {
                    databases.remove(&db_key);
                }
                return Err(error);
            }
        };

        {
            let mut databases = shard.databases.lock().await;
            if databases
                .entries
                .get(&db_key)
                .map(|entry| Arc::ptr_eq(&entry.slot, &slot))
                .unwrap_or(false)
            {
                databases.touch(&db_key, Instant::now());
            }
            self.prune_databases_locked(shard_index, &mut databases, Instant::now());
        }

        Ok(handle)
    }

    async fn open_pooled_connection(
        &self,
        database: &Arc<Database>,
        pool_permit: Option<OwnedSemaphorePermit>,
    ) -> Result<PooledMemoryConnection> {
        let permit = self
            .db_connection_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PlatformError::runtime("memory connection pool is closed"))?;
        let conn = database.connect().map_err(memory_error)?;
        configure_connection(&conn).await?;
        let live = self
            .db_live_connections
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.update_peak_connections(live);
        self.record_profile(MemoryProfileMetricKind::StoreConnectionCreate, 0, 1);
        self.record_profile(
            MemoryProfileMetricKind::StoreConnectionsLive,
            live as u64,
            1,
        );
        Ok(PooledMemoryConnection {
            conn,
            _permit: MemoryConnectionPermit {
                _permit: permit,
                live_connections: Arc::clone(&self.db_live_connections),
            },
            _pool_permit: pool_permit,
        })
    }

    async fn checkout_reader(
        &self,
        handle: Arc<MemoryDatabaseHandle>,
    ) -> Result<MemoryReadConnection> {
        let started_at = Instant::now();
        let pooled = {
            let mut idle = handle
                .readers
                .idle
                .lock()
                .expect("memory read pool mutex should not be poisoned");
            idle.pop()
        };
        let pooled = if let Some(pooled) = pooled {
            self.record_profile(MemoryProfileMetricKind::StoreConnectionReuse, 0, 1);
            pooled
        } else {
            let pool_permit = handle
                .readers
                .permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| PlatformError::runtime("memory read pool is closed"))?;
            self.open_pooled_connection(&handle.database, Some(pool_permit))
                .await?
        };
        self.record_profile(
            MemoryProfileMetricKind::StoreReaderPoolWait,
            started_at.elapsed().as_micros() as u64,
            1,
        );
        Ok(MemoryReadConnection {
            pool: Arc::clone(&handle.readers),
            pooled: Some(pooled),
            healthy: true,
        })
    }

    async fn checkout_writer(
        &self,
        handle: Arc<MemoryDatabaseHandle>,
    ) -> Result<MemoryWriterConnection> {
        let started_at = Instant::now();
        let mut guard = handle.writer.clone().lock_owned().await;
        self.record_profile(
            MemoryProfileMetricKind::StoreWriterLaneWait,
            started_at.elapsed().as_micros() as u64,
            1,
        );
        if guard.is_none() {
            *guard = Some(self.open_pooled_connection(&handle.database, None).await?);
        } else {
            self.record_profile(MemoryProfileMetricKind::StoreConnectionReuse, 0, 1);
        }
        Ok(MemoryWriterConnection {
            guard,
            profile: Arc::clone(&self.profile),
            healthy: true,
        })
    }

    fn update_peak_connections(&self, live: usize) {
        let mut current = self.db_peak_connections.load(Ordering::Relaxed);
        while live > current {
            match self.db_peak_connections.compare_exchange(
                current,
                live,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.record_profile(
                        MemoryProfileMetricKind::StoreConnectionsPeak,
                        live as u64,
                        1,
                    );
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    async fn max_version_for_memory(
        &self,
        conn: &Connection,
        memory_key: &str,
    ) -> Result<Option<i64>> {
        Ok(self
            .memory_meta_for_commit(conn, memory_key)
            .await
            .map_err(PlatformError::from)?
            .0)
    }

    async fn memory_meta_for_commit(
        &self,
        conn: &Connection,
        memory_key: &str,
    ) -> MemoryTransactionResult<(Option<i64>, Option<i64>)> {
        let mut rows = query_cached(
            conn,
            "SELECT max_version, owner_epoch FROM memory_meta
                 WHERE entity_key = ?1
                 LIMIT 1",
            (memory_key,),
        )
        .await?;
        let meta = if let Some(row) = rows.next().await? {
            (row.get::<Option<i64>>(0)?, row.get::<Option<i64>>(1)?)
        } else {
            return Ok((None, None));
        };
        let _ = rows.next().await?;
        Ok(meta)
    }

    async fn record_for_key(
        &self,
        conn: &Connection,
        memory_key: &str,
        item_key: &str,
    ) -> Result<Option<MemorySnapshotEntry>> {
        let mut rows = query_cached(
            conn,
            "SELECT value_blob, encoding, value, version, deleted
                 FROM memory_state
                 WHERE entity_key = ?1 AND item_key = ?2
                 LIMIT 1",
            (memory_key, item_key),
        )
        .await
        .map_err(memory_error)?;
        let Some(row) = rows.next().await.map_err(memory_error)? else {
            return Ok(None);
        };
        let value_blob: Option<Vec<u8>> = row.get::<Option<Vec<u8>>>(0).map_err(memory_error)?;
        let encoding: String = row.get::<String>(1).map_err(memory_error)?;
        let legacy_value: String = row.get::<String>(2).map_err(memory_error)?;
        let version: i64 = row.get::<i64>(3).map_err(memory_error)?;
        let deleted: i64 = row.get::<i64>(4).map_err(memory_error)?;
        let _ = rows.next().await.map_err(memory_error)?;
        Ok(Some(MemorySnapshotEntry {
            key: item_key.to_string(),
            value: value_blob.unwrap_or_else(|| legacy_value.into_bytes()),
            encoding: normalize_encoding(&encoding),
            version,
            deleted: deleted != 0,
        }))
    }

    async fn discover_namespaces_for_shard(&self, shard_index: usize) -> Result<Vec<String>> {
        let mut namespaces = Vec::new();
        let entries = match std::fs::read_dir(self.root_dir.as_ref()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(namespaces),
            Err(error) => return Err(memory_error(error)),
        };
        for entry in entries {
            let entry = entry.map_err(memory_error)?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let shard_path = path.join(format!("shard-{shard_index:04}.db"));
            if !shard_path.exists() {
                continue;
            }
            let raw = entry.file_name();
            let Some(namespace) = hex_decode_to_utf8(raw.to_string_lossy().as_ref()) else {
                continue;
            };
            namespaces.push(namespace);
        }
        namespaces.sort();
        namespaces.dedup();
        Ok(namespaces)
    }

    fn database_key(&self, namespace: &str, shard_index: usize) -> String {
        // The cache stores a shareable Database handle per namespace and
        // physical shard. Connections are created per request and configured
        // independently, so the handle itself does not need thread-local cache
        // identity.
        format!("{namespace}\u{1f}{shard_index}")
    }

}
