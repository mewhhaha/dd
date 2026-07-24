#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("dd-memory-store-{name}-{}", Uuid::new_v4()))
    }

    fn utf8_mutation(key: &str, value: &str) -> MemoryBatchMutation {
        MemoryBatchMutation {
            key: key.to_string(),
            value: value.as_bytes().to_vec(),
            encoding: ENCODING_UTF8.to_string(),
            deleted: false,
        }
    }

    async fn snapshot_value(
        store: &MemoryStore,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let snapshot = store.snapshot(namespace, memory_key).await?;
        Ok(snapshot
            .entries
            .into_iter()
            .find(|entry| entry.key == item_key && !entry.deleted)
            .map(|entry| entry.value))
    }

    struct PersistedOutboxRow {
        effect_id: String,
        kind: String,
        payload: Vec<u8>,
        revision: i64,
        status: String,
    }

    async fn persisted_outbox_rows(
        store: &MemoryStore,
        namespace: &str,
        memory_key: &str,
    ) -> Result<Vec<PersistedOutboxRow>> {
        let conn = store.connect(namespace, memory_key).await?;
        let mut rows = conn
            .query(
                "SELECT effect_id, kind, payload_blob, revision, status
                 FROM memory_outbox
                 WHERE entity_key = ?1
                 ORDER BY revision, effect_id",
                (memory_key,),
            )
            .await
            .map_err(memory_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            records.push(PersistedOutboxRow {
                effect_id: row.get::<String>(0).map_err(memory_error)?,
                kind: row.get::<String>(1).map_err(memory_error)?,
                payload: row.get::<Vec<u8>>(2).map_err(memory_error)?,
                revision: row.get::<i64>(3).map_err(memory_error)?,
                status: row.get::<String>(4).map_err(memory_error)?,
            });
        }
        Ok(records)
    }

    async fn persisted_command_results(
        store: &MemoryStore,
        namespace: &str,
        memory_key: &str,
        idempotency_key: &str,
    ) -> Result<Vec<(Vec<u8>, i64)>> {
        let conn = store.connect(namespace, memory_key).await?;
        let mut rows = conn
            .query(
                "SELECT result_blob, revision
                 FROM memory_commands
                 WHERE entity_key = ?1 AND idempotency_key = ?2",
                (memory_key, idempotency_key),
            )
            .await
            .map_err(memory_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(memory_error)? {
            records.push((
                row.get::<Vec<u8>>(0).map_err(memory_error)?,
                row.get::<i64>(1).map_err(memory_error)?,
            ));
        }
        Ok(records)
    }

    fn same_entity_cache_stripe_key(
        store: &MemoryStore,
        namespace: &str,
        base_key: &str,
        prefix: &str,
    ) -> String {
        let target = store.test_entity_cache_stripe_index(namespace, base_key);
        (0..100_000)
            .map(|index| format!("{prefix}-{index}"))
            .find(|candidate| {
                candidate != base_key
                    && store.test_entity_cache_stripe_index(namespace, candidate) == target
            })
            .expect("test should find another key in the same entity cache stripe")
    }

    fn enable_one_snapshot_slot_per_entity_stripe(store: &mut MemoryStore) {
        store.snapshot_cache_max_entries = store.namespace_shards * MEMORY_ENTITY_CACHE_STRIPES;
        store.snapshot_cache_max_bytes = usize::MAX / 2;
    }

    fn key_with_different_hash_layout(shards: usize) -> (String, usize, usize) {
        (0..10_000)
            .map(|index| format!("legacy-entity-{index}"))
            .find_map(|key| {
                let legacy = legacy_default_hasher_memory_shard_index(&key, shards);
                let stable = stable_memory_shard_index(&key, shards);
                (legacy != stable).then_some((key, legacy, stable))
            })
            .expect("test should find key with different legacy and stable shards")
    }

    fn key_with_distinct_physical_shard(shards: usize) -> (String, usize, usize, usize) {
        (0..10_000)
            .map(|index| format!("physical-entity-{index}"))
            .find_map(|key| {
                let stable = stable_memory_shard_index(&key, shards);
                let legacy = legacy_default_hasher_memory_shard_index(&key, shards);
                let physical = (0..shards).find(|shard| *shard != stable && *shard != legacy)?;
                Some((key, physical, stable, legacy))
            })
            .expect("test should find key with a distinct physical shard")
    }

    fn key_at_or_above_shard(shards: usize, minimum_shard: usize) -> String {
        (0..10_000)
            .map(|index| format!("entity-{index}"))
            .find(|key| stable_memory_shard_index(key, shards) >= minimum_shard)
            .expect("test should find key at or above requested shard")
    }

    async fn seed_legacy_default_hasher_memory_state(
        root: &Path,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
        value: &str,
        version: i64,
        namespace_shards: usize,
    ) -> Result<usize> {
        let shard_index = legacy_default_hasher_memory_shard_index(memory_key, namespace_shards);
        seed_memory_state_at_shard(
            root,
            namespace,
            memory_key,
            item_key,
            value,
            version,
            shard_index,
        )
        .await?;
        Ok(shard_index)
    }

    async fn seed_memory_state_at_shard(
        root: &Path,
        namespace: &str,
        memory_key: &str,
        item_key: &str,
        value: &str,
        version: i64,
        shard_index: usize,
    ) -> Result<()> {
        let path = root
            .join(hex_encode(namespace.as_bytes()))
            .join(format!("shard-{shard_index:04}.db"));
        ensure_parent_dir(&path)?;
        let path_str = path.to_string_lossy().to_string();
        let database = Builder::new_local(&path_str)
            .build()
            .await
            .map_err(memory_error)?;
        ensure_schema(&database).await?;
        let conn = database.connect().map_err(memory_error)?;
        configure_connection(&conn).await?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(memory_error)?;
        let outcome = async {
            upsert_memory_state_row(
                &conn,
                memory_key,
                item_key,
                value.as_bytes(),
                ENCODING_UTF8,
                false,
                version,
            )
            .await?;
            upsert_memory_meta_row(&conn, memory_key, version, None).await?;
            Ok::<(), PlatformError>(())
        }
        .await;
        match outcome {
            Ok(()) => {
                conn.execute("COMMIT", ()).await.map_err(memory_error)?;
            }
            Err(error) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn seed_legacy_memory_database(
        path: &Path,
        state: Option<(&str, &str, &str, i64)>,
    ) -> Result<()> {
        ensure_parent_dir(path)?;
        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(memory_error)?;
        let conn = database.connect().map_err(memory_error)?;
        conn.execute(
            "CREATE TABLE memory_state (
               entity_key TEXT NOT NULL,
               item_key TEXT NOT NULL,
               value TEXT NOT NULL,
               deleted INTEGER NOT NULL DEFAULT 0,
               version INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL,
               PRIMARY KEY (entity_key, item_key)
             )",
            (),
        )
        .await
        .map_err(memory_error)?;
        conn.execute(
            "CREATE TABLE memory_meta (
               entity_key TEXT NOT NULL PRIMARY KEY,
               max_version INTEGER NOT NULL,
               updated_at_ms INTEGER NOT NULL
             )",
            (),
        )
        .await
        .map_err(memory_error)?;
        if let Some((entity_key, item_key, value, version)) = state {
            conn.execute(
                "INSERT INTO memory_state
                   (entity_key, item_key, value, deleted, version, updated_at_ms)
                 VALUES (?1, ?2, ?3, 0, ?4, 1)",
                (entity_key, item_key, value, version),
            )
            .await
            .map_err(memory_error)?;
            conn.execute(
                "INSERT INTO memory_meta (entity_key, max_version, updated_at_ms)
                 VALUES (?1, ?2, 1)",
                (entity_key, version),
            )
            .await
            .map_err(memory_error)?;
        }
        Ok(())
    }

    async fn persisted_memory_schema_version(path: &Path) -> Result<i64> {
        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(memory_error)?;
        let conn = database.connect().map_err(memory_error)?;
        storage_schema_version(&conn, "memory")
            .await
            .map_err(memory_error)
    }

    fn assert_send_sync<T: Send + Sync>() {}

    async fn expect_memory_store_new_error(
        root: PathBuf,
        namespace_shards: usize,
        message: &str,
    ) -> PlatformError {
        match MemoryStore::new(root, namespace_shards, 4, Duration::from_secs(60)).await {
            Ok(_) => panic!("{message}"),
            Err(error) => error,
        }
    }

    #[test]
    fn memory_outbox_effect_id_is_stable_and_ordinal_specific() {
        let effect = MemoryOutboxEffectWrite {
            kind: "audit.created".to_string(),
            payload: b"payload".to_vec(),
        };
        let first = memory_outbox_effect_id("entity", 7, 0, &effect);
        assert_eq!(first, memory_outbox_effect_id("entity", 7, 0, &effect));
        assert_ne!(first, memory_outbox_effect_id("entity", 7, 1, &effect));
        assert_ne!(first, memory_outbox_effect_id("entity", 8, 0, &effect));
        assert!(first.starts_with("memfx_"));
        assert_eq!(first.len(), "memfx_".len() + 64);
    }

    #[tokio::test]
    async fn memory_connections_use_full_synchronous_durability() -> Result<()> {
        let path = temp_root("full-synchronous").join("memory.db");
        ensure_parent_dir(&path)?;
        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(memory_error)?;
        let conn = database.connect().map_err(memory_error)?;
        configure_connection(&conn).await?;
        let mut rows = conn
            .query("PRAGMA synchronous", ())
            .await
            .map_err(memory_error)?;
        let row = rows
            .next()
            .await
            .map_err(memory_error)?
            .expect("synchronous row");
        assert_eq!(row.get::<i64>(0).map_err(memory_error)?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn schema_migration_drops_redundant_state_lookup_index() -> Result<()> {
        let path = temp_root("drop-redundant-index").join("memory.db");
        ensure_parent_dir(&path)?;
        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(memory_error)?;
        ensure_schema(&database).await?;
        let conn = database.connect().map_err(memory_error)?;
        configure_connection(&conn).await?;
        conn.execute(
            "CREATE INDEX idx_memory_state_lookup ON memory_state(entity_key, item_key)",
            (),
        )
        .await
        .map_err(memory_error)?;
        drop(conn);

        ensure_schema(&database).await?;

        let conn = database.connect().map_err(memory_error)?;
        configure_connection(&conn).await?;
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                ("idx_memory_state_lookup",),
            )
            .await
            .map_err(memory_error)?;
        let row = rows.next().await.map_err(memory_error)?.expect("count row");
        assert_eq!(row.get::<i64>(0).map_err(memory_error)?, 0);
        drop(rows);

        let insert = "INSERT INTO memory_state (
            entity_key, item_key, value, encoding, deleted, version, updated_at_ms
        ) VALUES (?1, ?2, ?3, 'utf8', 0, 1, 0)";
        conn.execute(insert, ("entity", "item", "one"))
            .await
            .map_err(memory_error)?;
        assert!(
            conn.execute(insert, ("entity", "item", "two"))
                .await
                .is_err(),
            "the primary key must continue enforcing uniqueness"
        );
        Ok(())
    }

    #[tokio::test]
    async fn startup_migrates_every_legacy_shard_before_detecting_floors() -> Result<()> {
        let root = temp_root("legacy-schema-startup");
        std::fs::create_dir_all(&root).map_err(memory_error)?;
        write_memory_layout_atomic(
            &root.join(MEMORY_LAYOUT_MANIFEST_FILE),
            &MemoryLayoutManifest::current(4),
        )?;

        let namespace = "ns";
        let entity_key = "legacy-entity";
        let data_shard = stable_memory_shard_index(entity_key, 4);
        let empty_shard = (data_shard + 1) % 4;
        let namespace_dir = root.join(hex_encode(namespace.as_bytes()));
        let data_path = namespace_dir.join(format!("shard-{data_shard:04}.db"));
        let empty_path = namespace_dir.join(format!("shard-{empty_shard:04}.db"));
        seed_legacy_memory_database(&data_path, Some((entity_key, "count", "41", 41))).await?;
        seed_legacy_memory_database(&empty_path, None).await?;

        let store = MemoryStore::new(root, 4, 4, Duration::from_secs(60)).await?;
        assert_eq!(
            persisted_memory_schema_version(&data_path).await?,
            MEMORY_SCHEMA_VERSION
        );
        assert_eq!(
            persisted_memory_schema_version(&empty_path).await?,
            MEMORY_SCHEMA_VERSION
        );

        let snapshot = store.snapshot(namespace, entity_key).await?;
        assert_eq!(snapshot.max_version, 41);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].key, "count");
        assert_eq!(snapshot.entries[0].value, b"41");
        let result = store
            .apply_batch(
                namespace,
                entity_key,
                &[utf8_mutation("count", "42")],
                None,
                &[],
                None,
            )
            .await?;
        assert!(result.max_version > 41);
        store.health_check().await?;
        Ok(())
    }

    #[tokio::test]
    async fn current_memory_schema_without_migration_metadata_is_adopted() -> Result<()> {
        let root = temp_root("current-schema-adoption");
        let store = MemoryStore::new(root.clone(), 4, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "preserved")],
                None,
                &[],
                None,
            )
            .await?;
        let path = store.db_path("ns", store.shard_index("ns", "memory-a"));
        drop(store);

        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(memory_error)?;
        let conn = database.connect().map_err(memory_error)?;
        conn.execute(
            "DELETE FROM dd_storage_schema_migrations WHERE component = 'memory'",
            (),
        )
        .await
        .map_err(memory_error)?;
        drop(conn);
        drop(database);

        let store = MemoryStore::new(root, 4, 4, Duration::from_secs(60)).await?;
        assert_eq!(
            persisted_memory_schema_version(&path).await?,
            MEMORY_SCHEMA_VERSION
        );
        let snapshot = store.snapshot("ns", "memory-a").await?;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].value, b"preserved");
        store.health_check().await?;
        Ok(())
    }

    #[tokio::test]
    async fn future_memory_schema_version_fails_startup_without_mutating_data() -> Result<()> {
        let root = temp_root("future-schema");
        let store = MemoryStore::new(root.clone(), 4, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "future-guard")],
                None,
                &[],
                None,
            )
            .await?;
        let path = store.db_path("ns", store.shard_index("ns", "memory-a"));
        drop(store);

        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(memory_error)?;
        let conn = database.connect().map_err(memory_error)?;
        conn.execute(
            "INSERT INTO dd_storage_schema_migrations (component, version, applied_at_ms)
             VALUES ('memory', ?1, 1)",
            (MEMORY_SCHEMA_VERSION + 1,),
        )
        .await
        .map_err(memory_error)?;
        drop(conn);
        drop(database);

        let error = expect_memory_store_new_error(root, 4, "future schema must fail startup").await;
        assert!(
            error
                .to_string()
                .contains("unsupported memory schema version")
        );

        let database = Builder::new_local(&path.to_string_lossy())
            .build()
            .await
            .map_err(memory_error)?;
        let conn = database.connect().map_err(memory_error)?;
        let mut rows = conn
            .query(
                "SELECT value FROM memory_state
                 WHERE entity_key = 'memory-a' AND item_key = 'count'",
                (),
            )
            .await
            .map_err(memory_error)?;
        assert_eq!(
            rows.next()
                .await
                .map_err(memory_error)?
                .expect("preserved memory row")
                .get::<String>(0)
                .map_err(memory_error)?,
            "future-guard"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_db_paths_are_stable_per_namespace() -> Result<()> {
        let store = MemoryStore::new(temp_root("paths"), 16, 4, Duration::from_secs(60)).await?;
        let shard = store.shard_index("ns", "memory-a");
        let ns_a = store.db_path("ns", shard);
        let ns_a_again = store.db_path("ns", shard);
        let ns_b = store.db_path("other", shard);

        assert_eq!(ns_a, ns_a_again);
        assert_ne!(ns_a, ns_b);
        Ok(())
    }

    #[tokio::test]
    async fn health_check_and_checkpoint_cover_all_persisted_memory_databases() -> Result<()> {
        let store =
            MemoryStore::new(temp_root("checkpoint-open"), 4, 4, Duration::from_secs(60)).await?;
        store.snapshot("ns", "memory-a").await?;
        store.health_check().await?;
        assert_eq!(store.checkpoint_all_databases().await?, 1);
        Ok(())
    }

    #[test]
    fn memory_shard_index_is_pinned_to_stable_sha256_routing() {
        assert_eq!(stable_memory_shard_index("", 1), 0);
        assert_eq!(stable_memory_shard_index("memory-a", 4), 2);
        assert_eq!(stable_memory_shard_index("memory-b", 4), 3);
        assert_eq!(stable_memory_shard_index("room/42", 16), 0);
        assert_eq!(stable_memory_shard_index("alpha", 16), 6);
        assert_eq!(stable_memory_shard_index("memory-a", 4096), 3386);
        assert_eq!(stable_memory_shard_index("memory-b", 4096), 2551);
    }

    #[test]
    fn turso_database_handle_is_shareable_across_threads() {
        assert_send_sync::<Database>();
        assert_send_sync::<Connection>();
    }

    #[test]
    fn memory_cache_budget_partitioning_is_global_and_deterministic() {
        assert_eq!(
            (0..4)
                .map(|shard| partitioned_budget(2, 4, shard))
                .collect::<Vec<_>>(),
            vec![1, 1, 0, 0]
        );
        assert_eq!(
            (0..4)
                .map(|shard| partitioned_budget(6, 4, shard))
                .collect::<Vec<_>>(),
            vec![2, 2, 1, 1]
        );
    }

    #[tokio::test]
    async fn concurrent_cold_database_open_uses_shared_slot() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("concurrent-cold-open"),
            1,
            4,
            Duration::from_secs(60),
        )
        .await?;
        let store = Arc::new(store);
        let mut tasks = Vec::new();
        for index in 0..16 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                store
                    .apply_batch(
                        "ns",
                        "memory-a",
                        &[utf8_mutation("count", &index.to_string())],
                        None,
                        &[],
                        None,
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await
                .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        }
        let shard = store.shard_for_key("ns", "memory-a");
        assert_eq!(shard.databases.lock().await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn memory_database_cache_budget_is_not_multiplied_by_shards() -> Result<()> {
        let store =
            MemoryStore::new(temp_root("db-global-budget"), 4, 2, Duration::from_secs(60)).await?;
        for index in 0..64 {
            let key = format!("memory-{index}");
            store
                .apply_batch("ns", &key, &[utf8_mutation("count", "1")], None, &[], None)
                .await?;
        }
        let mut cached = 0usize;
        for shard in store.shards.iter() {
            cached = cached.saturating_add(shard.databases.lock().await.len());
        }
        assert!(
            cached <= 2,
            "cached database slots exceeded global budget: {cached}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn repeated_memory_reads_reuse_configured_connection() -> Result<()> {
        let store = MemoryStore::new_with_connection_limits(
            temp_root("read-connection-reuse"),
            1,
            4,
            Duration::from_secs(60),
            2,
            4,
        )
        .await?;

        // Empty batches take the read-only path through the pooled reader.
        store.apply_batch("ns", "memory-a", &[], None, &[], None).await?;
        store.apply_batch("ns", "memory-a", &[], None, &[], None).await?;

        assert_eq!(
            store.db_live_connections.load(Ordering::Relaxed),
            1,
            "second read should reuse the idle pooled connection"
        );
        Ok(())
    }

    #[tokio::test]
    async fn repeated_memory_writes_reuse_single_writer_connection() -> Result<()> {
        let store = MemoryStore::new_with_connection_limits(
            temp_root("writer-connection-reuse"),
            1,
            4,
            Duration::from_secs(60),
            1,
            4,
        )
        .await?;

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                None,
            )
            .await?;

        assert_eq!(
            store.db_live_connections.load(Ordering::Relaxed),
            1,
            "second write should reuse the cached writer connection"
        );
        Ok(())
    }

    #[tokio::test]
    async fn global_memory_connection_cap_blocks_new_database_checkout_without_leaking()
    -> Result<()> {
        let store = Arc::new(
            MemoryStore::new_with_connection_limits(
                temp_root("connection-global-cap"),
                2,
                4,
                Duration::from_secs(60),
                1,
                1,
            )
            .await?,
        );
        let held = store.connect("ns", "memory-a").await?;
        assert_eq!(store.db_live_connections.load(Ordering::Relaxed), 1);

        let waiting_store = Arc::clone(&store);
        let waiting = tokio::spawn(async move {
            waiting_store
                .apply_batch("ns", "memory-b", &[], None, &[], None)
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!waiting.is_finished());

        drop(held);
        waiting
            .await
            .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        assert_eq!(store.db_live_connections.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn same_database_writer_lane_serializes_before_begin_immediate() -> Result<()> {
        let store = Arc::new(
            MemoryStore::new_with_connection_limits(
                temp_root("writer-lane-serializes"),
                1,
                4,
                Duration::from_secs(60),
                1,
                2,
            )
            .await?,
        );
        let writer = store.writer_connection("ns", "memory-a").await?;
        let waiting_store = Arc::clone(&store);
        let waiting = tokio::spawn(async move {
            waiting_store
                .apply_batch(
                    "ns",
                    "memory-a",
                    &[utf8_mutation("count", "1")],
                    None,
                    &[],
                    None,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!waiting.is_finished());

        drop(writer);
        waiting
            .await
            .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        assert_eq!(
            snapshot_value(&store, "ns", "memory-a", "count")
                .await?
                .expect("count should be written"),
            b"1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn different_shard_writer_lanes_can_progress_independently() -> Result<()> {
        let store = Arc::new(
            MemoryStore::new_with_connection_limits(
                temp_root("writer-lane-cross-shard"),
                2,
                4,
                Duration::from_secs(60),
                1,
                4,
            )
            .await?,
        );
        assert_ne!(
            store.shard_index("ns", "memory-a"),
            store.shard_index("ns", "memory-b")
        );

        let writer = store.writer_connection("ns", "memory-a").await?;
        let other_store = Arc::clone(&store);
        let other = tokio::spawn(async move {
            other_store
                .apply_batch(
                    "ns",
                    "memory-b",
                    &[utf8_mutation("count", "1")],
                    None,
                    &[],
                    None,
                )
                .await
        });

        other
            .await
            .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        drop(writer);
        assert_eq!(
            snapshot_value(&store, "ns", "memory-b", "count")
                .await?
                .expect("count should be written"),
            b"1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn per_database_reader_limit_blocks_second_reader_without_leaking() -> Result<()> {
        let store = Arc::new(
            MemoryStore::new_with_connection_limits(
                temp_root("reader-pool-limit"),
                1,
                4,
                Duration::from_secs(60),
                1,
                4,
            )
            .await?,
        );
        let held = store.connect("ns", "memory-a").await?;
        let waiting_store = Arc::clone(&store);
        let waiting = tokio::spawn(async move { waiting_store.connect("ns", "memory-a").await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!waiting.is_finished());

        drop(held);
        let second = waiting
            .await
            .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        drop(second);
        assert!(store.db_live_connections.load(Ordering::Relaxed) <= 1);
        Ok(())
    }

    #[tokio::test]
    async fn failed_memory_write_rolls_back_and_discards_writer_before_next_write() -> Result<()> {
        let store = MemoryStore::new_with_connection_limits(
            temp_root("writer-rollback-discard"),
            1,
            4,
            Duration::from_secs(60),
            1,
            4,
        )
        .await?;
        let command = MemoryCommandResultWrite {
            idempotency_key: "command-1".to_string(),
            result: b"ok".to_vec(),
        };
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                Some(&command),
                &[],
                None,
            )
            .await?;
        let duplicate = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "duplicate")],
                Some(&command),
                &[],
                None,
            )
            .await;
        assert!(duplicate.is_err());
        assert_eq!(
            store.db_live_connections.load(Ordering::Relaxed),
            0,
            "failed commit must discard the poisoned writer connection"
        );
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                None,
            )
            .await?;

        assert_eq!(
            snapshot_value(&store, "ns", "memory-a", "count")
                .await?
                .expect("count should exist"),
            b"2"
        );
        Ok(())
    }

    #[tokio::test]
    async fn entity_cache_stripe_selection_is_stable_and_uses_namespace() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("entity-stripe-stable"),
            4,
            4,
            Duration::from_secs(60),
        )
        .await?;
        let first = store.test_entity_cache_stripe_index("ns-a", "memory-a");
        let second = store.test_entity_cache_stripe_index("ns-a", "memory-a");
        assert_eq!(first, second);

        let namespace_sensitive = (0..10_000)
            .map(|index| format!("entity-{index}"))
            .any(|key| {
                store.test_entity_cache_stripe_index("ns-a", &key)
                    != store.test_entity_cache_stripe_index("ns-b", &key)
            });
        assert!(namespace_sensitive, "namespace should affect stripe choice");
        Ok(())
    }

    #[tokio::test]
    async fn generated_entities_cover_multiple_entity_cache_stripes() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("entity-stripe-coverage"),
            4,
            4,
            Duration::from_secs(60),
        )
        .await?;
        let stripes = (0..256)
            .map(|index| store.test_entity_cache_stripe_index("ns", &format!("memory-{index}")))
            .collect::<HashSet<_>>();
        assert!(
            stripes.len() > 8,
            "generated keys should cover many stripes, got {}",
            stripes.len()
        );
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_cache_budget_is_partitioned_across_entity_stripes() -> Result<()> {
        let mut store = MemoryStore::new(
            temp_root("entity-stripe-budget"),
            1,
            4,
            Duration::from_secs(60),
        )
        .await?;
        store.snapshot_cache_max_entries = MEMORY_ENTITY_CACHE_STRIPES + 7;
        store.snapshot_cache_max_bytes = MEMORY_ENTITY_CACHE_STRIPES * 10 + 3;

        let total_entry_budget = (0..MEMORY_ENTITY_CACHE_STRIPES)
            .map(|stripe| store.snapshot_cache_entry_budget_for_stripe(0, stripe))
            .sum::<usize>();
        let total_byte_budget = (0..MEMORY_ENTITY_CACHE_STRIPES)
            .map(|stripe| store.snapshot_cache_byte_budget_for_stripe(0, stripe))
            .sum::<usize>();

        assert_eq!(total_entry_budget, store.snapshot_cache_max_entries);
        assert_eq!(total_byte_budget, store.snapshot_cache_max_bytes);
        assert!(
            (0..MEMORY_ENTITY_CACHE_STRIPES)
                .all(|stripe| store.snapshot_cache_entry_budget_for_stripe(0, stripe) > 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_snapshot_cache_eviction_keeps_recent_entry() -> Result<()> {
        let mut store = MemoryStore::new(
            temp_root("snapshot-lru-budget"),
            1,
            4,
            Duration::from_secs(60),
        )
        .await?;
        store.snapshot_cache_max_entries = MEMORY_ENTITY_CACHE_STRIPES;
        store.snapshot_cache_max_bytes = usize::MAX / 2;
        let memory_a = "memory-a";
        let memory_b = same_entity_cache_stripe_key(&store, "ns", memory_a, "memory-b");
        store
            .apply_batch(
                "ns",
                memory_a,
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        store
            .apply_batch(
                "ns",
                &memory_b,
                &[utf8_mutation("count", "2")],
                None,
                &[],
                None,
            )
            .await?;
        store.snapshot("ns", memory_a).await?;
        store.snapshot("ns", &memory_b).await?;

        let cache = store.test_entity_cache_snapshot("ns", memory_a).await;
        assert!(
            !cache
                .snapshot_keys
                .contains(&MemoryStore::memory_snapshot_key("ns", memory_a))
        );
        assert!(
            cache
                .snapshot_keys
                .contains(&MemoryStore::memory_snapshot_key("ns", &memory_b))
        );
        assert_eq!(cache.snapshot_entries, 1);
        assert_eq!(cache.version_entries, 1);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_memory_snapshot_is_not_cached() -> Result<()> {
        let mut store = MemoryStore::new(
            temp_root("snapshot-oversized"),
            1,
            4,
            Duration::from_secs(60),
        )
        .await?;
        store.snapshot_cache_max_entries = 16;
        store.snapshot_cache_max_bytes = 1;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("large", "value")],
                None,
                &[],
                None,
            )
            .await?;
        store.snapshot("ns", "memory-a").await?;
        let cache = store.test_entity_cache_snapshot("ns", "memory-a").await;
        assert_eq!(cache.snapshot_entries, 0);
        assert_eq!(cache.snapshot_approximate_bytes, 0);
        assert_eq!(cache.version_entries, 0);
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_is_created_for_fresh_root() -> Result<()> {
        let root = temp_root("layout-fresh");
        let _store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;

        let manifest = read_memory_layout(&memory_layout_manifest_path(&root))?;
        assert_eq!(manifest, MemoryLayoutManifest::current(16));
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_accepts_matching_reopen() -> Result<()> {
        let root = temp_root("layout-reopen");
        let _store = MemoryStore::new(root.clone(), 8, 4, Duration::from_secs(60)).await?;
        let _store = MemoryStore::new(root, 8, 4, Duration::from_secs(60)).await?;
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_rejects_shard_count_change_before_new_shard_file() -> Result<()>
    {
        let root = temp_root("layout-mismatch");
        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        let before = discover_legacy_memory_shard_files(&root)?.len();

        let error = expect_memory_store_new_error(
            root.clone(),
            4,
            "shard-count mismatch must fail startup",
        )
        .await;
        let message = error.to_string();
        assert!(message.contains("memory layout mismatch"), "{message}");
        assert!(
            message.contains("configured memory_namespace_shards=4"),
            "{message}"
        );
        assert!(
            message.contains("persisted namespace_shards=16"),
            "{message}"
        );
        assert_eq!(discover_legacy_memory_shard_files(&root)?.len(), before);
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_rejects_unsupported_format_version() -> Result<()> {
        let root = temp_root("layout-format");
        std::fs::create_dir_all(&root).map_err(memory_error)?;
        write_memory_layout_atomic(
            &memory_layout_manifest_path(&root),
            &MemoryLayoutManifest {
                format_version: 2,
                shard_hash_version: MEMORY_SHARD_HASH_VERSION,
                namespace_shards: 16,
                namespace_shard_hash_versions: BTreeMap::new(),
                namespace_key_shard_overrides: BTreeMap::new(),
            },
        )?;

        let error = expect_memory_store_new_error(
            root,
            16,
            "unsupported manifest format must fail startup",
        )
        .await;
        assert!(
            error
                .to_string()
                .contains("unsupported memory layout manifest format_version"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_manifest_rejects_unsupported_hash_version() -> Result<()> {
        let root = temp_root("layout-hash");
        std::fs::create_dir_all(&root).map_err(memory_error)?;
        write_memory_layout_atomic(
            &memory_layout_manifest_path(&root),
            &MemoryLayoutManifest {
                format_version: MEMORY_LAYOUT_FORMAT_VERSION,
                shard_hash_version: 2,
                namespace_shards: 16,
                namespace_shard_hash_versions: BTreeMap::new(),
                namespace_key_shard_overrides: BTreeMap::new(),
            },
        )?;

        let error =
            expect_memory_store_new_error(root, 16, "unsupported hash version must fail startup")
                .await;
        assert!(
            error
                .to_string()
                .contains("unsupported memory layout shard_hash_version"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_adopts_compatible_legacy_store() -> Result<()> {
        let root = temp_root("layout-legacy-compatible");
        let store = MemoryStore::new(root.clone(), 4, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        std::fs::remove_file(memory_layout_manifest_path(&root)).map_err(memory_error)?;

        let store = MemoryStore::new(root.clone(), 4, 4, Duration::from_secs(60)).await?;
        assert_eq!(
            snapshot_value(&store, "ns", "memory-a", "count")
                .await?
                .expect("record should survive adoption"),
            b"1"
        );
        assert_eq!(
            read_memory_layout(&memory_layout_manifest_path(&root))?.namespace_shards,
            4
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_adopts_legacy_default_hasher_store() -> Result<()> {
        let root = temp_root("layout-legacy-default-hasher");
        let (memory_key, expected_legacy_shard, stable_shard) = key_with_different_hash_layout(16);
        let written_shard =
            seed_legacy_default_hasher_memory_state(&root, "ns", &memory_key, "count", "1", 1, 16)
                .await?;
        assert_eq!(written_shard, expected_legacy_shard);
        assert_ne!(expected_legacy_shard, stable_shard);

        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        let manifest = read_memory_layout(&memory_layout_manifest_path(&root))?;
        assert_eq!(
            manifest.shard_hash_version,
            STABLE_SHA256_MEMORY_SHARD_HASH_VERSION
        );
        assert_eq!(
            manifest.namespace_shard_hash_versions.get("ns").copied(),
            Some(LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION)
        );
        assert_eq!(store.shard_index("ns", &memory_key), expected_legacy_shard);
        assert_eq!(
            snapshot_value(&store, "ns", &memory_key, "count")
                .await?
                .expect("legacy state should remain readable"),
            b"1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_adopts_mixed_namespace_hash_layouts() -> Result<()> {
        let root = temp_root("layout-mixed-namespace-hash");
        let (stable_key, legacy_shard_for_stable_key, stable_shard) =
            key_with_different_hash_layout(16);
        let stable_store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        stable_store
            .apply_batch(
                "AUTH_STATE",
                &stable_key,
                &[utf8_mutation("token", "stable")],
                None,
                &[],
                None,
            )
            .await?;
        assert_eq!(
            stable_store.shard_index("AUTH_STATE", &stable_key),
            stable_shard
        );
        assert_ne!(legacy_shard_for_stable_key, stable_shard);
        drop(stable_store);
        std::fs::remove_file(memory_layout_manifest_path(&root)).map_err(memory_error)?;

        let (legacy_key, legacy_shard, stable_shard_for_legacy_key) =
            key_with_different_hash_layout(16);
        seed_legacy_default_hasher_memory_state(
            &root,
            "CHAT_ROOM",
            &legacy_key,
            "count",
            "legacy",
            1,
            16,
        )
        .await?;
        assert_ne!(legacy_shard, stable_shard_for_legacy_key);

        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        let manifest = read_memory_layout(&memory_layout_manifest_path(&root))?;
        assert_eq!(
            manifest.shard_hash_version,
            STABLE_SHA256_MEMORY_SHARD_HASH_VERSION
        );
        assert_eq!(
            manifest
                .namespace_shard_hash_versions
                .get("CHAT_ROOM")
                .copied(),
            Some(LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION)
        );
        assert!(
            !manifest
                .namespace_shard_hash_versions
                .contains_key("AUTH_STATE")
        );
        assert_eq!(store.shard_index("AUTH_STATE", &stable_key), stable_shard);
        assert_eq!(store.shard_index("CHAT_ROOM", &legacy_key), legacy_shard);
        assert_eq!(
            snapshot_value(&store, "AUTH_STATE", &stable_key, "token")
                .await?
                .expect("stable namespace state should remain readable"),
            b"stable"
        );
        assert_eq!(
            snapshot_value(&store, "CHAT_ROOM", &legacy_key, "count")
                .await?
                .expect("legacy namespace state should remain readable"),
            b"legacy"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_adopts_physical_key_shard_overrides() -> Result<()> {
        let root = temp_root("layout-physical-key-overrides");
        let (memory_key, physical_shard, stable_shard, legacy_shard) =
            key_with_distinct_physical_shard(16);
        seed_memory_state_at_shard(
            &root,
            "EXAMPLE_MEMORY",
            &memory_key,
            "count",
            "physical",
            1,
            physical_shard,
        )
        .await?;
        assert_ne!(physical_shard, stable_shard);
        assert_ne!(physical_shard, legacy_shard);

        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        let manifest = read_memory_layout(&memory_layout_manifest_path(&root))?;
        assert_eq!(
            manifest
                .namespace_key_shard_overrides
                .get("EXAMPLE_MEMORY")
                .and_then(|keys| keys.get(&memory_key))
                .copied(),
            Some(physical_shard)
        );
        assert_eq!(
            store.shard_index("EXAMPLE_MEMORY", &memory_key),
            physical_shard
        );
        assert_eq!(
            snapshot_value(&store, "EXAMPLE_MEMORY", &memory_key, "count")
                .await?
                .expect("physically routed state should remain readable"),
            b"physical"
        );

        let new_key = "new-example-memory-key";
        assert_eq!(
            store.shard_index("EXAMPLE_MEMORY", new_key),
            stable_memory_shard_index(new_key, 16)
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_adopts_newest_physical_duplicate_key_shard() -> Result<()> {
        let root = temp_root("layout-physical-duplicate");
        let (memory_key, first_shard, stable_shard, legacy_shard) =
            key_with_distinct_physical_shard(16);
        let second_shard = (0..16)
            .find(|shard| *shard != first_shard && *shard != stable_shard && *shard != legacy_shard)
            .expect("test should find a second non-hash shard");
        seed_memory_state_at_shard(
            &root,
            "EXAMPLE_MEMORY",
            &memory_key,
            "count",
            "one",
            1,
            first_shard,
        )
        .await?;
        seed_memory_state_at_shard(
            &root,
            "EXAMPLE_MEMORY",
            &memory_key,
            "count",
            "two",
            2,
            second_shard,
        )
        .await?;

        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        let manifest = read_memory_layout(&memory_layout_manifest_path(&root))?;
        assert_eq!(
            manifest
                .namespace_key_shard_overrides
                .get("EXAMPLE_MEMORY")
                .and_then(|keys| keys.get(&memory_key))
                .copied(),
            Some(second_shard)
        );
        assert_eq!(
            store.shard_index("EXAMPLE_MEMORY", &memory_key),
            second_shard
        );
        assert_eq!(
            snapshot_value(&store, "EXAMPLE_MEMORY", &memory_key, "count")
                .await?
                .expect("newest duplicate physical shard should remain readable"),
            b"two"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_rejects_legacy_shard_outside_configured_range() -> Result<()> {
        let root = temp_root("layout-legacy-high-shard");
        let key = key_at_or_above_shard(16, 4);
        let store = MemoryStore::new(root.clone(), 16, 4, Duration::from_secs(60)).await?;
        store
            .apply_batch("ns", &key, &[utf8_mutation("count", "1")], None, &[], None)
            .await?;
        std::fs::remove_file(memory_layout_manifest_path(&root)).map_err(memory_error)?;

        let error =
            expect_memory_store_new_error(root, 4, "higher shard files must not be ignored").await;
        assert!(
            error
                .to_string()
                .contains("outside configured memory_namespace_shards=4")
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_rejects_truncated_manifest_without_overwrite() -> Result<()> {
        let root = temp_root("layout-truncated");
        std::fs::create_dir_all(&root).map_err(memory_error)?;
        let path = memory_layout_manifest_path(&root);
        std::fs::write(&path, b"{").map_err(memory_error)?;

        let error =
            expect_memory_store_new_error(root.clone(), 16, "truncated manifest must fail startup")
                .await;
        assert!(
            error
                .to_string()
                .contains("failed to parse memory layout manifest")
        );
        assert_eq!(std::fs::read(&path).map_err(memory_error)?, b"{");
        Ok(())
    }

    #[tokio::test]
    async fn memory_layout_concurrent_initialization_writes_valid_manifest() -> Result<()> {
        let root = temp_root("layout-concurrent");
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let root = root.clone();
            tasks.push(tokio::spawn(async move {
                MemoryStore::new(root, 16, 4, Duration::from_secs(60)).await
            }));
        }
        for task in tasks {
            task.await
                .map_err(|error| PlatformError::runtime(format!("join error: {error}")))??;
        }

        assert_eq!(
            read_memory_layout(&memory_layout_manifest_path(&root))?,
            MemoryLayoutManifest::current(16)
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_versions_are_shard_local() -> Result<()> {
        let store =
            MemoryStore::new(temp_root("shard-versions"), 4, 4, Duration::from_secs(60)).await?;
        assert_ne!(
            store.shard_index("ns", "memory-a"),
            store.shard_index("ns", "memory-b")
        );

        let first = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        let second = store
            .apply_batch(
                "ns",
                "memory-b",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;

        assert_eq!(first.max_version, 1);
        assert_eq!(second.max_version, 1);
        Ok(())
    }

    #[tokio::test]
    async fn memory_db_cache_eviction_keeps_persisted_state() -> Result<()> {
        let store = MemoryStore::new(temp_root("eviction"), 1, 1, Duration::from_secs(60)).await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        store
            .apply_batch(
                "ns",
                "memory-b",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                None,
            )
            .await?;

        let warm_snapshot = store.snapshot("ns", "memory-b").await?;
        assert_eq!(warm_snapshot.entries.len(), 1);
        assert_eq!(
            store
                .shard_for_key("ns", "memory-b")
                .databases
                .lock()
                .await
                .len(),
            1
        );

        let snapshot = store.snapshot("ns", "memory-a").await?;
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(
            String::from_utf8(snapshot.entries[0].value.clone()).expect("utf8"),
            "1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_snapshot_cache_updates_after_transactional_commit() -> Result<()> {
        let mut store =
            MemoryStore::new(temp_root("snapshot-cache"), 16, 4, Duration::from_secs(60)).await?;
        enable_one_snapshot_slot_per_entity_stripe(&mut store);

        let initial = store.snapshot("ns", "memory-a").await?;
        assert_eq!(initial.max_version, -1);

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;

        let snapshot = store.snapshot("ns", "memory-a").await?;
        assert_eq!(snapshot.max_version, 1);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].key, "count");
        assert_eq!(
            String::from_utf8(snapshot.entries[0].value.clone()).expect("utf8"),
            "1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_snapshot_cache_uses_committed_version() -> Result<()> {
        let mut store = MemoryStore::new(
            temp_root("transactional-cache-version"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;
        enable_one_snapshot_slot_per_entity_stripe(&mut store);

        let initial = store.snapshot("ns", "memory-a").await?;
        assert_eq!(initial.max_version, -1);

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                None,
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let entry = store
            .cached_snapshot_entry("ns", "memory-a")
            .await
            .expect("shared cache entry should be updated after commit");
        assert_eq!(entry.max_version, result.max_version);
        assert_eq!(
            entry
                .records
                .get("count")
                .expect("count should be cached")
                .version,
            result.max_version
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_persists_and_advances_owner_epoch() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-owner-epoch"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                Some(42),
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let conn = store.connect("ns", "memory-a").await?;
        assert_eq!(
            read_single_i64(
                &conn,
                "SELECT owner_epoch FROM memory_meta WHERE entity_key = 'memory-a'"
            )
            .await?,
            42
        );

        let next = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                Some(43),
            )
            .await?;
        assert_eq!(next.max_version, 2);
        assert_eq!(
            read_single_i64(
                &conn,
                "SELECT owner_epoch FROM memory_meta WHERE entity_key = 'memory-a'"
            )
            .await?,
            43
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_rejects_stale_owner_epoch() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-stale-owner-epoch"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                Some(42),
            )
            .await?;
        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "2")],
                None,
                &[],
                Some(43),
            )
            .await?;

        let stale = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "stale")],
                None,
                &[],
                Some(42),
            )
            .await
            .expect_err("stale owner must not commit");
        assert!(
            stale
                .to_string()
                .contains("stale memory entity owner epoch 42"),
            "{stale}"
        );

        let conn = store.connect("ns", "memory-a").await?;
        assert_eq!(
            read_single_i64(
                &conn,
                "SELECT owner_epoch FROM memory_meta WHERE entity_key = 'memory-a'"
            )
            .await?,
            43
        );
        drop(conn);
        let value = snapshot_value(&store, "ns", "memory-a", "count")
            .await?
            .expect("count should remain committed by current owner");
        assert_eq!(String::from_utf8(value).expect("utf8"), "2");
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_stores_command_result_atomically() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-command-result"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                Some(&MemoryCommandResultWrite {
                    idempotency_key: "command-1".to_string(),
                    result: b"ok".to_vec(),
                }),
                &[],
                None,
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let persisted = persisted_command_results(&store, "ns", "memory-a", "command-1").await?;
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].0, b"ok");
        assert_eq!(persisted[0].1, result.max_version);

        let duplicate = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "duplicate")],
                Some(&MemoryCommandResultWrite {
                    idempotency_key: "command-1".to_string(),
                    result: b"duplicate".to_vec(),
                }),
                &[],
                None,
            )
            .await
            .expect_err("duplicate command result must roll back the batch");
        assert!(
            duplicate
                .to_string()
                .to_ascii_lowercase()
                .contains("unique")
                || duplicate
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("constraint"),
            "{duplicate}"
        );
        let value = snapshot_value(&store, "ns", "memory-a", "count")
            .await?
            .expect("count should remain from original command");
        assert_eq!(String::from_utf8(value).expect("utf8"), "1");
        let after_duplicate =
            persisted_command_results(&store, "ns", "memory-a", "command-1").await?;
        assert_eq!(after_duplicate.len(), 1);
        assert_eq!(after_duplicate[0].0, b"ok");

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "owner")],
                None,
                &[],
                Some(2),
            )
            .await?;

        let stale = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "stale")],
                Some(&MemoryCommandResultWrite {
                    idempotency_key: "command-2".to_string(),
                    result: b"conflicted".to_vec(),
                }),
                &[],
                Some(1),
            )
            .await
            .expect_err("stale owner must not commit command result");
        assert!(
            stale
                .to_string()
                .contains("stale memory entity owner epoch 1"),
            "{stale}"
        );
        assert!(
            persisted_command_results(&store, "ns", "memory-a", "command-2")
                .await?
                .is_empty(),
            "failed memory commit must not persist a command result",
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_commit_stores_outbox_effects_atomically() -> Result<()> {
        let store = MemoryStore::new(
            temp_root("transactional-outbox"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[MemoryOutboxEffectWrite {
                    kind: "audit.created".to_string(),
                    payload: br#"{"count":1}"#.to_vec(),
                }],
                None,
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let outbox = persisted_outbox_rows(&store, "ns", "memory-a").await?;
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].kind, "audit.created");
        assert_eq!(outbox[0].payload, br#"{"count":1}"#);
        assert_eq!(outbox[0].revision, result.max_version);
        assert_eq!(outbox[0].status, "pending");
        assert_eq!(
            outbox[0].effect_id,
            memory_outbox_effect_id(
                "memory-a",
                result.max_version,
                0,
                &MemoryOutboxEffectWrite {
                    kind: "audit.created".to_string(),
                    payload: br#"{"count":1}"#.to_vec(),
                },
            )
        );

        let effect_only = store
            .apply_batch(
                "ns",
                "memory-a",
                &[],
                None,
                &[MemoryOutboxEffectWrite {
                    kind: "audit.effect-only".to_string(),
                    payload: b"ok".to_vec(),
                }],
                None,
            )
            .await?;
        assert_eq!(effect_only.max_version, 2);

        store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "owner")],
                None,
                &[],
                Some(2),
            )
            .await?;

        let stale = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "stale")],
                None,
                &[MemoryOutboxEffectWrite {
                    kind: "audit.conflicted".to_string(),
                    payload: b"no".to_vec(),
                }],
                Some(1),
            )
            .await
            .expect_err("stale owner must not commit outbox effect");
        assert!(
            stale
                .to_string()
                .contains("stale memory entity owner epoch 1"),
            "{stale}"
        );

        let outbox = persisted_outbox_rows(&store, "ns", "memory-a").await?;
        assert_eq!(outbox.len(), 2);
        assert_eq!(outbox[1].kind, "audit.effect-only");
        assert_eq!(outbox[1].revision, effect_only.max_version);
        assert_eq!(
            outbox[1].effect_id,
            memory_outbox_effect_id(
                "memory-a",
                effect_only.max_version,
                0,
                &MemoryOutboxEffectWrite {
                    kind: "audit.effect-only".to_string(),
                    payload: b"ok".to_vec(),
                },
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_commit_snapshot_cache_uses_committed_version() -> Result<()> {
        let mut store = MemoryStore::new(
            temp_root("coordinator-cache-version"),
            16,
            4,
            Duration::from_secs(60),
        )
        .await?;
        enable_one_snapshot_slot_per_entity_stripe(&mut store);

        let initial = store.snapshot("ns", "memory-a").await?;
        assert_eq!(initial.max_version, -1);

        let result = store
            .apply_batch(
                "ns",
                "memory-a",
                &[utf8_mutation("count", "1")],
                None,
                &[],
                Some(1),
            )
            .await?;
        assert_eq!(result.max_version, 1);

        let entry = store
            .cached_snapshot_entry("ns", "memory-a")
            .await
            .expect("shared cache entry should be updated after commit");
        assert_eq!(entry.max_version, result.max_version);
        assert_eq!(
            entry
                .records
                .get("count")
                .expect("count should be cached")
                .version,
            result.max_version
        );
        Ok(())
    }

    #[tokio::test]
    async fn memory_transactional_writes_complete_past_repeated_commit_threshold() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let store = MemoryStore::new(
                temp_root("transactional-write-threshold"),
                16,
                4,
                Duration::from_secs(60),
            )
            .await?;

            for idx in 0..64 {
                let next_value = (idx + 1).to_string();
                store
                    .apply_batch(
                        "ns",
                        "memory-a",
                        &[utf8_mutation("count", &next_value)],
                        None,
                        &[],
                        Some(idx as i64 + 1),
                    )
                    .await?;
            }

            let final_value = snapshot_value(&store, "ns", "memory-a", "count")
                .await?
                .expect("count should be present after repeated transactional writes");
            assert_eq!(String::from_utf8(final_value).expect("utf8"), "64");
            Ok::<(), PlatformError>(())
        })
        .await
        .map_err(|_| {
            PlatformError::runtime(
                "transactional write threshold test timed out before completing 64 commits",
            )
        })??;
        Ok(())
    }
}
