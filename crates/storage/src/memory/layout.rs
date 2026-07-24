async fn load_or_adopt_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
) -> Result<MemoryLayoutManifest> {
    let manifest_path = memory_layout_manifest_path(root_dir);
    if manifest_path.exists() {
        let manifest = read_memory_layout(&manifest_path)?;
        validate_memory_layout(root_dir, configured_shards, &manifest)?;
        return Ok(manifest);
    }

    let manifest = adopt_legacy_memory_layout(root_dir, configured_shards).await?;
    write_memory_layout_atomic(&manifest_path, &manifest)?;
    Ok(manifest)
}

fn validate_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
    manifest: &MemoryLayoutManifest,
) -> Result<()> {
    if manifest.format_version != MEMORY_LAYOUT_FORMAT_VERSION {
        return Err(PlatformError::runtime(format!(
            "unsupported memory layout manifest format_version {} at {}; supported format_version is {}",
            manifest.format_version,
            memory_layout_manifest_path(root_dir).display(),
            MEMORY_LAYOUT_FORMAT_VERSION
        )));
    }
    if !is_supported_memory_shard_hash_version(manifest.shard_hash_version) {
        return Err(PlatformError::runtime(format!(
            "unsupported memory layout shard_hash_version {} at {}; supported shard_hash_versions are {}",
            manifest.shard_hash_version,
            memory_layout_manifest_path(root_dir).display(),
            supported_memory_shard_hash_versions_label()
        )));
    }
    for (namespace, version) in &manifest.namespace_shard_hash_versions {
        if !is_supported_memory_shard_hash_version(*version) {
            return Err(PlatformError::runtime(format!(
                "unsupported memory layout shard_hash_version {} for namespace {} at {}; supported shard_hash_versions are {}",
                version,
                namespace,
                memory_layout_manifest_path(root_dir).display(),
                supported_memory_shard_hash_versions_label()
            )));
        }
    }
    for (namespace, key_overrides) in &manifest.namespace_key_shard_overrides {
        for (memory_key, shard_index) in key_overrides {
            if *shard_index >= configured_shards {
                return Err(PlatformError::runtime(format!(
                    "memory layout key shard override for namespace {} key {} points to shard {} outside configured memory_namespace_shards={} at {}",
                    namespace,
                    memory_key,
                    shard_index,
                    configured_shards,
                    memory_layout_manifest_path(root_dir).display()
                )));
            }
        }
    }
    if manifest.namespace_shards != configured_shards {
        return Err(memory_layout_mismatch_error(
            root_dir,
            configured_shards,
            manifest.namespace_shards,
        ));
    }
    Ok(())
}

fn memory_layout_mismatch_error(
    root_dir: &Path,
    configured_shards: usize,
    persisted_shards: usize,
) -> PlatformError {
    PlatformError::runtime(format!(
        "memory layout mismatch for root {}: configured memory_namespace_shards={} but persisted namespace_shards={}. Changing the shard count requires an explicit reshard operation before startup.",
        root_dir.display(),
        configured_shards,
        persisted_shards
    ))
}

fn memory_layout_manifest_path(root_dir: &Path) -> PathBuf {
    root_dir.join(MEMORY_LAYOUT_MANIFEST_FILE)
}

fn read_memory_layout(path: &Path) -> Result<MemoryLayoutManifest> {
    let bytes = std::fs::read(path).map_err(memory_error)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        PlatformError::runtime(format!(
            "memory store error: failed to parse memory layout manifest {}: {error}",
            path.display()
        ))
    })
}

fn write_memory_layout_atomic(path: &Path, manifest: &MemoryLayoutManifest) -> Result<()> {
    ensure_parent_dir(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp_path = parent.join(format!(
        ".{}.tmp-{}-{:?}",
        MEMORY_LAYOUT_MANIFEST_FILE,
        std::process::id(),
        std::thread::current().id()
    ));
    let json = serde_json::to_vec_pretty(manifest).map_err(memory_error)?;
    {
        let mut file = std::fs::File::create(&tmp_path).map_err(memory_error)?;
        use std::io::Write as _;
        file.write_all(&json).map_err(memory_error)?;
        file.write_all(b"\n").map_err(memory_error)?;
        file.sync_all().map_err(memory_error)?;
    }
    if let Err(error) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(memory_error(error));
    }
    Ok(())
}

async fn adopt_legacy_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
) -> Result<MemoryLayoutManifest> {
    let shard_files = discover_legacy_memory_shard_files(root_dir)?;
    if shard_files.is_empty() {
        return Ok(MemoryLayoutManifest::current(configured_shards));
    }

    let mut files_by_namespace = BTreeMap::<String, Vec<LegacyMemoryShardFile>>::new();
    for shard_file in shard_files {
        files_by_namespace
            .entry(shard_file.namespace.clone())
            .or_default()
            .push(shard_file);
    }

    let mut namespace_shard_hash_versions = BTreeMap::new();
    let mut namespace_key_shard_overrides = BTreeMap::new();
    for (namespace, namespace_files) in &files_by_namespace {
        match validate_legacy_memory_layout(
            root_dir,
            configured_shards,
            STABLE_SHA256_MEMORY_SHARD_HASH_VERSION,
            namespace_files,
        )
        .await
        {
            Ok(()) => {}
            Err(stable_error) => {
                match validate_legacy_memory_layout(
                    root_dir,
                    configured_shards,
                    LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION,
                    namespace_files,
                )
                .await
                {
                    Ok(()) => {
                        namespace_shard_hash_versions.insert(
                            namespace.clone(),
                            LEGACY_DEFAULT_HASHER_MEMORY_SHARD_HASH_VERSION,
                        );
                    }
                    Err(legacy_error) => {
                        let key_overrides = collect_physical_memory_key_shard_overrides(
                            root_dir,
                            configured_shards,
                            namespace_files,
                        )
                        .await
                        .map_err(|physical_error| {
                            PlatformError::runtime(format!(
                                "memory legacy layout at {} is incompatible with supported shard layouts for namespace {} and configured memory_namespace_shards={}; stable layout error: {}; legacy layout error: {}; physical layout error: {}; changing the shard count or hash layout requires an explicit reshard operation before startup",
                                root_dir.display(),
                                namespace,
                                configured_shards,
                                stable_error,
                                legacy_error,
                                physical_error
                            ))
                        })?;
                        if !key_overrides.is_empty() {
                            namespace_key_shard_overrides.insert(namespace.clone(), key_overrides);
                        }
                    }
                }
            }
        }
    }

    Ok(MemoryLayoutManifest {
        format_version: MEMORY_LAYOUT_FORMAT_VERSION,
        shard_hash_version: STABLE_SHA256_MEMORY_SHARD_HASH_VERSION,
        namespace_shards: configured_shards,
        namespace_shard_hash_versions,
        namespace_key_shard_overrides,
    })
}

async fn validate_legacy_memory_layout(
    root_dir: &Path,
    configured_shards: usize,
    shard_hash_version: u32,
    shard_files: &[LegacyMemoryShardFile],
) -> Result<()> {
    for shard_file in shard_files {
        if shard_file.shard_index >= configured_shards {
            return Err(PlatformError::runtime(format!(
                "memory legacy layout at {} namespace {} contains shard index {} outside configured memory_namespace_shards={} for shard_hash_version={}; changing the shard count requires an explicit reshard operation before startup",
                root_dir.display(),
                shard_file.namespace,
                shard_file.shard_index,
                configured_shards,
                shard_hash_version
            )));
        }
        let keys = distinct_entity_keys_in_memory_db(&shard_file.path).await?;
        for entity_key in keys {
            let expected = memory_shard_index_for_hash_version(
                &entity_key,
                configured_shards,
                shard_hash_version,
            )?;
            if expected != shard_file.shard_index {
                return Err(PlatformError::runtime(format!(
                    "memory legacy layout at {} namespace {} is incompatible with configured memory_namespace_shards={} and shard_hash_version={}: entity key in {} belongs to shard {} under the configured layout but is stored in shard {}; changing the shard count or hash layout requires an explicit reshard operation before startup",
                    root_dir.display(),
                    shard_file.namespace,
                    configured_shards,
                    shard_hash_version,
                    shard_file.path.display(),
                    expected,
                    shard_file.shard_index
                )));
            }
        }
        // Empty legacy shard files cannot prove the previous shard count because
        // shard databases are created lazily. They are accepted only because all
        // discovered persisted entity keys above are compatible.
    }
    Ok(())
}

async fn collect_physical_memory_key_shard_overrides(
    root_dir: &Path,
    configured_shards: usize,
    shard_files: &[LegacyMemoryShardFile],
) -> Result<BTreeMap<String, usize>> {
    let mut key_shards = BTreeMap::<String, PhysicalMemoryKeyShard>::new();
    for shard_file in shard_files {
        if shard_file.shard_index >= configured_shards {
            return Err(PlatformError::runtime(format!(
                "memory legacy layout at {} namespace {} contains shard index {} outside configured memory_namespace_shards={}; changing the shard count requires an explicit reshard operation before startup",
                root_dir.display(),
                shard_file.namespace,
                shard_file.shard_index,
                configured_shards
            )));
        }
        let key_revisions = entity_key_revisions_in_memory_db(&shard_file.path).await?;
        for (entity_key, max_revision) in key_revisions {
            let candidate = PhysicalMemoryKeyShard {
                shard_index: shard_file.shard_index,
                max_revision,
            };
            match key_shards.get_mut(&entity_key) {
                Some(current) if current.is_older_than(candidate) => {
                    *current = candidate;
                }
                Some(_) => {}
                None => {
                    key_shards.insert(entity_key, candidate);
                }
            }
        }
    }
    Ok(key_shards
        .into_iter()
        .map(|(key, shard)| (key, shard.shard_index))
        .collect())
}

#[derive(Clone, Copy)]
struct PhysicalMemoryKeyShard {
    shard_index: usize,
    max_revision: i64,
}

impl PhysicalMemoryKeyShard {
    fn is_older_than(self, other: Self) -> bool {
        self.max_revision < other.max_revision
            || (self.max_revision == other.max_revision && self.shard_index < other.shard_index)
    }
}

struct LegacyMemoryShardFile {
    namespace: String,
    path: PathBuf,
    shard_index: usize,
}

fn discover_legacy_memory_shard_files(root_dir: &Path) -> Result<Vec<LegacyMemoryShardFile>> {
    let mut files = Vec::new();
    let entries = match std::fs::read_dir(root_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(error) => return Err(memory_error(error)),
    };
    for entry in entries {
        let entry = entry.map_err(memory_error)?;
        let namespace_path = entry.path();
        if !namespace_path.is_dir() {
            continue;
        }
        let Some(raw_namespace) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(namespace) = hex_decode_to_utf8(&raw_namespace) else {
            continue;
        };
        let shard_entries = std::fs::read_dir(&namespace_path).map_err(memory_error)?;
        for shard_entry in shard_entries {
            let shard_entry = shard_entry.map_err(memory_error)?;
            let path = shard_entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(shard_index) = parse_managed_memory_shard_file_name(file_name)? else {
                continue;
            };
            files.push(LegacyMemoryShardFile {
                namespace: namespace.clone(),
                path,
                shard_index,
            });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn parse_managed_memory_shard_file_name(file_name: &str) -> Result<Option<usize>> {
    let Some(rest) = file_name.strip_prefix("shard-") else {
        return Ok(None);
    };
    let Some(index) = rest.strip_suffix(".db") else {
        return Ok(None);
    };
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PlatformError::runtime(format!(
            "malformed managed memory shard database file name: {file_name}"
        )));
    }
    index.parse::<usize>().map(Some).map_err(|error| {
        PlatformError::runtime(format!(
            "malformed managed memory shard database file name {file_name}: {error}"
        ))
    })
}

async fn distinct_entity_keys_in_memory_db(path: &Path) -> Result<Vec<String>> {
    let path_str = path.to_string_lossy().to_string();
    let database = Builder::new_local(&path_str)
        .build()
        .await
        .map_err(memory_error)?;
    let conn = database.connect().map_err(memory_error)?;
    configure_connection(&conn).await?;
    let mut keys = HashSet::new();
    for table in [
        "memory_meta",
        "memory_state",
        "memory_commands",
        "memory_outbox",
    ] {
        read_distinct_entity_keys_from_table(&conn, table, &mut keys).await?;
    }
    let mut keys = keys.into_iter().collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

async fn entity_key_revisions_in_memory_db(path: &Path) -> Result<BTreeMap<String, i64>> {
    let path_str = path.to_string_lossy().to_string();
    let database = Builder::new_local(&path_str)
        .build()
        .await
        .map_err(memory_error)?;
    let conn = database.connect().map_err(memory_error)?;
    configure_connection(&conn).await?;
    let mut revisions = BTreeMap::new();
    for (table, revision_column) in [
        ("memory_meta", "max_version"),
        ("memory_state", "version"),
        ("memory_commands", "revision"),
        ("memory_outbox", "revision"),
    ] {
        read_entity_revisions_from_table(&conn, table, revision_column, &mut revisions).await?;
    }
    Ok(revisions)
}

async fn read_distinct_entity_keys_from_table(
    conn: &Connection,
    table: &str,
    keys: &mut HashSet<String>,
) -> Result<()> {
    let sql = format!("SELECT DISTINCT entity_key FROM {table}");
    let mut rows = match conn.query(&sql, ()).await {
        Ok(rows) => rows,
        Err(error) => {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("no such table") || message.contains("no such column") {
                return Ok(());
            }
            return Err(memory_error(error));
        }
    };
    while let Some(row) = rows.next().await.map_err(memory_error)? {
        keys.insert(row.get::<String>(0).map_err(memory_error)?);
    }
    Ok(())
}

async fn read_entity_revisions_from_table(
    conn: &Connection,
    table: &str,
    revision_column: &str,
    revisions: &mut BTreeMap<String, i64>,
) -> Result<()> {
    let sql = format!("SELECT entity_key, MAX({revision_column}) FROM {table} GROUP BY entity_key");
    let mut rows = match conn.query(&sql, ()).await {
        Ok(rows) => rows,
        Err(error) => {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("no such table") || message.contains("no such column") {
                return Ok(());
            }
            return Err(memory_error(error));
        }
    };
    while let Some(row) = rows.next().await.map_err(memory_error)? {
        let entity_key = row.get::<String>(0).map_err(memory_error)?;
        let revision = row
            .get::<Option<i64>>(1)
            .map_err(memory_error)?
            .unwrap_or(-1);
        revisions
            .entry(entity_key)
            .and_modify(|current| *current = (*current).max(revision))
            .or_insert(revision);
    }
    Ok(())
}

fn shard_index_from_db_path(path: &Path) -> Option<usize> {
    let name = path.file_stem()?.to_str()?;
    let index = name.strip_prefix("shard-")?;
    index.parse::<usize>().ok()
}
