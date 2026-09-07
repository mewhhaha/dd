use common::{PlatformError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use turso::{Builder, Value};

use crate::memory::namespace_owner;
use crate::state::{
    COMPLETE, STATE_SHARDS, StateStore, configure_connection, storage_error, write_file_synced,
};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct NamespaceOwner {
    pub worker: String,
    pub binding: String,
}

struct NamespaceMap(BTreeMap<String, NamespaceOwner>);
impl<'de> Deserialize<'de> for NamespaceMap {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct NamespaceMapVisitor;
        impl<'de> serde::de::Visitor<'de> for NamespaceMapVisitor {
            type Value = NamespaceMap;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a namespace object with unique keys")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut access: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut owners = BTreeMap::new();
                while let Some((namespace, owner)) =
                    access.next_entry::<String, NamespaceOwner>()?
                {
                    if owners.insert(namespace.clone(), owner).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate namespace map key {namespace:?}"
                        )));
                    }
                }
                Ok(NamespaceMap(owners))
            }
        }
        deserializer.deserialize_map(NamespaceMapVisitor)
    }
}

#[derive(Debug, Serialize)]
pub struct ConversionReport {
    pub rows: BTreeMap<String, usize>,
    pub sha256: BTreeMap<String, String>,
    pub archived_deployments: usize,
    pub namespaces: BTreeMap<String, NamespaceOwner>,
}

const TABLES: &[(&str, &str, &str)] = &[
    (
        "worker_kv",
        "worker,binding,key,value,encoding,deleted,version",
        "worker_name,binding,key,COALESCE(value_blob,CAST(value AS BLOB)),encoding,deleted,version",
    ),
    (
        "memory_state",
        "worker,binding,entity_key,item_key,value,encoding,deleted,version",
        "entity_key,item_key,COALESCE(value_blob,CAST(value AS BLOB)),encoding,deleted,version",
    ),
    (
        "memory_meta",
        "worker,binding,entity_key,max_version,owner_epoch",
        "entity_key,max_version,owner_epoch",
    ),
    (
        "memory_commands",
        "worker,binding,entity_key,idempotency_key,result_blob,revision",
        "entity_key,idempotency_key,result_blob,revision",
    ),
    (
        "memory_outbox",
        "worker,binding,entity_key,effect_id,revision,kind,payload_blob,status,attempt_count,next_attempt_at_ms",
        "entity_key,effect_id,revision,kind,payload_blob,status,attempt_count,next_attempt_at_ms",
    ),
];

pub async fn convert(
    source: &Path,
    destination: &Path,
    namespace_map: Option<&Path>,
) -> Result<ConversionReport> {
    let source = source.canonicalize().map_err(storage_error)?;
    if source.join("state").exists() || source.join("state-layout.json").exists() {
        return Err(storage_error(format!(
            "source {} already contains the new state layout",
            source.display()
        )));
    }
    if destination.exists() {
        return Err(PlatformError::bad_request(format!(
            "conversion destination {} already exists; choose a new directory",
            destination.display()
        )));
    }
    let destination_parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let destination_parent = destination_parent.canonicalize().map_err(storage_error)?;
    if destination_parent.starts_with(&source) {
        return Err(PlatformError::bad_request(format!(
            "conversion destination {} is inside source {}",
            destination.display(),
            source.display()
        )));
    }
    let explicit: BTreeMap<String, NamespaceOwner> = match namespace_map {
        Some(path) => {
            serde_json::from_slice::<NamespaceMap>(&std::fs::read(path).map_err(storage_error)?)
                .map_err(storage_error)?
                .0
        }
        None => BTreeMap::new(),
    };
    for (namespace, owner) in &explicit {
        if owner.worker.is_empty() || owner.binding.is_empty() {
            return Err(PlatformError::bad_request(format!(
                "namespace map entry {namespace:?} has empty worker or binding"
            )));
        }
    }
    let before = tree_hashes(&source)?;
    if !source.join("control.db").exists() {
        let legacy_control = before.keys().find(|path| {
            matches!(
                path.to_str(),
                Some("tokens.json" | "deploy-tokens.json" | "workers.json")
            ) || (path.starts_with("workers")
                && path.extension().and_then(|extension| extension.to_str()) == Some("json"))
        });
        if let Some(path) = legacy_control {
            return Err(PlatformError::bad_request(format!(
                "unsupported legacy control file {}: source {} has no control.db; JSON token and worker records are not imported, so migrate them into control.db before conversion",
                source.join(path).display(),
                source.display()
            )));
        }
    }
    std::fs::create_dir(destination).map_err(storage_error)?;
    write_file_synced(
        &destination.join("conversion-incomplete"),
        b"source must remain offline until conversion completes\n",
    )?;
    let archive = destination.join("archive");
    copy_tree(&source, &archive)?;
    let control_path = archive.join("control.db");
    let mut candidates: BTreeMap<String, BTreeSet<NamespaceOwner>> = BTreeMap::new();
    let mut archived_deployments = 0;
    let mut control_hashes = BTreeMap::new();
    if control_path.exists() {
        let database = Builder::new_local(control_path.to_string_lossy().as_ref())
            .build()
            .await
            .map_err(storage_error)?;
        let conn = database.connect().map_err(storage_error)?;
        configure_connection(&conn).await?;
        let mut rows = conn
            .query("SELECT worker_name,config_json FROM deployments", ())
            .await
            .map_err(storage_error)?;
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            archived_deployments += 1;
            let worker = row.get::<String>(0).map_err(storage_error)?;
            let config: serde_json::Value =
                serde_json::from_str(&row.get::<String>(1).map_err(storage_error)?)
                    .map_err(storage_error)?;
            if let Some(bindings) = config.get("bindings").and_then(serde_json::Value::as_array) {
                for binding in bindings {
                    if !matches!(
                        binding.get("type").and_then(serde_json::Value::as_str),
                        Some("memory" | "actor")
                    ) {
                        continue;
                    }
                    let Some(name) = binding.get("binding").and_then(serde_json::Value::as_str)
                    else {
                        continue;
                    };
                    candidates
                        .entry(name.into())
                        .or_default()
                        .insert(NamespaceOwner {
                            worker: worker.clone(),
                            binding: name.into(),
                        });
                }
            }
        }
        drop(rows);
        for table in ["deploy_tokens", "control_migrations", "migration_state"] {
            control_hashes.insert(table.to_string(), table_hashes(&conn, table).await?);
        }
        crate::turso_util::checkpoint_database(&database)
            .await
            .map_err(storage_error)?;
        drop(conn);
        drop(database);
        std::fs::copy(&control_path, destination.join("control.db")).map_err(storage_error)?;
        let database =
            Builder::new_local(destination.join("control.db").to_string_lossy().as_ref())
                .build()
                .await
                .map_err(storage_error)?;
        let conn = database.connect().map_err(storage_error)?;
        configure_connection(&conn).await?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(storage_error)?;
        for table in ["active_deployments", "restore_diagnostics", "deployments"] {
            conn.execute(&format!("DELETE FROM {table}"), ())
                .await
                .map_err(storage_error)?;
        }
        conn.execute("COMMIT", ()).await.map_err(storage_error)?;
        for (table, expected) in &control_hashes {
            if table_hashes(&conn, table).await? != *expected {
                return Err(storage_error(format!(
                    "conversion control verification failed for {table}"
                )));
            }
        }
        crate::turso_util::checkpoint_database(&database)
            .await
            .map_err(storage_error)?;
    }
    let mut memory_files = Vec::new();
    for path in list_files(&archive.join("memory"))? {
        if path.extension().and_then(|extension| extension.to_str()) != Some("db") {
            continue;
        }
        let encoded = path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                storage_error(format!("invalid memory shard path {}", path.display()))
            })?;
        let namespace = decode_namespace(encoded)?;
        memory_files.push((namespace, path));
    }
    let mut namespaces = BTreeMap::new();
    for (namespace, _) in &memory_files {
        if namespaces.contains_key(namespace) {
            continue;
        }
        let owner = if let Some(owner) = explicit.get(namespace) {
            owner.clone()
        } else if let Ok((worker, binding)) = namespace_owner(namespace) {
            NamespaceOwner {
                worker: worker.into(),
                binding: binding.into(),
            }
        } else {
            let owners = candidates.get(namespace);
            if owners.map_or(0, BTreeSet::len) != 1 {
                return Err(PlatformError::bad_request(format!(
                    "memory namespace {namespace:?} has {} possible owners; provide one explicit worker/binding in --namespace-map; shared namespaces are never split",
                    owners.map_or(0, BTreeSet::len)
                )));
            }
            owners
                .and_then(|owners| owners.first())
                .expect("unique namespace owner")
                .clone()
        };
        namespaces.insert(namespace.clone(), owner);
    }
    let state_root = destination.join("state");
    let state = StateStore::create_conversion(&state_root).await?;
    let mut expected: BTreeMap<String, Vec<Vec<u8>>> = TABLES
        .iter()
        .map(|(table, _, _)| ((*table).into(), Vec::new()))
        .collect();
    let kv_path = archive.join("dd-kv.db");
    if kv_path.exists() {
        import_database(&kv_path, None, &state, &mut expected).await?;
    }
    for (namespace, path) in memory_files {
        import_database(&path, Some(&namespaces[&namespace]), &state, &mut expected).await?;
    }
    state.checkpoint().await?;
    let mut actual: BTreeMap<String, Vec<Vec<u8>>> = TABLES
        .iter()
        .map(|(table, _, _)| ((*table).into(), Vec::new()))
        .collect();
    for shard in 0..STATE_SHARDS {
        let conn = state.read(shard).await?;
        for (table, columns, _) in TABLES {
            let mut rows = conn
                .query(&format!("SELECT {columns} FROM {table}"), ())
                .await
                .map_err(storage_error)?;
            while let Some(row) = rows.next().await.map_err(storage_error)? {
                let values = (0..columns.split(',').count())
                    .map(|index| row.get_value(index))
                    .collect::<turso::Result<Vec<_>>>()
                    .map_err(storage_error)?;
                actual
                    .get_mut(*table)
                    .expect("known table")
                    .push(row_hash(&values));
            }
        }
    }
    let mut report = ConversionReport {
        rows: BTreeMap::new(),
        sha256: BTreeMap::new(),
        archived_deployments,
        namespaces,
    };
    for (table, hashes) in control_hashes {
        let mut hash = Sha256::new();
        for row in &hashes {
            hash.update(row);
        }
        report.rows.insert(format!("control.{table}"), hashes.len());
        report.sha256.insert(
            format!("control.{table}"),
            hash.finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        );
    }
    for (table, hashes) in &mut expected {
        hashes.sort();
        let verified = actual.get_mut(table).expect("known table");
        verified.sort();
        if hashes != verified {
            return Err(storage_error(format!(
                "conversion verification failed for {table}: source {} rows, destination {} rows",
                hashes.len(),
                verified.len()
            )));
        }
        let mut hash = Sha256::new();
        for row in hashes.iter() {
            hash.update(row);
        }
        report.rows.insert(table.clone(), hashes.len());
        report.sha256.insert(
            table.clone(),
            hash.finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        );
    }
    if tree_hashes(&source)? != before {
        return Err(storage_error(format!(
            "source {} changed during conversion; stop the source runtime and convert into a new destination",
            source.display()
        )));
    }
    write_file_synced(
        &destination.join("conversion-report.json"),
        &serde_json::to_vec_pretty(&report).map_err(storage_error)?,
    )?;
    write_file_synced(&state_root.join(COMPLETE), b"2\n")?;
    std::fs::remove_file(destination.join("conversion-incomplete")).map_err(storage_error)?;
    write_file_synced(&destination.join("conversion-complete"), b"2\n")?;
    Ok(report)
}

async fn import_database(
    path: &Path,
    owner: Option<&NamespaceOwner>,
    state: &StateStore,
    expected: &mut BTreeMap<String, Vec<Vec<u8>>>,
) -> Result<()> {
    let database = Builder::new_local(path.to_string_lossy().as_ref())
        .build()
        .await
        .map_err(storage_error)?;
    let conn = database.connect().map_err(storage_error)?;
    configure_connection(&conn).await?;
    for (table, columns, legacy_columns) in TABLES {
        if (*table == "worker_kv") != owner.is_none() {
            continue;
        }
        let mut exists = conn
            .query(
                "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1",
                (*table,),
            )
            .await
            .map_err(storage_error)?;
        if exists.next().await.map_err(storage_error)?.is_none() {
            continue;
        }
        drop(exists);
        let width = columns.split(',').count();
        let legacy_width = if owner.is_some() { width - 2 } else { width };
        let mut rows = conn
            .query(&format!("SELECT {legacy_columns} FROM {table}"), ())
            .await
            .map_err(storage_error)?;
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            let mut values = Vec::with_capacity(width);
            if let Some(owner) = owner {
                values.extend([
                    Value::Text(owner.worker.clone()),
                    Value::Text(owner.binding.clone()),
                ]);
            }
            for index in 0..legacy_width {
                values.push(row.get_value(index).map_err(storage_error)?);
            }
            let worker = value_text(&values[0])?;
            let binding = value_text(&values[1])?;
            let entity = value_text(&values[2])?;
            let shard = StateStore::shard_index(worker, binding, entity);
            let destination = state.read(shard).await?;
            let placeholders = (1..=width)
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(",");
            destination
                .execute("BEGIN IMMEDIATE", ())
                .await
                .map_err(storage_error)?;
            let insert = destination
                .execute(
                    &format!("INSERT INTO {table}({columns}) VALUES ({placeholders})"),
                    values.clone(),
                )
                .await;
            if let Err(error) = insert {
                return Err(storage_error(format!(
                    "cannot import {table} from {} for {worker}/{binding}/{entity}: {error}; conflicting legacy entities cannot be merged automatically",
                    path.display()
                )));
            }
            let version_index = match *table {
                "worker_kv" => 6,
                "memory_state" => 7,
                "memory_meta" => 3,
                "memory_commands" => 5,
                "memory_outbox" => 4,
                _ => unreachable!(),
            };
            if let Value::Integer(version) = values[version_index] {
                destination
                    .execute(
                        "UPDATE state_floor SET version=MAX(version,?1) WHERE singleton=1",
                        (version,),
                    )
                    .await
                    .map_err(storage_error)?;
            }
            destination
                .execute("COMMIT", ())
                .await
                .map_err(storage_error)?;
            expected
                .get_mut(*table)
                .expect("known conversion table")
                .push(row_hash(&values));
        }
    }
    Ok(())
}

fn value_text(value: &Value) -> Result<&str> {
    match value {
        Value::Text(text) => Ok(text),
        _ => Err(storage_error(format!(
            "expected text identity, received {value:?}"
        ))),
    }
}
fn row_hash(values: &[Value]) -> Vec<u8> {
    let mut hash = Sha256::new();
    for value in values {
        let (kind, bytes) = match value {
            Value::Null => (0, Vec::new()),
            Value::Integer(value) => (1, value.to_be_bytes().to_vec()),
            Value::Real(value) => (2, value.to_bits().to_be_bytes().to_vec()),
            Value::Text(value) => (3, value.as_bytes().to_vec()),
            Value::Blob(value) => (4, value.clone()),
        };
        hash.update([kind]);
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    hash.finalize().to_vec()
}
fn decode_namespace(encoded: &str) -> Result<String> {
    if !encoded.len().is_multiple_of(2) {
        return Err(storage_error(format!(
            "invalid hex memory namespace directory {encoded:?}"
        )));
    }
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).map_err(storage_error))
        .collect::<Result<Vec<_>>>()?;
    String::from_utf8(bytes).map_err(storage_error)
}
fn list_files(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(root).map_err(storage_error)? {
        let entry = entry.map_err(storage_error)?;
        let kind = entry.file_type().map_err(storage_error)?;
        if kind.is_symlink() {
            return Err(storage_error(format!(
                "conversion source contains symlink {}",
                entry.path().display()
            )));
        }
        if kind.is_dir() {
            files.extend(list_files(&entry.path())?);
        } else if kind.is_file() {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}
fn tree_hashes(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>> {
    let mut hashes = BTreeMap::new();
    for path in list_files(root)? {
        use std::io::Read;
        let mut file = std::fs::File::open(&path).map_err(storage_error)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let length = file.read(&mut buffer).map_err(storage_error)?;
            if length == 0 {
                break;
            }
            hash.update(&buffer[..length]);
        }
        hashes.insert(
            path.strip_prefix(root)
                .expect("source file prefix")
                .to_owned(),
            hash.finalize().to_vec(),
        );
    }
    Ok(hashes)
}
fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir(destination).map_err(storage_error)?;
    for path in list_files(source)? {
        let relative = path.strip_prefix(source).expect("source file prefix");
        let target = destination.join(relative);
        std::fs::create_dir_all(target.parent().expect("archive file parent"))
            .map_err(storage_error)?;
        std::fs::copy(&path, &target).map_err(storage_error)?;
        std::fs::File::open(&target)
            .and_then(|file| file.sync_all())
            .map_err(storage_error)?;
    }
    Ok(())
}

async fn table_hashes(conn: &turso::Connection, table: &str) -> Result<Vec<Vec<u8>>> {
    let mut rows = conn
        .query(&format!("SELECT * FROM {table}"), ())
        .await
        .map_err(storage_error)?;
    let mut hashes = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage_error)? {
        let values = (0..row.column_count())
            .map(|index| row.get_value(index))
            .collect::<turso::Result<Vec<_>>>()
            .map_err(storage_error)?;
        hashes.push(row_hash(&values));
    }
    hashes.sort();
    Ok(hashes)
}
