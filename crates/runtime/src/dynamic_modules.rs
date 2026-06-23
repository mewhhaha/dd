use common::{PlatformError, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

const MAX_DYNAMIC_MODULE_COUNT: usize = 256;
const MAX_DYNAMIC_GRAPH_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct DynamicModuleGraph {
    modules: Arc<HashMap<String, String>>,
}

static DYNAMIC_MODULE_GRAPHS: OnceLock<Mutex<HashMap<String, DynamicModuleGraph>>> =
    OnceLock::new();

pub(crate) fn register_dynamic_module_graph(
    entrypoint: &str,
    modules: HashMap<String, String>,
) -> Result<(String, String)> {
    if modules.is_empty() {
        return Err(PlatformError::bad_request(
            "dynamic module graph must not be empty",
        ));
    }
    if modules.len() > MAX_DYNAMIC_MODULE_COUNT {
        return Err(PlatformError::bad_request(format!(
            "dynamic module graph exceeds {MAX_DYNAMIC_MODULE_COUNT} modules"
        )));
    }

    let mut normalized = HashMap::with_capacity(modules.len());
    let mut total_bytes = 0usize;
    for (path, source) in modules {
        let path = normalize_dynamic_module_path(&path)
            .map_err(|error| PlatformError::bad_request(format!("invalid module path: {error}")))?;
        let source = source.to_string();
        if source.trim().is_empty() {
            return Err(PlatformError::bad_request(format!(
                "dynamic module must not be empty: {path}"
            )));
        }
        total_bytes = total_bytes
            .saturating_add(path.len())
            .saturating_add(source.len());
        if total_bytes > MAX_DYNAMIC_GRAPH_BYTES {
            return Err(PlatformError::bad_request(format!(
                "dynamic module graph exceeds {MAX_DYNAMIC_GRAPH_BYTES} bytes"
            )));
        }
        if normalized.insert(path.clone(), source).is_some() {
            return Err(PlatformError::bad_request(format!(
                "dynamic module graph contains duplicate normalized path: {path}"
            )));
        }
    }
    let entrypoint = normalize_dynamic_module_path(entrypoint).map_err(|error| {
        PlatformError::bad_request(format!("invalid module entrypoint: {error}"))
    })?;
    if !normalized.contains_key(&entrypoint) {
        return Err(PlatformError::bad_request(format!(
            "dynamic module graph missing entrypoint module: {entrypoint}"
        )));
    }

    let graph_id = dynamic_module_graph_id(&normalized);
    let graphs = DYNAMIC_MODULE_GRAPHS.get_or_init(|| Mutex::new(HashMap::new()));
    graphs
        .lock()
        .expect("dynamic module graph registry mutex poisoned")
        .entry(graph_id.clone())
        .or_insert_with(|| DynamicModuleGraph {
            modules: Arc::new(normalized),
        });
    Ok((graph_id, entrypoint))
}

pub(crate) fn dynamic_module_source(graph_id: &str, module_path: &str) -> Option<String> {
    let graphs = DYNAMIC_MODULE_GRAPHS.get_or_init(|| Mutex::new(HashMap::new()));
    let graphs = graphs
        .lock()
        .expect("dynamic module graph registry mutex poisoned");
    graphs
        .get(graph_id)
        .and_then(|graph| graph.modules.get(module_path).cloned())
}

pub(crate) fn normalize_dynamic_module_path(value: &str) -> std::result::Result<String, String> {
    normalize_dynamic_module_parts(value.replace('\\', "/").split('/').map(ToOwned::to_owned))
}

pub(crate) fn resolve_dynamic_module_path(
    referrer_path: &str,
    specifier: &str,
) -> std::result::Result<String, String> {
    let specifier = specifier.trim();
    if specifier.is_empty() {
        return Err("dynamic module specifier must not be empty".to_string());
    }
    if specifier.starts_with('/') {
        return normalize_dynamic_module_path(specifier.trim_start_matches('/'));
    }
    if !specifier.starts_with("./") && !specifier.starts_with("../") {
        return Err(format!(
            "dynamic module bare imports are unsupported: {specifier}"
        ));
    }
    let mut base = referrer_path
        .split('/')
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let _ = base.pop();
    base.extend(specifier.split('/').map(ToOwned::to_owned));
    normalize_dynamic_module_parts(base)
}

fn normalize_dynamic_module_parts(
    parts: impl IntoIterator<Item = String>,
) -> std::result::Result<String, String> {
    let mut out = Vec::new();
    for part in parts {
        let part = part.trim();
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            if out.pop().is_none() {
                return Err("dynamic module path must not escape graph root".to_string());
            }
            continue;
        }
        out.push(part.to_string());
    }
    if out.is_empty() {
        return Err("dynamic module path must not be empty".to_string());
    }
    Ok(out.join("/"))
}

fn dynamic_module_graph_id(modules: &HashMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    let mut keys = modules.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        hasher.update((key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update([0]);
        let source = modules.get(key).expect("key came from modules map");
        hasher.update((source.len() as u64).to_le_bytes());
        hasher.update(source.as_bytes());
        hasher.update([0xff]);
    }
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_module_registry_uses_stable_graph_ids_and_normalized_paths() {
        let (graph_id, entrypoint) = register_dynamic_module_graph(
            "./worker.js",
            HashMap::from([
                (
                    "./worker.js".to_string(),
                    "import { value } from './lib/value.js'; export default value;".to_string(),
                ),
                (
                    "lib/./value.js".to_string(),
                    "export const value = 7;".to_string(),
                ),
            ]),
        )
        .expect("graph should register");
        let (same_graph_id, same_entrypoint) = register_dynamic_module_graph(
            "worker.js",
            HashMap::from([
                (
                    "lib/value.js".to_string(),
                    "export const value = 7;".to_string(),
                ),
                (
                    "worker.js".to_string(),
                    "import { value } from './lib/value.js'; export default value;".to_string(),
                ),
            ]),
        )
        .expect("same normalized graph should register");

        assert_eq!(graph_id, same_graph_id);
        assert_eq!(entrypoint, "worker.js");
        assert_eq!(same_entrypoint, "worker.js");
        assert_eq!(
            dynamic_module_source(&graph_id, "lib/value.js").as_deref(),
            Some("export const value = 7;")
        );
        assert_eq!(
            resolve_dynamic_module_path("worker.js", "./lib/value.js").as_deref(),
            Ok("lib/value.js")
        );
    }

    #[test]
    fn dynamic_module_paths_must_stay_inside_graph_root() {
        let error = register_dynamic_module_graph(
            "../worker.js",
            HashMap::from([("../worker.js".to_string(), "export default {};".to_string())]),
        )
        .expect_err("graph path should not escape root");

        assert!(error
            .to_string()
            .contains("dynamic module path must not escape graph root"));

        assert_eq!(
            resolve_dynamic_module_path("worker.js", "../outside.js").unwrap_err(),
            "dynamic module path must not escape graph root"
        );
    }

    #[test]
    fn dynamic_module_import_resolver_rejects_bare_specifiers() {
        assert_eq!(
            resolve_dynamic_module_path("worker.js", "lib/value.js").unwrap_err(),
            "dynamic module bare imports are unsupported: lib/value.js"
        );
        assert_eq!(
            resolve_dynamic_module_path("worker.js", "./lib/value.js").as_deref(),
            Ok("lib/value.js")
        );
        assert_eq!(
            resolve_dynamic_module_path("dir/worker.js", "/shared/value.js").as_deref(),
            Ok("shared/value.js")
        );
    }

    #[test]
    fn dynamic_module_registration_validates_entrypoint_in_rust() {
        let error = register_dynamic_module_graph(
            "missing.js",
            HashMap::from([("worker.js".to_string(), "export default {};".to_string())]),
        )
        .expect_err("missing entrypoint should fail");

        assert!(error
            .to_string()
            .contains("dynamic module graph missing entrypoint module: missing.js"));
    }

    #[test]
    fn dynamic_module_registration_rejects_duplicate_normalized_paths() {
        let error = register_dynamic_module_graph(
            "worker.js",
            HashMap::from([
                ("./worker.js".to_string(), "export default {};".to_string()),
                (
                    "worker.js".to_string(),
                    "export const duplicate = true;".to_string(),
                ),
            ]),
        )
        .expect_err("duplicate normalized path should fail");

        assert!(error
            .to_string()
            .contains("dynamic module graph contains duplicate normalized path: worker.js"));
    }
}
