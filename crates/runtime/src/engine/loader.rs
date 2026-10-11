//! Worker module resolution and loading.

use super::*;

#[derive(Default)]
pub(super) struct RuntimeModuleLoader {
    pub(super) module_registry: ModuleRegistry,
}

impl ModuleLoader for RuntimeModuleLoader {
    fn resolve(&self, specifier: &str, referrer: &str) -> std::result::Result<String, String> {
        if referrer.starts_with("dd-module:")
            && !specifier.starts_with("//")
            && !has_url_scheme(specifier)
        {
            return resolve_dd_module_import(specifier, referrer).map(String::from);
        }
        resolve_import(specifier, referrer).map(String::from)
    }

    fn load(
        &self,
        name: &str,
        requested_type: ModuleType,
    ) -> std::result::Result<ModuleSource, String> {
        let specifier = Url::parse(name).map_err(|error| error.to_string())?;
        match specifier.scheme() {
            "dd-module" => load_dd_module(&self.module_registry, &specifier, requested_type),
            "node" if specifier.as_str() == "node:async_hooks" => Ok(ModuleSource::new(
                ModuleType::JavaScript,
                ModuleCode::String(NODE_ASYNC_HOOKS_SOURCE.to_string()),
            )),
            _ => Err(format!(
                "runtime module loader does not support: {specifier}"
            )),
        }
    }
}

/// Resolves an import the way a browser does: an absolute URL as is, a path
/// starting with `/`, `./` or `../` against the referrer, anything else
/// (a bare specifier) is an error.
pub(super) fn resolve_import(specifier: &str, referrer: &str) -> std::result::Result<Url, String> {
    match Url::parse(specifier) {
        Ok(url) => Ok(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            if !(specifier.starts_with('/')
                || specifier.starts_with("./")
                || specifier.starts_with("../"))
            {
                let from = if referrer.is_empty() {
                    String::new()
                } else {
                    format!(" from \"{referrer}\"")
                };
                return Err(format!(
                    "Relative import path \"{specifier}\" not prefixed with / or ./ or ../{from}"
                ));
            }
            let base = Url::parse(referrer).map_err(|error| {
                format!("invalid referrer {referrer:?} for import {specifier:?}: {error}")
            })?;
            base.join(specifier)
                .map_err(|error| format!("invalid import {specifier:?}: {error}"))
        }
        Err(error) => Err(format!("invalid import {specifier:?}: {error}")),
    }
}

pub(super) fn resolve_dd_module_import(
    specifier: &str,
    referrer: &str,
) -> std::result::Result<Url, String> {
    let referrer = Url::parse(referrer).map_err(|error| error.to_string())?;
    let (graph_id, referrer_path) = dd_module_parts(&referrer)?;
    let module_path = resolve_module_path(&referrer_path, specifier)?;
    let encoded_path = module_path
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    Url::parse(&format!("dd-module://graph/{graph_id}/{encoded_path}"))
        .map_err(|error| error.to_string())
}

pub(super) fn load_dd_module(
    module_registry: &ModuleRegistry,
    specifier: &Url,
    requested_type: ModuleType,
) -> std::result::Result<ModuleSource, String> {
    let (graph_id, module_path) = dd_module_parts(specifier)?;
    let module = module_registry
        .module(&graph_id, &module_path)
        .ok_or_else(|| format!("module graph {graph_id} does not contain module: {module_path}"))?;
    let module_type = module_type(module.kind);
    if requested_type != module_type {
        return Err(format!(
            "requested module type {requested_type} does not match {module_type} module: {module_path}"
        ));
    }
    let code = match module.kind {
        RuntimeModuleKind::Wasm => ModuleCode::String(compiled_wasm_module_source(&module.code)),
        RuntimeModuleKind::JavaScript
        | RuntimeModuleKind::Json
        | RuntimeModuleKind::Text
        | RuntimeModuleKind::Bytes => ModuleCode::Bytes(module.code),
    };
    Ok(ModuleSource::new(module_type, code))
}

pub(super) fn module_type(kind: RuntimeModuleKind) -> ModuleType {
    match kind {
        RuntimeModuleKind::JavaScript | RuntimeModuleKind::Wasm => ModuleType::JavaScript,
        RuntimeModuleKind::Json => ModuleType::Json,
        RuntimeModuleKind::Text => ModuleType::Text,
        RuntimeModuleKind::Bytes => ModuleType::Bytes,
    }
}

pub(super) fn compiled_wasm_module_source(bytes: &[u8]) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    format!(
        r#"
const encoded = {encoded:?};
const binary = atob(encoded);
const bytes = new Uint8Array(binary.length);
for (let index = 0; index < binary.length; index += 1) {{
  bytes[index] = binary.charCodeAt(index);
}}
export default new WebAssembly.Module(bytes);
"#
    )
}

pub(super) fn dd_module_parts(specifier: &Url) -> std::result::Result<(String, String), String> {
    if specifier.scheme() != "dd-module" {
        return Err(format!("expected dd-module module URL, got {specifier}"));
    }
    if specifier.host_str() != Some("graph") {
        return Err(format!(
            "expected dd-module://graph module URL, got {specifier}"
        ));
    }
    let path = specifier.path().trim_start_matches('/');
    let (graph_id, module_path) = path.split_once('/').ok_or_else(|| {
        format!("dd-module module URL is missing graph id or module path: {specifier}")
    })?;
    let graph_id = graph_id.trim();
    if graph_id.is_empty()
        || graph_id.len() > 128
        || !graph_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(format!(
            "invalid dd-module graph id in module URL: {specifier}"
        ));
    }
    let bytes = percent_decode(module_path)?;
    let path = String::from_utf8(bytes)
        .map_err(|error| format!("invalid UTF-8 in dd-module module path: {error}"))?;
    normalize_module_path(&path).map(|path| (graph_id.to_string(), path))
}

pub(super) fn has_url_scheme(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    for ch in chars {
        if ch == ':' {
            return true;
        }
        if !(ch.is_ascii_alphanumeric() || ch == '+' || ch == '-' || ch == '.') {
            return false;
        }
    }
    false
}

pub(super) fn percent_encode_path_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            other => {
                use std::fmt::Write as _;
                let _ = write!(&mut out, "%{other:02X}");
            }
        }
    }
    out
}

pub(super) fn worker_source_text<'a>(
    source: &'a WorkerSource,
    module_registry: &ModuleRegistry,
) -> Result<Cow<'a, str>> {
    match source {
        WorkerSource::Inline(source) => Ok(Cow::Borrowed(source.as_ref())),
        WorkerSource::Module {
            graph_id,
            entrypoint,
        } => {
            let specifier = module_entrypoint_specifier(module_registry, graph_id, entrypoint)?;
            Ok(Cow::Owned(format!(
                "export {{ default }} from {specifier:?};\n"
            )))
        }
    }
}

pub(super) fn module_entrypoint_specifier(
    module_registry: &ModuleRegistry,
    graph_id: &str,
    entrypoint: &str,
) -> Result<String> {
    let graph_id = graph_id.trim();
    if graph_id.is_empty()
        || graph_id.len() > 128
        || !graph_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PlatformError::bad_request("module graph id is invalid"));
    }
    let entrypoint = normalize_module_path(entrypoint).map_err(|error| {
        PlatformError::bad_request(format!("invalid module entrypoint: {error}"))
    })?;
    if module_registry.source(graph_id, &entrypoint).is_none() {
        return Err(PlatformError::bad_request(format!(
            "module graph {graph_id} does not contain entrypoint: {entrypoint}"
        )));
    }
    let encoded_path = entrypoint
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    Ok(format!("dd-module://graph/{graph_id}/{encoded_path}"))
}

pub(super) fn percent_decode(value: &str) -> std::result::Result<Vec<u8>, String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'%' => {
                if idx + 2 >= bytes.len() {
                    return Err("truncated percent-escape in module URL".to_string());
                }
                let hi = decode_hex_digit(bytes[idx + 1])?;
                let lo = decode_hex_digit(bytes[idx + 2])?;
                out.push((hi << 4) | lo);
                idx += 3;
            }
            b => {
                out.push(b);
                idx += 1;
            }
        }
    }
    Ok(out)
}

pub(super) fn decode_hex_digit(value: u8) -> std::result::Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(format!(
            "invalid hex digit in module URL escape: {}",
            value as char
        )),
    }
}
