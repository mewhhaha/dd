//! Reading assets and server modules from disk into a deploy request. Paths a
//! deploy config names stay inside its directory unless the caller allows
//! otherwise.
use crate::config::DeployFileServerModule;
use base64::Engine;
use common::{DeployAsset, DeployServerModule, DeployServerModuleKind};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

pub fn package_assets_dir(dir: Option<&str>) -> Result<(Vec<DeployAsset>, Option<String>), String> {
    package_assets_dir_with_excludes(dir, &[])
}

pub fn package_server_module_specs(
    specs: &[String],
    base_dir: &Path,
    allow_outside_config_root: bool,
) -> Result<Vec<DeployServerModule>, String> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let (kind, path, file) = parse_server_module_spec(spec)
                .map_err(|error| format!("server module {index}: {error}"))?;
            package_server_module(
                kind,
                &path,
                file.as_deref(),
                base_dir,
                allow_outside_config_root,
                &format!("server_modules[{index}].file"),
            )
        })
        .collect()
}

pub fn package_deploy_file_server_modules(
    base_dir: &Path,
    modules: Vec<DeployFileServerModule>,
    allow_outside_config_root: bool,
) -> Result<Vec<DeployServerModule>, String> {
    modules
        .into_iter()
        .enumerate()
        .map(|(index, module)| {
            let file = module.file.as_deref();
            package_server_module(
                module.kind,
                &module.path,
                file,
                base_dir,
                allow_outside_config_root,
                &format!("server_modules[{index}].file"),
            )
        })
        .collect()
}

fn parse_server_module_spec(
    spec: &str,
) -> Result<(DeployServerModuleKind, String, Option<String>), String> {
    let (kind, rest) = spec
        .split_once(':')
        .ok_or_else(|| "expected TYPE:MODULE_PATH[=FILE]".to_string())?;
    let kind = parse_server_module_kind(kind)?;
    let (path, file) = match rest.split_once('=') {
        Some((path, file)) => (path.trim(), Some(file.trim().to_string())),
        None => (rest.trim(), None),
    };
    if path.is_empty() {
        return Err("module path must not be empty".to_string());
    }
    Ok((
        kind,
        path.to_string(),
        file.filter(|value| !value.is_empty()),
    ))
}

fn parse_server_module_kind(value: &str) -> Result<DeployServerModuleKind, String> {
    let normalized = value
        .trim()
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_' && !ch.is_ascii_whitespace())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    match normalized.as_str() {
        "esmodule" | "esm" | "javascript" | "module" => Ok(DeployServerModuleKind::EsModule),
        "compiledwasm" | "wasm" => Ok(DeployServerModuleKind::CompiledWasm),
        "text" => Ok(DeployServerModuleKind::Text),
        "data" | "bytes" => Ok(DeployServerModuleKind::Data),
        "json" => Ok(DeployServerModuleKind::Json),
        _ => Err(format!("unsupported server module type: {value}")),
    }
}

fn package_server_module(
    kind: DeployServerModuleKind,
    module_path: &str,
    file: Option<&str>,
    base_dir: &Path,
    allow_outside_config_root: bool,
    field: &str,
) -> Result<DeployServerModule, String> {
    let path = normalize_server_module_path(module_path)?;
    let file = file.unwrap_or(module_path).trim();
    if file.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    let file_path = resolve_config_file_path(base_dir, file, allow_outside_config_root, field)?;
    let metadata = fs::symlink_metadata(&file_path).map_err(|error| {
        format!(
            "failed to stat server module {}: {error}",
            file_path.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "server module symlinks are not supported: {}",
            file_path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(format!(
            "server module is not a file: {}",
            file_path.display()
        ));
    }
    let bytes = fs::read(&file_path).map_err(|error| {
        format!(
            "failed to read server module {}: {error}",
            file_path.display()
        )
    })?;
    Ok(DeployServerModule {
        path,
        kind,
        content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
    })
}

fn normalize_server_module_path(value: &str) -> Result<String, String> {
    let value = value.trim().replace('\\', "/");
    if value.is_empty() {
        return Err("server module path must not be empty".to_string());
    }
    if has_url_scheme(&value) {
        return Err(format!("server module URL paths are unsupported: {value}"));
    }
    let mut segments = Vec::new();
    for segment in value.trim_start_matches('/').split('/') {
        let segment = segment.trim();
        if segment.is_empty() || segment == "." {
            continue;
        }
        if segment == ".." {
            if segments.pop().is_none() {
                return Err("server module path must not escape module root".to_string());
            }
            continue;
        }
        segments.push(segment.to_string());
    }
    if segments.is_empty() {
        return Err("server module path must not be empty".to_string());
    }
    Ok(segments.join("/"))
}

fn has_url_scheme(value: &str) -> bool {
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

pub fn package_assets_dir_with_excludes(
    dir: Option<&str>,
    excludes: &[String],
) -> Result<(Vec<DeployAsset>, Option<String>), String> {
    let Some(dir) = dir else {
        return Ok((Vec::new(), None));
    };
    let excluded_paths = normalized_asset_excludes(excludes)?;

    let root = Path::new(dir);
    if !root.exists() {
        return Err(format!("assets dir not found: {}", root.display()));
    }
    if !root.is_dir() {
        return Err(format!("assets dir is not a directory: {}", root.display()));
    }

    let mut assets = Vec::new();
    let mut asset_headers = None;
    collect_assets_from_dir(root, root, &excluded_paths, &mut assets, &mut asset_headers)?;
    assets.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((assets, asset_headers))
}

fn collect_assets_from_dir(
    root: &Path,
    current: &Path,
    excluded_paths: &HashSet<String>,
    assets: &mut Vec<DeployAsset>,
    asset_headers: &mut Option<String>,
) -> Result<(), String> {
    let mut entries = fs::read_dir(current)
        .map_err(|error| format!("failed to read assets dir {}: {error}", current.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to read assets dir {}: {error}", current.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("failed to stat asset path {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "asset symlinks are not supported: {}",
                path.display()
            ));
        }
        if metadata.is_dir() {
            collect_assets_from_dir(root, &path, excluded_paths, assets, asset_headers)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        let relative = path.strip_prefix(root).map_err(|error| {
            format!("failed to normalize asset path {}: {error}", path.display())
        })?;
        let relative_path = normalize_asset_relative_path(relative)?;
        if excluded_paths.contains(&relative_path) {
            continue;
        }
        if relative_path == "_headers" {
            *asset_headers = Some(
                fs::read_to_string(&path)
                    .map_err(|error| format!("failed to read {}: {error}", path.display()))?,
            );
            continue;
        }

        let normalized_path = format!("/{relative_path}");
        if assets.iter().any(|asset| asset.path == normalized_path) {
            return Err(format!("duplicate asset path: {normalized_path}"));
        }

        let bytes = fs::read(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        assets.push(DeployAsset {
            path: normalized_path,
            content_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        });
    }

    Ok(())
}

fn normalize_asset_relative_path(path: &Path) -> Result<String, String> {
    let mut segments = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| format!("asset path is not valid UTF-8: {}", path.display()))?;
                if value.is_empty() || value == "." || value == ".." {
                    return Err(format!("invalid asset path segment: {value}"));
                }
                segments.push(value.to_string());
            }
            _ => {
                return Err(format!(
                    "asset path must stay within the assets dir: {}",
                    path.display()
                ));
            }
        }
    }

    if segments.is_empty() {
        return Err("asset path must not be empty".to_string());
    }

    Ok(segments.join("/"))
}

fn normalized_asset_excludes(excludes: &[String]) -> Result<HashSet<String>, String> {
    excludes
        .iter()
        .map(|path| normalize_asset_exclude(path))
        .collect()
}

fn normalize_asset_exclude(path: &str) -> Result<String, String> {
    let trimmed = path.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        return Err("asset exclude path must not be empty".to_string());
    }
    normalize_asset_relative_path(Path::new(trimmed))
}

fn config_relative_path(base_dir: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

pub fn resolve_config_file_path(
    base_dir: &Path,
    value: &str,
    allow_outside_config_root: bool,
    field: &str,
) -> Result<PathBuf, String> {
    let path = config_relative_path(base_dir, value);
    if allow_outside_config_root {
        return Ok(path);
    }

    let root = canonical_or_absolute_path(base_dir)?;
    let target = canonical_or_absolute_path(&path)?;
    if target.strip_prefix(&root).is_err() {
        return Err(format!(
            "deploy config {field} must stay within the config directory (use --allow-outside-config-root to allow {})",
            target.display()
        ));
    }
    Ok(target)
}

pub fn push_asset_exclude_if_within(
    excludes: &mut Vec<String>,
    assets_root: &Path,
    path: &Path,
) -> Result<(), String> {
    let assets_root = canonical_or_absolute_path(assets_root)?;
    let path = canonical_or_absolute_path(path)?;
    if let Ok(relative) = path.strip_prefix(&assets_root) {
        excludes.push(normalize_asset_relative_path(relative)?);
    }
    Ok(())
}

fn canonical_or_absolute_path(path: &Path) -> Result<PathBuf, String> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(_) => absolute_path(path),
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|error| error.to_string())
    }
}

pub fn path_to_string(path: &Path) -> Result<String, String> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{normalize_asset_relative_path, package_assets_dir, package_server_module_specs};
    use crate::temp_dir;
    use base64::Engine;
    use common::DeployServerModuleKind;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn packages_nested_assets_and_extracts_root_headers() {
        let root = temp_dir("assets");
        fs::create_dir_all(root.join("nested")).expect("create nested");
        fs::write(root.join("a.js"), "console.log('a');").expect("write a.js");
        fs::write(root.join("nested").join("b.css"), "body{}").expect("write b.css");
        fs::write(
            root.join("_headers"),
            "/a.js\n  Cache-Control: public, max-age=60\n",
        )
        .expect("write headers");

        let (assets, asset_headers) = package_assets_dir(root.to_str()).expect("package assets");
        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].path, "/a.js");
        assert_eq!(assets[1].path, "/nested/b.css");
        assert!(
            asset_headers
                .expect("headers should be present")
                .contains("Cache-Control")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn package_server_module_specs_accepts_file_aliases() {
        let root = temp_dir("server-modules");
        fs::create_dir_all(root.join("files")).expect("create files");
        let source = root.join("files").join("message.txt");
        fs::write(&source, "hello").expect("write module");

        let modules = package_server_module_specs(
            &[format!("Text:./modules/message.txt={}", source.display())],
            Path::new("."),
            true,
        )
        .expect("package server module");

        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].path, "modules/message.txt");
        assert_eq!(modules[0].kind, DeployServerModuleKind::Text);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(modules[0].content_base64.as_bytes())
            .expect("server module should decode");
        assert_eq!(decoded, b"hello");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn normalize_asset_relative_path_rejects_non_normal_segments() {
        let error = normalize_asset_relative_path(PathBuf::from("../escape.js").as_path())
            .expect_err("path should fail");
        assert!(error.contains("within the assets dir"));
    }

    #[cfg(unix)]
    #[test]
    fn package_assets_dir_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let root = temp_dir("symlink");
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("target.js"), "export{}").expect("write target");
        symlink(root.join("target.js"), root.join("link.js")).expect("create symlink");

        let error = package_assets_dir(root.to_str()).expect_err("symlink should fail");
        assert!(error.contains("symlinks"));

        let _ = fs::remove_dir_all(root);
    }
}
