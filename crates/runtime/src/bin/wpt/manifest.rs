//! Finds the tests to run in the checkout and reads their `// META:` lines.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// How a test file is wrapped, as wptserve would serve it to a worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `foo.any.js`, run as wptserve's classic worker wrapper
    /// (`foo.any.worker.js`) runs it: testharness.js, the META scripts, the
    /// test, then `done()`.
    Any,
    /// `foo.worker.js`, a whole dedicated worker script that imports
    /// testharness.js itself and calls `done()`.
    Worker,
}

#[derive(Debug, Default)]
pub struct Meta {
    pub title: Option<String>,
    pub scripts: Vec<String>,
    pub long_timeout: bool,
    pub variants: Vec<String>,
    pub global: Option<String>,
}

#[derive(Debug)]
pub struct TestFile {
    /// Relative to the checkout, with `/` separators.
    pub path: String,
    pub kind: Kind,
    pub meta: Meta,
    /// For `.worker.js` tests, the scripts its importScripts calls name.
    pub imports: Vec<String>,
}

/// One run of a test file: the file, and the variant query it runs with.
#[derive(Clone, Debug)]
pub struct Job {
    pub file: Arc<TestFile>,
    pub variant: String,
}

impl Job {
    /// The job's key in the expectations file.
    pub fn id(&self) -> String {
        format!("{}{}", self.file.path, self.variant)
    }
}

/// The worker globals of wptserve's `// META: global=` variants this runner
/// stands in for: a classic dedicated, shared, or service worker.
const WORKER_GLOBALS: [&str; 3] = ["dedicatedworker", "sharedworker", "serviceworker"];

/// Whether a `// META: global=` value includes a classic worker scope.
fn runs_in_worker(global: Option<&str>) -> bool {
    let Some(global) = global.map(str::trim).filter(|global| !global.is_empty()) else {
        // wptserve's default is window and dedicatedworker.
        return true;
    };
    global.split(',').map(str::trim).any(|item| match item {
        "worker" => true,
        item => WORKER_GLOBALS.contains(&item),
    })
}

/// Reads the leading `// META: key=value` lines, as wptserve does: they end
/// at the first line that is not one.
pub fn parse_meta(source: &str) -> Meta {
    let mut meta = Meta::default();
    for line in source.lines() {
        let Some(rest) = line.trim_start().strip_prefix("//") else {
            break;
        };
        let Some(rest) = rest.trim_start().strip_prefix("META:") else {
            break;
        };
        let Some((key, value)) = rest.trim_start().split_once('=') else {
            break;
        };
        let value = value.trim().to_string();
        match key.trim() {
            "title" => meta.title = Some(value),
            "script" => meta.scripts.push(value),
            "timeout" => meta.long_timeout = value == "long",
            "variant" => meta.variants.push(value),
            "global" => meta.global = Some(value),
            _ => {}
        }
    }
    meta
}

/// The string literals passed to `importScripts(...)` calls, in order.
pub fn parse_import_scripts(source: &str) -> Vec<String> {
    let mut imports = Vec::new();
    let mut rest = source;
    while let Some(start) = rest.find("importScripts(") {
        rest = &rest[start + "importScripts(".len()..];
        let end = rest.find(')').unwrap_or(rest.len());
        for argument in rest[..end].split(',') {
            let argument = argument.trim();
            let quoted = argument.len() >= 2
                && (argument.starts_with('"') && argument.ends_with('"')
                    || argument.starts_with('\'') && argument.ends_with('\''));
            if quoted {
                imports.push(argument[1..argument.len() - 1].to_string());
            }
        }
        rest = &rest[end..];
    }
    imports
}

/// Directories that hold helpers rather than tests, as in WPT's manifest.
fn is_helper_dir(name: &str) -> bool {
    matches!(name, "resources" | "support" | "tools")
}

/// Every test under `area` (a directory relative to `root`) that runs in a
/// worker scope, sorted by path.
pub fn discover(root: &Path, area: &str) -> Result<Vec<TestFile>, String> {
    let dir = root.join(area);
    if !dir.is_dir() {
        return Err(format!(
            "{area} is not in the WPT checkout at {}; is scripts/wpt/fetch.sh's sparse list in sync with wpt/config.json?",
            root.display()
        ));
    }
    let mut paths = BTreeSet::new();
    collect(&dir, area, &mut paths)?;
    let mut tests = Vec::new();
    for path in paths {
        let kind = if path.ends_with(".any.js") {
            Kind::Any
        } else {
            Kind::Worker
        };
        let source = fs::read_to_string(root.join(&path))
            .map_err(|error| format!("failed to read {path}: {error}"))?;
        let meta = parse_meta(&source);
        if kind == Kind::Any && !runs_in_worker(meta.global.as_deref()) {
            continue;
        }
        let imports = match kind {
            Kind::Any => Vec::new(),
            Kind::Worker => parse_import_scripts(&source),
        };
        tests.push(TestFile {
            path,
            kind,
            meta,
            imports,
        });
    }
    Ok(tests)
}

fn collect(dir: &Path, relative: &str, paths: &mut BTreeSet<String>) -> Result<(), String> {
    let entries =
        fs::read_dir(dir).map_err(|error| format!("failed to list {relative}: {error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("failed to list {relative}: {error}"))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = format!("{relative}/{name}");
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to stat {path}: {error}"))?;
        if file_type.is_dir() {
            if !is_helper_dir(&name) {
                collect(&entry.path(), &path, paths)?;
            }
        } else if file_type.is_file()
            && (name.ends_with(".any.js")
                || (name.ends_with(".worker.js") && !name.ends_with(".any.worker.js")))
        {
            paths.insert(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_lines_end_at_the_first_other_line() {
        let meta = parse_meta(
            "// META: title=Streams\n// META: global=window,worker\n// META: script=../resources/test-utils.js\n//META: timeout=long\n// META: variant=?1-10\n// META: variant=?11-last\n'use strict';\n// META: script=ignored.js\n",
        );
        assert_eq!(meta.title.as_deref(), Some("Streams"));
        assert_eq!(meta.global.as_deref(), Some("window,worker"));
        assert_eq!(meta.scripts, ["../resources/test-utils.js"]);
        assert!(meta.long_timeout);
        assert_eq!(meta.variants, ["?1-10", "?11-last"]);
    }

    #[test]
    fn worker_globals_follow_wptserve_variants() {
        assert!(runs_in_worker(None));
        assert!(runs_in_worker(Some("")));
        assert!(runs_in_worker(Some("window,worker")));
        assert!(runs_in_worker(Some("window, dedicatedworker")));
        assert!(runs_in_worker(Some("serviceworker")));
        assert!(!runs_in_worker(Some("window")));
        assert!(!runs_in_worker(Some("window,shadowrealm")));
        assert!(!runs_in_worker(Some("dedicatedworker-module")));
    }

    #[test]
    fn import_scripts_arguments_are_read_in_order() {
        assert_eq!(
            parse_import_scripts(
                "importScripts(\"/resources/testharness.js\");\nimportScripts('/resources/WebIDLParser.js', \"/resources/idlharness.js\");"
            ),
            [
                "/resources/testharness.js",
                "/resources/WebIDLParser.js",
                "/resources/idlharness.js"
            ]
        );
    }
}
