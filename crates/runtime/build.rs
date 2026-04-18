use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const MEMORY_RPC_SCHEMA_PATH: &str = "schema/memory_rpc.capnp";
const MEMORY_RPC_SCHEMA_FINGERPRINT_PREFIX: &str = "// dd-memory-rpc-schema-fingerprint: ";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={MEMORY_RPC_SCHEMA_PATH}");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/init.js");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/http_client.js");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/telemetry.ts");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/telemetry_util.ts");
    println!("cargo:rerun-if-changed=../../patched-crates/deno_crypto/00_crypto.js");

    compile_memory_rpc_schema();

    generate_execute_worker_bundle();
    generate_deno_js_extension();
}

fn compile_memory_rpc_schema() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR should be set"));
    let generated = out_dir.join("memory_rpc_capnp.rs");
    let schema_fingerprint = current_memory_rpc_schema_fingerprint();
    let compile_result = capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file(MEMORY_RPC_SCHEMA_PATH)
        .run();

    if generated.is_file()
        && fs::metadata(&generated)
            .map(|metadata| metadata.len() > 0)
            .unwrap_or(false)
    {
        if compile_result.is_ok()
            || generated_memory_rpc_file_matches_schema(&generated, &schema_fingerprint)
        {
            normalize_generated_memory_rpc_file(&generated, &schema_fingerprint).unwrap_or_else(
                |error| panic!("failed to normalize {}: {error}", generated.display()),
            );
            return;
        }
    }

    if try_reuse_generated_memory_rpc(&schema_fingerprint).is_some() {
        return;
    }

    if let Err(error) = compile_result {
        panic!(
            "failed to compile Cap'n Proto schema for memory_rpc: {error}. install `capnp` or rebuild from cache generated for current schema"
        );
    }

    if !generated.is_file()
        || fs::metadata(&generated)
            .map(|metadata| metadata.len() == 0)
            .unwrap_or(true)
    {
        panic!(
            "memory_rpc Cap'n Proto generation produced no usable output at {}",
            generated.display()
        );
    }

    normalize_generated_memory_rpc_file(&generated, &schema_fingerprint)
        .unwrap_or_else(|error| panic!("failed to normalize {}: {error}", generated.display()));
}

fn generate_deno_js_extension() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR should be set"));
    let output_path = out_dir.join("dd_deno_js_extension.rs");

    let entries = [
        (
            "ext:deno_webidl/00_webidl.js",
            PathBuf::from("js/vendor/deno_webidl/00_webidl.js"),
        ),
        (
            "ext:deno_web/00_url.js",
            PathBuf::from("js/vendor/deno_web/00_url.js"),
        ),
        (
            "ext:deno_web/00_infra.js",
            PathBuf::from("js/vendor/deno_web/00_infra.js"),
        ),
        (
            "ext:deno_web/01_console.js",
            PathBuf::from("js/vendor/deno_web/01_console.js"),
        ),
        (
            "ext:deno_web/01_dom_exception.js",
            PathBuf::from("js/vendor/deno_web/01_dom_exception.js"),
        ),
        (
            "ext:deno_web/01_mimesniff.js",
            PathBuf::from("js/vendor/deno_web/01_mimesniff.js"),
        ),
        (
            "ext:deno_web/01_urlpattern.js",
            PathBuf::from("js/vendor/deno_web/01_urlpattern.js"),
        ),
        (
            "ext:deno_web/02_event.js",
            PathBuf::from("js/vendor/deno_web/02_event.js"),
        ),
        (
            "ext:deno_web/02_structured_clone.js",
            PathBuf::from("js/vendor/deno_web/02_structured_clone.js"),
        ),
        (
            "ext:deno_web/03_abort_signal.js",
            PathBuf::from("js/vendor/deno_web/03_abort_signal.js"),
        ),
        (
            "ext:deno_web/06_streams.js",
            PathBuf::from("js/vendor/deno_web/06_streams.js"),
        ),
        (
            "ext:deno_web/08_text_encoding.js",
            PathBuf::from("js/vendor/deno_web/08_text_encoding.js"),
        ),
        (
            "ext:deno_web/09_file.js",
            PathBuf::from("js/vendor/deno_web/09_file.js"),
        ),
        (
            "ext:deno_web/12_location.js",
            PathBuf::from("js/vendor/deno_web/12_location.js"),
        ),
        (
            "ext:deno_web/15_performance.js",
            PathBuf::from("js/vendor/deno_web/15_performance.js"),
        ),
        (
            "ext:deno_fetch/20_headers.js",
            PathBuf::from("js/vendor/deno_fetch/20_headers.js"),
        ),
        (
            "ext:deno_fetch/21_formdata.js",
            PathBuf::from("js/vendor/deno_fetch/21_formdata.js"),
        ),
        (
            "ext:deno_fetch/22_body.js",
            PathBuf::from("js/vendor/deno_fetch/22_body.js"),
        ),
        (
            "ext:deno_fetch/22_http_client.js",
            PathBuf::from("js/compat/dd_deno_runtime/http_client.js"),
        ),
        (
            "ext:deno_fetch/23_request.js",
            PathBuf::from("js/vendor/deno_fetch/23_request.js"),
        ),
        (
            "ext:deno_fetch/23_response.js",
            PathBuf::from("js/vendor/deno_fetch/23_response.js"),
        ),
        (
            "ext:deno_fetch/26_fetch.js",
            PathBuf::from("js/vendor/deno_fetch/26_fetch.js"),
        ),
        (
            "ext:deno_telemetry/telemetry.ts",
            PathBuf::from("js/compat/dd_deno_runtime/telemetry.ts"),
        ),
        (
            "ext:deno_telemetry/util.ts",
            PathBuf::from("js/compat/dd_deno_runtime/telemetry_util.ts"),
        ),
        (
            "ext:deno_crypto/00_crypto.js",
            PathBuf::from("../../patched-crates/deno_crypto/00_crypto.js"),
        ),
        (
            "ext:dd_deno_runtime/init.js",
            PathBuf::from("js/compat/dd_deno_runtime/init.js"),
        ),
    ];

    let mut generated = String::from(
        "deno_core::extension!(dd_deno_js,\n  esm_entry_point = \"ext:dd_deno_runtime/init.js\",\n  esm = [\n",
    );
    for (specifier, path) in entries {
        println!("cargo:rerun-if-changed={}", path.display());
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        generated.push_str(&format!("    {specifier:?} = {{ source = {source:?} }},\n"));
    }
    generated.push_str("  ],\n);\n");

    fs::write(&output_path, generated)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", output_path.display()));
}

fn generate_execute_worker_bundle() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR should be set"));
    let output_path = out_dir.join("execute_worker.generated.js");
    let units = [
        "js/execute_worker/core.js",
        "js/execute_worker/fetch_cache.js",
        "js/execute_worker/sockets_transport.js",
        "js/execute_worker/memory.js",
        "js/execute_worker/dynamic.js",
    ];

    let mut generated = String::new();
    for unit in units {
        println!("cargo:rerun-if-changed={unit}");
        let path = Path::new(unit);
        let label = path
            .strip_prefix("js/")
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let source = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        generated.push_str(&format!("// __dd_source_unit:{label}\n"));
        generated.push_str(&source);
        if !generated.ends_with('\n') {
            generated.push('\n');
        }
        generated.push('\n');
    }

    fs::write(&output_path, generated)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", output_path.display()));
}

fn try_reuse_generated_memory_rpc(schema_fingerprint: &str) -> Option<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR")?);
    let target_dir = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../../target"));
    let build_roots = [
        target_dir.join("debug").join("build"),
        target_dir.join("release").join("build"),
    ];
    let candidate = build_roots
        .into_iter()
        .filter(|path| path.is_dir())
        .filter_map(|build_dir| fs::read_dir(&build_dir).ok())
        .flat_map(|entries| entries.filter_map(|entry| entry.ok().map(|entry| entry.path())))
        .flat_map(|path| {
            fs::read_dir(path.join("out"))
                .ok()
                .into_iter()
                .flat_map(|entries| {
                    entries.filter_map(|entry| entry.ok().map(|entry| entry.path()))
                })
        })
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name == "memory_rpc_capnp.rs")
                .unwrap_or(false)
        })
        .filter(|path| {
            path.is_file()
                && path.parent().and_then(|parent| parent.parent()) != Some(out_dir.as_path())
        })
        .filter_map(|path| {
            let metadata = fs::metadata(&path).ok()?;
            let len = metadata.len();
            let modified = metadata.modified().ok()?;
            (len > 0).then_some((modified, path))
        })
        .max_by_key(|(modified, _)| *modified)
        .map(|(_, path)| path);
    let source = candidate?;
    let output_path = out_dir.join("memory_rpc_capnp.rs");
    let generated = fs::read_to_string(&source).ok()?;
    if generated_memory_rpc_fingerprint(&generated)? != schema_fingerprint {
        return None;
    }
    let generated = normalize_generated_memory_rpc(&generated, schema_fingerprint);
    fs::write(output_path, generated).ok()?;
    Some(())
}

fn normalize_generated_memory_rpc_file(path: &Path, schema_fingerprint: &str) -> std::io::Result<()> {
    let generated = fs::read_to_string(path)?;
    let generated = normalize_generated_memory_rpc(&generated, schema_fingerprint);
    fs::write(path, generated)
}

fn generated_memory_rpc_file_matches_schema(path: &Path, schema_fingerprint: &str) -> bool {
    fs::read_to_string(path)
        .ok()
        .and_then(|source| generated_memory_rpc_fingerprint(&source))
        .as_deref()
        == Some(schema_fingerprint)
}

fn normalize_generated_memory_rpc(source: &str, schema_fingerprint: &str) -> String {
    let source = rewrite_generated_rpc_module_refs(source);
    if generated_memory_rpc_fingerprint(&source).as_deref() == Some(schema_fingerprint) {
        return source;
    }
    let mut out =
        String::with_capacity(source.len() + MEMORY_RPC_SCHEMA_FINGERPRINT_PREFIX.len() + 20);
    out.push_str(MEMORY_RPC_SCHEMA_FINGERPRINT_PREFIX);
    out.push_str(schema_fingerprint);
    out.push('\n');
    out.push_str(&source);
    out
}

fn generated_memory_rpc_fingerprint(source: &str) -> Option<String> {
    source
        .lines()
        .find_map(|line| line.strip_prefix(MEMORY_RPC_SCHEMA_FINGERPRINT_PREFIX))
        .map(|value| value.trim().to_string())
}

fn current_memory_rpc_schema_fingerprint() -> String {
    let schema = fs::read(MEMORY_RPC_SCHEMA_PATH)
        .unwrap_or_else(|error| panic!("failed to read {MEMORY_RPC_SCHEMA_PATH}: {error}"));
    format!("{:016x}", stable_fnv1a64(&schema))
}

fn stable_fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn rewrite_generated_rpc_module_refs(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut rest = source;

    while let Some(idx) = rest.find("crate::") {
        let (before, after) = rest.split_at(idx);
        out.push_str(before);
        let after = &after["crate::".len()..];
        let ident_len = after
            .chars()
            .take_while(|char| char.is_ascii_lowercase() || *char == '_')
            .count();
        if ident_len > 0 {
            let ident = &after[..ident_len];
            if ident.ends_with("rpc_capnp") {
                out.push_str("crate::memory_rpc_capnp");
                rest = &after[ident_len..];
                continue;
            }
        }
        out.push_str("crate::");
        rest = after;
    }

    out.push_str(rest);
    out
}
