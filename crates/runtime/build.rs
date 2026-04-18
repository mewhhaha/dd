use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const MEMORY_RPC_SCHEMA_PATH: &str = "schema/memory_rpc.capnp";
const CHECKED_IN_MEMORY_RPC_PATH: &str = "src/generated/memory_rpc_capnp.rs";
const MEMORY_RPC_SCHEMA_FINGERPRINT_PREFIX: &str = "// dd-memory-rpc-schema-fingerprint: ";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={MEMORY_RPC_SCHEMA_PATH}");
    println!("cargo:rerun-if-changed={CHECKED_IN_MEMORY_RPC_PATH}");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/init.js");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/http_client.js");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/telemetry.ts");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/telemetry_util.ts");
    println!("cargo:rerun-if-changed=../../patched-crates/deno_crypto/00_crypto.js");

    validate_checked_in_memory_rpc();

    generate_execute_worker_bundle();
    generate_deno_js_extension();
}

fn validate_checked_in_memory_rpc() {
    let schema_fingerprint = current_memory_rpc_schema_fingerprint();
    let generated = fs::read_to_string(CHECKED_IN_MEMORY_RPC_PATH)
        .unwrap_or_else(|error| panic!("failed to read {CHECKED_IN_MEMORY_RPC_PATH}: {error}"));
    if generated_memory_rpc_fingerprint(&generated).as_deref() != Some(schema_fingerprint.as_str())
    {
        panic!(
            "checked-in memory RPC bindings at {CHECKED_IN_MEMORY_RPC_PATH} do not match {MEMORY_RPC_SCHEMA_PATH}; regenerate them with `capnp` installed"
        );
    }
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
