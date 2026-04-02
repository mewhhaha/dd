use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=schema/actor_rpc.capnp");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/init.js");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/http_client.js");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/telemetry.ts");
    println!("cargo:rerun-if-changed=js/compat/dd_deno_runtime/telemetry_util.ts");
    println!("cargo:rerun-if-changed=../../vendor/deno_crypto/00_crypto.js");

    compile_actor_rpc_schema();

    generate_deno_js_extension();
}

fn compile_actor_rpc_schema() {
    match capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/actor_rpc.capnp")
        .run()
    {
        Ok(()) => {}
        Err(error) => {
            if try_reuse_generated_actor_rpc().is_none() {
                panic!("failed to compile Cap'n Proto schema for actor_rpc: {error}");
            }
        }
    }
}

fn generate_deno_js_extension() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR should be set"));
    let output_path = out_dir.join("dd_deno_js_extension.rs");

    let registry_root = cargo_registry_src_root();
    let entries = [
        (
            "ext:deno_webidl/00_webidl.js",
            registry_root.join("deno_webidl-0.241.0/00_webidl.js"),
        ),
        (
            "ext:deno_web/00_url.js",
            registry_root.join("deno_web-0.272.0/00_url.js"),
        ),
        (
            "ext:deno_web/00_infra.js",
            registry_root.join("deno_web-0.272.0/00_infra.js"),
        ),
        (
            "ext:deno_web/01_console.js",
            registry_root.join("deno_web-0.272.0/01_console.js"),
        ),
        (
            "ext:deno_web/01_dom_exception.js",
            registry_root.join("deno_web-0.272.0/01_dom_exception.js"),
        ),
        (
            "ext:deno_web/01_mimesniff.js",
            registry_root.join("deno_web-0.272.0/01_mimesniff.js"),
        ),
        (
            "ext:deno_web/01_urlpattern.js",
            registry_root.join("deno_web-0.272.0/01_urlpattern.js"),
        ),
        (
            "ext:deno_web/02_event.js",
            registry_root.join("deno_web-0.272.0/02_event.js"),
        ),
        (
            "ext:deno_web/02_structured_clone.js",
            registry_root.join("deno_web-0.272.0/02_structured_clone.js"),
        ),
        (
            "ext:deno_web/03_abort_signal.js",
            registry_root.join("deno_web-0.272.0/03_abort_signal.js"),
        ),
        (
            "ext:deno_web/06_streams.js",
            registry_root.join("deno_web-0.272.0/06_streams.js"),
        ),
        (
            "ext:deno_web/08_text_encoding.js",
            registry_root.join("deno_web-0.272.0/08_text_encoding.js"),
        ),
        (
            "ext:deno_web/09_file.js",
            registry_root.join("deno_web-0.272.0/09_file.js"),
        ),
        (
            "ext:deno_web/12_location.js",
            registry_root.join("deno_web-0.272.0/12_location.js"),
        ),
        (
            "ext:deno_web/15_performance.js",
            registry_root.join("deno_web-0.272.0/15_performance.js"),
        ),
        (
            "ext:deno_fetch/20_headers.js",
            registry_root.join("deno_fetch-0.265.0/20_headers.js"),
        ),
        (
            "ext:deno_fetch/21_formdata.js",
            registry_root.join("deno_fetch-0.265.0/21_formdata.js"),
        ),
        (
            "ext:deno_fetch/22_body.js",
            registry_root.join("deno_fetch-0.265.0/22_body.js"),
        ),
        (
            "ext:deno_fetch/22_http_client.js",
            PathBuf::from("js/compat/dd_deno_runtime/http_client.js"),
        ),
        (
            "ext:deno_fetch/23_request.js",
            registry_root.join("deno_fetch-0.265.0/23_request.js"),
        ),
        (
            "ext:deno_fetch/23_response.js",
            registry_root.join("deno_fetch-0.265.0/23_response.js"),
        ),
        (
            "ext:deno_fetch/26_fetch.js",
            registry_root.join("deno_fetch-0.265.0/26_fetch.js"),
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
            PathBuf::from("../../vendor/deno_crypto/00_crypto.js"),
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

fn cargo_registry_src_root() -> PathBuf {
    let cargo_home = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| Path::new(&home).join(".cargo")))
        .expect("CARGO_HOME or HOME should be set");
    let registry_src = cargo_home.join("registry/src");
    let mut roots = fs::read_dir(&registry_src)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", registry_src.display()))
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .collect::<Vec<_>>();
    roots.sort();
    roots
        .into_iter()
        .find(|path| path.join("deno_web-0.272.0").is_dir())
        .unwrap_or_else(|| {
            panic!(
                "failed to locate deno_web source under {}",
                registry_src.display()
            )
        })
}

fn try_reuse_generated_actor_rpc() -> Option<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR")?);
    let target_dir = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../../target"));
    let profile = env::var("PROFILE").ok()?;
    let build_roots = [
        target_dir.join(&profile).join("build"),
        target_dir.join("debug").join("build"),
        target_dir.join("release").join("build"),
    ];
    let candidate = build_roots
        .into_iter()
        .filter(|path| path.is_dir())
        .filter_map(|build_dir| fs::read_dir(&build_dir).ok())
        .flat_map(|entries| entries.filter_map(|entry| entry.ok().map(|entry| entry.path())))
        .map(|path| path.join("out/actor_rpc_capnp.rs"))
        .filter(|path| {
            path.is_file()
                && path.parent().and_then(|parent| parent.parent()) != Some(out_dir.as_path())
        })
        .filter_map(|path| {
            let len = fs::metadata(&path).ok()?.len();
            (len > 0).then_some((len, path))
        })
        .max_by_key(|(len, _)| *len)
        .map(|(_, path)| path);
    let source = candidate?;
    fs::copy(source, out_dir.join("actor_rpc_capnp.rs")).ok()?;
    Some(())
}
