use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Must match the exact list and order in `src/web/mod.rs::EXT_SCRIPTS`.
const EXT_SCRIPT_PATHS: &[&str] = &[
    "vendor/webidl/00_webidl.js",
    "vendor/web/00_infra.js",
    "vendor/web/00_url.js",
    "web/console.js",
    "vendor/web/01_dom_exception.js",
    "vendor/web/01_mimesniff.js",
    "vendor/web/01_urlpattern.js",
    "vendor/web/02_event.js",
    "vendor/web/02_structured_clone.js",
    "vendor/web/03_abort_signal.js",
    "vendor/web/06_streams.js",
    "vendor/web/08_text_encoding.js",
    "vendor/web/09_file.js",
    "vendor/web/15_performance.js",
    "vendor/fetch/20_headers.js",
    "vendor/fetch/21_formdata.js",
    "vendor/fetch/22_body.js",
    "vendor/fetch/23_request.js",
    "vendor/fetch/23_response.js",
    "vendor/crypto/00_crypto.js",
];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let execute_worker_js = generate_execute_worker_bundle();
    precompile_bootstrap_bundle(&execute_worker_js);
}

fn generate_execute_worker_bundle() -> String {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR should be set"));
    let output_path = out_dir.join("execute_worker.generated.js");
    let manifest = Path::new("js/execute_worker/units.txt");
    println!("cargo:rerun-if-changed={}", manifest.display());
    let units = fs::read_to_string(manifest)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", manifest.display()));

    let mut generated = String::new();
    for unit in units.lines().map(str::trim).filter(|unit| !unit.is_empty()) {
        let path = manifest
            .parent()
            .expect("source manifest directory")
            .join(unit);
        println!("cargo:rerun-if-changed={}", path.display());
        let label = path
            .strip_prefix("js/")
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        generated.push_str(&format!("// __dd_source_unit:{label}\n"));
        generated.push_str(&source);
        if !generated.ends_with('\n') {
            generated.push('\n');
        }
        generated.push('\n');
    }

    fs::write(&output_path, &generated)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", output_path.display()));
    generated
}

fn precompile_bootstrap_bundle(execute_worker_js: &str) {
    println!("cargo:rerun-if-changed=../dd_v8/src/kernel.mjs");
    println!("cargo:rerun-if-env-changed=DD_SKIP_PRECOMPILE_BOOTSTRAP");

    for path in [
        "js/core/00_primordials.js",
        "js/core/core.js",
        "js/web/init.js",
        "js/bootstrap.js",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    for rel in EXT_SCRIPT_PATHS {
        println!("cargo:rerun-if-changed=js/{rel}");
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR should be set"));
    let wtmn_path = out_dir.join("dd_bootstrap.wtmn");
    let sha_path = out_dir.join("dd_bootstrap.sha256");

    let host = env::var("HOST").unwrap_or_default();
    let target = env::var("TARGET").unwrap_or_default();
    if host != target || env::var_os("DD_SKIP_PRECOMPILE_BOOTSTRAP").is_some() {
        fs::write(&wtmn_path, []).expect("write empty precompiled bootstrap placeholder");
        fs::write(&sha_path, [0u8; 32]).expect("write placeholder bootstrap sha256");
        return;
    }

    let ext_scripts_vec: Vec<(&'static str, &'static str)> = EXT_SCRIPT_PATHS
        .iter()
        .map(|rel| {
            let spec: &'static str = Box::leak(format!("ext:{rel}").into_boxed_str());
            let src_path = format!("js/{rel}");
            let src_str = fs::read_to_string(&src_path)
                .unwrap_or_else(|e| panic!("failed to read {src_path}: {e}"));
            let src: &'static str = Box::leak(src_str.into_boxed_str());
            (spec, src)
        })
        .collect();
    let ext_scripts: &'static [(&'static str, &'static str)] =
        Box::leak(ext_scripts_vec.into_boxed_slice());

    let primordials =
        fs::read_to_string("js/core/00_primordials.js").expect("read js/core/00_primordials.js");
    let core_js = fs::read_to_string("js/core/core.js").expect("read js/core/core.js");
    let web_init = fs::read_to_string("js/web/init.js").expect("read js/web/init.js");
    let bootstrap_js = fs::read_to_string("js/bootstrap.js").expect("read js/bootstrap.js");

    let mut runtime = dd_v8::JsRuntime::new_for_snapshot(dd_v8::RuntimeOptions {
        ext_scripts,
        ..Default::default()
    })
    .expect("create snapshot recorder");
    runtime
        .execute_script("ext:core/00_primordials.js", &primordials)
        .expect("record primordials");
    let bootstrap = runtime
        .execute_with_ops("ext:core/core.js", &core_js)
        .expect("record core.js");
    for (name, source) in [
        ("ext:web/init.js", web_init.as_str()),
        ("ext:bootstrap.js", bootstrap_js.as_str()),
        ("ext:execute_worker", execute_worker_js),
    ] {
        runtime
            .execute_function(
                name,
                source,
                &["__bootstrap"],
                std::slice::from_ref(&bootstrap),
            )
            .unwrap_or_else(|e| panic!("record {name}: {e}"));
    }
    runtime.set_internals(bootstrap);
    let bundle_src = runtime
        .snapshot_bundle_source()
        .expect("render bootstrap bundle source");
    let (sha256, bytes) = dd_v8::precompile_snapshot_bundle(&bundle_src)
        .expect("precompile dd runtime bootstrap snapshot bundle");
    fs::write(&wtmn_path, bytes).expect("write precompiled dd bootstrap image");
    fs::write(&sha_path, sha256).expect("write precompiled dd bootstrap sha256");
}
