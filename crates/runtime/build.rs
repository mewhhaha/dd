use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    generate_execute_worker_bundle();
}

fn generate_execute_worker_bundle() {
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

    fs::write(&output_path, generated)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", output_path.display()));
}
