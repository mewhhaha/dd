use scriptw_engine::{
    CompileOptions, EmbeddedRuntime, Engine as ScriptwCompiler, NativeCompilationPolicy,
    RealmBuilder, configure_wasm_runtime,
};
use scriptw_ir::foreign::{
    ForeignFunctionDeclaration, ForeignModuleDeclaration, ForeignModuleRegistry, ForeignResultType,
    ForeignValueType,
};
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/kernel.mjs");
    println!("cargo:rerun-if-env-changed=DD_SKIP_PRECOMPILE_KERNEL");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set"));
    let out_path = out_dir.join("dd_v8_kernel.wtmn");

    let host = env::var("HOST").unwrap_or_default();
    let target = env::var("TARGET").unwrap_or_default();
    if host != target || env::var_os("DD_SKIP_PRECOMPILE_KERNEL").is_some() {
        fs::write(&out_path, []).expect("write empty precompiled kernel placeholder");
        return;
    }

    let kernel_src = fs::read_to_string("src/kernel.mjs").expect("read src/kernel.mjs");
    let declarations = ForeignModuleRegistry::new(vec![
        ForeignModuleDeclaration::new(
            "dd:host",
            vec![
                ForeignFunctionDeclaration::new(
                    "call_sync",
                    "dd",
                    "call_sync",
                    vec![ForeignValueType::Number, ForeignValueType::Bytes],
                    ForeignResultType::Bytes,
                )
                .expect("foreign decl call_sync"),
                ForeignFunctionDeclaration::new(
                    "start_async",
                    "dd",
                    "start_async",
                    vec![ForeignValueType::Number, ForeignValueType::Bytes],
                    ForeignResultType::Bytes,
                )
                .expect("foreign decl start_async"),
                ForeignFunctionDeclaration::new(
                    "load_module",
                    "dd",
                    "load_module",
                    vec![
                        ForeignValueType::StringUtf16,
                        ForeignValueType::StringUtf16,
                        ForeignValueType::StringUtf16,
                    ],
                    ForeignResultType::Bytes,
                )
                .expect("foreign decl load_module"),
                ForeignFunctionDeclaration::new(
                    "wasm_op",
                    "dd",
                    "wasm_op",
                    vec![ForeignValueType::Number, ForeignValueType::Bytes],
                    ForeignResultType::Bytes,
                )
                .expect("foreign decl wasm_op"),
                ForeignFunctionDeclaration::new(
                    "inspector_op",
                    "dd",
                    "inspector_op",
                    vec![ForeignValueType::Number, ForeignValueType::Bytes],
                    ForeignResultType::Bytes,
                )
                .expect("foreign decl inspector_op"),
            ],
        )
        .expect("foreign module dd:host"),
    ])
    .expect("foreign registry");

    let mut config = wasmtime::Config::new();
    configure_wasm_runtime(
        &mut config,
        NativeCompilationPolicy::default(),
        scriptw_engine::CompilationJobs::current(),
    );
    config.cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize);
    let engine = wasmtime::Engine::new(&config).expect("wtime build engine init");

    let compiler = ScriptwCompiler::new(RealmBuilder::new().build());
    let artifact = compiler
        .compile_and_emit_module_native_host_cached(
            &kernel_src,
            CompileOptions {
                foreign_modules: declarations,
                ..CompileOptions::default()
            },
        )
        .expect("compile dd_v8 kernel.mjs for precompilation");
    let bytes = EmbeddedRuntime::precompile_native_host_program(&engine, &artifact)
        .expect("precompile dd_v8 kernel.mjs native program");
    fs::write(&out_path, bytes).expect("write precompiled dd_v8 kernel image");
}
