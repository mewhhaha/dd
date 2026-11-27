//! `JsRuntime`, `JsValue`, `RuntimeHandle`, and `RuntimeOptions` backed by
//! Scriptw + `wtime` x64 AOT (`wtime-aot-host`).

use crate::error::Error;
use crate::inspector::{InspectorCore, InspectorHandle, PauseStep, PendingEvalRequest};
use crate::instrument;
use crate::modules::{
    self, CompiledPayload, ModuleCode, ModuleId, ModuleLoader, ModuleMap, ModuleType, NoModules,
};
use crate::ops::{OpDecl, OpError, OpKind, RawBuffer};
use crate::platform;
use crate::serde_v8::{
    self, WireValue, decode_wire_args, encode_err_envelope, encode_ok_envelope,
    encode_pending_envelope,
};
use crate::state::OpState;
use crate::wasm_api::WasmHostState;
use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::task::AtomicWaker;
use scriptw_engine::{
    CompileOptions, EmbeddedEvaluation, EmbeddedInstance, EmbeddedRuntime,
    Engine as ScriptwCompiler, ForeignValue, JsCompletion, JsPromiseState,
    JsValue as ScriptwJsValue, NativeCompilationPolicy, RealmBuilder, WasmStoreState,
    configure_wasm_runtime,
};
use scriptw_ir::foreign::{
    ForeignFunctionDeclaration, ForeignModuleDeclaration, ForeignModuleRegistry, ForeignResultType,
    ForeignValueType,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

const MAX_OPS_PER_TICK: usize = 1024;
const KERNEL_MJS: &str = include_str!("kernel.mjs");
const PRECOMPILED_KERNEL_BYTES: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/dd_v8_kernel.wtmn"));
const SNAPSHOT_MAGIC: &[u8; 8] = b"DDSW0001";

#[derive(Default)]
pub struct RuntimeOptions {
    /// The ops, in the order their functions are created. A runtime restored
    /// from a snapshot must list the same ops in the same order.
    pub ops: Vec<OpDecl>,
    pub module_loader: Option<Rc<dyn ModuleLoader>>,
    pub startup_snapshot: Option<&'static [u8]>,
    /// The GC heap limit in bytes; 0 keeps the default.
    pub max_heap_bytes: usize,
    /// The most one op argument may copy into Rust; 0 keeps
    /// [`serde_v8::DEFAULT_MAX_OP_ARGUMENT_BYTES`].
    pub max_op_argument_bytes: usize,
    /// Pre-registered extension scripts (`(specifier, source)`) AOT-compiled
    /// directly into the snapshot bundle (eliminates runtime `eval` of web APIs).
    pub ext_scripts: &'static [(&'static str, &'static str)],
}

/// An opaque handle to a live JavaScript value in a [`JsRuntime`], or a
/// recorded slot during snapshot creation (or both when a snapshot builder also
/// runs live).
#[derive(Clone, Debug)]
pub struct JsValue {
    live: Option<ScriptwJsValue>,
    slot: Option<usize>,
}

impl JsValue {
    fn live(v: ScriptwJsValue) -> Self {
        Self {
            live: Some(v),
            slot: None,
        }
    }

    fn slot(idx: usize) -> Self {
        Self {
            live: None,
            slot: Some(idx),
        }
    }

    fn live_and_slot(v: Option<ScriptwJsValue>, slot: Option<usize>) -> Self {
        Self { live: v, slot }
    }

    pub fn is_undefined(&self) -> bool {
        self.live.as_ref().is_some_and(ScriptwJsValue::is_undefined)
    }

    pub fn as_number(&self) -> Option<f64> {
        self.live.as_ref().and_then(ScriptwJsValue::as_number)
    }

    pub fn as_boolean(&self) -> Option<bool> {
        self.live.as_ref().and_then(ScriptwJsValue::as_boolean)
    }

    fn as_live(&self) -> Result<&ScriptwJsValue, Error> {
        self.live
            .as_ref()
            .ok_or_else(|| Error::new("cannot use snapshot slot in live runtime"))
    }
}

/// Stops a runtime's JavaScript from any thread.
#[derive(Clone)]
pub struct RuntimeHandle {
    interrupt: Option<wasmtime::InterruptHandle>,
    terminated: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    inspector: Arc<OnceLock<InspectorHandle>>,
}

impl RuntimeHandle {
    /// Terminates the JavaScript running on the runtime's thread. Returns false
    /// once the runtime is gone.
    pub fn terminate_execution(&self) -> bool {
        if !self.alive.load(Ordering::Acquire) {
            return false;
        }
        self.terminated.store(true, Ordering::Release);
        if let Some(ref handle) = self.interrupt {
            handle.interrupt();
        }
        if let Some(inspector) = self.inspector.get() {
            inspector.wake_for_termination();
        }
        true
    }
}

impl std::fmt::Debug for RuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeHandle").finish_non_exhaustive()
    }
}

// =========================================================================
// Shared Wasmtime Engine, Foreign Registry, and Snapshot Store
// =========================================================================

static SHARED_ENGINE: OnceLock<Result<wasmtime::Engine, String>> = OnceLock::new();
static BASE_KERNEL: OnceLock<Result<Arc<EmbeddedRuntime>, String>> = OnceLock::new();
static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);
static SNAPSHOTS: LazyLock<Mutex<HashMap<u64, Arc<CompiledSnapshot>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct CompiledSnapshot {
    runtime: Arc<EmbeddedRuntime>,
    op_names: Vec<String>,
}

fn shared_engine() -> Result<&'static wasmtime::Engine, Error> {
    platform::init();
    SHARED_ENGINE
        .get_or_init(|| {
            let mut config = wasmtime::Config::new();
            configure_wasm_runtime(
                &mut config,
                NativeCompilationPolicy::default(),
                scriptw_engine::CompilationJobs::current(),
            );
            // Match `scriptw-runtime-image`'s embedded Cargo `NativeHost` runtime `R`
            // (`SizeOptimized` / `OptLevel::SpeedAndSize`) so embedded `R` and build-time
            // precompiled `P` images load directly without runtime recompilation.
            config.cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize);
            // Allow unbounded runtimes (e.g. >2 GiB typed-array views plus wire copies)
            // to use the full 8 GiB virtual semi-space reserved by wtime-aot-host.
            config.gc_heap_reservation(8u64 << 30);
            wasmtime::Engine::new(&config).map_err(|e| format!("wtime engine init failed: {e}"))
        })
        .as_ref()
        .map_err(|e| Error::new(e.clone()))
}

fn dd_foreign_registry() -> Result<ForeignModuleRegistry, Error> {
    ForeignModuleRegistry::new(vec![
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
                .map_err(|e| Error::new(format!("foreign decl call_sync: {e}")))?,
                ForeignFunctionDeclaration::new(
                    "start_async",
                    "dd",
                    "start_async",
                    vec![ForeignValueType::Number, ForeignValueType::Bytes],
                    ForeignResultType::Bytes,
                )
                .map_err(|e| Error::new(format!("foreign decl start_async: {e}")))?,
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
                .map_err(|e| Error::new(format!("foreign decl load_module: {e}")))?,
                ForeignFunctionDeclaration::new(
                    "wasm_op",
                    "dd",
                    "wasm_op",
                    vec![ForeignValueType::Number, ForeignValueType::Bytes],
                    ForeignResultType::Bytes,
                )
                .map_err(|e| Error::new(format!("foreign decl wasm_op: {e}")))?,
                ForeignFunctionDeclaration::new(
                    "inspector_op",
                    "dd",
                    "inspector_op",
                    vec![ForeignValueType::Number, ForeignValueType::Bytes],
                    ForeignResultType::Bytes,
                )
                .map_err(|e| Error::new(format!("foreign decl inspector_op: {e}")))?,
            ],
        )
        .map_err(|e| Error::new(format!("foreign module dd:host: {e}")))?,
    ])
    .map_err(|e| Error::new(format!("foreign registry: {e}")))
}

static PRECOMPILED_BUNDLES: LazyLock<Mutex<HashMap<[u8; 32], &'static [u8]>>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        if !PRECOMPILED_KERNEL_BYTES.is_empty() {
            map.insert(bundle_source_sha256(KERNEL_MJS), PRECOMPILED_KERNEL_BYTES);
        }
        Mutex::new(map)
    });

/// Pure-Rust FIPS 180-4 SHA-256 digest of a generated bundle source string.
pub fn bundle_source_sha256(source: &str) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bytes = source.as_bytes();
    let bit_len = (bytes.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity((bytes.len() + 9).div_ceil(64) * 64);
    padded.extend_from_slice(bytes);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, word) in chunk.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(*word);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// Registers a build-time precompiled native program image for a snapshot bundle
/// produced by [`precompile_snapshot_bundle`].
pub fn register_precompiled_bundle(source_sha256: [u8; 32], serialized_program: &'static [u8]) {
    if serialized_program.is_empty() {
        return;
    }
    PRECOMPILED_BUNDLES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(source_sha256, serialized_program);
}

/// Returns whether a non-empty build-time precompiled bundle is registered for `source_sha256`.
pub fn has_precompiled_bundle(source_sha256: [u8; 32]) -> bool {
    PRECOMPILED_BUNDLES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(&source_sha256)
}

/// Compiles a snapshot bundle source string into serialized native `wtime-aot`
/// program bytes (for use in `build.rs`) and returns `(source_sha256, bytes)`.
pub fn precompile_snapshot_bundle(source: &str) -> Result<([u8; 32], Vec<u8>), Error> {
    let engine = shared_engine()?;
    let declarations = dd_foreign_registry()?;
    let compiler = ScriptwCompiler::new(RealmBuilder::new().build());
    let artifact = compiler
        .compile_and_emit_module_native_host_cached(
            source,
            CompileOptions {
                foreign_modules: declarations,
                ..CompileOptions::default()
            },
        )
        .map_err(|e| Error::new(format!("scriptw precompile emit failed: {e}")))?;
    let bytes =
        EmbeddedRuntime::precompile_native_host_program(engine, &artifact).map_err(|e| {
            Error::new(format!(
                "wtime-aot precompile_native_host_program failed: {e}"
            ))
        })?;
    Ok((bundle_source_sha256(source), bytes))
}

fn compile_embedded_module(source: &str) -> Result<Arc<EmbeddedRuntime>, Error> {
    let engine = shared_engine()?;
    let declarations = dd_foreign_registry()?;
    let digest = bundle_source_sha256(source);
    let precompiled = PRECOMPILED_BUNDLES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&digest)
        .copied();
    if let Some(bytes) = precompiled {
        // SAFETY: `bytes` were produced by `precompile_snapshot_bundle` / `dd_v8/build.rs`
        // for this exact compiler/target build and matched by SHA-256 of `source`.
        if let Ok(embedded) = unsafe {
            EmbeddedRuntime::from_precompiled_native_host_program(
                engine,
                bytes,
                declarations.clone(),
            )
        } {
            return Ok(Arc::new(embedded));
        }
    }
    let compiler = ScriptwCompiler::new(RealmBuilder::new().build());
    let artifact = compiler
        .compile_and_emit_module_native_host_cached(
            source,
            CompileOptions {
                foreign_modules: declarations,
                ..CompileOptions::default()
            },
        )
        .map_err(|e| {
            Error::new(format!(
                "scriptw compile_and_emit_module_native_host_cached failed: {e}"
            ))
        })?;
    let embedded = EmbeddedRuntime::compile_native_host(engine, &artifact)
        .map_err(|e| Error::new(format!("wtime-aot compile_native_host failed: {e}")))?;
    Ok(Arc::new(embedded))
}

fn base_kernel_runtime() -> Result<Arc<EmbeddedRuntime>, Error> {
    BASE_KERNEL
        .get_or_init(|| compile_embedded_module(KERNEL_MJS).map_err(|e| e.message().to_string()))
        .as_ref()
        .map(Arc::clone)
        .map_err(|e| Error::new(e.clone()))
}

// =========================================================================
// Per-Runtime State & Pending Async Ops
// =========================================================================

struct PendingOp {
    id: u32,
    future: Pin<Box<dyn Future<Output = Result<WireValue, OpError>>>>,
}

impl Future for PendingOp {
    type Output = (u32, Result<WireValue, OpError>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = std::task::ready!(self.future.as_mut().poll(cx));
        Poll::Ready((self.id, res))
    }
}

struct RuntimeState {
    op_state: Rc<RefCell<OpState>>,
    max_op_argument_bytes: usize,
    ops: Vec<OpDecl>,
    pending_ops: RefCell<FuturesUnordered<PendingOp>>,
    next_pending_id: RefCell<u32>,
    waker: Arc<AtomicWaker>,
    loader: Rc<dyn ModuleLoader>,
    modules: RefCell<ModuleMap>,
    wasm: RefCell<WasmHostState>,
    inspector: Arc<OnceLock<InspectorHandle>>,
    last_wire_out: RefCell<Vec<u8>>,
}

impl RuntimeState {
    fn has_pending_work(&self) -> bool {
        !self.pending_ops.borrow().is_empty()
    }

    fn alloc_pending_id(&self) -> u32 {
        let mut next = self.next_pending_id.borrow_mut();
        let id = *next;
        *next = next.wrapping_add(1).max(1);
        id
    }
}

/// Raw non-owning pointer stored inside `WasmStoreState::user_data` while the
/// owning `JsRuntime` is alive. `JsRuntime` is `!Send` and drops `store` BEFORE
/// `state`, so callbacks only ever dereference this on a synchronous call stack
/// owned exclusively by `&mut JsRuntime`.
struct StatePtr(*const RuntimeState);
// SAFETY: Accessed exclusively during synchronous calls inside `&mut JsRuntime`.
unsafe impl Send for StatePtr {}

struct KernelExports {
    init_ops: ScriptwJsValue,
    eval_script: ScriptwJsValue,
    eval_with_ops: ScriptwJsValue,
    eval_function: ScriptwJsValue,
    call_function: ScriptwJsValue,
    call_u32: ScriptwJsValue,
    call_f64_2: ScriptwJsValue,
    get_prop: ScriptwJsValue,
    to_string: ScriptwJsValue,
    describe_error: ScriptwJsValue,
    set_internals: ScriptwJsValue,
    get_internals: ScriptwJsValue,
    set_allow_codegen: ScriptwJsValue,
    set_inspector_enabled: ScriptwJsValue,
    dbg_eval_global: ScriptwJsValue,
    wire_to_js: ScriptwJsValue,
    js_to_wire: ScriptwJsValue,
    settle_async: ScriptwJsValue,
    take_unhandled_rejection: ScriptwJsValue,
    register_module: ScriptwJsValue,
    eval_module: ScriptwJsValue,
    module_namespace: ScriptwJsValue,
    make_array: ScriptwJsValue,
    push_array: ScriptwJsValue,
}

struct LiveVm {
    // Dropped BEFORE `RuntimeState` (guaranteed by field order in `JsRuntime`).
    store: wasmtime::Store<WasmStoreState>,
    instance: EmbeddedInstance,
    exports: KernelExports,
}

// =========================================================================
// Snapshot Recording
// =========================================================================

enum SnapshotStep {
    ExecuteScript {
        _name: String,
        source: String,
        slot: usize,
    },
    ExecuteWithOps {
        _name: String,
        source: String,
        slot: usize,
    },
    ExecuteFunction {
        _name: String,
        source: String,
        params: Vec<String>,
        arg_slots: Vec<usize>,
        slot: usize,
    },
    SetInternals {
        slot: usize,
    },
}

struct SnapshotRecorder {
    ext_scripts: Vec<(String, String)>,
    steps: Vec<SnapshotStep>,
    next_slot: usize,
    internals_slot: Option<usize>,
}

impl SnapshotRecorder {
    fn new(ext_scripts: &[(&str, &str)]) -> Self {
        Self {
            ext_scripts: ext_scripts
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            steps: Vec::new(),
            next_slot: 0,
            internals_slot: None,
        }
    }

    fn alloc_slot(&mut self) -> usize {
        let s = self.next_slot;
        self.next_slot += 1;
        s
    }

    fn generate_bundle_mjs(&self) -> String {
        let mut out = String::with_capacity(KERNEL_MJS.len() + 800 * 1024);
        out.push_str(KERNEL_MJS);
        out.push_str("\n// ---- AOT-compiled extension scripts ----\n");
        for (i, (spec, src)) in self.ext_scripts.iter().enumerate() {
            out.push_str(&format!(
                "function __dd_ext_{i}(__bootstrap) {{\n{src}\n}}\n"
            ));
            out.push_str(&format!(
                "__dd_register_ext_factory({}, __dd_ext_{i});\n",
                js_string_literal(spec)
            ));
        }

        out.push_str("\n// ---- AOT-compiled snapshot bootstrap steps ----\n");
        for (idx, step) in self.steps.iter().enumerate() {
            match step {
                SnapshotStep::ExecuteScript { source, .. } => {
                    out.push_str(&format!(
                        "function __dd_snap_step_{idx}() {{\n{source}\n}}\n"
                    ));
                }
                SnapshotStep::ExecuteWithOps { source, .. } => {
                    out.push_str(&format!(
                        "function __dd_snap_step_{idx}(ops) {{\n{source}\n}}\n"
                    ));
                }
                SnapshotStep::ExecuteFunction { source, params, .. } => {
                    let plist = params.join(", ");
                    out.push_str(&format!(
                        "function __dd_snap_step_{idx}({plist}) {{\n{source}\n}}\n"
                    ));
                }
                SnapshotStep::SetInternals { .. } => {}
            }
        }

        out.push_str(
            "\nexport function __dd_run_snapshot(opSpecsJson, maxArgBytes, asyncContextFn) {\n\
             \x20 const __dd_ops = __dd_init_ops(opSpecsJson, maxArgBytes, asyncContextFn);\n\
             \x20 const __dd_slots = [];\n",
        );
        for (idx, step) in self.steps.iter().enumerate() {
            match step {
                SnapshotStep::ExecuteScript { slot, .. } => {
                    out.push_str(&format!("  __dd_slots[{slot}] = __dd_snap_step_{idx}();\n"));
                }
                SnapshotStep::ExecuteWithOps { slot, .. } => {
                    out.push_str(&format!(
                        "  __dd_slots[{slot}] = __dd_snap_step_{idx}(__dd_ops);\n"
                    ));
                }
                SnapshotStep::ExecuteFunction {
                    arg_slots, slot, ..
                } => {
                    let args: Vec<String> = arg_slots
                        .iter()
                        .map(|s| format!("__dd_slots[{s}]"))
                        .collect();
                    out.push_str(&format!(
                        "  __dd_slots[{slot}] = __dd_snap_step_{idx}({});\n",
                        args.join(", ")
                    ));
                }
                SnapshotStep::SetInternals { slot } => {
                    out.push_str(&format!("  __dd_set_internals(__dd_slots[{slot}]);\n"));
                }
            }
        }
        out.push_str("}\n");
        out
    }
}

fn js_string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn build_op_specs_json(ops: &[OpDecl]) -> String {
    let mut out = String::from("[");
    for (i, op) in ops.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let kind = if op.is_builtin_js() {
            2
        } else if op.is_raw_buffer() {
            3
        } else if op.is_async() {
            1
        } else {
            0
        };
        out.push('[');
        out.push_str(&js_string_literal(op.name));
        out.push(',');
        out.push_str(&kind.to_string());
        out.push(']');
    }
    out.push(']');
    out
}

// =========================================================================
// `JsRuntime`
// =========================================================================

pub struct JsRuntime {
    /// Dropped BEFORE `state` so `StatePtr` inside `store` is never dangling.
    vm: Option<LiveVm>,
    recorder: Option<SnapshotRecorder>,
    state: Rc<RuntimeState>,
    terminated: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    interrupt_handle: Option<wasmtime::InterruptHandle>,
    inspector_handle: Arc<OnceLock<InspectorHandle>>,
    max_heap_bytes: usize,
}

impl JsRuntime {
    pub fn new(options: RuntimeOptions) -> Result<Self, Error> {
        Self::build(options, false)
    }

    /// A runtime whose bootstrap steps can be AOT-compiled into a reusable
    /// native x64 snapshot with [`JsRuntime::snapshot`].
    pub fn new_for_snapshot(options: RuntimeOptions) -> Result<Self, Error> {
        Self::build(options, true)
    }

    fn build(options: RuntimeOptions, will_snapshot: bool) -> Result<Self, Error> {
        platform::init();
        let RuntimeOptions {
            ops,
            module_loader,
            startup_snapshot,
            max_heap_bytes,
            max_op_argument_bytes,
            ext_scripts,
        } = options;

        let waker = Arc::new(AtomicWaker::new());
        let max_arg_bytes = match max_op_argument_bytes {
            0 => serde_v8::DEFAULT_MAX_OP_ARGUMENT_BYTES,
            n => n,
        };
        let terminated = Arc::new(AtomicBool::new(false));
        let alive = Arc::new(AtomicBool::new(true));
        let inspector_handle = Arc::new(OnceLock::new());
        let state = Rc::new(RuntimeState {
            op_state: Rc::new(RefCell::new(OpState::new(Arc::clone(&waker)))),
            max_op_argument_bytes: max_arg_bytes,
            ops,
            pending_ops: RefCell::new(FuturesUnordered::new()),
            next_pending_id: RefCell::new(1),
            waker,
            loader: module_loader.unwrap_or_else(|| Rc::new(NoModules)),
            modules: RefCell::new(ModuleMap::default()),
            wasm: RefCell::new(WasmHostState::new()),
            inspector: Arc::clone(&inspector_handle),
            last_wire_out: RefCell::new(Vec::new()),
        });

        if will_snapshot {
            let (vm, interrupt) = if ext_scripts.is_empty() {
                let embedded = base_kernel_runtime()?;
                let (mut vm, interrupt) = instantiate_vm(&embedded, &state, max_heap_bytes, false)?;
                // Mirror V8's SnapshotCreator behavior where globalThis.queueMicrotask
                // is absent during snapshot construction (ops.op_queue_microtask is used).
                let n = str_val(&mut vm, "<snapshot-init>")?;
                let s = str_val(&mut vm, "delete globalThis.queueMicrotask;")?;
                let eval_fn = vm.exports.eval_script.clone();
                let _ = call_js(&mut vm, &terminated, &eval_fn, &[n, s]);
                (Some(vm), Some(interrupt))
            } else {
                (None, None)
            };
            return Ok(Self {
                vm,
                recorder: Some(SnapshotRecorder::new(ext_scripts)),
                state,
                terminated,
                alive,
                interrupt_handle: interrupt,
                inspector_handle,
                max_heap_bytes,
            });
        }

        let (embedded, run_snapshot_export) = if let Some(blob) = startup_snapshot {
            let snap = lookup_snapshot(blob, &state.ops)?;
            (Arc::clone(&snap.runtime), true)
        } else if !ext_scripts.is_empty() {
            let rec = SnapshotRecorder::new(ext_scripts);
            let src = rec.generate_bundle_mjs();
            (compile_embedded_module(&src)?, false)
        } else {
            (base_kernel_runtime()?, false)
        };

        let (vm, interrupt) =
            instantiate_vm(&embedded, &state, max_heap_bytes, run_snapshot_export)?;

        Ok(Self {
            vm: Some(vm),
            recorder: None,
            state,
            terminated,
            alive,
            interrupt_handle: Some(interrupt),
            inspector_handle,
            max_heap_bytes,
        })
    }

    pub fn op_state(&self) -> Rc<RefCell<OpState>> {
        Rc::clone(&self.state.op_state)
    }

    /// A handle that terminates this runtime's JavaScript from any thread.
    pub fn handle(&mut self) -> RuntimeHandle {
        RuntimeHandle {
            interrupt: self.interrupt_handle.clone(),
            terminated: Arc::clone(&self.terminated),
            alive: Arc::clone(&self.alive),
            inspector: Arc::clone(&self.inspector_handle),
        }
    }

    /// Lets the runtime run JavaScript again after termination.
    pub fn cancel_termination(&mut self) {
        self.terminated.store(false, Ordering::Release);
        if let Some(ref handle) = self.interrupt_handle {
            handle.cancel();
        }
        if let Some(ref mut vm) = self.vm {
            vm.instance.recover_after_interrupt(&mut vm.store);
        }
    }

    pub fn enable_inspector(&mut self, name: &str) -> Result<InspectorHandle, Error> {
        if self.recorder.is_some() {
            return Err(Error::new("a snapshot runtime cannot have an inspector"));
        }
        let handle = self
            .inspector_handle
            .get_or_init(|| {
                let core = InspectorCore::new(
                    name.to_string(),
                    Arc::clone(&self.state.waker),
                    Arc::clone(&self.terminated),
                );
                InspectorHandle::new(core)
            })
            .clone();
        if let Some(ref mut vm) = self.vm {
            let arg = vm
                .instance
                .value(&mut vm.store, ForeignValue::Number(1.0))
                .map_err(|e| Error::new(e.to_string()))?;
            let _ = vm.instance.call(
                &mut vm.store,
                &vm.exports.set_inspector_enabled.clone(),
                None,
                &[arg],
            );
        }
        Ok(handle)
    }

    pub fn wait_for_debugger(&mut self) {
        if let Some(handle) = self.inspector_handle.get() {
            handle.core().wait_for_debugger();
        }
    }

    /// Controls whether user code may invoke `eval` or `new Function`.
    pub fn set_allow_code_generation(&mut self, allow: bool) {
        if let Some(ref mut vm) = self.vm {
            let arg = vm.instance.value(
                &mut vm.store,
                ForeignValue::Number(if allow { 1.0 } else { 0.0 }),
            );
            if let Ok(arg) = arg {
                let _ =
                    vm.instance
                        .call(&mut vm.store, &vm.exports.set_allow_codegen, None, &[arg]);
            }
        }
    }

    /// Runs a classic script and returns its completion value.
    pub fn execute_script(&mut self, name: &str, source: &str) -> Result<JsValue, Error> {
        let live = if let Some(vm) = self.vm.as_mut() {
            if self.terminated.load(Ordering::Acquire) {
                return Err(Error::terminated());
            }
            let rewritten = modules::rewrite_script_dynamic_import(source);
            let (base_src, has_dyn) = match &rewritten {
                Some(s) => (s.as_str(), true),
                None => (source, false),
            };
            let instrumented = self.inspector_handle.get().map(|handle| {
                let sid = handle.core().register_script(name, source);
                instrument::instrument_js(sid, base_src)
            });
            let effective_src = instrumented.as_deref().unwrap_or(base_src);
            let name_v = str_val(vm, name)?;
            let src_v = str_val(vm, effective_src)?;
            let has_dyn_v = vm
                .instance
                .value(
                    &mut vm.store,
                    ForeignValue::Number(if has_dyn { 1.0 } else { 0.0 }),
                )
                .map_err(|e| Error::new(e.to_string()))?;
            let eval_fn = vm.exports.eval_script.clone();
            Some(call_js(
                vm,
                &self.terminated,
                &eval_fn,
                &[name_v, src_v, has_dyn_v],
            )?)
        } else {
            None
        };
        let slot = if let Some(ref mut rec) = self.recorder {
            let slot = rec.alloc_slot();
            rec.steps.push(SnapshotStep::ExecuteScript {
                _name: name.to_string(),
                source: source.to_string(),
                slot,
            });
            Some(slot)
        } else {
            None
        };
        Ok(JsValue::live_and_slot(live, slot))
    }

    /// Runs `source` as the body of a function whose one parameter is `ops`.
    pub fn execute_with_ops(&mut self, name: &str, source: &str) -> Result<JsValue, Error> {
        let live = if let Some(vm) = self.vm.as_mut() {
            if self.terminated.load(Ordering::Acquire) {
                return Err(Error::terminated());
            }
            let name_v = str_val(vm, name)?;
            let src_v = str_val(vm, source)?;
            Some(call_js(
                vm,
                &self.terminated,
                &vm.exports.eval_with_ops.clone(),
                &[name_v, src_v],
            )?)
        } else {
            None
        };
        let slot = if let Some(ref mut rec) = self.recorder {
            let slot = rec.alloc_slot();
            rec.steps.push(SnapshotStep::ExecuteWithOps {
                _name: name.to_string(),
                source: source.to_string(),
                slot,
            });
            Some(slot)
        } else {
            None
        };
        Ok(JsValue::live_and_slot(live, slot))
    }

    /// Runs `source` as the body of a function with `params`, called with `args`.
    pub fn execute_function(
        &mut self,
        name: &str,
        source: &str,
        params: &[&str],
        args: &[JsValue],
    ) -> Result<JsValue, Error> {
        let live = if let Some(vm) = self.vm.as_mut() {
            if self.terminated.load(Ordering::Acquire) {
                return Err(Error::terminated());
            }
            let name_v = str_val(vm, name)?;
            let src_v = str_val(vm, source)?;
            let mut params_json = String::from("[");
            for (i, p) in params.iter().enumerate() {
                if i > 0 {
                    params_json.push(',');
                }
                params_json.push_str(&js_string_literal(p));
            }
            params_json.push(']');
            let params_v = str_val(vm, &params_json)?;
            let args_arr = make_js_array(vm, &self.terminated, args)?;
            Some(call_js(
                vm,
                &self.terminated,
                &vm.exports.eval_function.clone(),
                &[name_v, src_v, params_v, args_arr],
            )?)
        } else {
            None
        };
        let slot = if let Some(ref mut rec) = self.recorder {
            let mut arg_slots = Vec::with_capacity(args.len());
            for a in args {
                match a.slot {
                    Some(s) => arg_slots.push(s),
                    None => {
                        return Err(Error::new(
                            "live value without snapshot slot passed to snapshot recorder",
                        ));
                    }
                }
            }
            let slot = rec.alloc_slot();
            rec.steps.push(SnapshotStep::ExecuteFunction {
                _name: name.to_string(),
                source: source.to_string(),
                params: params.iter().map(|p| (*p).to_string()).collect(),
                arg_slots,
                slot,
            });
            Some(slot)
        } else {
            None
        };
        Ok(JsValue::live_and_slot(live, slot))
    }

    /// Calls `function` with an undefined receiver and returns its result.
    pub fn call_function(
        &mut self,
        function: &JsValue,
        args: &[JsValue],
    ) -> Result<JsValue, Error> {
        self.check_terminated()?;
        let fn_v = function.as_live()?.clone();
        let vm = self.vm.as_mut().expect("live vm");
        let args_arr = make_js_array(vm, &self.terminated, args)?;
        let val = call_js(
            vm,
            &self.terminated,
            &vm.exports.call_function.clone(),
            &[fn_v, args_arr],
        )?;
        Ok(JsValue::live(val))
    }

    /// Fast path calling a 1-arg `u32` entrypoint (used by `dd` worker dispatch).
    pub fn call_function_u32(&mut self, function: &JsValue, arg: u32) -> Result<JsValue, Error> {
        self.check_terminated()?;
        let fn_v = function.as_live()?.clone();
        let vm = self.vm.as_mut().expect("live vm");
        let arg_v = vm
            .instance
            .value(&mut vm.store, ForeignValue::Number(f64::from(arg)))
            .map_err(|e| Error::new(e.to_string()))?;
        let val = call_js(
            vm,
            &self.terminated,
            &vm.exports.call_u32.clone(),
            &[fn_v, arg_v],
        )?;
        Ok(JsValue::live(val))
    }

    /// Fast path calling a 2-arg `f64` entrypoint (used by `sync_isolate_clock`).
    pub fn call_function_f64_2(
        &mut self,
        function: &JsValue,
        a: f64,
        b: f64,
    ) -> Result<JsValue, Error> {
        self.check_terminated()?;
        let fn_v = function.as_live()?.clone();
        let vm = self.vm.as_mut().expect("live vm");
        let a_v = vm
            .instance
            .value(&mut vm.store, ForeignValue::Number(a))
            .map_err(|e| Error::new(e.to_string()))?;
        let b_v = vm
            .instance
            .value(&mut vm.store, ForeignValue::Number(b))
            .map_err(|e| Error::new(e.to_string()))?;
        let val = call_js(
            vm,
            &self.terminated,
            &vm.exports.call_f64_2.clone(),
            &[fn_v, a_v, b_v],
        )?;
        Ok(JsValue::live(val))
    }

    /// Reads property `key` from `object`.
    pub fn get_property(&mut self, object: &JsValue, key: &str) -> Result<JsValue, Error> {
        self.check_terminated()?;
        let obj_v = object.as_live()?.clone();
        let vm = self.vm.as_mut().expect("live vm");
        let key_v = str_val(vm, key)?;
        let val = call_js(
            vm,
            &self.terminated,
            &vm.exports.get_prop.clone(),
            &[obj_v, key_v],
        )?;
        Ok(JsValue::live(val))
    }

    /// Converts a JS value to a Rust `String` using `String(value)`.
    pub fn value_to_string(&mut self, value: &JsValue) -> Result<String, Error> {
        self.check_terminated()?;
        let val = value.as_live()?.clone();
        let vm = self.vm.as_mut().expect("live vm");
        let s = call_js(vm, &self.terminated, &vm.exports.to_string.clone(), &[val])?;
        js_value_to_rust_string(&mut vm.store, &s)
    }

    /// Serializes a Rust value into a live JavaScript value via `serde_v8`.
    pub fn to_js<T: serde::Serialize>(&mut self, value: &T) -> Result<JsValue, Error> {
        self.check_terminated()?;
        let wire = serde_v8::to_wire(value).map_err(|e| Error::new(e.to_string()))?;
        let env = encode_ok_envelope(&wire);
        let vm = self.vm.as_mut().expect("live vm");
        let bytes_v = vm
            .instance
            .value(&mut vm.store, ForeignValue::Bytes(env))
            .map_err(|e| Error::new(e.to_string()))?;
        let val = call_js(
            vm,
            &self.terminated,
            &vm.exports.wire_to_js.clone(),
            &[bytes_v],
        )?;
        Ok(JsValue::live(val))
    }

    /// Deserializes a live JavaScript value into a Rust value via `serde_v8`.
    pub fn from_js<T: serde::de::DeserializeOwned>(&mut self, value: &JsValue) -> Result<T, Error> {
        self.check_terminated()?;
        let live = value.as_live()?.clone();
        self.state.last_wire_out.borrow_mut().clear();
        let vm = self.vm.as_mut().expect("live vm");
        let _ = call_js(
            vm,
            &self.terminated,
            &vm.exports.js_to_wire.clone(),
            &[live],
        )?;
        let raw = std::mem::take(&mut *self.state.last_wire_out.borrow_mut());
        let args = decode_wire_args(&raw, self.state.max_op_argument_bytes)
            .map_err(|e| Error::new(e.to_string()))?;
        let first = args.first().unwrap_or(&WireValue::Undefined);
        serde_v8::from_wire(first).map_err(|e| Error::new(e.to_string()))
    }

    /// Creates a live JavaScript `Uint8Array` containing `bytes`.
    pub fn uint8_array(&mut self, bytes: &[u8]) -> Result<JsValue, Error> {
        self.check_terminated()?;
        let vm = self.vm.as_mut().expect("live vm");
        let val = vm
            .instance
            .value(&mut vm.store, ForeignValue::Bytes(bytes.to_vec()))
            .map_err(|e| Error::new(e.to_string()))?;
        Ok(JsValue::live(val))
    }

    pub fn set_internals(&mut self, value: JsValue) {
        if let Some(ref mut rec) = self.recorder
            && let Some(slot) = value.slot
        {
            rec.internals_slot = Some(slot);
            rec.steps.push(SnapshotStep::SetInternals { slot });
        }
        if let Some(live) = value.live.as_ref()
            && let Some(ref mut vm) = self.vm
        {
            let _ = vm.instance.call(
                &mut vm.store,
                &vm.exports.set_internals.clone(),
                None,
                std::slice::from_ref(live),
            );
        }
    }

    pub fn internals(&mut self) -> Option<JsValue> {
        let slot = self.recorder.as_ref().and_then(|r| r.internals_slot);
        if let Some(ref mut vm) = self.vm {
            let val = call_js(vm, &self.terminated, &vm.exports.get_internals.clone(), &[]).ok()?;
            if val.is_undefined() {
                None
            } else {
                Some(JsValue::live_and_slot(Some(val), slot))
            }
        } else {
            slot.map(JsValue::slot)
        }
    }

    pub fn load_main_module(
        &mut self,
        name: &str,
        code: Option<String>,
    ) -> Result<ModuleId, Error> {
        self.load_module(name, code, true)
    }

    pub fn load_side_module(
        &mut self,
        name: &str,
        code: Option<String>,
    ) -> Result<ModuleId, Error> {
        self.load_module(name, code, false)
    }

    fn load_module(
        &mut self,
        name: &str,
        code: Option<String>,
        main: bool,
    ) -> Result<ModuleId, Error> {
        self.check_terminated()?;
        let (root_id, newly_added) = {
            let mut mods = self.state.modules.borrow_mut();
            let insp = self.inspector_handle.get().map(|h| h.core().as_ref());
            modules::load_graph(
                &mut mods,
                self.state.loader.as_ref(),
                name,
                ModuleType::JavaScript,
                code.map(ModuleCode::String),
                main,
                insp,
            )?
        };
        self.sync_registered_modules(&newly_added)?;
        Ok(root_id)
    }

    fn sync_registered_modules(&mut self, ids: &[ModuleId]) -> Result<(), Error> {
        let vm = self.vm.as_mut().expect("live vm");
        for &id in ids {
            let (name, kind, is_main, payload, deps_json) = {
                let mut mods = self.state.modules.borrow_mut();
                let rec = &mut mods.modules[id];
                if rec.registered_in_js {
                    continue;
                }
                rec.registered_in_js = true;
                (
                    rec.name.clone(),
                    rec.module_type.kind_code(),
                    rec.is_main,
                    rec.payload.clone(),
                    rec.deps_json(),
                )
            };
            let id_v = vm
                .instance
                .value(&mut vm.store, ForeignValue::Number(id as f64))
                .map_err(|e| Error::new(e.to_string()))?;
            let name_v = str_val(vm, &name)?;
            let kind_v = vm
                .instance
                .value(&mut vm.store, ForeignValue::Number(f64::from(kind)))
                .map_err(|e| Error::new(e.to_string()))?;
            let main_v = vm
                .instance
                .value(
                    &mut vm.store,
                    ForeignValue::Number(if is_main { 1.0 } else { 0.0 }),
                )
                .map_err(|e| Error::new(e.to_string()))?;
            let payload_v = match payload {
                CompiledPayload::String(s) => str_val(vm, &s)?,
                CompiledPayload::Bytes(b) => vm
                    .instance
                    .value(&mut vm.store, ForeignValue::Bytes(b))
                    .map_err(|e| Error::new(e.to_string()))?,
            };
            let deps_v = str_val(vm, &deps_json)?;
            let _ = call_js(
                vm,
                &self.terminated,
                &vm.exports.register_module.clone(),
                &[id_v, name_v, kind_v, main_v, payload_v, deps_v],
            )?;
        }
        Ok(())
    }

    /// Evaluates a loaded module, driving the event loop until top-level `await` settles.
    pub async fn evaluate_module(&mut self, id: ModuleId) -> Result<(), Error> {
        self.check_terminated()?;
        let promise = {
            let vm = self.vm.as_mut().expect("live vm");
            let id_v = vm
                .instance
                .value(&mut vm.store, ForeignValue::Number(id as f64))
                .map_err(|e| Error::new(e.to_string()))?;
            call_js(
                vm,
                &self.terminated,
                &vm.exports.eval_module.clone(),
                &[id_v],
            )?
        };
        self.settle_promise(promise, "Top-level await promise never resolved")
            .await
            .map(drop)
    }

    pub fn module_namespace(&mut self, id: ModuleId) -> Result<JsValue, Error> {
        self.check_terminated()?;
        let vm = self.vm.as_mut().expect("live vm");
        let id_v = vm
            .instance
            .value(&mut vm.store, ForeignValue::Number(id as f64))
            .map_err(|e| Error::new(e.to_string()))?;
        let ns = call_js(
            vm,
            &self.terminated,
            &vm.exports.module_namespace.clone(),
            &[id_v],
        )?;
        Ok(JsValue::live(ns))
    }

    /// Resolves `value` if it is a promise, running the event loop until it settles.
    pub async fn resolve(&mut self, value: JsValue) -> Result<JsValue, Error> {
        let live = value.as_live()?.clone();
        let is_promise = {
            let vm = self.vm.as_mut().expect("live vm");
            vm.instance.promise_state(&mut vm.store, &live).is_ok()
        };
        if !is_promise {
            return Ok(JsValue::live(live));
        }
        self.settle_promise(
            live,
            "Promise resolution is still pending but the event loop has already resolved",
        )
        .await
        .map(JsValue::live)
    }

    async fn settle_promise(
        &mut self,
        promise: ScriptwJsValue,
        stalled: &'static str,
    ) -> Result<ScriptwJsValue, Error> {
        poll_fn(|cx| {
            if let Some(res) = self.promise_outcome(&promise) {
                return Poll::Ready(res);
            }
            match self.poll_event_loop(cx) {
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => Poll::Ready(
                    self.promise_outcome(&promise)
                        .unwrap_or_else(|| Err(Error::new(stalled))),
                ),
                Poll::Pending => match self.promise_outcome(&promise) {
                    Some(res) => Poll::Ready(res),
                    None => Poll::Pending,
                },
            }
        })
        .await
    }

    fn promise_outcome(
        &mut self,
        promise: &ScriptwJsValue,
    ) -> Option<Result<ScriptwJsValue, Error>> {
        let vm = self.vm.as_mut()?;
        match vm.instance.promise_state(&mut vm.store, promise) {
            Ok(JsPromiseState::Pending) => None,
            Ok(JsPromiseState::Fulfilled(v)) => Some(Ok(v)),
            Ok(JsPromiseState::Rejected(reason)) => {
                let desc = describe_js_error(vm, &reason);
                Some(Err(Error::new(format!("Uncaught {desc}"))))
            }
            Err(_) => Some(Ok(promise.clone())),
        }
    }

    /// Runs the event loop until no async op is pending.
    pub async fn run_event_loop(&mut self) -> Result<(), Error> {
        poll_fn(|cx| self.poll_event_loop(cx)).await
    }

    /// One turn of the event loop: drains JS microtasks, polls settled async ops,
    /// resolves their promises, drains microtasks again, and checks unhandled rejections.
    pub fn poll_event_loop(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.state.waker.register(cx.waker());
        if self.terminated.load(Ordering::Acquire) {
            return Poll::Ready(Err(Error::terminated()));
        }

        // 0. Dispatch any queued DevTools inspector commands (including Runtime.evaluate).
        if let Err(err) = self.dispatch_inspector_messages() {
            return Poll::Ready(Err(err));
        }

        // 1. Drain pending JS microtasks.
        if let Err(err) = self.pump_microtasks() {
            return Poll::Ready(Err(err));
        }

        // 2. Poll settled Rust async ops.
        let mut settled = Vec::new();
        {
            let mut pending = self.state.pending_ops.borrow_mut();
            while settled.len() < MAX_OPS_PER_TICK {
                match pending.poll_next_unpin(cx) {
                    Poll::Ready(Some(done)) => settled.push(done),
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
        }
        if settled.len() == MAX_OPS_PER_TICK {
            cx.waker().wake_by_ref();
        }

        // 3. Deliver settled async results back to JS Promises.
        for (id, result) in settled {
            let env_bytes = match result {
                Ok(val) => encode_ok_envelope(&val),
                Err(err) => encode_err_envelope(&err),
            };
            let vm = self.vm.as_mut().expect("live vm");
            let id_v = match vm
                .instance
                .value(&mut vm.store, ForeignValue::Number(f64::from(id)))
            {
                Ok(v) => v,
                Err(e) => return Poll::Ready(Err(map_engine_err(&self.terminated, e))),
            };
            let bytes_v = match vm
                .instance
                .value(&mut vm.store, ForeignValue::Bytes(env_bytes))
            {
                Ok(v) => v,
                Err(e) => return Poll::Ready(Err(map_engine_err(&self.terminated, e))),
            };
            if let Err(err) = call_js(
                vm,
                &self.terminated,
                &vm.exports.settle_async.clone(),
                &[id_v, bytes_v],
            ) {
                return Poll::Ready(Err(err));
            }
        }

        // 4. Drain microtasks queued by settled Promises.
        if let Err(err) = self.pump_microtasks() {
            return Poll::Ready(Err(err));
        }

        // 5. Check for unhandled promise rejections.
        if let Some(err) = self.take_unhandled_rejection() {
            return Poll::Ready(Err(err));
        }

        if self.state.has_pending_work() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn dispatch_inspector_messages(&mut self) -> Result<(), Error> {
        let Some(handle) = self.inspector_handle.get().cloned() else {
            return Ok(());
        };
        let core = handle.core();
        let evals = core.drain_idle_messages();
        for req in evals {
            if let PendingEvalRequest::Global {
                session_id,
                req_id,
                expression,
            } = req
            {
                let sid = core.register_script("", &expression);
                let instrumented = instrument::instrument_js(sid, &expression);
                let vm = self.vm.as_mut().expect("live vm");
                let src_v = str_val(vm, &instrumented)?;
                let eval_fn = vm.exports.dbg_eval_global.clone();
                let res_v = call_js(vm, &self.terminated, &eval_fn, &[src_v])?;
                let json_str = js_value_to_rust_string(&mut vm.store, &res_v)?;
                let parsed: serde_json::Value = serde_json::from_str(&json_str)
                    .unwrap_or_else(|_| serde_json::json!(["{\"type\":\"undefined\"}", null]));
                let remote_json = parsed[0].as_str().unwrap_or("{\"type\":\"undefined\"}");
                let exc_desc = parsed[1].as_str();
                core.send_eval_response(session_id, req_id, remote_json, exc_desc);
            }
        }
        Ok(())
    }

    fn pump_microtasks(&mut self) -> Result<(), Error> {
        self.check_terminated()?;
        let vm = self.vm.as_mut().expect("live vm");
        match vm.instance.pump(&mut vm.store) {
            Ok(EmbeddedEvaluation::Settled(JsCompletion::Throw(reason))) => {
                let desc = describe_js_error(vm, &reason);
                Err(Error::new(format!("Uncaught (in promise) {desc}")))
            }
            Ok(_) => {
                if self.terminated.load(Ordering::Acquire) {
                    Err(Error::terminated())
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(map_engine_err(&self.terminated, e)),
        }
    }

    fn take_unhandled_rejection(&mut self) -> Option<Error> {
        let vm = self.vm.as_mut()?;
        let val = call_js(
            vm,
            &self.terminated,
            &vm.exports.take_unhandled_rejection.clone(),
            &[],
        )
        .ok()?;
        let text = js_value_to_rust_string(&mut vm.store, &val).ok()?;
        if text.is_empty() {
            None
        } else {
            Some(Error::new(format!("Uncaught (in promise) {text}")))
        }
    }

    /// Returns the generated ES module source for all recorded snapshot steps
    /// without compiling or executing it (used by `build.rs` to precompile the
    /// exact same bundle that [`Self::snapshot`] will load).
    pub fn snapshot_bundle_source(mut self) -> Result<String, Error> {
        let recorder = self
            .recorder
            .take()
            .ok_or_else(|| Error::new("runtime was not created with new_for_snapshot"))?;
        Ok(recorder.generate_bundle_mjs())
    }

    /// AOT-compiles all recorded bootstrap steps + extension scripts into a
    /// process-cached native x64 `EmbeddedRuntime` and returns its handle blob.
    pub fn snapshot(mut self) -> Result<Box<[u8]>, Error> {
        if !self.state.modules.borrow().is_empty() {
            return Err(Error::new("a snapshot cannot include ES modules"));
        }
        if self.state.has_pending_work() {
            return Err(Error::new("a snapshot cannot include pending async work"));
        }
        let recorder = self
            .recorder
            .take()
            .ok_or_else(|| Error::new("runtime was not created with new_for_snapshot"))?;
        let bundle_src = recorder.generate_bundle_mjs();
        let runtime = compile_embedded_module(&bundle_src)?;

        // Validate the compiled snapshot immediately by running `__dd_run_snapshot`
        // once inside a throwaway Store (catches any JS bootstrap error eagerly).
        let (test_vm, _) = instantiate_vm(&runtime, &self.state, self.max_heap_bytes, true)?;
        drop(test_vm);

        let op_names: Vec<String> = self
            .state
            .ops
            .iter()
            .map(|op| op.name.to_string())
            .collect();
        let snap_id = NEXT_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        SNAPSHOTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(snap_id, Arc::new(CompiledSnapshot { runtime, op_names }));

        let mut blob = Vec::with_capacity(16);
        blob.extend_from_slice(SNAPSHOT_MAGIC);
        blob.extend_from_slice(&snap_id.to_le_bytes());
        Ok(blob.into_boxed_slice())
    }

    fn check_terminated(&self) -> Result<(), Error> {
        if self.terminated.load(Ordering::Acquire) {
            Err(Error::terminated())
        } else {
            Ok(())
        }
    }
}

impl Drop for JsRuntime {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        if let Some(handle) = self.inspector_handle.get() {
            handle.close();
        }
        // Drop `vm` (`wasmtime::Store`) BEFORE `state` (`Rc<RuntimeState>`)!
        self.vm.take();
        self.state.pending_ops.borrow_mut().clear();
        self.state.modules.borrow_mut().clear();
    }
}

// =========================================================================
// Low-level Scriptw/Wasmtime Instantiation & Host Foreign Callbacks
// =========================================================================

fn lookup_snapshot(blob: &[u8], ops: &[OpDecl]) -> Result<Arc<CompiledSnapshot>, Error> {
    if blob.len() != 16 || &blob[..8] != SNAPSHOT_MAGIC {
        return Err(Error::new("invalid dd_v8 startup snapshot header"));
    }
    let id = u64::from_le_bytes(blob[8..16].try_into().unwrap());
    let snap = SNAPSHOTS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&id)
        .cloned()
        .ok_or_else(|| Error::new("startup snapshot id not found"))?;
    let actual_names: Vec<&str> = ops.iter().map(|o| o.name).collect();
    let expected_names: Vec<&str> = snap.op_names.iter().map(String::as_str).collect();
    if actual_names != expected_names {
        return Err(Error::new(format!(
            "startup snapshot ops {expected_names:?} do not match runtime ops {actual_names:?}"
        )));
    }
    Ok(snap)
}

fn instantiate_vm(
    embedded: &Arc<EmbeddedRuntime>,
    state: &Rc<RuntimeState>,
    max_heap_bytes: usize,
    run_snapshot_export: bool,
) -> Result<(LiveVm, wasmtime::InterruptHandle), Error> {
    let engine = shared_engine()?;
    let mut store_state = embedded
        .new_state(RealmBuilder::new().build(), StatePtr(Rc::as_ptr(state)))
        .map_err(|e| Error::new(format!("scriptw new_state failed: {e}")))?;
    let heap_limit = if max_heap_bytes > 0 {
        max_heap_bytes
    } else {
        usize::MAX
    };
    *store_state.limits_mut() = wasmtime::StoreLimitsBuilder::new()
        .memory_size(heap_limit)
        .build();
    let mut store = wasmtime::Store::new(engine, store_state);
    store.limiter(|s| s.limits_mut());
    let interrupt = store.interrupt_handle();

    let mut linker = wasmtime::Linker::new(engine);
    register_dd_foreigns(embedded, &mut linker)?;
    embedded
        .register(&mut store, &mut linker)
        .map_err(|e| Error::new(format!("scriptw register failed: {e}")))?;
    let mut instance = embedded
        .instantiate(&mut store, &mut linker)
        .map_err(|e| Error::new(format!("scriptw instantiate failed: {e}")))?;
    match instance
        .initialize(&mut store)
        .map_err(|e| Error::new(format!("scriptw initialize failed: {e}")))?
    {
        EmbeddedEvaluation::Settled(JsCompletion::Normal(_)) => {}
        EmbeddedEvaluation::Settled(JsCompletion::Throw(err)) => {
            let msg = format!("kernel module threw on init: {err:?}");
            return Err(Error::new(msg));
        }
        EmbeddedEvaluation::Pending => {
            return Err(Error::new("kernel module unexpectedly stayed pending"));
        }
    }

    let get_exp = |store: &mut wasmtime::Store<WasmStoreState>,
                   name: &str|
     -> Result<ScriptwJsValue, Error> {
        match instance
            .get_export(store, name)
            .map_err(|e| Error::new(format!("get_export {name}: {e}")))?
        {
            JsCompletion::Normal(v) => Ok(v),
            JsCompletion::Throw(_) => Err(Error::new(format!("export {name} threw"))),
        }
    };

    let exports = KernelExports {
        init_ops: get_exp(&mut store, "__dd_init_ops")?,
        eval_script: get_exp(&mut store, "__dd_eval_script")?,
        eval_with_ops: get_exp(&mut store, "__dd_eval_with_ops")?,
        eval_function: get_exp(&mut store, "__dd_eval_function")?,
        call_function: get_exp(&mut store, "__dd_call_function")?,
        call_u32: get_exp(&mut store, "__dd_call_u32")?,
        call_f64_2: get_exp(&mut store, "__dd_call_f64_2")?,
        get_prop: get_exp(&mut store, "__dd_get_prop")?,
        to_string: get_exp(&mut store, "__dd_to_string")?,
        describe_error: get_exp(&mut store, "__dd_describe_error")?,
        set_internals: get_exp(&mut store, "__dd_set_internals")?,
        get_internals: get_exp(&mut store, "__dd_get_internals")?,
        set_allow_codegen: get_exp(&mut store, "__dd_set_allow_codegen")?,
        set_inspector_enabled: get_exp(&mut store, "__dd_set_inspector_enabled")?,
        dbg_eval_global: get_exp(&mut store, "__dd_dbg_eval_global")?,
        wire_to_js: get_exp(&mut store, "__dd_wire_to_js")?,
        js_to_wire: get_exp(&mut store, "__dd_js_to_wire")?,
        settle_async: get_exp(&mut store, "__dd_settle_async")?,
        take_unhandled_rejection: get_exp(&mut store, "__dd_take_unhandled_rejection")?,
        register_module: get_exp(&mut store, "__dd_register_module")?,
        eval_module: get_exp(&mut store, "__dd_eval_module")?,
        module_namespace: get_exp(&mut store, "__dd_module_namespace")?,
        make_array: get_exp(&mut store, "__dd_make_array")?,
        push_array: get_exp(&mut store, "__dd_push_array")?,
    };

    let init_fn = if run_snapshot_export {
        get_exp(&mut store, "__dd_run_snapshot")?
    } else {
        exports.init_ops.clone()
    };

    let mut vm = LiveVm {
        store,
        instance,
        exports,
    };

    let op_specs_json = build_op_specs_json(&state.ops);
    let specs_v = str_val(&mut vm, &op_specs_json)?;
    let budget_v = vm
        .instance
        .value(
            &mut vm.store,
            ForeignValue::Number(state.max_op_argument_bytes as f64),
        )
        .map_err(|e| Error::new(e.to_string()))?;
    let async_ctx_v = vm
        .instance
        .async_context_accessor(&mut vm.store)
        .map_err(|e| Error::new(format!("async_context_accessor failed: {e}")))?;
    let dummy_term = AtomicBool::new(false);
    let _ = call_js(
        &mut vm,
        &dummy_term,
        &init_fn,
        &[specs_v, budget_v, async_ctx_v],
    )?;

    Ok((vm, interrupt))
}

fn register_dd_foreigns(
    embedded: &EmbeddedRuntime,
    linker: &mut wasmtime::Linker<WasmStoreState>,
) -> Result<(), Error> {
    // 1. `dd.call_sync(op_id: f64, args_bytes: Bytes) -> Bytes`
    embedded
        .register_foreign(linker, "dd", "call_sync", |caller, args| {
            let [
                ForeignValue::Number(op_id_f),
                ForeignValue::Bytes(arg_bytes),
            ] = args.as_slice()
            else {
                return Ok(ForeignValue::Bytes(encode_err_envelope(
                    &OpError::type_error("invalid call_sync arguments"),
                )));
            };
            let state_ptr = caller
                .data()
                .user_data::<StatePtr>()
                .ok_or_else(|| wasmtime::Error::msg("missing RuntimeState"))?
                .0;
            // SAFETY: Synchronous call inside `&mut JsRuntime`.
            let state = unsafe { &*state_ptr };
            let op_id = *op_id_f as usize;
            let Some(op) = state.ops.get(op_id) else {
                return Ok(ForeignValue::Bytes(encode_err_envelope(&OpError::new(
                    format!("unknown op id {op_id}"),
                ))));
            };
            let decoded = match decode_wire_args(arg_bytes, state.max_op_argument_bytes) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(ForeignValue::Bytes(encode_err_envelope(&OpError::from(e))));
                }
            };
            let result = match &op.kind {
                OpKind::Sync(f) => f(&state.op_state, &decoded),
                OpKind::RawBuffer(f) => {
                    let is_buf = matches!(decoded.first(), Some(WireValue::I32(1)));
                    let buf_bytes = if is_buf {
                        match decoded.get(1) {
                            Some(WireValue::Bytes(b)) => Some(b.clone()),
                            _ => Some(Vec::new()),
                        }
                    } else {
                        None
                    };
                    let mut raw = RawBuffer::from_bytes(buf_bytes);
                    match f(&state.op_state, &mut raw) {
                        Ok(ret) => {
                            let wb = match raw.take_writeback() {
                                Some(b) => WireValue::Bytes(b),
                                None => WireValue::Null,
                            };
                            Ok(WireValue::Array(vec![ret, wb]))
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => Err(OpError::new(format!("op {} is not synchronous", op.name))),
            };
            let out = match result {
                Ok(v) => encode_ok_envelope(&v),
                Err(e) => encode_err_envelope(&e),
            };
            Ok(ForeignValue::Bytes(out))
        })
        .map_err(|e| Error::new(format!("register_foreign call_sync: {e}")))?;

    // 2. `dd.start_async(op_id: f64, args_bytes: Bytes) -> Bytes`
    embedded
        .register_foreign(linker, "dd", "start_async", |caller, args| {
            let [
                ForeignValue::Number(op_id_f),
                ForeignValue::Bytes(arg_bytes),
            ] = args.as_slice()
            else {
                return Ok(ForeignValue::Bytes(encode_err_envelope(
                    &OpError::type_error("invalid start_async arguments"),
                )));
            };
            let state_ptr = caller
                .data()
                .user_data::<StatePtr>()
                .ok_or_else(|| wasmtime::Error::msg("missing RuntimeState"))?
                .0;
            // SAFETY: Synchronous call inside `&mut JsRuntime`.
            let state = unsafe { &*state_ptr };
            let op_id = *op_id_f as usize;
            let Some(op) = state.ops.get(op_id) else {
                return Ok(ForeignValue::Bytes(encode_err_envelope(&OpError::new(
                    format!("unknown op id {op_id}"),
                ))));
            };
            let decoded = match decode_wire_args(arg_bytes, state.max_op_argument_bytes) {
                Ok(v) => v,
                Err(e) => {
                    return Ok(ForeignValue::Bytes(encode_err_envelope(&OpError::from(e))));
                }
            };
            let mut fut = match &op.kind {
                OpKind::Async(f) => match f(&state.op_state, &decoded) {
                    Ok(fut) => fut,
                    Err(e) => return Ok(ForeignValue::Bytes(encode_err_envelope(&e))),
                },
                _ => {
                    return Ok(ForeignValue::Bytes(encode_err_envelope(&OpError::new(
                        format!("op {} is not async", op.name),
                    ))));
                }
            };
            if tokio::runtime::Handle::try_current().is_ok() {
                match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                    Poll::Ready(Ok(v)) => return Ok(ForeignValue::Bytes(encode_ok_envelope(&v))),
                    Poll::Ready(Err(e)) => return Ok(ForeignValue::Bytes(encode_err_envelope(&e))),
                    Poll::Pending => {}
                }
            }
            let id = state.alloc_pending_id();
            state
                .pending_ops
                .borrow_mut()
                .push(PendingOp { id, future: fut });
            state.waker.wake();
            Ok(ForeignValue::Bytes(encode_pending_envelope(id)))
        })
        .map_err(|e| Error::new(format!("register_foreign start_async: {e}")))?;

    // 3. `dd.load_module(specifier: StringUtf16, referrer: StringUtf16, kind: StringUtf16) -> Bytes`
    embedded
        .register_foreign(linker, "dd", "load_module", |caller, args| {
            let [
                ForeignValue::StringUtf16(spec_u16),
                ForeignValue::StringUtf16(ref_u16),
                ForeignValue::StringUtf16(kind_u16),
            ] = args.as_slice()
            else {
                return Ok(ForeignValue::Bytes(encode_err_envelope(
                    &OpError::type_error("invalid load_module arguments"),
                )));
            };
            let state_ptr = caller
                .data()
                .user_data::<StatePtr>()
                .ok_or_else(|| wasmtime::Error::msg("missing RuntimeState"))?
                .0;
            // SAFETY: Synchronous call inside `&mut JsRuntime`.
            let state = unsafe { &*state_ptr };
            let specifier = String::from_utf16_lossy(spec_u16);
            let referrer = String::from_utf16_lossy(ref_u16);
            let kind_str = String::from_utf16_lossy(kind_u16);

            let res: Result<WireValue, OpError> = (|| {
                let req_type = ModuleType::from_attr(&kind_str)
                    .map_err(|e| OpError::type_error(format!("{e} imported by {referrer}")))?;
                let resolved = state
                    .loader
                    .resolve(&specifier, &referrer)
                    .map_err(OpError::type_error)?;
                let mut mods = state.modules.borrow_mut();
                let insp = state.inspector.get().map(|h| h.core().as_ref());
                let (root_id, newly_added) = modules::load_graph(
                    &mut mods,
                    state.loader.as_ref(),
                    &resolved,
                    req_type,
                    None,
                    false,
                    insp,
                )
                .map_err(|e| OpError::new(e.message().to_string()))?;
                let mut wire_records = Vec::with_capacity(newly_added.len());
                for id in newly_added {
                    mods.modules[id].registered_in_js = true;
                    wire_records.push(mods.modules[id].to_wire());
                }
                Ok(WireValue::Object(vec![
                    ("rootId".to_string(), WireValue::I32(root_id as i32)),
                    ("records".to_string(), WireValue::Array(wire_records)),
                ]))
            })();

            let out = match res {
                Ok(v) => encode_ok_envelope(&v),
                Err(e) => encode_err_envelope(&e),
            };
            Ok(ForeignValue::Bytes(out))
        })
        .map_err(|e| Error::new(format!("register_foreign load_module: {e}")))?;

    // 4. `dd.wasm_op(kind: f64, payload: Bytes) -> Bytes`
    embedded
        .register_foreign(linker, "dd", "wasm_op", |caller, args| {
            let [ForeignValue::Number(kind_f), ForeignValue::Bytes(payload)] = args.as_slice()
            else {
                return Ok(ForeignValue::Bytes(encode_err_envelope(
                    &OpError::type_error("invalid wasm_op arguments"),
                )));
            };
            let state_ptr = caller
                .data()
                .user_data::<StatePtr>()
                .ok_or_else(|| wasmtime::Error::msg("missing RuntimeState"))?
                .0;
            // SAFETY: Synchronous call inside `&mut JsRuntime`.
            let state = unsafe { &*state_ptr };
            let out = state.wasm.borrow_mut().dispatch(
                *kind_f as u32,
                payload,
                state.max_op_argument_bytes,
            );
            Ok(ForeignValue::Bytes(out))
        })
        .map_err(|e| Error::new(format!("register_foreign wasm_op: {e}")))?;

    // 5. `dd.inspector_op(kind: f64, payload: Bytes) -> Bytes`
    embedded
        .register_foreign(linker, "dd", "inspector_op", |caller, args| {
            let [ForeignValue::Number(kind_f), ForeignValue::Bytes(payload)] = args.as_slice()
            else {
                return Ok(ForeignValue::Bytes(Vec::new()));
            };
            let state_ptr = caller
                .data()
                .user_data::<StatePtr>()
                .ok_or_else(|| wasmtime::Error::msg("missing RuntimeState"))?
                .0;
            // SAFETY: Synchronous call inside `&mut JsRuntime`.
            let state = unsafe { &*state_ptr };
            let kind = *kind_f as u32;
            if kind == 6 {
                *state.last_wire_out.borrow_mut() = payload.clone();
                return Ok(ForeignValue::Bytes(Vec::new()));
            }
            let Some(handle) = state.inspector.get() else {
                return Ok(ForeignValue::Bytes(Vec::new()));
            };
            let core = handle.core();
            match kind {
                // 0: console API call
                0 => {
                    if let Ok(decoded) = decode_wire_args(payload, state.max_op_argument_bytes) {
                        let cdp_type = match decoded.first() {
                            Some(WireValue::String(s)) => s.as_str(),
                            _ => "log",
                        };
                        let args_json = match decoded.get(1) {
                            Some(WireValue::String(s)) => s.as_str(),
                            _ => "[]",
                        };
                        let script_id = wire_to_u64(decoded.get(2));
                        let url = match decoded.get(3) {
                            Some(WireValue::String(s)) => s.as_str(),
                            _ => "",
                        };
                        let line = wire_to_u64(decoded.get(4)) as u32;
                        let col = wire_to_u64(decoded.get(5)) as u32;
                        core.emit_console_api_called(
                            cdp_type, args_json, script_id, url, line, col,
                        );
                    }
                    Ok(ForeignValue::Bytes(Vec::new()))
                }
                // 1: fast statement/debugger check + enter pause loop if triggered
                1 => {
                    if core.is_terminated() {
                        return Err(wasmtime::Error::msg("execution interrupted"));
                    }
                    if payload.len() < 16 {
                        return Ok(ForeignValue::Bytes(Vec::new()));
                    }
                    let sid = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as u64;
                    let line = u32::from_le_bytes(payload[4..8].try_into().unwrap());
                    let col = u32::from_le_bytes(payload[8..12].try_into().unwrap());
                    let is_dbg = payload[12] != 0;
                    let Some((reason, hit_bps)) = core.check_break(sid, line, is_dbg) else {
                        return Ok(ForeignValue::Bytes(Vec::new()));
                    };
                    let _cf = core.emit_paused(sid, line, col, reason, &hit_bps);
                    encode_pause_step(core.wait_pause_step())
                }
                // 2: submit eval response while paused + wait for next pause command
                2 => {
                    if let Ok(decoded) = decode_wire_args(payload, state.max_op_argument_bytes) {
                        let session_id = wire_to_u64(decoded.first());
                        let req_id_str = match decoded.get(1) {
                            Some(WireValue::String(s)) => s.as_str(),
                            _ => "null",
                        };
                        let req_id: serde_json::Value =
                            serde_json::from_str(req_id_str).unwrap_or(serde_json::Value::Null);
                        let remote_json = match decoded.get(2) {
                            Some(WireValue::String(s)) => s.as_str(),
                            _ => "{\"type\":\"undefined\"}",
                        };
                        let exc_desc = match decoded.get(3) {
                            Some(WireValue::String(s)) => Some(s.as_str()),
                            _ => None,
                        };
                        core.send_eval_response(session_id, req_id, remote_json, exc_desc);
                    }
                    encode_pause_step(core.wait_pause_step())
                }
                // 3: instrument expression for nested Runtime.evaluate
                3 => {
                    let expr = decode_wire_args(payload, state.max_op_argument_bytes)
                        .ok()
                        .and_then(|mut a| match a.drain(..).next() {
                            Some(WireValue::String(s)) => Some(s),
                            _ => None,
                        })
                        .unwrap_or_default();
                    let sid = core.register_script("", &expr);
                    let inst = instrument::instrument_js(sid, &expr);
                    Ok(ForeignValue::Bytes(encode_ok_envelope(&WireValue::String(
                        inst,
                    ))))
                }
                // 4: check whether any DevTools session is attached
                4 => Ok(ForeignValue::Bytes(vec![u8::from(core.has_sessions())])),
                // 5: lookup script URL by scriptId
                5 => {
                    let sid = decode_wire_args(payload, state.max_op_argument_bytes)
                        .ok()
                        .map(|a| wire_to_u64(a.first()))
                        .unwrap_or(0);
                    let url = core.script_url(sid);
                    Ok(ForeignValue::Bytes(encode_ok_envelope(&WireValue::String(
                        url,
                    ))))
                }
                _ => Ok(ForeignValue::Bytes(Vec::new())),
            }
        })
        .map_err(|e| Error::new(format!("register_foreign inspector_op: {e}")))?;

    Ok(())
}

fn wire_to_u64(v: Option<&WireValue>) -> u64 {
    match v {
        Some(WireValue::I32(n)) => (*n).max(0) as u64,
        Some(WireValue::F64(f)) if f.is_finite() && *f >= 0.0 => *f as u64,
        _ => 0,
    }
}

fn encode_pause_step(step: PauseStep) -> wasmtime::Result<ForeignValue> {
    match step {
        PauseStep::Resume => Ok(ForeignValue::Bytes(encode_ok_envelope(&WireValue::Array(
            vec![WireValue::I32(1)],
        )))),
        PauseStep::Terminate => Err(wasmtime::Error::msg("execution interrupted")),
        PauseStep::Eval(PendingEvalRequest::CallFrame {
            session_id,
            req_id,
            expression,
        }) => Ok(ForeignValue::Bytes(encode_ok_envelope(&WireValue::Array(
            vec![
                WireValue::I32(2),
                WireValue::Bool(true),
                WireValue::F64(session_id as f64),
                WireValue::String(req_id.to_string()),
                WireValue::String(expression),
            ],
        )))),
        PauseStep::Eval(PendingEvalRequest::Global {
            session_id,
            req_id,
            expression,
        }) => Ok(ForeignValue::Bytes(encode_ok_envelope(&WireValue::Array(
            vec![
                WireValue::I32(2),
                WireValue::Bool(false),
                WireValue::F64(session_id as f64),
                WireValue::String(req_id.to_string()),
                WireValue::String(expression),
            ],
        )))),
    }
}

fn str_val(vm: &mut LiveVm, s: &str) -> Result<ScriptwJsValue, Error> {
    vm.instance
        .value(
            &mut vm.store,
            ForeignValue::StringUtf16(s.encode_utf16().collect()),
        )
        .map_err(|e| Error::new(e.to_string()))
}

fn js_value_to_rust_string(
    store: &mut wasmtime::Store<WasmStoreState>,
    val: &ScriptwJsValue,
) -> Result<String, Error> {
    let units = val.to_utf16(store).map_err(|e| Error::new(e.to_string()))?;
    Ok(String::from_utf16_lossy(&units))
}

fn make_js_array(
    vm: &mut LiveVm,
    terminated: &AtomicBool,
    args: &[JsValue],
) -> Result<ScriptwJsValue, Error> {
    let arr = call_js(vm, terminated, &vm.exports.make_array.clone(), &[])?;
    for a in args {
        let live = a.as_live()?.clone();
        let _ = call_js(
            vm,
            terminated,
            &vm.exports.push_array.clone(),
            &[arr.clone(), live],
        )?;
    }
    Ok(arr)
}

fn call_js(
    vm: &mut LiveVm,
    terminated: &AtomicBool,
    callee: &ScriptwJsValue,
    args: &[ScriptwJsValue],
) -> Result<ScriptwJsValue, Error> {
    match vm.instance.call(&mut vm.store, callee, None, args) {
        Ok(JsCompletion::Normal(val)) => {
            if terminated.load(Ordering::Acquire) {
                Err(Error::terminated())
            } else {
                Ok(val)
            }
        }
        Ok(JsCompletion::Throw(err)) => {
            if terminated.load(Ordering::Acquire) {
                Err(Error::terminated())
            } else {
                let desc = describe_js_error(vm, &err);
                Err(Error::new(format!("Uncaught {desc}")))
            }
        }
        Err(e) => Err(map_engine_err(terminated, e)),
    }
}

fn describe_js_error(vm: &mut LiveVm, err: &ScriptwJsValue) -> String {
    if let Ok(JsCompletion::Normal(s)) = vm.instance.call(
        &mut vm.store,
        &vm.exports.describe_error.clone(),
        None,
        std::slice::from_ref(err),
    ) && let Ok(text) = js_value_to_rust_string(&mut vm.store, &s)
    {
        return text;
    }
    format!("{err:?}")
}

fn map_engine_err(terminated: &AtomicBool, err: scriptw_engine::EngineError) -> Error {
    let msg = err.to_string();
    if terminated.load(Ordering::Acquire)
        || msg.contains("interrupted")
        || msg.contains("GC heap out of memory")
        || msg.contains("allocation too large")
    {
        Error::terminated()
    } else {
        Error::new(msg)
    }
}
