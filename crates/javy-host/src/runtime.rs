use common::{PlatformError, Result, WorkerInvocation, WorkerOutput};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use wasmtime::{
    Caller, Config, Engine, Extern, Linker, Module, OptLevel, Store, StoreLimits,
    StoreLimitsBuilder,
};
use wasmtime_wasi::WasiCtxBuilder;
use wasmtime_wasi::p1::WasiP1Ctx;
use wasmtime_wasi::p2::pipe::{MemoryInputPipe, MemoryOutputPipe};

const EPOCH_TICK: Duration = Duration::from_millis(10);
const MAX_LOG_BYTES: usize = 1024 * 1024;
const MAX_HOST_CALL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const MAX_WASM_MEMORY_BYTES: usize = 128 * 1024 * 1024;
const RANDOM_BYTES_PER_INVOCATION: usize = 4 * 1024;

fn shared_engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config.cranelift_opt_level(OptLevel::SpeedAndSize);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("Javy engine construction cannot fail");
        let ticker = engine.clone();
        std::thread::Builder::new()
            .name("javy-wasm-epoch".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(EPOCH_TICK);
                    ticker.increment_epoch();
                }
            })
            .expect("Javy epoch ticker failed to spawn");
        engine
    })
}

#[derive(Clone, Copy)]
pub struct InvokeOptions {
    pub timeout: Duration,
}

impl Default for InvokeOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Default)]
pub struct WorkerOptions {
    pub env: HashMap<String, String>,
    pub kv_bindings: Vec<String>,
    pub memory_bindings: Vec<String>,
}

#[derive(Serialize)]
struct InvocationEnvelope<'a> {
    invocation: &'a WorkerInvocation,
    env: &'a HashMap<String, String>,
    bindings: Vec<BindingDescriptor<'a>>,
    random_bytes: &'a [u8],
}

#[derive(Serialize)]
struct BindingDescriptor<'a> {
    name: &'a str,
    kind: &'static str,
}

struct StoreState {
    wasi: WasiP1Ctx,
    stdout: MemoryOutputPipe,
    stderr: MemoryOutputPipe,
    limits: StoreLimits,
    host_bindings: Arc<HostBindings>,
    host_response: Vec<u8>,
}

pub struct JavyWorker {
    module: Module,
    env: HashMap<String, String>,
    host_bindings: Arc<HostBindings>,
}

struct HostBindings {
    kv_bindings: HashSet<String>,
    memory_bindings: HashSet<String>,
    state: Mutex<HostBindingState>,
}

#[derive(Default)]
struct HostBindingState {
    kv: HashMap<(String, String), Value>,
    memory: HashMap<(String, String, String), VersionedMemoryValue>,
}

struct VersionedMemoryValue {
    value: Value,
    version: u64,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum HostCall {
    KvDelete {
        binding: String,
        key: String,
    },
    KvGet {
        binding: String,
        key: String,
    },
    KvList {
        binding: String,
        prefix: String,
        limit: usize,
    },
    KvPut {
        binding: String,
        key: String,
        value: Value,
    },
    MemoryCommit {
        binding: String,
        shard: String,
        reads: Vec<MemoryReadVersion>,
        writes: Vec<MemoryWrite>,
    },
    MemoryRead {
        binding: String,
        shard: String,
        key: String,
    },
}

#[derive(Deserialize)]
struct MemoryReadVersion {
    key: String,
    version: u64,
}

#[derive(Deserialize)]
struct MemoryWrite {
    key: String,
    value: Value,
}

impl HostBindings {
    fn new(kv_bindings: Vec<String>, memory_bindings: Vec<String>) -> Result<Self> {
        let kv_bindings = binding_names(kv_bindings, "KV")?;
        let memory_bindings = binding_names(memory_bindings, "memory")?;
        if let Some(name) = kv_bindings.intersection(&memory_bindings).next() {
            return Err(PlatformError::bad_request(format!(
                "binding {name:?} cannot be both KV and memory"
            )));
        }
        Ok(Self {
            kv_bindings,
            memory_bindings,
            state: Mutex::new(HostBindingState::default()),
        })
    }

    fn descriptors(&self) -> Vec<BindingDescriptor<'_>> {
        let mut descriptors = self
            .kv_bindings
            .iter()
            .map(|name| BindingDescriptor { name, kind: "kv" })
            .chain(self.memory_bindings.iter().map(|name| BindingDescriptor {
                name,
                kind: "memory",
            }))
            .collect::<Vec<_>>();
        descriptors.sort_unstable_by(|left, right| left.name.cmp(right.name));
        descriptors
    }

    fn call(&self, request: &[u8]) -> Vec<u8> {
        let outcome = serde_json::from_slice(request)
            .map_err(|error| format!("invalid dd_host request JSON: {error}"))
            .and_then(|call| self.execute(call));
        let response = match outcome {
            Ok(value) => json!({ "ok": true, "value": value }),
            Err(error) => json!({ "ok": false, "error": error }),
        };
        serde_json::to_vec(&response).expect("JSON host response must serialize")
    }

    fn execute(&self, call: HostCall) -> std::result::Result<Value, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Javy host binding state lock was poisoned".to_string())?;
        match call {
            HostCall::KvDelete { binding, key } => {
                self.require_kv(&binding)?;
                state.kv.remove(&(binding, key));
                Ok(Value::Null)
            }
            HostCall::KvGet { binding, key } => {
                self.require_kv(&binding)?;
                Ok(state
                    .kv
                    .get(&(binding, key))
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            HostCall::KvList {
                binding,
                prefix,
                limit,
            } => {
                self.require_kv(&binding)?;
                let mut entries = state
                    .kv
                    .iter()
                    .filter(|((candidate_binding, key), _)| {
                        candidate_binding == &binding && key.starts_with(&prefix)
                    })
                    .map(|((_, key), value)| json!({ "key": key, "value": value }))
                    .collect::<Vec<_>>();
                entries.sort_unstable_by(|left, right| {
                    left["key"].as_str().cmp(&right["key"].as_str())
                });
                entries.truncate(limit.min(1000));
                Ok(Value::Array(entries))
            }
            HostCall::KvPut {
                binding,
                key,
                value,
            } => {
                self.require_kv(&binding)?;
                state.kv.insert((binding, key), value);
                Ok(Value::Null)
            }
            HostCall::MemoryRead {
                binding,
                shard,
                key,
            } => {
                self.require_memory(&binding)?;
                let stored = state.memory.get(&(binding, shard, key));
                Ok(match stored {
                    Some(stored) => json!({
                        "found": true,
                        "value": stored.value,
                        "version": stored.version,
                    }),
                    None => json!({ "found": false, "version": 0 }),
                })
            }
            HostCall::MemoryCommit {
                binding,
                shard,
                reads,
                writes,
            } => {
                self.require_memory(&binding)?;
                let has_conflict = reads.iter().any(|read| {
                    let version = state
                        .memory
                        .get(&(binding.clone(), shard.clone(), read.key.clone()))
                        .map_or(0, |stored| stored.version);
                    version != read.version
                });
                if has_conflict {
                    return Ok(Value::Bool(false));
                }
                for write in writes {
                    let storage_key = (binding.clone(), shard.clone(), write.key);
                    let version = state
                        .memory
                        .get(&storage_key)
                        .map_or(1, |stored| stored.version + 1);
                    state.memory.insert(
                        storage_key,
                        VersionedMemoryValue {
                            value: write.value,
                            version,
                        },
                    );
                }
                Ok(Value::Bool(true))
            }
        }
    }

    fn require_kv(&self, binding: &str) -> std::result::Result<(), String> {
        if self.kv_bindings.contains(binding) {
            Ok(())
        } else {
            Err(format!("KV binding {binding:?} is not configured"))
        }
    }

    fn require_memory(&self, binding: &str) -> std::result::Result<(), String> {
        if self.memory_bindings.contains(binding) {
            Ok(())
        } else {
            Err(format!("memory binding {binding:?} is not configured"))
        }
    }
}

fn binding_names(names: Vec<String>, kind: &str) -> Result<HashSet<String>> {
    let mut unique = HashSet::new();
    for name in names {
        if name.trim().is_empty() {
            return Err(PlatformError::bad_request(format!(
                "{kind} binding name cannot be empty"
            )));
        }
        if !unique.insert(name.clone()) {
            return Err(PlatformError::bad_request(format!(
                "{kind} binding {name:?} is configured more than once"
            )));
        }
    }
    Ok(unique)
}

impl JavyWorker {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::new(bytes, WorkerOptions::default())
    }

    pub fn new(bytes: &[u8], options: WorkerOptions) -> Result<Self> {
        let module = Module::new(shared_engine(), bytes).map_err(|error| {
            PlatformError::bad_request(format!("invalid Javy worker Wasm module: {error}"))
        })?;
        for import in module.imports() {
            if import.module() != "wasi_snapshot_preview1" && import.module() != "dd_host" {
                return Err(PlatformError::bad_request(format!(
                    "Javy worker imports unsupported module {:?}; expected \
                     \"wasi_snapshot_preview1\" or \"dd_host\"",
                    import.module()
                )));
            }
        }
        if module.get_export("_start").is_none() {
            return Err(PlatformError::bad_request(
                "Javy worker is missing the required _start export",
            ));
        }
        let host_bindings = HostBindings::new(options.kv_bindings, options.memory_bindings)?;
        Ok(Self {
            module,
            env: options.env,
            host_bindings: Arc::new(host_bindings),
        })
    }

    pub fn invoke(
        &self,
        invocation: WorkerInvocation,
        options: InvokeOptions,
    ) -> Result<WorkerOutput> {
        let mut random_bytes = [0; RANDOM_BYTES_PER_INVOCATION];
        getrandom::fill(&mut random_bytes).map_err(|error| {
            PlatformError::internal(format!(
                "secure random bytes could not be generated for request {}: {error}",
                invocation.request_id
            ))
        })?;
        let request_bytes = serde_json::to_vec(&InvocationEnvelope {
            invocation: &invocation,
            env: &self.env,
            bindings: self.host_bindings.descriptors(),
            random_bytes: &random_bytes,
        })
        .map_err(|error| {
            PlatformError::internal(format!(
                "request {} could not be encoded for Javy: {error}",
                invocation.request_id
            ))
        })?;
        let stdin = MemoryInputPipe::new(request_bytes);
        let stdout = MemoryOutputPipe::new(MAX_RESPONSE_BYTES);
        let stderr = MemoryOutputPipe::new(MAX_LOG_BYTES);
        let wasi = WasiCtxBuilder::new()
            .stdin(stdin)
            .stdout(stdout.clone())
            .stderr(stderr.clone())
            .build_p1();
        let limits = StoreLimitsBuilder::new()
            .memory_size(MAX_WASM_MEMORY_BYTES)
            .instances(1)
            .tables(8)
            .build();
        let mut store = Store::new(
            shared_engine(),
            StoreState {
                wasi,
                stdout,
                stderr,
                limits,
                host_bindings: Arc::clone(&self.host_bindings),
                host_response: Vec::new(),
            },
        );
        store.limiter(|state| &mut state.limits);
        let deadline_ticks =
            (options.timeout.as_millis() / EPOCH_TICK.as_millis()).max(1) as u64 + 1;
        store.set_epoch_deadline(deadline_ticks);

        let mut linker = Linker::new(shared_engine());
        wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut StoreState| {
            &mut state.wasi
        })
        .map_err(|error| {
            PlatformError::internal(format!("could not link the Javy WASI runtime: {error}"))
        })?;
        add_host_calls(&mut linker)?;
        let instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(|error| {
                execution_error(&invocation.request_id, "instantiate", error, &store)
            })?;
        let start = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .map_err(|error| {
                execution_error(&invocation.request_id, "resolve _start", error, &store)
            })?;
        start
            .call(&mut store, ())
            .map_err(|error| execution_error(&invocation.request_id, "execute", error, &store))?;

        let worker_logs = worker_stderr(&store);
        if worker_logs != "<empty>" {
            tracing::info!(
                request_id = %invocation.request_id,
                "Javy worker logs:\n{worker_logs}"
            );
        }
        let response_bytes = store.data().stdout.contents();
        if response_bytes.is_empty() {
            return Err(PlatformError::runtime(format!(
                "Javy worker returned no response for request {}; stderr: {}",
                invocation.request_id,
                worker_stderr(&store)
            )));
        }
        serde_json::from_slice(&response_bytes).map_err(|error| {
            PlatformError::runtime(format!(
                "Javy worker returned invalid response JSON for request {}: {error}; \
                 stdout: {}; stderr: {}",
                invocation.request_id,
                String::from_utf8_lossy(&response_bytes),
                worker_stderr(&store)
            ))
        })
    }
}

fn add_host_calls(linker: &mut Linker<StoreState>) -> Result<()> {
    linker
        .func_wrap(
            "dd_host",
            "call",
            |mut caller: Caller<'_, StoreState>, pointer: i32, length: i32| -> i32 {
                let response = read_caller_bytes(&mut caller, pointer, length)
                    .map(|request| caller.data().host_bindings.call(&request))
                    .unwrap_or_else(|error| {
                        serde_json::to_vec(&json!({ "ok": false, "error": error }))
                            .expect("static host error response must serialize")
                    });
                let response = if response.len() <= MAX_HOST_CALL_BYTES {
                    response
                } else {
                    serde_json::to_vec(&json!({
                        "ok": false,
                        "error": format!(
                            "dd_host response exceeded {MAX_HOST_CALL_BYTES} bytes"
                        ),
                    }))
                    .expect("static host error response must serialize")
                };
                let response_length = i32::try_from(response.len()).unwrap_or(i32::MAX);
                caller.data_mut().host_response = response;
                response_length
            },
        )
        .map_err(|error| {
            PlatformError::internal(format!("could not link dd_host.call: {error}"))
        })?;
    linker
        .func_wrap(
            "dd_host",
            "read",
            |mut caller: Caller<'_, StoreState>, pointer: i32, length: i32| {
                let response = caller.data().host_response.clone();
                if usize::try_from(length).ok() != Some(response.len()) {
                    return;
                }
                let Some(Extern::Memory(memory)) = caller.get_export("memory") else {
                    return;
                };
                let Ok(pointer) = usize::try_from(pointer) else {
                    return;
                };
                let _ = memory.write(&mut caller, pointer, &response);
            },
        )
        .map_err(|error| {
            PlatformError::internal(format!("could not link dd_host.read: {error}"))
        })?;
    Ok(())
}

fn read_caller_bytes(
    caller: &mut Caller<'_, StoreState>,
    pointer: i32,
    length: i32,
) -> std::result::Result<Vec<u8>, String> {
    let pointer =
        usize::try_from(pointer).map_err(|_| format!("negative request pointer {pointer}"))?;
    let length =
        usize::try_from(length).map_err(|_| format!("negative request length {length}"))?;
    if length > MAX_HOST_CALL_BYTES {
        return Err(format!(
            "dd_host request length {length} exceeds {MAX_HOST_CALL_BYTES} bytes"
        ));
    }
    let Some(Extern::Memory(memory)) = caller.get_export("memory") else {
        return Err("Javy worker has no exported memory".to_string());
    };
    let mut bytes = vec![0; length];
    memory.read(caller, pointer, &mut bytes).map_err(|error| {
        format!("could not read dd_host request at {pointer}+{length}: {error}")
    })?;
    Ok(bytes)
}

fn execution_error(
    request_id: &str,
    phase: &str,
    error: wasmtime::Error,
    store: &Store<StoreState>,
) -> PlatformError {
    PlatformError::runtime(format!(
        "Javy worker failed to {phase} request {request_id}: {error:#}; stderr: {}",
        worker_stderr(store)
    ))
}

fn worker_stderr(store: &Store<StoreState>) -> String {
    let stderr = store.data().stderr.contents();
    let text = String::from_utf8_lossy(&stderr);
    if text.trim().is_empty() {
        "<empty>".to_string()
    } else {
        text.trim().to_string()
    }
}
