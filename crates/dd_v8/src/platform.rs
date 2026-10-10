//! Process-wide V8 setup.
//!
//! V8 posts foreground tasks (finishing async WebAssembly compilation, GC
//! follow-ups, `Atomics.waitAsync` wakeups) from its worker threads. The
//! platform below queues them per isolate and wakes that isolate's event
//! loop, which runs them on its own thread. Delayed tasks sleep on the tokio
//! runtime the isolate was created under.

use futures_util::task::AtomicWaker;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Arc, LazyLock, Mutex, Once, OnceLock};
use std::time::Duration;

// V8 stops reading a flag string at the first flag it does not know, so
// every flag here must exist in the bundled V8.
const BASE_FLAGS: &str = concat!(
    "--turbo-fast-api-calls",
    " --harmony-temporal",
    " --js-float16array",
    " --js-explicit-resource-management",
    " --js-source-phase-imports",
    " --js-defer-import-eval",
    " --enable-queue-microtask",
    " --no-extensible-ro-snapshot",
);

static INIT: Once = Once::new();
static USER_FLAGS: OnceLock<Vec<String>> = OnceLock::new();

/// Sets V8 flags in command-line form (`["--max-lazy", ...]`). Must run
/// before the first isolate is created; a later call with the same flags is a
/// no-op and a call with different flags is an error. Returns the flags V8
/// did not recognize.
pub fn set_flags(flags: &[String]) -> Result<(), String> {
    let normalized = flags
        .iter()
        .map(|flag| flag.trim())
        .filter(|flag| !flag.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if INIT.is_completed() && !normalized.is_empty() && USER_FLAGS.get() != Some(&normalized) {
        return Err(format!(
            "v8 flags {normalized:?} must be set before the first isolate is created"
        ));
    }
    if let Some(existing) = USER_FLAGS.get() {
        if existing != &normalized {
            return Err(format!(
                "v8 flags were already initialized as {existing:?}; cannot reinitialize with {normalized:?}"
            ));
        }
        return Ok(());
    }
    let mut argv = Vec::with_capacity(normalized.len() + 1);
    argv.push("dd".to_string());
    argv.extend(normalized.iter().cloned());
    let leftovers = v8::V8::set_flags_from_command_line(argv);
    if leftovers.len() > 1 {
        return Err(format!("unsupported v8 flags: {:?}", &leftovers[1..]));
    }
    let _ = USER_FLAGS.set(normalized);
    Ok(())
}

/// Initializes V8 once per process. Every runtime constructor calls this.
pub fn init() {
    INIT.call_once(|| {
        v8::V8::set_flags_from_string(BASE_FLAGS);
        let threads = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(4)
            .min(4);
        let platform =
            v8::new_custom_platform(threads, false, false, ForegroundTasks).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    });
}

/// The foreground task queue of one isolate.
pub(crate) struct IsolateTasks {
    pub(crate) queue: Arc<Mutex<Vec<v8::Task>>>,
    pub(crate) waker: Arc<AtomicWaker>,
    runtime: Option<tokio::runtime::Handle>,
}

static ISOLATES: LazyLock<Mutex<HashMap<usize, IsolateTasks>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn isolate_key(isolate: &v8::Isolate) -> usize {
    // SAFETY: `UnsafeRawIsolatePtr` is a transparent wrapper around the
    // isolate pointer V8 passes to the platform callbacks.
    unsafe { std::mem::transmute::<v8::UnsafeRawIsolatePtr, usize>(isolate.as_raw_isolate_ptr()) }
}

const _: () =
    assert!(std::mem::size_of::<v8::UnsafeRawIsolatePtr>() == std::mem::size_of::<usize>());

pub(crate) fn register_isolate(key: usize, waker: Arc<AtomicWaker>) -> Arc<Mutex<Vec<v8::Task>>> {
    let queue = Arc::new(Mutex::new(Vec::new()));
    ISOLATES.lock().expect("isolate registry poisoned").insert(
        key,
        IsolateTasks {
            queue: Arc::clone(&queue),
            waker,
            runtime: tokio::runtime::Handle::try_current().ok(),
        },
    );
    queue
}

pub(crate) fn unregister_isolate(key: usize) {
    ISOLATES
        .lock()
        .expect("isolate registry poisoned")
        .remove(&key);
}

fn queue_task(key: usize, task: v8::Task) {
    let isolates = ISOLATES.lock().expect("isolate registry poisoned");
    if let Some(entry) = isolates.get(&key) {
        entry.queue.lock().expect("task queue poisoned").push(task);
        entry.waker.wake();
    }
}

fn queue_delayed_task(key: usize, task: v8::Task, delay_in_seconds: f64) {
    let isolates = ISOLATES.lock().expect("isolate registry poisoned");
    let Some(entry) = isolates.get(&key) else {
        return;
    };
    let queue = Arc::clone(&entry.queue);
    let waker = Arc::clone(&entry.waker);
    let delay = Duration::from_secs_f64(delay_in_seconds.max(0.0));
    let deliver = move || {
        queue.lock().expect("task queue poisoned").push(task);
        waker.wake();
    };
    match &entry.runtime {
        Some(runtime) => {
            runtime.spawn(async move {
                tokio::time::sleep(delay).await;
                deliver();
            });
        }
        None => {
            let _ = std::thread::Builder::new()
                .name("dd-v8-delayed-task".to_string())
                .spawn(move || {
                    std::thread::sleep(delay);
                    deliver();
                });
        }
    }
}

struct ForegroundTasks;

impl v8::PlatformImpl for ForegroundTasks {
    fn post_task(&self, isolate: *mut c_void, task: v8::Task) {
        queue_task(isolate as usize, task);
    }

    fn post_non_nestable_task(&self, isolate: *mut c_void, task: v8::Task) {
        queue_task(isolate as usize, task);
    }

    fn post_delayed_task(&self, isolate: *mut c_void, task: v8::Task, delay_in_seconds: f64) {
        queue_delayed_task(isolate as usize, task, delay_in_seconds);
    }

    fn post_non_nestable_delayed_task(
        &self,
        isolate: *mut c_void,
        task: v8::Task,
        delay_in_seconds: f64,
    ) {
        queue_delayed_task(isolate as usize, task, delay_in_seconds);
    }
}
