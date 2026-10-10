use crate::error::Error;
use crate::inspector::{Inspector, InspectorHandle};
use crate::modules::{
    self, DynamicImport, ModuleCode, ModuleId, ModuleLoader, ModuleMap, ModuleType, NoModules,
    PendingEvaluation,
};
use crate::ops::{self, AsyncResult, OpDecl};
use crate::platform;
use crate::serde_v8;
use crate::state::OpState;
use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::task::AtomicWaker;
use std::borrow::Cow;
use std::cell::RefCell;
use std::ffi::c_void;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use v8::MapFnTo;

/// At most this many settled ops resolve per event loop tick, so a flood of
/// completions cannot starve the host loop.
const MAX_OPS_PER_TICK: usize = 1024;

/// Context data indices of a snapshot.
const SNAPSHOT_OPS_OBJECT: usize = 0;
const SNAPSHOT_OP_NAMES: usize = 1;
const SNAPSHOT_INTERNALS: usize = 2;

#[derive(Default)]
pub struct RuntimeOptions {
    /// The ops, in the order their functions are created. A runtime restored
    /// from a snapshot must list the same ops in the same order.
    pub ops: Vec<OpDecl>,
    pub module_loader: Option<Rc<dyn ModuleLoader>>,
    pub startup_snapshot: Option<&'static [u8]>,
    /// The V8 heap limit; 0 keeps V8's default.
    pub max_heap_bytes: usize,
    /// The most one op argument may copy into Rust; 0 keeps
    /// [`serde_v8::DEFAULT_MAX_OP_ARGUMENT_BYTES`].
    pub max_op_argument_bytes: usize,
}

/// An async op waiting on its future.
pub(crate) struct PendingOp {
    resolver: Option<v8::Global<v8::PromiseResolver>>,
    future: Pin<Box<dyn Future<Output = Box<dyn AsyncResult>>>>,
}

impl PendingOp {
    pub(crate) fn new(
        resolver: v8::Global<v8::PromiseResolver>,
        future: Pin<Box<dyn Future<Output = Box<dyn AsyncResult>>>>,
    ) -> Self {
        Self {
            resolver: Some(resolver),
            future,
        }
    }
}

impl Future for PendingOp {
    type Output = (v8::Global<v8::PromiseResolver>, Box<dyn AsyncResult>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = std::task::ready!(self.future.as_mut().poll(cx));
        let resolver = self
            .resolver
            .take()
            .expect("pending op polled after completion");
        Poll::Ready((resolver, result))
    }
}

/// Runtime state the V8 callbacks reach through the isolate's slot.
pub(crate) struct RuntimeState {
    pub(crate) op_state: Rc<RefCell<OpState>>,
    pub(crate) max_op_argument_bytes: usize,
    pub(crate) ops: Vec<OpDecl>,
    pub(crate) pending_ops: RefCell<FuturesUnordered<PendingOp>>,
    pub(crate) waker: Arc<AtomicWaker>,
    pub(crate) loader: Rc<dyn ModuleLoader>,
    pub(crate) modules: RefCell<ModuleMap>,
    pub(crate) dynamic_imports: RefCell<Vec<DynamicImport>>,
    pub(crate) pending_evaluations: RefCell<Vec<PendingEvaluation>>,
    rejections: RefCell<Vec<(v8::Global<v8::Promise>, v8::Global<v8::Value>)>>,
    ops_object: RefCell<Option<v8::Global<v8::Object>>>,
    internals: RefCell<Option<v8::Global<v8::Value>>>,
    /// Streaming WebAssembly compilations JavaScript is feeding, by id.
    pub(crate) wasm_streams: RefCell<WasmStreams>,
    tasks: Arc<Mutex<Vec<v8::Task>>>,
    inspector: RefCell<Option<Rc<Inspector>>>,
}

#[derive(Default)]
pub(crate) struct WasmStreams {
    pub(crate) next: u32,
    pub(crate) streams: std::collections::HashMap<u32, v8::WasmStreaming<false>>,
}

impl RuntimeState {
    fn has_pending_work(&self, isolate: &v8::Isolate) -> bool {
        isolate.has_pending_background_tasks()
            || !self.pending_ops.borrow().is_empty()
            || !self.dynamic_imports.borrow().is_empty()
            || !self.pending_evaluations.borrow().is_empty()
    }

    /// Drops every V8 handle the state holds.
    fn clear_handles(&self) {
        self.pending_ops.borrow_mut().clear();
        self.modules.borrow_mut().clear();
        self.dynamic_imports.borrow_mut().clear();
        self.pending_evaluations.borrow_mut().clear();
        self.rejections.borrow_mut().clear();
        self.ops_object.borrow_mut().take();
        self.internals.borrow_mut().take();
        self.wasm_streams.borrow_mut().streams.clear();
        if let Ok(mut op_state) = self.op_state.try_borrow_mut() {
            *op_state = OpState::new(Arc::clone(&self.waker));
        }
    }
}

pub(crate) fn runtime_state(isolate: &v8::Isolate) -> Rc<RuntimeState> {
    Rc::clone(
        isolate
            .get_slot::<Rc<RuntimeState>>()
            .expect("isolate belongs to a dd_v8 runtime"),
    )
}

/// The object holding the ops of the runtime that owns the scope's isolate.
pub(crate) fn runtime_ops_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Option<v8::Local<'s, v8::Object>> {
    let state = runtime_state(scope);
    let ops = state.ops_object.borrow();
    Some(v8::Local::new(scope, ops.as_ref()?))
}

/// The op state of the runtime that owns `isolate`, for raw ops.
pub fn runtime_op_state(isolate: &v8::Isolate) -> Rc<RefCell<OpState>> {
    Rc::clone(&runtime_state(isolate).op_state)
}

/// The inspector of the runtime that owns `isolate`, if it has one.
pub(crate) fn runtime_inspector(isolate: &v8::Isolate) -> Option<Rc<Inspector>> {
    isolate
        .get_slot::<Rc<RuntimeState>>()?
        .inspector
        .borrow()
        .clone()
}

/// Stops a runtime's JavaScript from any thread.
#[derive(Clone)]
pub struct RuntimeHandle {
    isolate: v8::IsolateHandle,
    inspector: Arc<OnceLock<InspectorHandle>>,
}

impl RuntimeHandle {
    /// Terminates the JavaScript running on the runtime's thread, letting go
    /// of a thread the debugger holds at a breakpoint or one waiting for a
    /// debugger; the inspector stops pausing for good. Returns false once the
    /// isolate is gone.
    pub fn terminate_execution(&self) -> bool {
        let terminated = self.isolate.terminate_execution();
        if let Some(inspector) = self.inspector.get() {
            inspector.close();
        }
        terminated
    }
}

impl std::fmt::Debug for RuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeHandle").finish_non_exhaustive()
    }
}

type HeapLimitCallback = Box<dyn FnMut(usize, usize) -> usize>;

/// One isolate with one context, its ops, module map, and event loop.
pub struct JsRuntime {
    state: Rc<RuntimeState>,
    context: Option<v8::Global<v8::Context>>,
    heap_limit_callback: Option<*mut HeapLimitCallback>,
    isolate_key: usize,
    isolate: Option<v8::OwnedIsolate>,
    /// Filled once the inspector is enabled, for [`RuntimeHandle`]s.
    inspector_handle: Arc<OnceLock<InspectorHandle>>,
    /// Created by `new_for_snapshot`: V8 aborts unless such an isolate is
    /// consumed by `create_blob`, even when no snapshot is taken.
    will_snapshot: bool,
}

/// Opens a handle scope on `$runtime`'s isolate entered into its context, as
/// `&mut v8::PinScope` named `$scope`.
#[macro_export]
macro_rules! scope {
    ($scope:ident, $runtime:expr) => {
        let (__dd_v8_isolate, __dd_v8_context) = $runtime.isolate_and_context();
        $crate::v8::scope!(let $scope, __dd_v8_isolate);
        let __dd_v8_context = $crate::v8::Local::new($scope, __dd_v8_context);
        let $scope = &mut $crate::v8::ContextScope::new($scope, __dd_v8_context);
    };
}

impl JsRuntime {
    pub fn new(options: RuntimeOptions) -> Result<Self, Error> {
        Self::build(options, false)
    }

    /// A runtime whose context can be captured with [`JsRuntime::snapshot`].
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
        } = options;
        let mut params = v8::CreateParams::default();
        if max_heap_bytes > 0 {
            params = params.heap_limits(0, max_heap_bytes);
        }
        let external_references = Cow::Owned(vec![v8::ExternalReference {
            function: ops::dispatch.map_fn_to(),
        }]);
        let restore = startup_snapshot.filter(|_| !will_snapshot);
        let mut isolate = if will_snapshot {
            v8::Isolate::snapshot_creator(Some(external_references), Some(params))
        } else {
            if let Some(snapshot) = restore {
                params = params.snapshot_blob(v8::StartupData::from(snapshot));
            }
            v8::Isolate::new(params.external_references(external_references))
        };
        isolate.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);
        isolate.set_promise_reject_callback(promise_reject_callback);
        isolate.set_host_initialize_import_meta_object_callback(modules::import_meta_callback);
        isolate.set_host_import_module_dynamically_callback(modules::dynamic_import_callback);
        isolate.set_wasm_streaming_callback(crate::builtins::wasm_streaming_callback);

        let waker = Arc::new(AtomicWaker::new());
        let isolate_key = platform::isolate_key(&isolate);
        let tasks = platform::register_isolate(isolate_key, Arc::clone(&waker));
        let state = Rc::new(RuntimeState {
            max_op_argument_bytes: match max_op_argument_bytes {
                0 => serde_v8::DEFAULT_MAX_OP_ARGUMENT_BYTES,
                bytes => bytes,
            },
            op_state: Rc::new(RefCell::new(OpState::new(Arc::clone(&waker)))),
            ops,
            pending_ops: RefCell::new(FuturesUnordered::new()),
            waker,
            loader: module_loader.unwrap_or_else(|| Rc::new(NoModules)),
            modules: RefCell::new(ModuleMap::default()),
            dynamic_imports: RefCell::new(Vec::new()),
            pending_evaluations: RefCell::new(Vec::new()),
            rejections: RefCell::new(Vec::new()),
            ops_object: RefCell::new(None),
            internals: RefCell::new(None),
            wasm_streams: RefCell::new(WasmStreams::default()),
            tasks,
            inspector: RefCell::new(None),
        });
        isolate.set_slot(Rc::clone(&state));

        let mut runtime = Self {
            state: Rc::clone(&state),
            context: None,
            heap_limit_callback: None,
            isolate_key,
            isolate: None,
            inspector_handle: Arc::new(OnceLock::new()),
            will_snapshot,
        };
        let context = {
            v8::scope!(let scope, &mut isolate);
            let context = match restore {
                Some(_) => v8::Context::from_snapshot(scope, 0, Default::default())
                    .ok_or_else(|| Error::new("startup snapshot has no context"))?,
                None => v8::Context::new(scope, Default::default()),
            };
            let scope = &mut v8::ContextScope::new(scope, context);
            let ops_object = match restore {
                Some(_) => restore_ops_object(scope, &state.ops)?,
                None => ops::ops_object(scope, &state.ops),
            };
            *state.ops_object.borrow_mut() = Some(v8::Global::new(scope, ops_object));
            if restore.is_some() {
                let internals = scope
                    .get_context_data_from_snapshot_once::<v8::Value>(SNAPSHOT_INTERNALS)
                    .map_err(|error| {
                        Error::new(format!("startup snapshot has no internals: {error:?}"))
                    })?;
                if !internals.is_undefined() {
                    *state.internals.borrow_mut() = Some(v8::Global::new(scope, internals));
                }
            }
            v8::Global::new(scope, context)
        };
        runtime.context = Some(context);
        runtime.isolate = Some(isolate);
        Ok(runtime)
    }

    pub fn op_state(&self) -> Rc<RefCell<OpState>> {
        Rc::clone(&self.state.op_state)
    }

    pub fn v8_isolate(&mut self) -> &mut v8::OwnedIsolate {
        self.isolate.as_mut().expect("runtime isolate")
    }

    /// A handle that terminates this runtime's JavaScript from any thread.
    pub fn handle(&mut self) -> RuntimeHandle {
        RuntimeHandle {
            isolate: self.v8_isolate().thread_safe_handle(),
            inspector: Arc::clone(&self.inspector_handle),
        }
    }

    /// Registers the context with a V8 inspector, named `name` in DevTools,
    /// and returns the handle sessions connect through. Scripts the runtime
    /// already ran show up too; enable it before the code to debug runs.
    /// Enabling it again returns the same handle.
    pub fn enable_inspector(&mut self, name: &str) -> Result<InspectorHandle, Error> {
        if self.will_snapshot {
            return Err(Error::new("a snapshot runtime cannot have an inspector"));
        }
        if let Some(handle) = self.inspector_handle.get() {
            return Ok(handle.clone());
        }
        let waker = Arc::clone(&self.state.waker);
        let inspector = {
            crate::scope!(scope, self);
            let context = scope.get_current_context();
            Inspector::new(scope, context, waker, name)
        };
        let handle = inspector.handle();
        *self.state.inspector.borrow_mut() = Some(Rc::new(inspector));
        let _ = self.inspector_handle.set(handle.clone());
        Ok(handle)
    }

    /// Blocks until a DevTools session sends
    /// `Runtime.runIfWaitingForDebugger` (as DevTools does once it has set
    /// its breakpoints), answering every message meanwhile. Returns at once
    /// without an inspector, and when the runtime is terminated.
    pub fn wait_for_debugger(&mut self) {
        let inspector = self.state.inspector.borrow().clone();
        if let Some(inspector) = inspector {
            crate::scope!(_scope, self);
            inspector.wait_for_debugger();
        }
    }

    pub fn main_context(&self) -> v8::Global<v8::Context> {
        self.context.clone().expect("runtime context")
    }

    /// The isolate and context, borrowed together for [`scope!`].
    pub fn isolate_and_context(&mut self) -> (&mut v8::OwnedIsolate, &v8::Global<v8::Context>) {
        (
            self.isolate.as_mut().expect("runtime isolate"),
            self.context.as_ref().expect("runtime context"),
        )
    }

    /// Replaces the callback V8 calls as the heap nears its limit. It gets
    /// the current and initial limits and returns the new limit.
    pub fn set_near_heap_limit_callback(
        &mut self,
        callback: impl FnMut(usize, usize) -> usize + 'static,
    ) {
        self.remove_near_heap_limit_callback();
        let data = Box::into_raw(Box::new(Box::new(callback) as HeapLimitCallback));
        self.v8_isolate()
            .add_near_heap_limit_callback(near_heap_limit_callback, data.cast::<c_void>());
        self.heap_limit_callback = Some(data);
    }

    fn remove_near_heap_limit_callback(&mut self) {
        if let Some(data) = self.heap_limit_callback.take() {
            if let Some(isolate) = self.isolate.as_mut() {
                isolate.remove_near_heap_limit_callback(near_heap_limit_callback, 0);
            }
            // SAFETY: `data` came from `Box::into_raw` and V8 no longer holds it.
            drop(unsafe { Box::from_raw(data) });
        }
    }

    /// Runs a classic script and returns its completion value.
    pub fn execute_script(
        &mut self,
        name: &str,
        source: &str,
    ) -> Result<v8::Global<v8::Value>, Error> {
        crate::scope!(scope, self);
        v8::tc_scope!(let tc, scope);
        let origin = script_origin(tc, name, false)?;
        let source = v8::String::new(tc, source)
            .ok_or_else(|| Error::new(format!("script is too large: {name}")))?;
        let Some(script) = v8::Script::compile(tc, source, Some(&origin)) else {
            return Err(Error::from_try_catch(tc, "Uncaught"));
        };
        match script.run(tc) {
            Some(value) => Ok(v8::Global::new(tc, value)),
            None => Err(Error::from_try_catch(tc, "Uncaught")),
        }
    }

    /// Runs `source` as the body of a function whose one parameter, `ops`,
    /// holds this runtime's op functions by name, and returns its result.
    pub fn execute_with_ops(
        &mut self,
        name: &str,
        source: &str,
    ) -> Result<v8::Global<v8::Value>, Error> {
        let ops_object = self
            .state
            .ops_object
            .borrow()
            .clone()
            .ok_or_else(|| Error::new("runtime has no ops object"))?;
        let ops_object = {
            crate::scope!(scope, self);
            let ops_object = v8::Local::new(scope, &ops_object);
            v8::Global::new(scope, v8::Local::<v8::Value>::from(ops_object))
        };
        self.execute_function(name, source, &["ops"], &[ops_object])
    }

    /// Runs `source` as the body of a function with the parameters `params`,
    /// called with `args`, and returns its result. Nothing the source
    /// declares becomes global.
    pub fn execute_function(
        &mut self,
        name: &str,
        source: &str,
        params: &[&str],
        args: &[v8::Global<v8::Value>],
    ) -> Result<v8::Global<v8::Value>, Error> {
        crate::scope!(scope, self);
        v8::tc_scope!(let tc, scope);
        let origin = script_origin(tc, name, false)?;
        let source = v8::String::new(tc, source)
            .ok_or_else(|| Error::new(format!("script is too large: {name}")))?;
        let mut source = v8::script_compiler::Source::new(source, Some(&origin));
        let params = params
            .iter()
            .map(|param| serde_v8::key(tc, param))
            .collect::<Vec<_>>();
        let Some(function) = v8::script_compiler::compile_function(
            tc,
            &mut source,
            &params,
            &[],
            v8::script_compiler::CompileOptions::NoCompileOptions,
            v8::script_compiler::NoCacheReason::NoReason,
        ) else {
            return Err(Error::from_try_catch(tc, "Uncaught"));
        };
        let args = args
            .iter()
            .map(|arg| v8::Local::new(tc, arg))
            .collect::<Vec<_>>();
        let receiver = v8::undefined(tc).into();
        match function.call(tc, receiver, &args) {
            Some(value) => Ok(v8::Global::new(tc, value)),
            None => Err(Error::from_try_catch(tc, "Uncaught")),
        }
    }

    /// Calls `function` with an undefined receiver and returns its result.
    pub fn call_function(
        &mut self,
        function: &v8::Global<v8::Function>,
        args: &[v8::Global<v8::Value>],
    ) -> Result<v8::Global<v8::Value>, Error> {
        crate::scope!(scope, self);
        v8::tc_scope!(let tc, scope);
        let function = v8::Local::new(tc, function);
        let args = args
            .iter()
            .map(|arg| v8::Local::new(tc, arg))
            .collect::<Vec<_>>();
        let receiver = v8::undefined(tc).into();
        match function.call(tc, receiver, &args) {
            Some(value) => Ok(v8::Global::new(tc, value)),
            None => Err(Error::from_try_catch(tc, "Uncaught")),
        }
    }

    /// Keeps `value` as the runtime's internals: the embedder's private
    /// handle on its own JavaScript, carried through snapshots and reachable
    /// from no script.
    pub fn set_internals(&mut self, value: v8::Global<v8::Value>) {
        *self.state.internals.borrow_mut() = Some(value);
    }

    pub fn internals(&self) -> Option<v8::Global<v8::Value>> {
        self.state.internals.borrow().clone()
    }

    /// Loads `name` (from `code`, or through the module loader) and its
    /// imports as the main module, which sees `import.meta.main === true`.
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
        let state = Rc::clone(&self.state);
        crate::scope!(scope, self);
        modules::load_graph(
            scope,
            &state,
            name,
            ModuleType::JavaScript,
            code.map(ModuleCode::String),
            main,
        )
    }

    /// Evaluates a loaded module, running the event loop until its top-level
    /// code (including any top-level `await`) settles.
    pub async fn evaluate_module(&mut self, id: ModuleId) -> Result<(), Error> {
        let promise = {
            let state = Rc::clone(&self.state);
            crate::scope!(scope, self);
            let module = {
                let modules = state.modules.borrow();
                let handle = modules
                    .handle(id)
                    .ok_or_else(|| Error::new(format!("no module with id {id}")))?;
                v8::Local::new(scope, handle)
            };
            v8::tc_scope!(let tc, scope);
            let Some(result) = module.evaluate(tc) else {
                return Err(Error::from_try_catch(tc, "Uncaught"));
            };
            let promise = v8::Local::<v8::Promise>::try_from(result)
                .map_err(|_| Error::new("module evaluation did not return a promise"))?;
            promise.mark_as_handled();
            v8::Global::new(tc, promise)
        };
        self.settle_promise(promise, "Top-level await promise never resolved")
            .await
            .map(drop)
    }

    pub fn module_namespace(&mut self, id: ModuleId) -> Result<v8::Global<v8::Object>, Error> {
        let state = Rc::clone(&self.state);
        crate::scope!(scope, self);
        let modules = state.modules.borrow();
        let handle = modules
            .handle(id)
            .ok_or_else(|| Error::new(format!("no module with id {id}")))?;
        let module = v8::Local::new(scope, handle);
        if !matches!(
            module.get_status(),
            v8::ModuleStatus::Evaluated | v8::ModuleStatus::Evaluating
        ) {
            return Err(Error::new(format!("module {id} has not been evaluated")));
        }
        let namespace = v8::Local::<v8::Object>::try_from(module.get_module_namespace())
            .map_err(|_| Error::new("module namespace is not an object"))?;
        Ok(v8::Global::new(scope, namespace))
    }

    /// Resolves `value` if it is a promise, running the event loop until it
    /// settles; returns any other value as is.
    pub async fn resolve(
        &mut self,
        value: v8::Global<v8::Value>,
    ) -> Result<v8::Global<v8::Value>, Error> {
        let promise = {
            crate::scope!(scope, self);
            let value = v8::Local::new(scope, &value);
            match v8::Local::<v8::Promise>::try_from(value) {
                Ok(promise) => v8::Global::new(scope, promise),
                Err(_) => return Ok(v8::Global::new(scope, value)),
            }
        };
        self.settle_promise(
            promise,
            "Promise resolution is still pending but the event loop has already resolved",
        )
        .await
    }

    async fn settle_promise(
        &mut self,
        promise: v8::Global<v8::Promise>,
        stalled: &'static str,
    ) -> Result<v8::Global<v8::Value>, Error> {
        poll_fn(|cx| {
            if let Some(result) = self.promise_outcome(&promise) {
                return Poll::Ready(result);
            }
            match self.poll_event_loop(cx) {
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => Poll::Ready(
                    self.promise_outcome(&promise)
                        .unwrap_or_else(|| Err(Error::new(stalled))),
                ),
                Poll::Pending => match self.promise_outcome(&promise) {
                    Some(result) => Poll::Ready(result),
                    None => Poll::Pending,
                },
            }
        })
        .await
    }

    fn promise_outcome(
        &mut self,
        promise: &v8::Global<v8::Promise>,
    ) -> Option<Result<v8::Global<v8::Value>, Error>> {
        crate::scope!(scope, self);
        let promise = v8::Local::new(scope, promise);
        match promise.state() {
            v8::PromiseState::Pending => None,
            v8::PromiseState::Fulfilled => {
                let value = promise.result(scope);
                Some(Ok(v8::Global::new(scope, value)))
            }
            v8::PromiseState::Rejected => {
                let reason = promise.result(scope);
                Some(Err(Error::from_exception(scope, reason, "Uncaught")))
            }
        }
    }

    /// Runs the event loop until no op, import, or evaluation is pending.
    pub async fn run_event_loop(&mut self) -> Result<(), Error> {
        poll_fn(|cx| self.poll_event_loop(cx)).await
    }

    /// One turn of the event loop: V8's foreground tasks, microtasks, settled
    /// async ops, and dynamic imports. Ready once nothing is pending; an
    /// unhandled promise rejection ends the loop with an error.
    pub fn poll_event_loop(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let state = Rc::clone(&self.state);
        state.waker.register(cx.waker());
        crate::scope!(scope, self);

        let inspector = state.inspector.borrow().clone();
        if let Some(inspector) = inspector {
            inspector.poll();
        }
        let tasks = std::mem::take(&mut *state.tasks.lock().expect("task queue poisoned"));
        for task in tasks {
            task.run();
        }
        scope.perform_microtask_checkpoint();
        if scope.is_execution_terminating() {
            return Poll::Ready(Err(Error::terminated()));
        }

        let mut settled = Vec::new();
        {
            let mut pending = state.pending_ops.borrow_mut();
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
        for (resolver, result) in settled {
            let resolver = v8::Local::new(scope, &resolver);
            ops::settle(scope, resolver, result);
        }
        scope.perform_microtask_checkpoint();
        while modules::poll_dynamic_imports(scope, &state) {
            scope.perform_microtask_checkpoint();
        }
        if scope.is_execution_terminating() {
            return Poll::Ready(Err(Error::terminated()));
        }

        if let Some(error) = take_unhandled_rejection(scope, &state) {
            return Poll::Ready(Err(error));
        }
        if state.has_pending_work(scope) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Captures the context, consuming the runtime. A runtime with loaded
    /// modules or pending ops cannot be captured.
    pub fn snapshot(mut self) -> Result<Box<[u8]>, Error> {
        if !self.state.modules.borrow().is_empty() {
            return Err(Error::new("a snapshot cannot include ES modules"));
        }
        let state = Rc::clone(&self.state);
        if state.has_pending_work(self.v8_isolate()) {
            return Err(Error::new("a snapshot cannot include pending async work"));
        }
        {
            let ops_object = self
                .state
                .ops_object
                .borrow()
                .clone()
                .ok_or_else(|| Error::new("runtime has no ops object"))?;
            let op_names = self.state.ops.iter().map(|op| op.name).collect::<Vec<_>>();
            let internals = self.internals();
            let (isolate, context) = self.isolate_and_context();
            v8::scope!(let scope, isolate);
            let context = v8::Local::new(scope, context);
            let scope = &mut v8::ContextScope::new(scope, context);
            let ops_object = v8::Local::new(scope, &ops_object);
            let internals = match internals.as_ref() {
                Some(internals) => v8::Local::new(scope, internals),
                None => v8::undefined(scope).into(),
            };
            let names = op_names
                .iter()
                .map(|name| serde_v8::key(scope, name).into())
                .collect::<Vec<v8::Local<v8::Value>>>();
            let names = v8::Array::new_with_elements(scope, &names);
            let default_context = v8::Context::new(scope, Default::default());
            scope.set_default_context(default_context);
            let ops_index = scope.add_context_data(context, ops_object);
            let names_index = scope.add_context_data(context, names);
            let internals_index = scope.add_context_data(context, internals);
            debug_assert_eq!(
                (ops_index, names_index, internals_index),
                (SNAPSHOT_OPS_OBJECT, SNAPSHOT_OP_NAMES, SNAPSHOT_INTERNALS)
            );
            scope.add_context(context);
        }
        self.release_handles();
        let isolate = self.isolate.take().expect("runtime isolate");
        let blob = isolate
            .create_blob(v8::FunctionCodeHandling::Keep)
            .ok_or_else(|| Error::new("V8 could not create the snapshot"))?;
        Ok(blob.to_vec().into_boxed_slice())
    }

    fn release_handles(&mut self) {
        if let Some(handle) = self.inspector_handle.get() {
            handle.close();
        }
        let inspector = self.state.inspector.borrow_mut().take();
        if let Some(inspector) = inspector {
            if let (Some(isolate), Some(context)) = (self.isolate.as_mut(), self.context.as_ref()) {
                v8::scope!(let scope, isolate);
                let context = v8::Local::new(scope, context);
                inspector.context_destroyed(context);
            }
            drop(inspector);
        }
        platform::unregister_isolate(self.isolate_key);
        self.remove_near_heap_limit_callback();
        self.state.clear_handles();
        self.context = None;
        if let Some(isolate) = self.isolate.as_mut() {
            isolate.remove_slot::<Rc<RuntimeState>>();
        }
    }
}

impl Drop for JsRuntime {
    fn drop(&mut self) {
        self.release_handles();
        let Some(mut isolate) = self.isolate.take() else {
            return;
        };
        if self.will_snapshot {
            // A snapshot runtime dropped without `snapshot()` (its setup
            // failed) still has to go through `create_blob`.
            {
                v8::scope!(let scope, &mut isolate);
                let context = v8::Context::new(scope, Default::default());
                scope.set_default_context(context);
            }
            let _ = isolate.create_blob(v8::FunctionCodeHandling::Clear);
        }
    }
}

fn restore_ops_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    ops: &[OpDecl],
) -> Result<v8::Local<'s, v8::Object>, Error> {
    let object = scope
        .get_context_data_from_snapshot_once::<v8::Object>(SNAPSHOT_OPS_OBJECT)
        .map_err(|error| Error::new(format!("startup snapshot has no ops object: {error:?}")))?;
    let names = scope
        .get_context_data_from_snapshot_once::<v8::Array>(SNAPSHOT_OP_NAMES)
        .map_err(|error| Error::new(format!("startup snapshot has no op names: {error:?}")))?;
    let mut snapshot_names = Vec::with_capacity(names.length() as usize);
    for index in 0..names.length() {
        let name = names
            .get_index(scope, index)
            .map(|name| name.to_rust_string_lossy(scope))
            .unwrap_or_default();
        snapshot_names.push(name);
    }
    let names = ops.iter().map(|op| op.name).collect::<Vec<_>>();
    if snapshot_names != names {
        return Err(Error::new(format!(
            "startup snapshot ops {snapshot_names:?} do not match runtime ops {names:?}"
        )));
    }
    Ok(object)
}

fn script_origin<'s>(
    scope: &v8::PinScope<'s, '_>,
    name: &str,
    is_module: bool,
) -> Result<v8::ScriptOrigin<'s>, Error> {
    let name = v8::String::new(scope, name)
        .ok_or_else(|| Error::new(format!("script name is too long: {name}")))?;
    Ok(v8::ScriptOrigin::new(
        scope,
        name.into(),
        0,
        0,
        false,
        -1,
        None,
        false,
        false,
        is_module,
        None,
    ))
}

fn take_unhandled_rejection(
    scope: &mut v8::PinScope<'_, '_>,
    state: &RuntimeState,
) -> Option<Error> {
    let rejections = std::mem::take(&mut *state.rejections.borrow_mut());
    let (_, reason) = rejections.into_iter().next()?;
    let reason = v8::Local::new(scope, &reason);
    Some(Error::from_exception(
        scope,
        reason,
        "Uncaught (in promise)",
    ))
}

extern "C" fn promise_reject_callback(message: v8::PromiseRejectMessage) {
    // SAFETY: V8 calls this on the isolate's thread while it is entered.
    v8::callback_scope!(unsafe scope, &message);
    let Some(state) = scope.get_slot::<Rc<RuntimeState>>().cloned() else {
        return;
    };
    let promise = message.get_promise();
    match message.get_event() {
        v8::PromiseRejectEvent::PromiseRejectWithNoHandler => {
            let reason = message
                .get_value()
                .unwrap_or_else(|| v8::undefined(scope).into());
            state.rejections.borrow_mut().push((
                v8::Global::new(scope, promise),
                v8::Global::new(scope, reason),
            ));
        }
        v8::PromiseRejectEvent::PromiseHandlerAddedAfterReject => {
            state
                .rejections
                .borrow_mut()
                .retain(|(rejected, _)| v8::Local::new(scope, rejected) != promise);
        }
        _ => {}
    }
}

extern "C" fn near_heap_limit_callback(
    data: *mut c_void,
    current_heap_limit: usize,
    initial_heap_limit: usize,
) -> usize {
    // SAFETY: `data` is the boxed callback registered with this function and
    // stays alive until it is removed.
    let callback = unsafe { &mut *data.cast::<HeapLimitCallback>() };
    callback(current_heap_limit, initial_heap_limit)
}
