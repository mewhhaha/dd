//! ES modules: loading a graph through the embedder's [`ModuleLoader`],
//! instantiating it, and the V8 callbacks that resolve imports,
//! initialize `import.meta`, and start dynamic imports.

use crate::error::Error;
use crate::runtime::{RuntimeState, runtime_state};
use crate::serde_v8;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub type ModuleId = usize;

/// What a module evaluates to, chosen by the import's `type` attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModuleType {
    JavaScript,
    /// `with { type: "json" }`: the parsed JSON as the default export.
    Json,
    /// `with { type: "text" }`: the source as a string default export.
    Text,
    /// `with { type: "bytes" }`: the source as a `Uint8Array` default export.
    Bytes,
}

impl ModuleType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::JavaScript => "javascript",
            Self::Json => "json",
            Self::Text => "text",
            Self::Bytes => "bytes",
        }
    }
}

impl fmt::Display for ModuleType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub enum ModuleCode {
    String(String),
    Bytes(Arc<[u8]>),
}

pub struct ModuleSource {
    pub module_type: ModuleType,
    pub code: ModuleCode,
}

impl ModuleSource {
    pub fn new(module_type: ModuleType, code: ModuleCode) -> Self {
        Self { module_type, code }
    }
}

pub trait ModuleLoader {
    /// Resolves `specifier`, imported by the module named `referrer`, to the
    /// name of the module it refers to.
    fn resolve(&self, specifier: &str, referrer: &str) -> Result<String, String>;

    /// Loads the module named `name` for an import that requested
    /// `requested_type`.
    fn load(&self, name: &str, requested_type: ModuleType) -> Result<ModuleSource, String>;
}

/// A loader for runtimes that load no modules.
pub struct NoModules;

impl ModuleLoader for NoModules {
    fn resolve(&self, specifier: &str, _referrer: &str) -> Result<String, String> {
        Err(format!("module loading is not supported: {specifier}"))
    }

    fn load(&self, name: &str, _requested_type: ModuleType) -> Result<ModuleSource, String> {
        Err(format!("module loading is not supported: {name}"))
    }
}

struct ModuleRecord {
    name: String,
    handle: v8::Global<v8::Module>,
    /// The default export of a JSON, text, or bytes module.
    synthetic_value: Option<v8::Global<v8::Value>>,
    /// Each static import of this module and the module it resolved to.
    requests: Vec<(String, ModuleType, ModuleId)>,
}

#[derive(Default)]
pub(crate) struct ModuleMap {
    modules: Vec<ModuleRecord>,
    ids: HashMap<(String, ModuleType), ModuleId>,
    by_hash: HashMap<i32, Vec<ModuleId>>,
    main: Option<ModuleId>,
}

impl ModuleMap {
    pub(crate) fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    pub(crate) fn handle(&self, id: ModuleId) -> Option<&v8::Global<v8::Module>> {
        self.modules.get(id).map(|record| &record.handle)
    }

    fn id_of(
        &self,
        scope: &v8::PinScope<'_, '_>,
        module: v8::Local<v8::Module>,
    ) -> Option<ModuleId> {
        let ids = self.by_hash.get(&module.get_identity_hash().get())?;
        ids.iter()
            .copied()
            .find(|id| v8::Local::new(scope, &self.modules[*id].handle) == module)
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }
}

/// Reads the `type` attribute from `[key, value, (offset,) ...]`.
pub(crate) fn requested_type(
    scope: &v8::PinScope<'_, '_>,
    attributes: v8::Local<v8::FixedArray>,
    stride: usize,
) -> Result<ModuleType, String> {
    let mut index = 0;
    while index + 1 < attributes.length() {
        let key = attributes
            .get(scope, index)
            .and_then(|key| v8::Local::<v8::Value>::try_from(key).ok())
            .map(|key| key.to_rust_string_lossy(scope));
        if key.as_deref() == Some("type") {
            let value = attributes
                .get(scope, index + 1)
                .and_then(|value| v8::Local::<v8::Value>::try_from(value).ok())
                .map(|value| value.to_rust_string_lossy(scope))
                .unwrap_or_default();
            return match value.as_str() {
                "json" => Ok(ModuleType::Json),
                "text" => Ok(ModuleType::Text),
                "bytes" => Ok(ModuleType::Bytes),
                other => Err(format!("unsupported module type \"{other}\"")),
            };
        }
        index += stride;
    }
    Ok(ModuleType::JavaScript)
}

fn code_string<'s>(
    scope: &v8::PinScope<'s, '_>,
    name: &str,
    code: &ModuleCode,
) -> Result<v8::Local<'s, v8::String>, Error> {
    let text = match code {
        ModuleCode::String(text) => text.as_bytes(),
        ModuleCode::Bytes(bytes) => bytes,
    };
    if std::str::from_utf8(text).is_err() {
        return Err(Error::new(format!("module is not valid UTF-8: {name}")));
    }
    v8::String::new_from_utf8(scope, text, v8::NewStringType::Normal)
        .ok_or_else(|| Error::new(format!("module source is too large: {name}")))
}

/// Compiles (or finds) one module. Returns its id and whether it is new.
fn register(
    scope: &mut v8::PinScope<'_, '_>,
    state: &RuntimeState,
    name: &str,
    module_type: ModuleType,
    code: Option<ModuleCode>,
) -> Result<(ModuleId, bool), Error> {
    if let Some(id) = state
        .modules
        .borrow()
        .ids
        .get(&(name.to_string(), module_type))
    {
        return Ok((*id, false));
    }
    let code = match code {
        Some(code) => code,
        None => {
            let source = state.loader.load(name, module_type).map_err(Error::new)?;
            if source.module_type != module_type {
                return Err(Error::new(format!(
                    "requested module type {module_type} does not match {} module: {name}",
                    source.module_type
                )));
            }
            source.code
        }
    };

    v8::tc_scope!(let tc, scope);
    let resource_name = v8::String::new(tc, name)
        .ok_or_else(|| Error::new(format!("module name is too long: {name}")))?;
    let (module, synthetic_value) = match module_type {
        ModuleType::JavaScript => {
            let source_text = code_string(tc, name, &code)?;
            let origin = v8::ScriptOrigin::new(
                tc,
                resource_name.into(),
                0,
                0,
                false,
                -1,
                None,
                false,
                false,
                true,
                None,
            );
            let mut source = v8::script_compiler::Source::new(source_text, Some(&origin));
            let Some(module) = v8::script_compiler::compile_module(tc, &mut source) else {
                return Err(Error::from_try_catch(tc, "Uncaught"));
            };
            (module, None)
        }
        synthetic => {
            let value: v8::Local<v8::Value> = match synthetic {
                ModuleType::Json => {
                    let text = code_string(tc, name, &code)?;
                    match v8::json::parse(tc, text) {
                        Some(value) => value,
                        None => return Err(Error::from_try_catch(tc, "Uncaught")),
                    }
                }
                ModuleType::Text => code_string(tc, name, &code)?.into(),
                ModuleType::Bytes => {
                    let bytes = match code {
                        ModuleCode::String(text) => text.into_bytes(),
                        ModuleCode::Bytes(bytes) => bytes.to_vec(),
                    };
                    serde_v8::uint8_array(tc, bytes).into()
                }
                ModuleType::JavaScript => unreachable!(),
            };
            let default = serde_v8::key(tc, "default");
            let module = v8::Module::create_synthetic_module(
                tc,
                resource_name,
                &[default],
                synthetic_module_evaluation_steps,
            );
            (module, Some(v8::Global::new(tc, value)))
        }
    };

    let mut modules = state.modules.borrow_mut();
    let id = modules.modules.len();
    modules.modules.push(ModuleRecord {
        name: name.to_string(),
        handle: v8::Global::new(tc, module),
        synthetic_value,
        requests: Vec::new(),
    });
    modules.ids.insert((name.to_string(), module_type), id);
    modules
        .by_hash
        .entry(module.get_identity_hash().get())
        .or_default()
        .push(id);
    Ok((id, true))
}

/// Loads `name` and every module it imports, then instantiates the graph.
pub(crate) fn load_graph(
    scope: &mut v8::PinScope<'_, '_>,
    state: &RuntimeState,
    name: &str,
    module_type: ModuleType,
    code: Option<ModuleCode>,
    main: bool,
) -> Result<ModuleId, Error> {
    let (root, new) = register(scope, state, name, module_type, code)?;
    if main {
        let mut modules = state.modules.borrow_mut();
        if modules.main.is_some_and(|existing| existing != root) {
            return Err(Error::new(format!(
                "a main module is already loaded; cannot load {name} as main"
            )));
        }
        modules.main = Some(root);
    }
    let mut queue = if new { vec![root] } else { Vec::new() };
    while let Some(id) = queue.pop() {
        let (referrer, module) = {
            let modules = state.modules.borrow();
            let record = &modules.modules[id];
            (record.name.clone(), v8::Local::new(scope, &record.handle))
        };
        if !module.is_source_text_module() {
            continue;
        }
        let requests = module.get_module_requests();
        for index in 0..requests.length() {
            let Some(request) = requests
                .get(scope, index)
                .and_then(|request| v8::Local::<v8::ModuleRequest>::try_from(request).ok())
            else {
                continue;
            };
            let specifier = request.get_specifier().to_rust_string_lossy(scope);
            let requested = requested_type(scope, request.get_import_attributes(), 3)
                .map_err(|error| Error::new(format!("{error} imported by {referrer}")))?;
            let resolved = state
                .loader
                .resolve(&specifier, &referrer)
                .map_err(Error::new)?;
            let (child, child_new) = register(scope, state, &resolved, requested, None)?;
            state.modules.borrow_mut().modules[id]
                .requests
                .push((specifier, requested, child));
            if child_new {
                queue.push(child);
            }
        }
    }

    let module = {
        let modules = state.modules.borrow();
        v8::Local::new(scope, &modules.modules[root].handle)
    };
    if module.get_status() == v8::ModuleStatus::Uninstantiated {
        v8::tc_scope!(let tc, scope);
        if module
            .instantiate_module(tc, resolve_module_callback)
            .is_none()
        {
            return Err(Error::from_try_catch(tc, "Uncaught"));
        }
    }
    Ok(root)
}

fn resolve_module_callback<'s>(
    context: v8::Local<'s, v8::Context>,
    specifier: v8::Local<'s, v8::String>,
    import_attributes: v8::Local<'s, v8::FixedArray>,
    referrer: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    // SAFETY: V8 calls this with a live context on the current thread.
    v8::callback_scope!(unsafe scope, context);
    let state = runtime_state(scope);
    let modules = state.modules.borrow();
    let referrer_id = modules.id_of(scope, referrer)?;
    let specifier = specifier.to_rust_string_lossy(scope);
    let requested = requested_type(scope, import_attributes, 3).ok()?;
    let (_, _, child) = modules.modules[referrer_id]
        .requests
        .iter()
        .find(|(name, module_type, _)| *name == specifier && *module_type == requested)?;
    Some(v8::Local::new(scope, &modules.modules[*child].handle))
}

fn synthetic_module_evaluation_steps<'s>(
    context: v8::Local<'s, v8::Context>,
    module: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Value>> {
    // SAFETY: V8 calls this with a live context on the current thread.
    v8::callback_scope!(unsafe scope, context);
    let state = runtime_state(scope);
    let value = {
        let modules = state.modules.borrow();
        let id = modules.id_of(scope, module)?;
        v8::Local::new(scope, modules.modules[id].synthetic_value.as_ref()?)
    };
    let default = serde_v8::key(scope, "default");
    module.set_synthetic_module_export(scope, default, value)?;
    let resolver = v8::PromiseResolver::new(scope)?;
    let undefined = v8::undefined(scope);
    resolver.resolve(scope, undefined.into());
    Some(resolver.get_promise(scope).into())
}

pub(crate) extern "C" fn import_meta_callback(
    context: v8::Local<v8::Context>,
    module: v8::Local<v8::Module>,
    meta: v8::Local<v8::Object>,
) {
    // SAFETY: V8 calls this with a live context on the current thread.
    v8::callback_scope!(unsafe scope, context);
    let state = runtime_state(scope);
    let modules = state.modules.borrow();
    let Some(id) = modules.id_of(scope, module) else {
        return;
    };
    let url_key = serde_v8::key(scope, "url");
    if let Some(url) = v8::String::new(scope, &modules.modules[id].name) {
        meta.create_data_property(scope, url_key.into(), url.into());
    }
    let main_key = serde_v8::key(scope, "main");
    let main = v8::Boolean::new(scope, modules.main == Some(id));
    meta.create_data_property(scope, main_key.into(), main.into());
}

/// A dynamic `import()` waiting for the event loop.
pub(crate) struct DynamicImport {
    pub(crate) resolver: v8::Global<v8::PromiseResolver>,
    pub(crate) specifier: String,
    pub(crate) referrer: String,
    pub(crate) module_type: ModuleType,
}

/// A dynamically imported module whose evaluation is still running.
pub(crate) struct PendingEvaluation {
    pub(crate) resolver: v8::Global<v8::PromiseResolver>,
    pub(crate) module: v8::Global<v8::Module>,
    pub(crate) promise: v8::Global<v8::Promise>,
}

pub(crate) fn dynamic_import_callback<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _host_defined_options: v8::Local<'s, v8::Data>,
    resource_name: v8::Local<'s, v8::Value>,
    specifier: v8::Local<'s, v8::String>,
    import_attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
    let resolver = v8::PromiseResolver::new(scope)?;
    let promise = resolver.get_promise(scope);
    let module_type = match requested_type(scope, import_attributes, 2) {
        Ok(module_type) => module_type,
        Err(error) => {
            let message = v8::String::new(scope, &error)?;
            let exception = v8::Exception::type_error(scope, message);
            resolver.reject(scope, exception);
            return Some(promise);
        }
    };
    let state = runtime_state(scope);
    state.dynamic_imports.borrow_mut().push(DynamicImport {
        resolver: v8::Global::new(scope, resolver),
        specifier: specifier.to_rust_string_lossy(scope),
        referrer: resource_name.to_rust_string_lossy(scope),
        module_type,
    });
    state.waker.wake();
    Some(promise)
}

/// Starts the queued dynamic imports and settles finished ones. Returns
/// whether anything started or settled, so the caller runs microtasks and
/// polls again.
pub(crate) fn poll_dynamic_imports(scope: &mut v8::PinScope<'_, '_>, state: &RuntimeState) -> bool {
    let mut progressed = false;
    let imports = std::mem::take(&mut *state.dynamic_imports.borrow_mut());
    for import in imports {
        let resolver = v8::Local::new(scope, &import.resolver);
        let loaded = state
            .loader
            .resolve(&import.specifier, &import.referrer)
            .map_err(Error::new)
            .and_then(|name| load_graph(scope, state, &name, import.module_type, None, false));
        let id = match loaded {
            Ok(id) => id,
            Err(error) => {
                progressed = true;
                reject_with_message(scope, resolver, error.message());
                continue;
            }
        };
        let module = {
            let modules = state.modules.borrow();
            v8::Local::new(scope, modules.handle(id).expect("loaded module"))
        };
        v8::tc_scope!(let tc, scope);
        match module.evaluate(tc) {
            Some(result) => {
                let promise = v8::Local::<v8::Promise>::try_from(result).ok();
                match promise {
                    Some(promise) => {
                        progressed = true;
                        promise.mark_as_handled();
                        state
                            .pending_evaluations
                            .borrow_mut()
                            .push(PendingEvaluation {
                                resolver: import.resolver,
                                module: v8::Global::new(tc, module),
                                promise: v8::Global::new(tc, promise),
                            });
                    }
                    None => {
                        let namespace = module.get_module_namespace();
                        resolver.resolve(tc, namespace);
                        progressed = true;
                    }
                }
            }
            None => {
                progressed = true;
                if let Some(exception) = tc.exception() {
                    resolver.reject(tc, exception);
                }
            }
        }
    }

    let evaluations = std::mem::take(&mut *state.pending_evaluations.borrow_mut());
    let mut still_pending = Vec::new();
    for evaluation in evaluations {
        let promise = v8::Local::new(scope, &evaluation.promise);
        let resolver = v8::Local::new(scope, &evaluation.resolver);
        match promise.state() {
            v8::PromiseState::Pending => still_pending.push(evaluation),
            v8::PromiseState::Fulfilled => {
                let module = v8::Local::new(scope, &evaluation.module);
                let namespace = module.get_module_namespace();
                resolver.resolve(scope, namespace);
                progressed = true;
            }
            v8::PromiseState::Rejected => {
                let reason = promise.result(scope);
                resolver.reject(scope, reason);
                progressed = true;
            }
        }
    }
    state.pending_evaluations.borrow_mut().extend(still_pending);
    progressed
}

fn reject_with_message(
    scope: &mut v8::PinScope<'_, '_>,
    resolver: v8::Local<v8::PromiseResolver>,
    message: &str,
) {
    let message = v8::String::new(scope, message).unwrap_or_else(|| v8::String::empty(scope));
    let exception = v8::Exception::type_error(scope, message);
    resolver.reject(scope, exception);
}
