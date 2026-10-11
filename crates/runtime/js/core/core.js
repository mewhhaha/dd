// The runtime's `core` object: the op functions plus the helpers the web
// layer (vendored from Deno) and dd's own scripts are written against. Runs
// once while the bootstrap snapshot is built, as the body of a function
// whose parameter `ops` holds every op function by name, and returns the
// bootstrap object every later runtime script receives as `__bootstrap`.
// Nothing here is global: worker code reaches no op.
"use strict";

const { primordials } = globalThis.__bootstrap;
delete globalThis.__bootstrap;
const {
  Error,
  MapPrototypeGet,
  MapPrototypeHas,
  MapPrototypeSet,
  PromiseReject,
  SafeMap,
  SymbolFor,
} = primordials;

const {
  op_decode,
  op_deserialize,
  op_encode,
  op_get_async_context,
  op_is_any_array_buffer,
  op_is_array_buffer,
  op_is_boxed_primitive,
  op_is_data_view,
  op_is_date,
  op_is_map,
  op_is_native_error,
  op_is_promise,
  op_is_proxy,
  op_is_reg_exp,
  op_is_set,
  op_is_shared_array_buffer,
  op_is_string_object,
  op_is_typed_array,
  op_load_ext_script,
  op_print,
  op_queue_microtask,
  op_serialize,
  op_set_async_context,
  op_structured_clone,
  op_timer_sleep,
} = ops;

// The web layer schedules tee and pipe chunk steps through
// primordials.queueMicrotask. V8's global queueMicrotask does not exist while
// the snapshot is built (and worker code may replace it later), so it goes
// through the op.
primordials.setQueueMicrotask((callback) => op_queue_microtask(callback));

const extScripts = new SafeMap();

/** Runs a web layer script once and returns what it exports. */
function loadExtScript(specifier) {
  if (MapPrototypeHas(extScripts, specifier)) {
    return MapPrototypeGet(extScripts, specifier);
  }
  const exports = op_load_ext_script(specifier, bootstrap);
  MapPrototypeSet(extScripts, specifier, exports);
  return exports;
}

const hostObjectBrand = SymbolFor("Deno.core.hostObject");
const cloneableDeserializers = { __proto__: null };

function registerCloneableResource(name, deserialize) {
  if (cloneableDeserializers[name]) {
    throw new Error(`${name} is already registered`);
  }
  cloneableDeserializers[name] = deserialize;
}

let nextTimerId = 1;

/** Runs `callback` once, after `timeout` milliseconds. */
function createSystemTimer(callback, timeout) {
  const id = nextTimerId++;
  (async () => {
    if (await op_timer_sleep(id, timeout)) {
      callback();
    }
  })();
}

const core = {
  ops,
  loadExtScript,
  hostObjectBrand,
  registerCloneableResource,
  encode: (text) => op_encode(text),
  decode: (buffer) => op_decode(buffer),
  // The runtime lists no host objects and transfers no buffers when it
  // serializes; `forStorage` words the errors for values bound for storage.
  serialize: (value, options) =>
    op_serialize(value, undefined, undefined, options?.forStorage ?? false),
  deserialize: (buffer, options) =>
    op_deserialize(buffer, undefined, undefined, undefined, options?.forStorage ?? false),
  structuredClone: (value) => op_structured_clone(value, cloneableDeserializers),
  isAnyArrayBuffer: (value) => op_is_any_array_buffer(value),
  isArrayBuffer: (value) => op_is_array_buffer(value),
  isBoxedPrimitive: (value) => op_is_boxed_primitive(value),
  isDataView: (value) => op_is_data_view(value),
  isDate: (value) => op_is_date(value),
  isMap: (value) => op_is_map(value),
  isNativeError: (value) => op_is_native_error(value),
  isPromise: (value) => op_is_promise(value),
  isProxy: (value) => op_is_proxy(value),
  isRegExp: (value) => op_is_reg_exp(value),
  isSet: (value) => op_is_set(value),
  isSharedArrayBuffer: (value) => op_is_shared_array_buffer(value),
  isStringObject: (value) => op_is_string_object(value),
  isTypedArray: (value) => op_is_typed_array(value),
  getAsyncContext: () => op_get_async_context(),
  setAsyncContext: (context) => op_set_async_context(context),
  createSystemTimer,
  // An exception nothing handled ends the event loop, as an unhandled
  // rejection does.
  reportUnhandledException: (error) => {
    PromiseReject(error);
  },
  print: (message, isErr = false) => op_print(message, isErr),
};

// `dd` holds the state dd's own runtime scripts share, and is what the host
// keeps as the runtime's internals.
const bootstrap = { core, primordials, dd: {} };
return bootstrap;
