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
  ArrayPrototypePush,
  Error,
  MapPrototypeDelete,
  MapPrototypeGet,
  MapPrototypeHas,
  MapPrototypeSet,
  ObjectFreeze,
  PromiseReject,
  Number,
  ReflectApply,
  RegExpPrototypeExec,
  SafeMap,
  SafeRegExp,
  String,
  StringPrototypeSplit,
  Symbol,
  SymbolFor,
} = primordials;

const {
  op_decode,
  op_deserialize,
  op_encode,
  op_get_async_context,
  op_is_any_array_buffer,
  op_is_array_buffer,
  op_is_array_buffer_view,
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
  op_timer_cancel,
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
const transferableResources = { __proto__: null };
const cloneableDeserializers = { __proto__: null };

function registerTransferableResource(name, send, receive) {
  if (transferableResources[name]) {
    throw new Error(`${name} is already registered`);
  }
  transferableResources[name] = { send, receive };
}

function registerCloneableResource(name, deserialize) {
  if (cloneableDeserializers[name]) {
    throw new Error(`${name} is already registered`);
  }
  cloneableDeserializers[name] = deserialize;
}

class BadResource extends Error {
  constructor(message) {
    super(message);
    this.name = "BadResource";
  }
}

class Interrupted extends Error {
  constructor(message) {
    super(message);
    this.name = "Interrupted";
  }
}

// dd has no resource table; streams backed by one cannot exist.
function noResources() {
  throw new BadResource("Bad resource ID");
}

async function noResourcesAsync() {
  noResources();
}

let nextTimerId = 1;
const activeTimers = new SafeMap();

/**
 * Runs `callback` after `timeout` milliseconds, again every `timeout`
 * milliseconds when `repeat` is set, until cancelled.
 */
function createTimer(callback, timeout, args, repeat) {
  const id = nextTimerId++;
  MapPrototypeSet(activeTimers, id, true);
  (async () => {
    for (;;) {
      const fired = await op_timer_sleep(id, timeout);
      if (!fired || !MapPrototypeHas(activeTimers, id)) {
        return;
      }
      if (!repeat) {
        MapPrototypeDelete(activeTimers, id);
      }
      ReflectApply(callback, undefined, args ?? []);
      if (!repeat) {
        return;
      }
    }
  })();
  return id;
}

function cancelTimer(id) {
  if (MapPrototypeDelete(activeTimers, id)) {
    op_timer_cancel(id);
  }
}

const STACK_FRAME = new SafeRegExp("^\\s*at (?:.*? \\()?(.+):(\\d+):(\\d+)\\)?$");
const NEWLINE = new SafeRegExp("\n");

/** The parts of an error the web layer's error reporting reads. */
function destructureError(error) {
  let exceptionMessage;
  let stack = "";
  try {
    if (error !== null && typeof error === "object" && "message" in error) {
      exceptionMessage = `Uncaught ${error.name ?? "Error"}: ${error.message}`;
      stack = typeof error.stack === "string" ? error.stack : "";
    } else {
      exceptionMessage = `Uncaught ${String(error)}`;
    }
  } catch {
    exceptionMessage = "Uncaught (unprintable error)";
  }
  const frames = [];
  const lines = StringPrototypeSplit(stack, NEWLINE);
  for (let i = 1; i < lines.length; i++) {
    const match = RegExpPrototypeExec(STACK_FRAME, lines[i]);
    if (match !== null) {
      ArrayPrototypePush(frames, {
        fileName: match[1],
        lineNumber: Number(match[2]),
        columnNumber: Number(match[3]),
      });
    }
  }
  return { exceptionMessage, frames, stack };
}

const build = ObjectFreeze({
  target: "unknown",
  arch: "unknown",
  os: "unknown",
  vendor: "unknown",
  env: undefined,
});

const core = {
  ops,
  loadExtScript,
  build,
  internalRidSymbol: Symbol("Deno.internal.rid"),
  BadResource,
  BadResourcePrototype: BadResource.prototype,
  Interrupted,
  InterruptedPrototype: Interrupted.prototype,
  hostObjectBrand,
  registerTransferableResource,
  getTransferableResource: (name) => transferableResources[name],
  registerCloneableResource,
  getCloneableDeserializers: () => cloneableDeserializers,
  encode: (text) => op_encode(text),
  decode: (buffer) => op_decode(buffer),
  serialize: (value, options, errorCallback) =>
    op_serialize(
      value,
      options?.hostObjects,
      options?.transferredArrayBuffers,
      options?.forStorage ?? false,
      errorCallback,
    ),
  deserialize: (buffer, options) =>
    op_deserialize(
      buffer,
      options?.hostObjects,
      options?.transferredArrayBuffers,
      options?.deserializers,
      options?.forStorage ?? false,
    ),
  structuredClone: (value, deserializers) =>
    op_structured_clone(value, deserializers ?? cloneableDeserializers),
  isAnyArrayBuffer: (value) => op_is_any_array_buffer(value),
  isArrayBuffer: (value) => op_is_array_buffer(value),
  isArrayBufferView: (value) => op_is_array_buffer_view(value),
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
  createTimer,
  createSystemTimer: (callback, timeout) => createTimer(callback, timeout, undefined, false),
  cancelTimer,
  refTimer: () => {},
  unrefTimer: () => {},
  refOpPromise: () => {},
  unrefOpPromise: () => {},
  destructureError,
  // An exception nothing handled ends the event loop, as an unhandled
  // rejection does.
  reportUnhandledException: (error) => {
    PromiseReject(error);
  },
  print: (message, isErr = false) => op_print(message, isErr),
  close: noResources,
  tryClose: () => {},
  read: noResourcesAsync,
  readAll: noResourcesAsync,
  write: noResourcesAsync,
  writeAll: noResourcesAsync,
  cancelRead: noResources,
  createCancelHandle: noResources,
};

// `internals` is shared by the vendored web scripts; `dd` holds the state
// dd's own runtime scripts share, and is what the host keeps as the
// runtime's internals.
const bootstrap = { core, primordials, internals: {}, dd: {} };
return bootstrap;
