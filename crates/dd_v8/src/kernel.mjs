// dd_v8 host kernel — runs natively on Scriptw + wtime x64 AOT.
// Everything stays in module-private lexical scope (zero globalThis pollution).
import {
  call_sync,
  start_async,
  call_sync_str,
  start_async_str,
  call_sync_0,
  call_sync_u32,
  call_sync_u32_str,
  call_sync_u32_str_str,
  call_sync_resp_inline,
  start_async_u32,
  start_async_u32_str2,
  start_async_str2,
  start_async_str3,
  start_async_u32_2_str2_u32,
  utf8_encode,
  utf8_decode,
  load_module,
  wasm_op,
  inspector_op,
} from "dd:host";

// ---- Capture genuine primordials before any user/bootstrap code runs ----
const _globalThis = globalThis;
const _Object = Object;
const _ObjectCreate = Object.create;
const _ObjectDefineProperty = Object.defineProperty;
const _ObjectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const _ObjectGetOwnPropertyNames = Object.getOwnPropertyNames;
const _ObjectGetOwnPropertySymbols = Object.getOwnPropertySymbols;
const _ObjectGetPrototypeOf = Object.getPrototypeOf;
const _ObjectHasOwn = Object.hasOwn;
const _ObjectKeys = Object.keys;
const _ObjectFreeze = Object.freeze;
const _ObjectPrototypeToString = Object.prototype.toString;
const _ObjectPrototypePropertyIsEnumerable = Object.prototype.propertyIsEnumerable;
const _ReflectApply = Reflect.apply;
const _ReflectOwnKeys = Reflect.ownKeys;
const _Array = Array;
const _ArrayIsArray = Array.isArray;
const _ArrayJoin = Array.prototype.join;
const _ArrayIndexOf = Array.prototype.indexOf;
const _Map = Map;
const _MapGet = Map.prototype.get;
const _MapSet = Map.prototype.set;
const _MapDelete = Map.prototype.delete;
const _MapHas = Map.prototype.has;
const _MapForEach = Map.prototype.forEach;
const _Set = Set;
const _SetAdd = Set.prototype.add;
const _SetHas = Set.prototype.has;
const _SetDelete = Set.prototype.delete;
const _SetForEach = Set.prototype.forEach;
const _WeakMap = WeakMap;
const _WeakMapGet = WeakMap.prototype.get;
const _WeakMapSet = WeakMap.prototype.set;
const _WeakMapHas = WeakMap.prototype.has;
const _WeakSet = WeakSet;
const _WeakSetHas = WeakSet.prototype.has;
const _Date = Date;
const _DateGetTime = Date.prototype.getTime;
const _RegExp = RegExp;
const _RegExpProto = RegExp.prototype;
const _RegExpGetSource = _ObjectGetOwnPropertyDescriptor(_RegExpProto, "source").get;
const _RegExpGetFlags = _ObjectGetOwnPropertyDescriptor(_RegExpProto, "flags").get;
const _RegExpGetDotAll = _ObjectGetOwnPropertyDescriptor(_RegExpProto, "dotAll").get;
const _MapGetSize = _ObjectGetOwnPropertyDescriptor(Map.prototype, "size").get;
const _SetGetSize = _ObjectGetOwnPropertyDescriptor(Set.prototype, "size").get;
const _ArrayBuffer = ArrayBuffer;
const _ArrayBufferIsView = ArrayBuffer.isView;
const _ABGetByteLength = _ObjectGetOwnPropertyDescriptor(ArrayBuffer.prototype, "byteLength").get;
const _ABGetDetached = _ObjectGetOwnPropertyDescriptor(ArrayBuffer.prototype, "detached")?.get;
const _ABGetResizable = _ObjectGetOwnPropertyDescriptor(ArrayBuffer.prototype, "resizable")?.get;
const _ABGetMaxByteLength = _ObjectGetOwnPropertyDescriptor(ArrayBuffer.prototype, "maxByteLength")?.get;
const _SharedArrayBuffer = typeof SharedArrayBuffer === "function" ? SharedArrayBuffer : null;
const _SABGetByteLength = _SharedArrayBuffer
  ? _ObjectGetOwnPropertyDescriptor(_SharedArrayBuffer.prototype, "byteLength").get
  : null;
const _DataView = DataView;
const _DVGetByteLength = _ObjectGetOwnPropertyDescriptor(DataView.prototype, "byteLength").get;
const _DVGetByteOffset = _ObjectGetOwnPropertyDescriptor(DataView.prototype, "byteOffset").get;
const _DVGetBuffer = _ObjectGetOwnPropertyDescriptor(DataView.prototype, "buffer").get;
const _DVGetUint8 = DataView.prototype.getUint8;
const _DVSetUint8 = DataView.prototype.setUint8;
const _DVGetUint32 = DataView.prototype.getUint32;
const _DVSetUint32 = DataView.prototype.setUint32;
const _DVGetFloat64 = DataView.prototype.getFloat64;
const _DVSetFloat64 = DataView.prototype.setFloat64;
const _Uint8Array = Uint8Array;
const _Uint32Array = Uint32Array;
const _Float64Array = Float64Array;
const _TypedArrayProto = _ObjectGetPrototypeOf(Uint8Array.prototype);
const _TAGetByteLength = _ObjectGetOwnPropertyDescriptor(_TypedArrayProto, "byteLength").get;
const _TAGetByteOffset = _ObjectGetOwnPropertyDescriptor(_TypedArrayProto, "byteOffset").get;
const _TAGetBuffer = _ObjectGetOwnPropertyDescriptor(_TypedArrayProto, "buffer").get;
const _TAGetToStringTag = _ObjectGetOwnPropertyDescriptor(_TypedArrayProto, Symbol.toStringTag).get;
const _TAValidate = _TypedArrayProto.slice;
const _TASet = _TypedArrayProto.set;
const _TASubarray = _TypedArrayProto.subarray;
const _Number = Number;
const _String = String;
const _Boolean = Boolean;
const _MathMin = Math.min;
const _StrCharCodeAt = String.prototype.charCodeAt;
const _StrFromCharCode = String.fromCharCode;
const _StrStartsWith = String.prototype.startsWith;
const _StrSlice = String.prototype.slice;
const _StrIndexOf = String.prototype.indexOf;
const _NumberValueOf = Number.prototype.valueOf;
const _StringValueOf = String.prototype.valueOf;
const _BooleanValueOf = Boolean.prototype.valueOf;
const _BigInt = BigInt;
const _BigIntValueOf = BigInt.prototype.valueOf;
const _BigIntToString = BigInt.prototype.toString;
const _Symbol = Symbol;
const _SymbolFor = Symbol.for;
const _SymbolValueOf = Symbol.prototype.valueOf;
const _SymbolToStringTag = Symbol.toStringTag;
const _Error = Error;
const _TypeError = TypeError;
const _RangeError = RangeError;
const _EvalError = EvalError;
const _SyntaxError = SyntaxError;
const _ReferenceError = ReferenceError;
const _URIError = URIError;
const _ErrorProto = Error.prototype;
const _Function = Function;
const _FunctionCall = Function.prototype.call;
const _FunctionApply = Function.prototype.apply;
const _FunctionBind = Function.prototype.bind;
const _call = _FunctionBind.call(_FunctionCall, _FunctionCall);
const _AsyncFunction = _ObjectGetPrototypeOf(async function () {}).constructor;
const _GeneratorFunction = _ObjectGetPrototypeOf(function* () {}).constructor;
const _AsyncGeneratorFunction = _ObjectGetPrototypeOf(async function* () {}).constructor;
const _eval = eval;
const _JSONParse = JSON.parse;
const _JSONStringify = JSON.stringify;
const _Promise = Promise;
const _PromiseResolve = _FunctionBind.call(Promise.resolve, Promise);
const _PromiseReject = _FunctionBind.call(Promise.reject, Promise);
const _PromiseThen = Promise.prototype.then;
const _Proxy = Proxy;
const _ProxyRevocable = Proxy.revocable;

// ---- DevTools Inspector state (inert until enable_inspector() is called) ----
let _dbgEnabled = false;
let _dbgInEval = false;
let _dbgLoc = { scriptId: 0, url: "", line: 0, col: 0 };
let _dbgTicks = 0;
let _dbgLastSid = -1;
let _dbgLastLine = -1;
const _dbgUrls = new _Map();
const _dbgEmptyBytes = new _Uint8Array(0);
const _dbgCheckBuf = new _Uint8Array(16);

function _dbgGetUrl(sid) {
  if (!sid) return "";
  let url = _call(_MapGet, _dbgUrls, sid);
  if (url !== undefined) return url;
  const env = _decodeEnvelope(inspector_op(5, _encodeWireArgs([sid])));
  url = env.ok && typeof env.value === "string" ? env.value : "";
  _call(_MapSet, _dbgUrls, sid, url);
  return url;
}

function _toRemoteObject(v) {
  if (v === undefined) return { type: "undefined" };
  if (v === null) return { type: "object", subtype: "null", value: null };
  const t = typeof v;
  if (t === "boolean") return { type: "boolean", value: v };
  if (t === "string") return { type: "string", value: v };
  if (t === "number") {
    if (v !== v) return { type: "number", unserializableValue: "NaN", description: "NaN" };
    if (v === Infinity) return { type: "number", unserializableValue: "Infinity", description: "Infinity" };
    if (v === -Infinity) return { type: "number", unserializableValue: "-Infinity", description: "-Infinity" };
    if (v === 0 && 1 / v < 0) return { type: "number", unserializableValue: "-0", description: "-0" };
    return { type: "number", value: v, description: _String(v) };
  }
  if (t === "bigint") {
    const d = `${_call(_BigIntToString, v)}n`;
    return { type: "bigint", unserializableValue: d, description: d };
  }
  if (t === "symbol") return { type: "symbol", description: _String(v) };
  if (t === "function") return { type: "function", className: "Function", description: _String(v) };
  try {
    if (_ArrayIsArray(v)) {
      return { type: "object", subtype: "array", className: "Array", description: `Array(${v.length})` };
    }
    if (_isError(v)) {
      const cname = typeof v.name === "string" && v.name ? v.name : "Error";
      return { type: "object", subtype: "error", className: cname, description: _describeException(v) };
    }
  } catch {}
  return { type: "object", className: "Object", description: "Object" };
}

function _inspectorConsoleCall(cdpType, args) {
  if (!_dbgEnabled) return;
  const remotes = new _Array(args.length);
  for (let i = 0; i < args.length; i++) remotes[i] = _toRemoteObject(args[i]);
  const argsJson = _JSONStringify(remotes);
  inspector_op(
    0,
    _encodeWireArgs([cdpType, argsJson, _dbgLoc.scriptId, _dbgLoc.url, _dbgLoc.line, _dbgLoc.col]),
  );
}

function _dbgEvalCommand(cmd, evalInFrame) {
  // cmd = [2, isCallFrame, sessionId, reqIdJson, expression]
  const isCallFrame = !!cmd[1];
  const sessionId = cmd[2];
  const reqIdJson = cmd[3];
  const expr = cmd[4];
  let remoteJson = "{\"type\":\"undefined\"}";
  let excDesc = null;
  const prevInEval = _dbgInEval;
  const prevGlobalEval = _globalThis.eval;
  _dbgInEval = true;
  // Temporarily restore intrinsic `%eval%` so `(__e) => eval(__e)` performs a
  // true direct eval inside the paused frame's lexical environment.
  _globalThis.eval = _eval;
  try {
    let val;
    if (isCallFrame && typeof evalInFrame === "function") {
      val = evalInFrame(expr);
    } else {
      const instEnv = _decodeEnvelope(inspector_op(3, _encodeWireArgs([expr])));
      const src = instEnv.ok && typeof instEnv.value === "string" ? instEnv.value : expr;
      val = _eval(src);
    }
    remoteJson = _JSONStringify(_toRemoteObject(val));
  } catch (err) {
    excDesc = _describeException(err);
    remoteJson = _JSONStringify(_toRemoteObject(err));
  } finally {
    _globalThis.eval = prevGlobalEval;
    _dbgInEval = prevInEval;
  }
  return _decodeEnvelope(
    inspector_op(2, _encodeWireArgs([sessionId, reqIdJson, remoteJson, excDesc])),
  );
}

function _dbgCheck(sid, line, col, isDbg, evalInFrame) {
  if (!_dbgEnabled) return;
  _dbgLoc.scriptId = sid;
  _dbgLoc.url = _dbgGetUrl(sid);
  _dbgLoc.line = line;
  _dbgLoc.col = col;
  if (!isDbg && sid === _dbgLastSid && line === _dbgLastLine && ((++_dbgTicks & 63) !== 0)) {
    return;
  }
  _dbgLastSid = sid;
  _dbgLastLine = line;
  const b = _dbgCheckBuf;
  b[0] = sid & 0xff; b[1] = (sid >>> 8) & 0xff; b[2] = (sid >>> 16) & 0xff; b[3] = (sid >>> 24) & 0xff;
  b[4] = line & 0xff; b[5] = (line >>> 8) & 0xff; b[6] = (line >>> 16) & 0xff; b[7] = (line >>> 24) & 0xff;
  b[8] = col & 0xff; b[9] = (col >>> 8) & 0xff; b[10] = (col >>> 16) & 0xff; b[11] = (col >>> 24) & 0xff;
  b[12] = isDbg ? 1 : 0; b[13] = 0; b[14] = 0; b[15] = 0;
  const resp = inspector_op(1, b);
  if (!resp || resp.length === 0) return;
  let env = _decodeEnvelope(resp);
  while (env.ok && _ArrayIsArray(env.value)) {
    const kind = env.value[0];
    if (kind === 1) break; // Resume
    if (kind === 2) {
      env = _dbgEvalCommand(env.value, evalInFrame);
      continue;
    }
    break;
  }
}

// Provide V8's Error.captureStackTrace when absent (expected by dd's bootstrap primordials).
if (typeof _Error.captureStackTrace !== "function") {
  _ObjectDefineProperty(_Error, "captureStackTrace", {
    value: function captureStackTrace(target, _constructorOpt) {
      if ((typeof target !== "object" && typeof target !== "function") || target === null) {
        throw new _TypeError("invalid_argument");
      }
      if (!_ObjectHasOwn(target, "stack")) {
        const name = typeof target.name === "string" && target.name.length > 0 ? target.name : "Error";
        const msg = typeof target.message === "string" && target.message.length > 0 ? target.message : "";
        let stack = msg.length > 0 ? `${name}: ${msg}` : name;
        if (_dbgLoc.url) {
          stack += `\n    at ${_dbgLoc.url}:${_dbgLoc.line + 1}:${_dbgLoc.col + 1}`;
        }
        _ObjectDefineProperty(target, "stack", {
          value: stack,
          writable: true,
          enumerable: false,
          configurable: true,
        });
      }
    },
    writable: true,
    enumerable: false,
    configurable: true,
  });
}

// Ensure a base inspector-backed console exists before dd's bootstrap.js binds it.
if (typeof _globalThis.console !== "object" || _globalThis.console === null) {
  const mk = (cdpType) => function (...args) { _inspectorConsoleCall(cdpType, args); };
  _globalThis.console = {
    log: mk("log"), info: mk("info"), warn: mk("warning"), error: mk("error"), debug: mk("debug"),
    trace: mk("trace"), dir: mk("dir"), dirxml: mk("dirxml"), table: mk("table"), count: mk("count"),
    countReset: mk("countReset"), group: mk("startGroup"), groupCollapsed: mk("startGroupCollapsed"),
    groupEnd: mk("endGroup"), clear: mk("clear"), time: mk("time"), timeLog: mk("timeLog"),
    timeEnd: mk("timeEnd"), assert: mk("assert"), profile: mk("profile"), profileEnd: mk("profileEnd"),
    timeStamp: mk("timeStamp"),
  };
}

// ---- Proxy tracking (for op_is_proxy / op_proxy_details without firing traps) ----
const _proxyDetails = new _WeakMap();
function _TrackedProxy(target, handler) {
  if (!new.target) {
    throw new _TypeError("Constructor Proxy requires 'new'");
  }
  const p = new _Proxy(target, handler);
  _call(_WeakMapSet, _proxyDetails, p, [target, handler]);
  return p;
}
_TrackedProxy.revocable = function revocable(target, handler) {
  const rec = _ProxyRevocable(target, handler);
  const entry = [target, handler];
  _call(_WeakMapSet, _proxyDetails, rec.proxy, entry);
  const origRevoke = rec.revoke;
  return {
    proxy: rec.proxy,
    revoke() {
      entry[0] = null;
      entry[1] = null;
      return origRevoke();
    },
  };
};
_ObjectDefineProperty(_TrackedProxy, "name", { value: "Proxy", configurable: true });
_ObjectDefineProperty(_TrackedProxy, "length", { value: 2, configurable: true });
_globalThis.Proxy = _TrackedProxy;

// ---- Async context (CPED) + native Promise inspection (handled in Scriptw Wasm GC) ----
let _asyncContext = undefined;
let _asyncContextFn = null;

// ---- Code generation gate (eval / Function) ----
let _allowCodeGen = true;
function _checkCodeGen() {
  if (!_allowCodeGen && !_dbgInEval) {
    throw new _EvalError("Code generation from strings disallowed for this context");
  }
}

_globalThis.eval = function _gatedEval(x) {
  if (typeof x !== "string") return x;
  _checkCodeGen();
  return _eval(x);
};
_ObjectDefineProperty(_globalThis.eval, "name", { value: "eval", configurable: true });
_ObjectDefineProperty(_globalThis.eval, "length", { value: 1, configurable: true });

function _GatedFunction(...args) {
  _checkCodeGen();
  return _ReflectApply(_Function, null, args);
}
_GatedFunction.prototype = _Function.prototype;
_ObjectDefineProperty(_Function.prototype, "constructor", {
  value: _GatedFunction,
  writable: true,
  configurable: true,
});
_ObjectDefineProperty(_GatedFunction, "name", { value: "Function", configurable: true });
_ObjectDefineProperty(_GatedFunction, "length", { value: 1, configurable: true });
_globalThis.Function = _GatedFunction;

// ---- Brand checks (tamper-resistant via captured primordial getters) ----
function _isArrayBuffer(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_ABGetByteLength, v); return true; } catch { return false; }
}
function _isSharedArrayBuffer(v) {
  if (!_SABGetByteLength || typeof v !== "object" || v === null) return false;
  try { _call(_SABGetByteLength, v); return true; } catch { return false; }
}
function _isDataView(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_DVGetBuffer, v); return true; } catch { return false; }
}
function _isTypedArray(v) {
  if (typeof v !== "object" || v === null) return false;
  try { return _call(_TAGetToStringTag, v) !== undefined; } catch { return false; }
}
function _isArrayBufferView(v) {
  return _isTypedArray(v) || _isDataView(v);
}
function _isDate(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_DateGetTime, v); return true; } catch { return false; }
}
function _isMap(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_MapGetSize, v); return true; } catch { return false; }
}
function _isSet(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_SetGetSize, v); return true; } catch { return false; }
}
function _isWeakMap(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_WeakMapHas, v, _globalThis); return true; } catch { return false; }
}
function _isWeakSet(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_WeakSetHas, v, _globalThis); return true; } catch { return false; }
}
function _isRegExp(v) {
  if (typeof v !== "object" || v === null || v === _RegExpProto) return false;
  try { return _call(_RegExpGetDotAll, v) !== undefined; } catch { return false; }
}
function _isPromise(v) {
  if (typeof v !== "object" || v === null) return false;
  if (_call(_WeakMapHas, _proxyDetails, v)) return false;
  if (_asyncContextFn) return _asyncContextFn(2, v) >= 0;
  try {
    _call(_PromiseThen, v, undefined, () => {});
    return true;
  } catch {
    return false;
  }
}
function _isProxy(v) {
  return (typeof v === "object" || typeof v === "function") && v !== null &&
    _call(_WeakMapHas, _proxyDetails, v);
}
function _isNativeError(v) {
  if (typeof v !== "object" || v === null) return false;
  if (typeof _Error.isError === "function") return _Error.isError(v);
  if (v instanceof _Error) return true;
  return _call(_ObjectPrototypeToString, v) === "[object Error]";
}
function _isStringObject(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_StringValueOf, v); return true; } catch { return false; }
}
function _isNumberObject(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_NumberValueOf, v); return true; } catch { return false; }
}
function _isBooleanObject(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_BooleanValueOf, v); return true; } catch { return false; }
}
function _isBigIntObject(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_BigIntValueOf, v); return true; } catch { return false; }
}
function _isSymbolObject(v) {
  if (typeof v !== "object" || v === null) return false;
  try { _call(_SymbolValueOf, v); return true; } catch { return false; }
}
function _isBoxedPrimitive(v) {
  return _isNumberObject(v) || _isStringObject(v) || _isBooleanObject(v) ||
    _isBigIntObject(v) || _isSymbolObject(v);
}

// Safely extract backing buffer + view range from an ArrayBuffer or ArrayBufferView
// (tamper-resistant, rejects Proxies and bare SharedArrayBuffer, treats detached &
// out-of-bounds resizable views as 0-length).
function _bufferViewParts(v) {
  if (_isProxy(v)) return null;
  if (_isTypedArray(v)) {
    try {
      const buf = _call(_TAGetBuffer, v);
      const off = _call(_TAGetByteOffset, v);
      const len = _call(_TAGetByteLength, v);
      return { buf, off, len };
    } catch {
      return { buf: null, off: 0, len: 0 };
    }
  }
  if (_isDataView(v)) {
    try {
      const buf = _call(_DVGetBuffer, v);
      const off = _call(_DVGetByteOffset, v);
      const len = _call(_DVGetByteLength, v);
      return { buf, off, len };
    } catch {
      return { buf: null, off: 0, len: 0 };
    }
  }
  if (_isArrayBuffer(v)) {
    try {
      const len = _call(_ABGetByteLength, v);
      return { buf: v, off: 0, len };
    } catch {
      return { buf: null, off: 0, len: 0 };
    }
  }
  return null;
}

function _bufferBytes(v) {
  const parts = _bufferViewParts(v);
  if (parts === null) return null;
  if (parts.len === 0 || !parts.buf) return new _Uint8Array(0);
  try {
    const view = new _Uint8Array(parts.buf, parts.off, parts.len);
    const out = new _Uint8Array(parts.len);
    _call(_TASet, out, view);
    return out;
  } catch {
    return new _Uint8Array(0);
  }
}

// ---- UTF-8 codec (delegates to native Rust foreigns; immune to sabotaged builtins) ----
function _utf8Encode(str) {
  if (str.length === 0) return new _Uint8Array(0);
  return utf8_encode(str);
}

function _utf8DecodeRange(bytes, start, end, stripBom) {
  const len = end - start;
  // Match V8's maximum string length (0x1fffffe8 UTF-16 code units) so multi-GiB
  // views fail fast with RangeError instead of exhausting memory.
  if (len > 0x1fffffe8) {
    throw new _RangeError("Invalid string length");
  }
  if (len === 0) return "";
  const slice = (start === 0 && end === bytes.length)
    ? bytes
    : _call(_TASubarray, bytes, start, end);
  return utf8_decode(slice, stripBom ? 1 : 0);
}

function _utf8Decode(bytes, stripBom) {
  return _utf8DecodeRange(bytes, 0, bytes.length, stripBom);
}

function _utf8EncodeInto(source, dest) {
  if (typeof source !== "string") throw new _TypeError("expected a string");
  if (!_isTypedArray(dest) || _call(_TAGetToStringTag, dest) !== "Uint8Array") {
    throw new _TypeError("expected a Uint8Array");
  }
  const destLen = _call(_TAGetByteLength, dest);
  let read = 0;
  let written = 0;
  const n = source.length;
  for (let i = 0; i < n; i++) {
    let cp = _call(_StrCharCodeAt, source, i);
    let step = 1;
    let need = 1;
    if (cp < 0x80) {
      need = 1;
    } else if (cp < 0x800) {
      need = 2;
    } else if (cp >= 0xd800 && cp <= 0xdbff) {
      if (i + 1 < n) {
        const lo = _call(_StrCharCodeAt, source, i + 1);
        if (lo >= 0xdc00 && lo <= 0xdfff) {
          cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
          step = 2;
          need = 4;
        } else {
          cp = 0xfffd;
          need = 3;
        }
      } else {
        cp = 0xfffd;
        need = 3;
      }
    } else if (cp >= 0xdc00 && cp <= 0xdfff) {
      cp = 0xfffd;
      need = 3;
    } else {
      need = 3;
    }
    if (written + need > destLen) break;
    if (need === 1) {
      dest[written++] = cp;
    } else if (need === 2) {
      dest[written++] = 0xc0 | (cp >> 6);
      dest[written++] = 0x80 | (cp & 0x3f);
    } else if (need === 3) {
      dest[written++] = 0xe0 | (cp >> 12);
      dest[written++] = 0x80 | ((cp >> 6) & 0x3f);
      dest[written++] = 0x80 | (cp & 0x3f);
    } else {
      dest[written++] = 0xf0 | (cp >> 18);
      dest[written++] = 0x80 | ((cp >> 12) & 0x3f);
      dest[written++] = 0x80 | ((cp >> 6) & 0x3f);
      dest[written++] = 0x80 | (cp & 0x3f);
    }
    read += step;
    if (step === 2) i++;
  }
  return [read, written];
}

// ---- Compact Binary Wire Encoder/Decoder (syncs with serde_v8.rs) ----
const TAG_UNDEFINED = 0;
const TAG_NULL = 1;
const TAG_FALSE = 2;
const TAG_TRUE = 3;
const TAG_I32 = 4;
const TAG_F64 = 5;
const TAG_BIGINT = 6;
const TAG_STRING = 7;
const TAG_BYTES = 8;
const TAG_ARRAY = 9;
const TAG_OBJECT = 10;
const TAG_ERROR = 11;

const ENV_OK = 0;
const ENV_ERR = 1;
const ENV_PENDING = 2;

let _maxOpArgBytes = 256 * 1024 * 1024;
const _f64Tmp = new _ArrayBuffer(8);
const _f64View = new _Float64Array(_f64Tmp);
const _f64Bytes = new _Uint8Array(_f64Tmp);

class _WireBuf {
  constructor(budget) {
    this.ab = new _ArrayBuffer(256);
    this.buf = new _Uint8Array(this.ab);
    this.dv = new _DataView(this.ab);
    this.pos = 0;
    this.budget = budget;
  }
  ensure(n) {
    if (this.pos + n > this.budget) {
      throw new _TypeError("op argument is too large");
    }
    if (this.pos + n > this.buf.length) {
      const needed = this.pos + n;
      let cap;
      if (needed > 64 * 1024 * 1024) {
        cap = _MathMin(this.budget, needed + 4096);
      } else {
        cap = this.buf.length * 2;
        while (cap < needed) cap *= 2;
        if (cap > this.budget) cap = this.budget;
      }
      const nextAb = new _ArrayBuffer(cap);
      const next = new _Uint8Array(nextAb);
      _call(_TASet, next, _call(_TASubarray, this.buf, 0, this.pos));
      this.ab = nextAb;
      this.buf = next;
      this.dv = new _DataView(nextAb);
    }
  }
  u8(v) {
    this.ensure(1);
    _call(_DVSetUint8, this.dv, this.pos++, v & 0xff);
  }
  u32(v) {
    this.ensure(4);
    _call(_DVSetUint32, this.dv, this.pos, v >>> 0, true);
    this.pos += 4;
  }
  f64(v) {
    this.ensure(8);
    _call(_DVSetFloat64, this.dv, this.pos, v, true);
    this.pos += 8;
  }
  bytes(arr) {
    this.ensure(arr.length);
    _call(_TASet, this.buf, arr, this.pos);
    this.pos += arr.length;
  }
  str(s) {
    const n = s.length;
    if (n === 0) {
      this.u32(0);
      return;
    }
    if (this.pos + 4 + n > this.budget) {
      throw new _TypeError("op argument is too large");
    }
    const b = utf8_encode(s);
    this.u32(b.length);
    this.bytes(b);
  }
  finish() {
    return _call(_TASubarray, this.buf, 0, this.pos);
  }
}

function _patchU32(w, pos, v) {
  _call(_DVSetUint32, w.dv, pos, v >>> 0, true);
}

function _encodeWireLeaf(w, val) {
  if (val === undefined) { w.u8(TAG_UNDEFINED); return true; }
  if (val === null) { w.u8(TAG_NULL); return true; }
  const t = typeof val;
  if (t === "boolean") { w.u8(val ? TAG_TRUE : TAG_FALSE); return true; }
  if (t === "number") {
    if ((val | 0) === val && (val !== 0 || 1 / val > 0)) {
      w.u8(TAG_I32);
      w.u32(val | 0);
    } else {
      w.u8(TAG_F64);
      w.f64(val);
    }
    return true;
  }
  if (t === "bigint") {
    w.u8(TAG_BIGINT);
    w.str(_call(_BigIntToString, val));
    return true;
  }
  if (t === "string") {
    w.u8(TAG_STRING);
    w.str(val);
    return true;
  }
  if (t === "symbol") {
    w.u8(TAG_ERROR);
    w.str("symbol is not serializable");
    return true;
  }
  return false;
}

// Fast zero-allocation pre-flight budget check so expanding arguments (e.g. 600 MiB of repeated strings)
// fail immediately with TypeError("op argument is too large") without growing a huge Uint8Array first.
function _checkWireBudget(args, budget) {
  let cost = 4;
  const ancestors = new _Set();
  const stack = new _Array(16);
  let sp = 0;
  for (let i = args.length - 1; i >= 0; i--) {
    stack[sp++] = { kind: 0, val: args[i], depth: 0 };
  }
  while (sp > 0) {
    const f = stack[sp - 1];
    if (f.kind === 1) {
      sp--;
      _call(_SetDelete, ancestors, f.obj);
      continue;
    }
    if (f.kind === 2) {
      let pushed = false;
      while (f.idx < f.len) {
        const i = f.idx++;
        let desc;
        try { desc = _ObjectGetOwnPropertyDescriptor(f.arr, i); } catch { cost += 16; continue; }
        if (!desc || desc.get || desc.set) { cost += 16; continue; }
        const elem = desc.value;
        if (typeof elem === "string") {
          cost += 16 + elem.length;
          if (cost > budget) throw new _TypeError("op argument is too large");
        } else if (elem === null || elem === undefined || typeof elem !== "object" && typeof elem !== "function") {
          cost += 16;
          if (cost > budget) throw new _TypeError("op argument is too large");
        } else {
          stack[sp++] = { kind: 0, val: elem, depth: f.depth };
          pushed = true;
          break;
        }
      }
      if (!pushed) sp--;
      continue;
    }
    if (f.kind === 3) {
      let pushed = false;
      while (f.idx < f.keys.length) {
        const k = f.keys[f.idx++];
        cost += 16 + k.length;
        if (cost > budget) throw new _TypeError("op argument is too large");
        let v;
        try { v = f.obj[k]; } catch { continue; }
        if (typeof v === "string") {
          cost += 16 + v.length;
          if (cost > budget) throw new _TypeError("op argument is too large");
        } else if (v !== null && v !== undefined && (typeof v === "object" || typeof v === "function")) {
          stack[sp++] = { kind: 0, val: v, depth: f.depth };
          pushed = true;
          break;
        }
      }
      if (!pushed) sp--;
      continue;
    }
    sp--;
    const val = f.val;
    cost += 16;
    if (cost > budget) throw new _TypeError("op argument is too large");
    if (val === null || val === undefined) continue;
    const t = typeof val;
    if (t === "string") {
      cost += val.length;
      if (cost > budget) throw new _TypeError("op argument is too large");
      continue;
    }
    if (t !== "object" && t !== "function") continue;
    if (!_isProxy(val)) {
      if (_isTypedArray(val)) {
        try { cost += _call(_TAGetByteLength, val); } catch {}
        if (cost > budget) throw new _TypeError("op argument is too large");
        continue;
      }
      if (_isDataView(val)) {
        try { cost += _call(_DVGetByteLength, val); } catch {}
        if (cost > budget) throw new _TypeError("op argument is too large");
        continue;
      }
      if (_isArrayBuffer(val)) {
        try { cost += _call(_ABGetByteLength, val); } catch {}
        if (cost > budget) throw new _TypeError("op argument is too large");
        continue;
      }
    }
    if (f.depth > 128 || _call(_SetHas, ancestors, val) || _isProxy(val)) continue;
    let isArr = false;
    try { isArr = _ArrayIsArray(val); } catch { continue; }
    if (isArr) {
      let len = 0;
      try { len = val.length >>> 0; } catch { continue; }
      if (len * 16 > budget - cost) throw new _TypeError("op argument is too large");
      if (len > 4096) {
        // Skip scanning huge sparse arrays twice; _encodeWireVal enforces exact budget.
        continue;
      }
      _call(_SetAdd, ancestors, val);
      stack[sp++] = { kind: 1, obj: val };
      stack[sp++] = { kind: 2, arr: val, idx: 0, len, depth: f.depth + 1 };
      continue;
    }
    // Only pre-scan plain objects without getters/proxies (avoid invoking user getters twice!).
    if (_isProxy(val)) continue;
    let keys;
    try { keys = _ObjectKeys(val); } catch { continue; }
    let hasAccessor = false;
    for (let i = 0; i < keys.length; i++) {
      const desc = _ObjectGetOwnPropertyDescriptor(val, keys[i]);
      if (desc && (desc.get || desc.set)) { hasAccessor = true; break; }
    }
    if (hasAccessor) continue;
    _call(_SetAdd, ancestors, val);
    stack[sp++] = { kind: 1, obj: val };
    stack[sp++] = { kind: 3, obj: val, keys, idx: 0, depth: f.depth + 1 };
  }
}

function _encodeWireVal(w, rootVal, startDepth) {
  const ancestors = new _Set();
  const stack = new _Array(16);
  let sp = 0;
  stack[sp++] = { kind: 0, val: rootVal, depth: startDepth };
  while (sp > 0) {
    const f = stack[sp - 1];
    if (f.kind === 1) {
      sp--;
      _call(_SetDelete, ancestors, f.obj);
      continue;
    }
    if (f.kind === 2) {
      let emittedNested = false;
      while (f.idx < f.len) {
        const i = f.idx++;
        let elem;
        try {
          elem = f.arr[i];
        } catch {
          w.u8(TAG_ERROR);
          w.str("element threw");
          continue;
        }
        if (!_encodeWireLeaf(w, elem)) {
          stack[sp++] = { kind: 0, val: elem, depth: f.depth };
          emittedNested = true;
          break;
        }
      }
      if (!emittedNested) {
        sp--;
      }
      continue;
    }
    if (f.kind === 3) {
      let emittedNested = false;
      while (f.idx < f.keys.length) {
        const idx = f.idx++;
        const k = f.keys[idx];
        if (idx > 0) {
          let stillPresent = true;
          try {
            stillPresent = _ObjectHasOwn(f.obj, k);
          } catch {
            stillPresent = true;
          }
          if (!stillPresent) continue;
        }
        let propVal;
        try {
          propVal = f.obj[k];
        } catch {
          w.str(k);
          w.u8(TAG_ERROR);
          w.str(`field ${k} threw (value threw)`);
          f.written++;
          continue;
        }
        w.str(k);
        f.written++;
        if (!_encodeWireLeaf(w, propVal)) {
          stack[sp++] = { kind: 0, val: propVal, depth: f.depth };
          emittedNested = true;
          break;
        }
      }
      if (!emittedNested) {
        _patchU32(w, f.countPos, f.written);
        sp--;
      }
      continue;
    }

    sp--;
    const val = f.val;
    const depth = f.depth;
    if (_encodeWireLeaf(w, val)) continue;

    const buf = _bufferBytes(val);
    if (buf !== null) {
      w.u8(TAG_BYTES);
      w.u32(buf.length);
      w.bytes(buf);
      continue;
    }
    if (depth > 128 || _call(_SetHas, ancestors, val)) {
      w.u8(TAG_ERROR);
      w.str("value is nested too deeply");
      continue;
    }
    let isArr = false;
    try {
      isArr = _ArrayIsArray(val);
    } catch {
      w.u8(TAG_ERROR);
      w.str("proxy threw");
      continue;
    }
    if (isArr) {
      let len;
      try {
        len = val.length >>> 0;
      } catch {
        w.u8(TAG_ERROR);
        w.str("element threw");
        continue;
      }
      if (len * 16 > w.budget) {
        throw new _TypeError("op argument is too large");
      }
      w.u8(TAG_ARRAY);
      w.u32(len);
      _call(_SetAdd, ancestors, val);
      stack[sp++] = { kind: 1, obj: val };
      stack[sp++] = { kind: 2, arr: val, idx: 0, len, depth: depth + 1 };
      continue;
    }
    let keys;
    try {
      keys = _ObjectKeys(val);
    } catch {
      w.u8(TAG_ERROR);
      w.str("keys threw");
      continue;
    }
    if (keys.length === 0 && _isProxy(val)) {
      try {
        void val[""];
      } catch {
        w.u8(TAG_ERROR);
        w.str("value threw");
        continue;
      }
    }
    w.u8(TAG_OBJECT);
    w.ensure(4);
    const countPos = w.pos;
    w.u32(0);
    _call(_SetAdd, ancestors, val);
    stack[sp++] = { kind: 1, obj: val };
    stack[sp++] = { kind: 3, obj: val, keys, idx: 0, depth: depth + 1, countPos, written: 0 };
  }
}

const _emptyArgsWire = new _Uint8Array([0, 0, 0, 0]);
const _wireScratchAB = new _ArrayBuffer(4096);
const _wireScratch = new _Uint8Array(_wireScratchAB);
const _wireScratchDV = new _DataView(_wireScratchAB);
const _sharedWireBuf = new _WireBuf(256 * 1024 * 1024);
let _sharedWireBufBusy = false;

function _tryFastEncodeLeaf(buf, dv, pos, val, limit) {
  if (val === undefined) {
    if (pos + 1 > limit) return -1;
    _call(_DVSetUint8, dv, pos, TAG_UNDEFINED);
    return pos + 1;
  }
  if (val === null) {
    if (pos + 1 > limit) return -1;
    _call(_DVSetUint8, dv, pos, TAG_NULL);
    return pos + 1;
  }
  const t = typeof val;
  if (t === "boolean") {
    if (pos + 1 > limit) return -1;
    _call(_DVSetUint8, dv, pos, val ? TAG_TRUE : TAG_FALSE);
    return pos + 1;
  }
  if (t === "number") {
    if ((val | 0) === val && (val !== 0 || 1 / val > 0)) {
      if (pos + 5 > limit) return -1;
      _call(_DVSetUint8, dv, pos, TAG_I32);
      _call(_DVSetUint32, dv, pos + 1, (val | 0) >>> 0, true);
      return pos + 5;
    }
    if (pos + 9 > limit) return -1;
    _call(_DVSetUint8, dv, pos, TAG_F64);
    _call(_DVSetFloat64, dv, pos + 1, val, true);
    return pos + 9;
  }
  if (t === "string") {
    const n = val.length;
    if (n === 0) {
      if (pos + 5 > limit) return -1;
      _call(_DVSetUint8, dv, pos, TAG_STRING);
      _call(_DVSetUint32, dv, pos + 1, 0, true);
      return pos + 5;
    }
    if (pos + 5 + n > limit) return -1;
    const b = utf8_encode(val);
    const blen = b.length;
    if (pos + 5 + blen > limit) return -1;
    _call(_DVSetUint8, dv, pos, TAG_STRING);
    _call(_DVSetUint32, dv, pos + 1, blen >>> 0, true);
    _call(_TASet, buf, b, pos + 5);
    return pos + 5 + blen;
  }
  if (t === "object") {
    // Fast path for non-proxy Uint8Array (e.g. op_http_body_store).
    if (!_isProxy(val) && _isTypedArray(val) && _call(_TAGetToStringTag, val) === "Uint8Array") {
      try {
        const len = _call(_TAGetByteLength, val);
        if (pos + 5 + len > limit) return -1;
        _call(_DVSetUint8, dv, pos, TAG_BYTES);
        _call(_DVSetUint32, dv, pos + 1, len >>> 0, true);
        if (len > 0) _call(_TASet, buf, val, pos + 5);
        return pos + 5 + len;
      } catch {
        return -1;
      }
    }
    // Fast path for shallow header pair arrays `[[k, v], ...]` (e.g. op_http_headers_store).
    if (!_isProxy(val) && _ArrayIsArray(val)) {
      try {
        const alen = val.length;
        if (alen > 64 || pos + 5 > limit) return -1;
        _call(_DVSetUint8, dv, pos, TAG_ARRAY);
        _call(_DVSetUint32, dv, pos + 1, alen >>> 0, true);
        let p = pos + 5;
        for (let i = 0; i < alen; i++) {
          const desc = _ObjectGetOwnPropertyDescriptor(val, i);
          if (!desc || desc.get || desc.set) return -1;
          const item = desc.value;
          if (typeof item === "string" || typeof item === "number" || typeof item === "boolean" || item === null || item === undefined) {
            p = _tryFastEncodeLeaf(buf, dv, p, item, limit);
            if (p < 0) return -1;
          } else if (item && !_isProxy(item) && _ArrayIsArray(item) && item.length === 2) {
            const d0 = _ObjectGetOwnPropertyDescriptor(item, 0);
            const d1 = _ObjectGetOwnPropertyDescriptor(item, 1);
            if (!d0 || d0.get || d0.set || typeof d0.value !== "string") return -1;
            if (!d1 || d1.get || d1.set || typeof d1.value !== "string") return -1;
            if (p + 5 > limit) return -1;
            _call(_DVSetUint8, dv, p, TAG_ARRAY);
            _call(_DVSetUint32, dv, p + 1, 2, true);
            p = _tryFastEncodeLeaf(buf, dv, p + 5, d0.value, limit);
            if (p < 0) return -1;
            p = _tryFastEncodeLeaf(buf, dv, p, d1.value, limit);
            if (p < 0) return -1;
          } else {
            return -1;
          }
        }
        return p;
      } catch {
        return -1;
      }
    }
  }
  return -1;
}

function _encodeWireArgs(args) {
  const n = args.length;
  if (n === 0) return _emptyArgsWire;
  const limit = _MathMin(4096, _maxOpArgBytes);
  const buf = _wireScratch;
  const dv = _wireScratchDV;
  _call(_DVSetUint32, dv, 0, n >>> 0, true);
  let p = 4;
  for (let i = 0; i < n; i++) {
    p = _tryFastEncodeLeaf(buf, dv, p, args[i], limit);
    if (p < 0) break;
  }
  if (p >= 0) {
    return _call(_TASubarray, buf, 0, p);
  }
  _checkWireBudget(args, _maxOpArgBytes);
  if (!_sharedWireBufBusy) {
    _sharedWireBufBusy = true;
    _sharedWireBuf.pos = 0;
    _sharedWireBuf.budget = _maxOpArgBytes;
    try {
      _sharedWireBuf.u32(n);
      for (let i = 0; i < n; i++) {
        if (!_encodeWireLeaf(_sharedWireBuf, args[i])) {
          _encodeWireVal(_sharedWireBuf, args[i], 0);
        }
      }
      const res = _sharedWireBuf.finish();
      // Copy out if buffer grew huge so scratch doesn't retain giant allocations.
      if (_sharedWireBuf.buf.length > 65536) {
        const copy = new _Uint8Array(res.length);
        _call(_TASet, copy, res);
        const freshAb = new _ArrayBuffer(4096);
        _sharedWireBuf.ab = freshAb;
        _sharedWireBuf.buf = new _Uint8Array(freshAb);
        _sharedWireBuf.dv = new _DataView(freshAb);
        return copy;
      }
      return res;
    } finally {
      _sharedWireBufBusy = false;
    }
  }
  const w = new _WireBuf(_maxOpArgBytes);
  w.u32(n);
  for (let i = 0; i < n; i++) {
    if (!_encodeWireLeaf(w, args[i])) {
      _encodeWireVal(w, args[i], 0);
    }
  }
  return w.finish();
}

let _rBuf = null;
let _rDV = null;
let _rPos = 0;
function _rInit(bytes) {
  _rBuf = bytes;
  _rDV = new _DataView(
    _call(_TAGetBuffer, bytes),
    _call(_TAGetByteOffset, bytes),
    _call(_TAGetByteLength, bytes),
  );
  _rPos = 0;
}
function _rU8() { return _call(_DVGetUint8, _rDV, _rPos++); }
function _rU32() {
  const v = _call(_DVGetUint32, _rDV, _rPos, true);
  _rPos += 4;
  return v;
}
function _rF64() {
  const v = _call(_DVGetFloat64, _rDV, _rPos, true);
  _rPos += 8;
  return v;
}
function _rStr() {
  const len = _rU32();
  const start = _rPos;
  _rPos = start + len;
  if (len === 0) return "";
  return utf8_decode(_call(_TASubarray, _rBuf, start, start + len), 0);
}
function _rBytesVal() {
  const len = _rU32();
  const out = new _Uint8Array(len);
  if (len > 0) {
    _call(_TASet, out, _call(_TASubarray, _rBuf, _rPos, _rPos + len));
    _rPos += len;
  }
  return out;
}
function _rValue() {
  const tag = _call(_DVGetUint8, _rDV, _rPos++);
  switch (tag) {
    case TAG_UNDEFINED: return undefined;
    case TAG_NULL: return null;
    case TAG_FALSE: return false;
    case TAG_TRUE: return true;
    case TAG_I32: return _rU32() | 0;
    case TAG_F64: return _rF64();
    case TAG_BIGINT: return _BigInt(_rStr());
    case TAG_STRING: return _rStr();
    case TAG_BYTES: return _rBytesVal();
    case TAG_ARRAY: {
      const len = _rU32();
      const arr = new _Array(len);
      for (let i = 0; i < len; i++) arr[i] = _rValue();
      return arr;
    }
    case TAG_OBJECT: {
      const len = _rU32();
      const obj = {};
      for (let i = 0; i < len; i++) {
        const k = _rStr();
        obj[k] = _rValue();
      }
      return obj;
    }
    default:
      throw new _Error(`unknown wire tag ${tag}`);
  }
}

const _errorBuilders = _ObjectCreate(null);
function _buildErrorParts(cls, customName, msg) {
  if (cls === 1) return new _TypeError(msg);
  if (cls === 2) return new _RangeError(msg);
  if (cls === 3) {
    const builder = _errorBuilders[customName];
    if (typeof builder === "function") {
      try { return builder(msg); } catch {}
    }
    const err = new _Error(msg);
    err.name = customName;
    return err;
  }
  return new _Error(msg);
}
function _rBuildError() {
  const cls = _rU8();
  let customName = "";
  if (cls === 3) customName = _rStr();
  const msg = _rStr();
  return _buildErrorParts(cls, customName, msg);
}

function _decodeEnvelope(bytes) {
  _rInit(bytes);
  const status = _rU8();
  if (status === ENV_OK) return { ok: true, value: _rValue() };
  if (status === ENV_ERR) return { ok: false, error: _rBuildError() };
  if (status === ENV_PENDING) return { pending: true, id: _rU32() };
  throw new _Error(`unknown wire envelope status ${status}`);
}

function _decodeSyncResult(bytes) {
  _rInit(bytes);
  const status = _rU8();
  if (status === ENV_OK) return _rValue();
  if (status === ENV_ERR) throw _rBuildError();
  throw new _Error(`unknown wire envelope status ${status}`);
}

// ---- Fast UTF-16 JSON/primitive op channel (zero Uint8Array indexing) ----
function _isStrFastPrim(v) {
  if (v === null) return true;
  const t = typeof v;
  if (t === "string" || t === "boolean") return true;
  if (t === "number") return v === v && v !== Infinity && v !== -Infinity && (v !== 0 || 1 / v > 0);
  return false;
}

function _tryCopyStrArray(v) {
  if (typeof v !== "object" || v === null || _isProxy(v) || !_ArrayIsArray(v)) return null;
  let len;
  try { len = v.length; } catch { return null; }
  if (len === 0) return [];
  if (len > 64) return null;
  const out = new _Array(len);
  for (let i = 0; i < len; i++) {
    let desc;
    try { desc = _ObjectGetOwnPropertyDescriptor(v, i); } catch { return null; }
    if (!desc || desc.get || desc.set) return null;
    const item = desc.value;
    if (_isStrFastPrim(item)) {
      out[i] = item;
      continue;
    }
    if (item && typeof item === "object" && !_isProxy(item) && _ArrayIsArray(item)) {
      let ilen;
      try { ilen = item.length; } catch { return null; }
      if (ilen !== 2) return null;
      let d0, d1;
      try {
        d0 = _ObjectGetOwnPropertyDescriptor(item, 0);
        d1 = _ObjectGetOwnPropertyDescriptor(item, 1);
      } catch {
        return null;
      }
      if (!d0 || d0.get || d0.set || typeof d0.value !== "string") return null;
      if (!d1 || d1.get || d1.set || typeof d1.value !== "string") return null;
      out[i] = [d0.value, d1.value];
      continue;
    }
    return null;
  }
  return out;
}

function _tryEncodeStrArgs(args, n) {
  if (n === 0) return "[]";
  if (n === 1) {
    const a0 = args[0];
    if (typeof a0 === "number" && (a0 | 0) === a0 && (a0 !== 0 || 1 / a0 > 0)) {
      return `[${a0}]`;
    }
    if (_isStrFastPrim(a0)) {
      const s = _JSONStringify([a0]);
      return s.length * 2 <= _maxOpArgBytes ? s : null;
    }
    const c0 = _tryCopyStrArray(a0);
    if (c0 !== null) {
      const s = _JSONStringify([c0]);
      return s.length * 2 <= _maxOpArgBytes ? s : null;
    }
    return null;
  }
  const arr = new _Array(n);
  for (let i = 0; i < n; i++) {
    const v = args[i];
    if (_isStrFastPrim(v)) {
      arr[i] = v;
    } else {
      const cv = _tryCopyStrArray(v);
      if (cv === null) return null;
      arr[i] = cv;
    }
  }
  const s = _JSONStringify(arr);
  return s.length * 2 <= _maxOpArgBytes ? s : null;
}

function _decodeStrErr(resp) {
  const parts = _JSONParse(_call(_StrSlice, resp, 1));
  return _buildErrorParts(parts[0], parts[1], parts[2]);
}

function _isI32(v) {
  return typeof v === "number" && (v | 0) === v && (v !== 0 || 1 / v > 0);
}

function _flattenHeaderPairs(headers) {
  if (typeof headers !== "object" || headers === null || _isProxy(headers) || !_ArrayIsArray(headers)) {
    return null;
  }
  let len;
  try { len = headers.length; } catch { return null; }
  if (len === 0) return "";
  if (len > 64) return null;
  let out = "";
  for (let i = 0; i < len; i++) {
    let desc;
    try { desc = _ObjectGetOwnPropertyDescriptor(headers, i); } catch { return null; }
    if (!desc || desc.get || desc.set) return null;
    const item = desc.value;
    if (!item || typeof item !== "object" || _isProxy(item) || !_ArrayIsArray(item)) return null;
    let ilen;
    try { ilen = item.length; } catch { return null; }
    if (ilen !== 2) return null;
    let d0, d1;
    try {
      d0 = _ObjectGetOwnPropertyDescriptor(item, 0);
      d1 = _ObjectGetOwnPropertyDescriptor(item, 1);
    } catch {
      return null;
    }
    if (!d0 || d0.get || d0.set || typeof d0.value !== "string") return null;
    if (!d1 || d1.get || d1.set || typeof d1.value !== "string") return null;
    const k = d0.value;
    const v = d1.value;
    if (_call(_StrIndexOf, k, "\0") !== -1 || _call(_StrIndexOf, v, "\0") !== -1) return null;
    out = i === 0 ? `${k}\0${v}` : `${out}\0${k}\0${v}`;
  }
  return out;
}

function _decodeStrValue(resp) {
  const tag = resp[0];
  if (tag === "u") return undefined;
  if (tag === "n") return null;
  if (tag === "t") return true;
  if (tag === "f") return false;
  if (tag === "i") return _Number(_call(_StrSlice, resp, 1)) | 0;
  if (tag === "d") return _Number(_call(_StrSlice, resp, 1));
  if (tag === "s") return _call(_StrSlice, resp, 1);
  if (tag === "m") { // { text, encoding: "utf8" }
    return { text: _call(_StrSlice, resp, 1), encoding: "utf8" };
  }
  if (tag === "r") { // { text, value_handle: 0, encoding: "utf8" }
    return { text: _call(_StrSlice, resp, 1), value_handle: 0, encoding: "utf8" };
  }
  if (tag === "2") { // [num, num]
    const c1 = _call(_StrIndexOf, resp, ",", 1);
    return [
      _Number(_call(_StrSlice, resp, 1, c1)),
      _Number(_call(_StrSlice, resp, c1 + 1)),
    ];
  }
  if (tag === "3") { // [num, num, num]
    const c1 = _call(_StrIndexOf, resp, ",", 1);
    const c2 = _call(_StrIndexOf, resp, ",", c1 + 1);
    return [
      _Number(_call(_StrSlice, resp, 1, c1)),
      _Number(_call(_StrSlice, resp, c1 + 1, c2)),
      _Number(_call(_StrSlice, resp, c2 + 1)),
    ];
  }
  if (tag === "5") { // [bool, num, bool, num, num]
    const c2 = _call(_StrIndexOf, resp, ",", 3);
    const c4 = _call(_StrIndexOf, resp, ",", c2 + 3);
    return [
      resp[1] === "1",
      _Number(_call(_StrSlice, resp, 3, c2)),
      resp[c2 + 1] === "1",
      _Number(_call(_StrSlice, resp, c2 + 3, c4)),
      _Number(_call(_StrSlice, resp, c4 + 1)),
    ];
  }
  if (tag === "j") return _JSONParse(_call(_StrSlice, resp, 1));
  if (tag === "e") throw _decodeStrErr(resp);
  throw new _Error(`unknown str envelope tag ${tag}`);
}

// ---- Structured Clone / Serialize / Deserialize (header [0xff, 0x0f]) ----
const _HOST_BRAND = _SymbolFor("dd.hostObject");
const SC_UNDEF = 1;
const SC_NULL = 2;
const SC_FALSE = 3;
const SC_TRUE = 4;
const SC_I32 = 5;
const SC_F64 = 6;
const SC_BIGINT = 7;
const SC_STRING16 = 8;
const SC_DATE = 9;
const SC_REGEXP = 10;
const SC_BOX_NUM = 11;
const SC_BOX_STR = 12;
const SC_BOX_BOOL = 13;
const SC_BOX_BIGINT = 14;
const SC_ERROR = 15;
const SC_AB = 16;
const SC_VIEW = 17;
const SC_ARRAY = 18;
const SC_OBJECT = 19;
const SC_MAP = 20;
const SC_SET = 21;
const SC_REF = 22;
const SC_HOST_OBJECT = 0x5c;
let _scRecursion = 0;

const _VIEW_KINDS = [
  "DataView", "Int8Array", "Uint8Array", "Uint8ClampedArray",
  "Int16Array", "Uint16Array", "Int32Array", "Uint32Array",
  "Float16Array", "Float32Array", "Float64Array", "BigInt64Array", "BigUint64Array",
];
const _VIEW_CTORS = [
  DataView, Int8Array, Uint8Array, Uint8ClampedArray,
  Int16Array, Uint16Array, Int32Array, Uint32Array,
  typeof Float16Array === "function" ? Float16Array : null,
  Float32Array, Float64Array, BigInt64Array, BigUint64Array,
];
const _ERR_CTORS = {
  Error: _Error,
  TypeError: _TypeError,
  RangeError: _RangeError,
  EvalError: _EvalError,
  SyntaxError: _SyntaxError,
  ReferenceError: _ReferenceError,
  URIError: _URIError,
};

function _throwCloneError(msg, errorCallback) {
  if (typeof errorCallback === "function") {
    errorCallback(msg);
  }
  throw new _TypeError(msg);
}

function _scSerialize(value, hostObjects, _transferred, forStorage, errorCallback) {
  if (hostObjects !== undefined && hostObjects !== null && !_ArrayIsArray(hostObjects)) {
    throw new _TypeError("hostObjects not an array");
  }
  if (errorCallback !== undefined && errorCallback !== null && typeof errorCallback !== "function") {
    throw new _TypeError("Invalid error callback");
  }
  if (_scRecursion > 48) {
    throw new _RangeError("Maximum call stack size exceeded");
  }
  _scRecursion++;
  try {
    const w = new _WireBuf(64 * 1024 * 1024);
    w.u8(0xff);
    w.u8(0x0f);
    const seen = new _Map();
    let nextId = 0;

    function writeVarint32(n) {
      let v = n >>> 0;
      while (v >= 0x80) {
        w.u8((v & 0x7f) | 0x80);
        v >>>= 7;
      }
      w.u8(v);
    }

    function writeStr16(s) {
      const n = s.length;
      w.u32(n);
      w.ensure(n * 2);
      const buf = w.buf;
      let p = w.pos;
      for (let i = 0; i < n; i++) {
        const cu = _call(_StrCharCodeAt, s, i);
        buf[p++] = cu & 0xff;
        buf[p++] = (cu >> 8) & 0xff;
      }
      w.pos = p;
    }

    function writeVal(v, depth) {
      if (depth > 128) throw new _RangeError("Maximum call stack size exceeded");
      if (v === undefined) { w.u8(SC_UNDEF); return; }
      if (v === null) { w.u8(SC_NULL); return; }
      const t = typeof v;
      if (t === "boolean") { w.u8(v ? SC_TRUE : SC_FALSE); return; }
      if (t === "number") {
        if ((v | 0) === v && (v !== 0 || 1 / v > 0)) {
          w.u8(SC_I32); w.u32(v | 0);
        } else {
          w.u8(SC_F64); w.f64(v);
        }
        return;
      }
      if (t === "bigint") { w.u8(SC_BIGINT); w.str(_call(_BigIntToString, v)); return; }
      if (t === "string") { w.u8(SC_STRING16); writeStr16(v); return; }
      if (t === "symbol" || t === "function") {
        _throwCloneError(`${t} could not be cloned`, errorCallback);
      }
      if (_isSharedArrayBuffer(v)) {
        _throwCloneError("SharedArrayBuffer cannot be serialized", errorCallback);
      }
      if (typeof WebAssembly === "object" && WebAssembly !== null &&
          typeof WebAssembly.Module === "function" && v instanceof WebAssembly.Module) {
        _throwCloneError(forStorage ? "Wasm modules cannot be stored" : "Wasm modules cannot be cloned", errorCallback);
      }
      if (_isProxy(v)) {
        _throwCloneError("Proxy could not be cloned", errorCallback);
      }
      if (_isPromise(v)) {
        _throwCloneError("Promise could not be cloned", errorCallback);
      }
      if (_isWeakMap(v) || _isWeakSet(v)) {
        _throwCloneError("Weak collection could not be cloned", errorCallback);
      }
      if (_call(_MapHas, seen, v)) {
        w.u8(SC_REF);
        w.u32(_call(_MapGet, seen, v));
        return;
      }
      // Check branded host object.
      if (_HOST_BRAND in v) {
        const brandVal = v[_HOST_BRAND];
        if (typeof brandVal === "function") {
          const id = nextId++;
          _call(_MapSet, seen, v, id);
          w.u8(SC_HOST_OBJECT);
          writeVarint32(0xffffffff);
          const desc = _call(brandVal, v);
          writeVal(desc, depth + 1);
          return;
        }
        if (hostObjects) {
          for (let i = 0; i < hostObjects.length; i++) {
            if (hostObjects[i] === v) {
              const id = nextId++;
              _call(_MapSet, seen, v, id);
              w.u8(SC_HOST_OBJECT);
              writeVarint32(i);
              return;
            }
          }
        }
        _throwCloneError("Unsupported object type", errorCallback);
      }

      const id = nextId++;
      _call(_MapSet, seen, v, id);

      if (_isDate(v)) {
        w.u8(SC_DATE);
        w.f64(_call(_DateGetTime, v));
        return;
      }
      if (_isRegExp(v)) {
        w.u8(SC_REGEXP);
        writeStr16(_call(_RegExpGetSource, v));
        writeStr16(_call(_RegExpGetFlags, v));
        return;
      }
      if (_isNumberObject(v)) { w.u8(SC_BOX_NUM); w.f64(_call(_NumberValueOf, v)); return; }
      if (_isStringObject(v)) { w.u8(SC_BOX_STR); writeStr16(_call(_StringValueOf, v)); return; }
      if (_isBooleanObject(v)) { w.u8(SC_BOX_BOOL); w.u8(_call(_BooleanValueOf, v) ? 1 : 0); return; }
      if (_isBigIntObject(v)) { w.u8(SC_BOX_BIGINT); w.str(_call(_BigIntToString, _call(_BigIntValueOf, v))); return; }
      if (_isSymbolObject(v)) { _throwCloneError("Symbol object could not be cloned", errorCallback); }
      if (_isNativeError(v)) {
        w.u8(SC_ERROR);
        writeStr16(typeof v.name === "string" ? v.name : "Error");
        writeStr16(typeof v.message === "string" ? v.message : "");
        const hasCause = _ObjectHasOwn(v, "cause");
        w.u8(hasCause ? 1 : 0);
        if (hasCause) writeVal(v.cause, depth + 1);
        return;
      }
      if (_isArrayBuffer(v)) {
        if (_ABGetDetached && _call(_ABGetDetached, v)) {
          _throwCloneError("ArrayBuffer is detached", errorCallback);
        }
        let byteLen;
        try { byteLen = _call(_ABGetByteLength, v); }
        catch { _throwCloneError("ArrayBuffer is detached", errorCallback); }
        const resizable = _ABGetResizable ? !!_call(_ABGetResizable, v) : false;
        const maxLen = resizable && _ABGetMaxByteLength ? _call(_ABGetMaxByteLength, v) : byteLen;
        w.u8(SC_AB);
        w.u8(resizable ? 1 : 0);
        w.u32(byteLen);
        w.u32(maxLen);
        if (byteLen > 0) w.bytes(new _Uint8Array(v, 0, byteLen));
        return;
      }
      if (_isArrayBufferView(v)) {
        const isDv = _isDataView(v);
        const kindName = isDv ? "DataView" : _call(_TAGetToStringTag, v);
        const kindIdx = _call(_ArrayIndexOf, _VIEW_KINDS, kindName);
        if (kindIdx < 0) _throwCloneError("Unsupported view type", errorCallback);
        let buf, off, len;
        try {
          buf = _call(isDv ? _DVGetBuffer : _TAGetBuffer, v);
          if (_isSharedArrayBuffer(buf)) {
            _throwCloneError("SharedArrayBuffer cannot be serialized", errorCallback);
          }
          if (_ABGetDetached && _call(_ABGetDetached, buf)) {
            _throwCloneError("ArrayBuffer is detached", errorCallback);
          }
          if (!isDv) {
            // Throws TypeError if detached or out of bounds over a shrunk resizable ArrayBuffer.
            _call(_TAValidate, v, 0, 0);
          }
          off = _call(isDv ? _DVGetByteOffset : _TAGetByteOffset, v);
          len = _call(isDv ? _DVGetByteLength : _TAGetByteLength, v);
        } catch (e) {
          if (e instanceof _TypeError && e.message === "SharedArrayBuffer cannot be serialized") throw e;
          _throwCloneError("ArrayBufferView is detached or out of bounds", errorCallback);
        }
        const resizable = _ABGetResizable ? !!_call(_ABGetResizable, buf) : false;
        const abLen = _call(_ABGetByteLength, buf);
        const lengthTracking = resizable && off + len === abLen;
        w.u8(SC_VIEW);
        w.u8(kindIdx | (lengthTracking ? 0x80 : 0));
        w.u32(off);
        w.u32(len);
        writeVal(buf, depth + 1);
        return;
      }
      if (_isMap(v)) {
        w.u8(SC_MAP);
        const entries = [];
        _call(_MapForEach, v, (mv, mk) => { entries[entries.length] = [mk, mv]; });
        w.u32(entries.length);
        for (let i = 0; i < entries.length; i++) {
          writeVal(entries[i][0], depth + 1);
          writeVal(entries[i][1], depth + 1);
        }
        return;
      }
      if (_isSet(v)) {
        w.u8(SC_SET);
        const items = [];
        _call(_SetForEach, v, (sv) => { items[items.length] = sv; });
        w.u32(items.length);
        for (let i = 0; i < items.length; i++) writeVal(items[i], depth + 1);
        return;
      }
      if (_ArrayIsArray(v)) {
        w.u8(SC_ARRAY);
        w.u32(v.length >>> 0);
        const keys = _ObjectKeys(v);
        w.u32(keys.length);
        for (let i = 0; i < keys.length; i++) {
          const k = keys[i];
          writeStr16(k);
          writeVal(v[k], depth + 1);
        }
        return;
      }
      w.u8(SC_OBJECT);
      const keys = _ObjectKeys(v);
      w.u32(keys.length);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        writeStr16(k);
        writeVal(v[k], depth + 1);
      }
    }

    writeVal(value, 0);
    const res = w.finish();
    const copy = new _Uint8Array(res.length);
    _call(_TASet, copy, res);
    return copy;
  } finally {
    _scRecursion--;
  }
}

function _scDeserialize(bufArg, hostObjects, _transferred, deserializers, _forStorage) {
  const bytes = _bufferBytes(bufArg);
  if (bytes === null) throw new _TypeError("expected an ArrayBuffer or ArrayBufferView");
  if (hostObjects !== undefined && hostObjects !== null && !_ArrayIsArray(hostObjects)) {
    throw new _TypeError("hostObjects not an array");
  }
  if (deserializers !== undefined && deserializers !== null && typeof deserializers !== "object") {
    throw new _TypeError("deserializers not an object");
  }
  if (_scRecursion > 48) {
    throw new _RangeError("Maximum call stack size exceeded");
  }
  if (bytes.length < 2 || bytes[0] !== 0xff || bytes[1] !== 0x0f) {
    throw new _RangeError("could not deserialize value");
  }
  _scRecursion++;
  try {
    let p = 2;
    const end = bytes.length;
    const objects = [];

    function need(n) {
      if (n < 0 || p + n > end) throw new _RangeError("could not deserialize value");
    }
    function u8() { need(1); return bytes[p++]; }
    function readVarint32() {
      let result = 0;
      let shift = 0;
      for (let i = 0; i < 10; i++) {
        const b = u8();
        if (shift < 32) {
          result = (result | ((b & 0x7f) << shift)) >>> 0;
        }
        if ((b & 0x80) === 0) return result >>> 0;
        shift += 7;
      }
      throw new _RangeError("could not deserialize value");
    }
    function u32() {
      need(4);
      const v = (bytes[p] | (bytes[p + 1] << 8) | (bytes[p + 2] << 16) | (bytes[p + 3] << 24)) >>> 0;
      p += 4;
      return v;
    }
    function f64() {
      need(8);
      for (let i = 0; i < 8; i++) _f64Bytes[i] = bytes[p + i];
      p += 8;
      return _f64View[0];
    }
    function str16() {
      const n = u32();
      need(n * 2);
      if (n === 0) return "";
      const chunks = [];
      let cLen = 0;
      const chunkBuf = new _Array(1024);
      let rem = n;
      while (rem > 0) {
        const take = rem > 1024 ? 1024 : rem;
        for (let i = 0; i < take; i++) {
          chunkBuf[i] = bytes[p] | (bytes[p + 1] << 8);
          p += 2;
        }
        chunkBuf.length = take;
        chunks[cLen++] = _ReflectApply(_StrFromCharCode, null, chunkBuf);
        rem -= take;
      }
      if (cLen === 1) return chunks[0];
      return _call(_ArrayJoin, chunks, "");
    }
    function str8() {
      const n = u32();
      need(n);
      const s = _utf8Decode(_call(_TASubarray, bytes, p, p + n), false);
      p += n;
      return s;
    }

    function readVal(depth) {
      if (depth > 128) throw new _RangeError("could not deserialize value");
      const tag = u8();
      switch (tag) {
        case SC_UNDEF: return undefined;
        case SC_NULL: return null;
        case SC_FALSE: return false;
        case SC_TRUE: return true;
        case SC_I32: return u32() | 0;
        case SC_F64: return f64();
        case SC_BIGINT: {
          const s = str8();
          try { return _BigInt(s); } catch { throw new _RangeError("could not deserialize value"); }
        }
        case SC_STRING16: return str16();
        case SC_REF: {
          const id = u32();
          if (id >= objects.length) throw new _RangeError("could not deserialize value");
          return objects[id];
        }
        case SC_HOST_OBJECT: {
          const idx = readVarint32();
          if (idx === 0xffffffff) {
            const slot = objects.length;
            objects[slot] = null;
            const desc = readVal(depth + 1);
            if (!deserializers || typeof desc !== "object" || desc === null) {
              throw new _Error("Failed to deserialize host object");
            }
            const typeKey = desc.type;
            if (typeKey === undefined || typeKey === null) {
              throw new _Error("Failed to deserialize host object");
            }
            const revive = deserializers[typeKey];
            if (typeof revive !== "function") {
              throw new _Error("Failed to deserialize host object");
            }
            const revived = _call(revive, null, desc);
            if (revived === null || revived === undefined) {
              throw new _Error("Failed to deserialize host object");
            }
            objects[slot] = revived;
            return revived;
          }
          if (!hostObjects || !(idx in hostObjects)) {
            throw new _Error("Failed to deserialize host object");
          }
          let obj = hostObjects[idx];
          if (obj === null || obj === undefined) {
            throw new _Error("Failed to deserialize host object");
          }
          if (typeof obj !== "object" && typeof obj !== "function") {
            obj = _Object(obj);
          }
          objects[objects.length] = obj;
          return obj;
        }
        case SC_DATE: {
          const d = new _Date(f64());
          objects[objects.length] = d;
          return d;
        }
        case SC_REGEXP: {
          const src = str16();
          const flags = str16();
          let re;
          try { re = new _RegExp(src, flags); } catch { throw new _RangeError("could not deserialize value"); }
          objects[objects.length] = re;
          return re;
        }
        case SC_BOX_NUM: { const o = new _Number(f64()); objects[objects.length] = o; return o; }
        case SC_BOX_STR: { const o = new _String(str16()); objects[objects.length] = o; return o; }
        case SC_BOX_BOOL: { const o = new _Boolean(u8() !== 0); objects[objects.length] = o; return o; }
        case SC_BOX_BIGINT: {
          const s = str8();
          try { const o = _Object(_BigInt(s)); objects[objects.length] = o; return o; }
          catch { throw new _RangeError("could not deserialize value"); }
        }
        case SC_ERROR: {
          const name = str16();
          const msg = str16();
          const hasCause = u8();
          const Ctor = _ERR_CTORS[name] || _Error;
          const err = new Ctor(msg);
          if (!_ERR_CTORS[name]) err.name = name;
          objects[objects.length] = err;
          if (hasCause) {
            const cause = readVal(depth + 1);
            _ObjectDefineProperty(err, "cause", { value: cause, writable: true, enumerable: false, configurable: true });
          }
          return err;
        }
        case SC_AB: {
          const resizable = u8() !== 0;
          const byteLen = u32();
          const maxLen = u32();
          need(byteLen);
          if (resizable && maxLen < byteLen) throw new _RangeError("could not deserialize value");
          const ab = resizable ? new _ArrayBuffer(byteLen, { maxByteLength: maxLen }) : new _ArrayBuffer(byteLen);
          if (byteLen > 0) _call(_TASet, new _Uint8Array(ab), _call(_TASubarray, bytes, p, p + byteLen));
          p += byteLen;
          objects[objects.length] = ab;
          return ab;
        }
        case SC_VIEW: {
          const rawKind = u8();
          const lengthTracking = (rawKind & 0x80) !== 0;
          const kindIdx = rawKind & 0x7f;
          const off = u32();
          const len = u32();
          const slot = objects.length;
          objects[slot] = null;
          const ab = readVal(depth + 1);
          if (!_isArrayBuffer(ab)) throw new _RangeError("could not deserialize value");
          if (_ABGetDetached && _call(_ABGetDetached, ab)) {
            throw new _RangeError("could not deserialize value");
          }
          const kindName = _VIEW_KINDS[kindIdx];
          const Ctor = _VIEW_CTORS[kindIdx];
          if (typeof Ctor !== "function") throw new _RangeError("could not deserialize value");
          const abLen = _call(_ABGetByteLength, ab);
          const abResizable = _ABGetResizable ? !!_call(_ABGetResizable, ab) : false;
          if (off > abLen) throw new _RangeError("could not deserialize value");
          let view;
          try {
            if (lengthTracking && abResizable && off + len > abLen) {
              if (kindName === "DataView") {
                view = new Ctor(ab, off);
              } else {
                const bpe = Ctor.BYTES_PER_ELEMENT;
                if (off % bpe !== 0) throw new _RangeError("could not deserialize value");
                view = new Ctor(ab, off);
              }
            } else {
              if (off + len > abLen) throw new _RangeError("could not deserialize value");
              if (kindName === "DataView") {
                view = new Ctor(ab, off, len);
              } else {
                const bpe = Ctor.BYTES_PER_ELEMENT;
                if (off % bpe !== 0 || len % bpe !== 0) throw new _RangeError("could not deserialize value");
                view = new Ctor(ab, off, len / bpe);
              }
            }
          } catch {
            throw new _RangeError("could not deserialize value");
          }
          objects[slot] = view;
          return view;
        }
        case SC_MAP: {
          const count = u32();
          need(count * 2);
          const m = new _Map();
          objects[objects.length] = m;
          for (let i = 0; i < count; i++) {
            const mk = readVal(depth + 1);
            const mv = readVal(depth + 1);
            _call(_MapSet, m, mk, mv);
          }
          return m;
        }
        case SC_SET: {
          const count = u32();
          need(count);
          const s = new _Set();
          objects[objects.length] = s;
          for (let i = 0; i < count; i++) {
            _call(_SetAdd, s, readVal(depth + 1));
          }
          return s;
        }
        case SC_ARRAY: {
          const arrLen = u32();
          const propCount = u32();
          need(propCount * 5);
          const arr = new _Array(arrLen);
          objects[objects.length] = arr;
          for (let i = 0; i < propCount; i++) {
            const k = str16();
            const val = readVal(depth + 1);
            _ObjectDefineProperty(arr, k, { value: val, writable: true, enumerable: true, configurable: true });
          }
          return arr;
        }
        case SC_OBJECT: {
          const propCount = u32();
          need(propCount * 5);
          const obj = {};
          objects[objects.length] = obj;
          for (let i = 0; i < propCount; i++) {
            const k = str16();
            const val = readVal(depth + 1);
            _ObjectDefineProperty(obj, k, { value: val, writable: true, enumerable: true, configurable: true });
          }
          return obj;
        }
        default:
          throw new _RangeError("could not deserialize value");
      }
    }

    const out = readVal(0);
    if (p !== end) throw new _RangeError("could not deserialize value");
    return out;
  } finally {
    _scRecursion--;
  }
}

// ---- Pending async op resolvers & ext script factories ----
const _pendingAsyncOps = new _Map();
const _extFactories = new _Map();
let _opsObject = _ObjectCreate(null);
let _internals = undefined;
let _wasmStreamingHandler = null;
const _wasmStreams = new _Map();
let _nextWasmStreamId = 1;
const _wasmModuleInfo = new _WeakMap();

class _WasmCompileError extends _Error {
  constructor(message) {
    super(message);
    this.name = "CompileError";
  }
}
class _WasmLinkError extends _Error {
  constructor(message) {
    super(message);
    this.name = "LinkError";
  }
}
class _WasmRuntimeError extends _Error {
  constructor(message) {
    super(message);
    this.name = "RuntimeError";
  }
}
_errorBuilders["WebAssemblyCompileError"] = (m) => new _WasmCompileError(m);
_errorBuilders["WebAssemblyLinkError"] = (m) => new _WasmLinkError(m);
_errorBuilders["WebAssemblyRuntimeError"] = (m) => new _WasmRuntimeError(m);

function _wasmHostCall(kind, args) {
  const bytes = _encodeWireArgs(args);
  const env = _decodeEnvelope(wasm_op(kind, bytes));
  if (!env.ok) throw env.error;
  return env.value;
}

class _WasmModule {
  constructor(bufferSource) {
    const bytes = _bufferBytes(bufferSource);
    if (bytes === null) {
      throw new _TypeError("WebAssembly.Module(): Argument 0 must be a buffer source");
    }
    const res = _wasmHostCall(1, [bytes]);
    _call(_WeakMapSet, _wasmModuleInfo, this, res);
  }
  static exports(mod) {
    const info = _call(_WeakMapGet, _wasmModuleInfo, mod);
    if (!info) throw new _TypeError("WebAssembly.Module.exports(): Argument 0 must be a WebAssembly.Module");
    return info.exports.map((e) => ({ name: e.name, kind: e.kind }));
  }
}

class _WasmInstance {
  constructor(mod, _importObject) {
    const info = _call(_WeakMapGet, _wasmModuleInfo, mod);
    if (!info) {
      throw new _TypeError("WebAssembly.Instance(): Argument 0 must be a WebAssembly.Module");
    }
    const res = _wasmHostCall(2, [info.id]);
    const instId = res.id;
    const exportsObj = _ObjectCreate(null);
    for (let i = 0; i < res.exports.length; i++) {
      const exp = res.exports[i];
      if (exp.kind === "function") {
        const expName = exp.name;
        exportsObj[expName] = function (...callArgs) {
          return _wasmHostCall(3, [instId, expName, callArgs]);
        };
      } else if (exp.kind === "global") {
        const expName = exp.name;
        exportsObj[expName] = {
          get value() { return _wasmHostCall(4, [instId, expName]); },
        };
      }
    }
    _ObjectFreeze(exportsObj);
    _ObjectDefineProperty(this, "exports", {
      value: exportsObj,
      enumerable: true,
      configurable: false,
      writable: false,
    });
  }
}

const _WebAssembly = {
  CompileError: _WasmCompileError,
  LinkError: _WasmLinkError,
  RuntimeError: _WasmRuntimeError,
  Module: _WasmModule,
  Instance: _WasmInstance,
  validate(bufferSource) {
    const bytes = _bufferBytes(bufferSource);
    if (bytes === null) {
      throw new _TypeError("WebAssembly.validate(): Argument 0 must be a buffer source");
    }
    return !!_wasmHostCall(0, [bytes]);
  },
  compile(bufferSource) {
    return new _Promise((resolve, reject) => {
      try {
        resolve(new _WasmModule(bufferSource));
      } catch (e) {
        reject(e);
      }
    });
  },
  instantiate(first, importObject) {
    return new _Promise((resolve, reject) => {
      try {
        if (_call(_WeakMapHas, _wasmModuleInfo, first)) {
          resolve(new _WasmInstance(first, importObject));
        } else {
          const module = new _WasmModule(first);
          const instance = new _WasmInstance(module, importObject);
          resolve({ module, instance });
        }
      } catch (e) {
        reject(e);
      }
    });
  },
  compileStreaming(source) {
    if (typeof _wasmStreamingHandler !== "function") {
      return _PromiseReject(new _TypeError("WebAssembly streaming compilation is not configured"));
    }
    const id = _nextWasmStreamId++;
    let resolve, reject;
    const promise = new _Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    _call(_MapSet, _wasmStreams, id, { resolve, reject, chunks: [], total: 0, url: "" });
    try {
      const ret = _wasmStreamingHandler(source, id);
      if (ret && typeof ret.then === "function") {
        _call(_PromiseThen, ret, undefined, (e) => {
          if (_call(_MapHas, _wasmStreams, id)) {
            _call(_MapDelete, _wasmStreams, id);
            reject(e);
          }
        });
      }
    } catch (e) {
      if (_call(_MapHas, _wasmStreams, id)) {
        _call(_MapDelete, _wasmStreams, id);
        reject(e);
      }
    }
    return promise;
  },
  instantiateStreaming(source, importObject) {
    return _call(
      _PromiseThen,
      _WebAssembly.compileStreaming(source),
      (module) => ({ module, instance: new _WasmInstance(module, importObject) }),
    );
  },
};
_ObjectDefineProperty(_WebAssembly, _SymbolToStringTag, { value: "WebAssembly", configurable: true });
_ObjectDefineProperty(_globalThis, "WebAssembly", {
  value: _WebAssembly,
  writable: true,
  enumerable: false,
  configurable: true,
});

export function __dd_register_ext_factory(specifier, fn) {
  _call(_MapSet, _extFactories, specifier, fn);
}

// Pure-JS builtins bound directly into `ops`.
const _BUILTIN_OPS = {
  op_encode(text) {
    if (typeof text !== "string") throw new _TypeError("Invalid argument");
    if (text.length === 0) return new _Uint8Array(0);
    return utf8_encode(text);
  },
  op_decode(buf) {
    if (!_isProxy(buf) && _isTypedArray(buf) && _call(_TAGetToStringTag, buf) === "Uint8Array") {
      let len = 0;
      try {
        len = _call(_TAGetByteLength, buf);
      } catch {
        return "";
      }
      if (len > 0x1fffffe8) throw new _RangeError("Invalid string length");
      if (len === 0) return "";
      try {
        return utf8_decode(buf, 1);
      } catch {
        return "";
      }
    }
    const bytes = _bufferBytes(buf);
    if (bytes === null) throw new _TypeError("expected an ArrayBuffer or ArrayBufferView");
    return _utf8Decode(bytes, true);
  },
  op_serialize: _scSerialize,
  op_deserialize: _scDeserialize,
  op_structured_clone(value, deserializers) {
    if (deserializers !== undefined && deserializers !== null && typeof deserializers !== "object") {
      throw new _TypeError("deserializers not an object");
    }
    const bytes = _scSerialize(value, undefined, undefined, false, undefined);
    return _scDeserialize(bytes, undefined, undefined, deserializers, false);
  },
  op_get_async_context() { return _asyncContextFn ? _asyncContextFn() : _asyncContext; },
  op_set_async_context(ctx) { if (_asyncContextFn) _asyncContextFn(ctx); else _asyncContext = ctx; },
  op_queue_microtask(cb) {
    if (typeof cb !== "function") throw new _TypeError("queueMicrotask requires a function");
    _call(_PromiseThen, _PromiseResolve(undefined), () => { cb(); });
  },
  op_promise_state(p) {
    if (!_isPromise(p)) throw new _TypeError("op_promise_state requires a promise");
    if (_asyncContextFn) {
      const st = _asyncContextFn(2, p);
      if (st === 0) return [0];
      if (st === 1 || st === 2) return [st, _asyncContextFn(3, p)];
    }
    return [0];
  },
  op_proxy_details(v) {
    if (!_isProxy(v)) return null;
    const d = _call(_WeakMapGet, _proxyDetails, v);
    return d ? [d[0], d[1]] : null;
  },
  op_own_non_index_keys(obj) {
    if ((typeof obj !== "object" && typeof obj !== "function") || obj === null) {
      throw new _TypeError("op_own_non_index_keys requires an object");
    }
    // For Arrays and TypedArrays, inspect their OrdinaryObject header directly in Scriptw
    // so a 10M-element array never allocates 10M index strings.
    let target = obj;
    if (_asyncContextFn && !_isProxy(obj) && (_ArrayIsArray(obj) || _isTypedArray(obj))) {
      const hdr = _asyncContextFn(1, obj);
      if (hdr !== undefined) target = hdr;
    }
    const all = _ReflectOwnKeys(target);
    const out = [];
    for (let i = 0; i < all.length; i++) {
      const k = all[i];
      if (typeof k === "string") {
        const idx = k >>> 0;
        if (_String(idx) === k && idx !== 0xffffffff) continue;
      }
      if (_call(_ObjectPrototypePropertyIsEnumerable, target, k)) {
        out[out.length] = k;
      }
    }
    return out;
  },
  op_is_any_array_buffer: (v) => _isArrayBuffer(v) || _isSharedArrayBuffer(v),
  op_is_array_buffer: _isArrayBuffer,
  op_is_array_buffer_view: _isArrayBufferView,
  op_is_data_view: _isDataView,
  op_is_date: _isDate,
  op_is_map: _isMap,
  op_is_native_error: _isNativeError,
  op_is_promise: _isPromise,
  op_is_proxy: _isProxy,
  op_is_reg_exp: _isRegExp,
  op_is_set: _isSet,
  op_is_shared_array_buffer: _isSharedArrayBuffer,
  op_is_string_object: _isStringObject,
  op_is_typed_array: _isTypedArray,
  op_is_boxed_primitive: _isBoxedPrimitive,
  op_register_error_builder(className, builder) {
    if (typeof builder !== "function") throw new _TypeError("error builder must be a function");
    _errorBuilders[_String(className)] = builder;
  },
  op_set_wasm_streaming_handler(handler) {
    if (typeof handler !== "function") {
      throw new _TypeError("the WebAssembly streaming handler must be a function");
    }
    _wasmStreamingHandler = handler;
  },
  op_wasm_streaming_feed(id, chunk) {
    const bytes = _bufferBytes(chunk);
    if (bytes === null) {
      throw new _TypeError("WebAssembly streaming chunk must be an ArrayBuffer or ArrayBufferView");
    }
    const rec = _call(_MapGet, _wasmStreams, id);
    if (!rec) return;
    if (bytes.length > 0) {
      rec.chunks[rec.chunks.length] = bytes;
      rec.total += bytes.length;
    }
  },
  op_wasm_streaming_set_url(id, url) {
    const rec = _call(_MapGet, _wasmStreams, id);
    if (rec) rec.url = _String(url);
  },
  op_wasm_streaming_finish(id) {
    const rec = _call(_MapGet, _wasmStreams, id);
    if (!rec) return;
    _call(_MapDelete, _wasmStreams, id);
    const out = new _Uint8Array(rec.total);
    let offset = 0;
    for (let i = 0; i < rec.chunks.length; i++) {
      _call(_TASet, out, rec.chunks[i], offset);
      offset += rec.chunks[i].length;
    }
    try {
      const mod = new _WasmModule(out);
      rec.resolve(mod);
    } catch (e) {
      rec.reject(e);
    }
  },
  op_wasm_streaming_abort(id, error) {
    const rec = _call(_MapGet, _wasmStreams, id);
    if (!rec) return;
    _call(_MapDelete, _wasmStreams, id);
    rec.reject(error);
  },
  // Arrow function has no [[Construct]] and binds cleanly with Function.prototype.bind!
  op_call_console: (_inspectorMethod, method, ...args) => {
    if (typeof _inspectorMethod !== "function" || typeof method !== "function") {
      throw new _TypeError("op_call_console needs two functions");
    }
    if (_dbgEnabled) {
      const st = inspector_op(4, _dbgEmptyBytes);
      if (st && st.length > 0 && st[0] === 1) {
        _ReflectApply(_inspectorMethod, undefined, args);
      }
    }
    return _ReflectApply(method, undefined, args);
  },
  op_encoding_encode_into: _utf8EncodeInto,
  op_load_ext_script(specifier, bootstrap) {
    const fn = _call(_MapGet, _extFactories, specifier);
    if (typeof fn === "function") return fn(bootstrap);
    throw new _TypeError(`unknown web layer script: ${specifier}`);
  },
};

function _writeU32Buf(buf, values) {
  if (_ArrayIsArray(buf)) {
    const count = _MathMin(values.length, buf.length);
    for (let i = 0; i < count; i++) buf[i] = values[i] >>> 0;
    return;
  }
  if (!_isArrayBufferView(buf)) return;
  const isDv = _isDataView(buf);
  const ab = _call(isDv ? _DVGetBuffer : _TAGetBuffer, buf);
  const off = _call(isDv ? _DVGetByteOffset : _TAGetByteOffset, buf);
  const len = _call(isDv ? _DVGetByteLength : _TAGetByteLength, buf);
  const dv = new _DataView(ab, off, len);
  const count = _MathMin(values.length, (len / 4) | 0);
  for (let i = 0; i < count; i++) {
    _call(_DVSetUint32, dv, i * 4, values[i] >>> 0, true);
  }
}

function _makeSyncOp(opId, name) {
  let fn;
  if (name === "op_url_parse") {
    fn = function op_url_parse(href, componentsBuf) {
      const val = _decodeStrValue(call_sync_str(opId, _JSONStringify([_String(href)])));
      const status = val[0];
      const comps = val[1];
      if (comps) _writeU32Buf(componentsBuf, comps);
      return status;
    };
  } else if (name === "op_url_parse_with_base") {
    fn = function op_url_parse_with_base(href, baseHref, componentsBuf) {
      const val = _decodeStrValue(call_sync_str(opId, _JSONStringify([_String(href), _String(baseHref)])));
      const status = val[0];
      const comps = val[1];
      if (comps) _writeU32Buf(componentsBuf, comps);
      return status;
    };
  } else if (name === "op_url_reparse") {
    fn = function op_url_reparse(href, setter, value, componentsBuf) {
      const val = _decodeStrValue(call_sync_str(opId, _JSONStringify([_String(href), setter >>> 0, _String(value)])));
      const status = val[0];
      const comps = val[1];
      if (comps) _writeU32Buf(componentsBuf, comps);
      return status;
    };
  } else if (name === "op_urlpattern_process_match_input") {
    fn = function op_urlpattern_process_match_input(input, baseURL, offsetsBuf) {
      const env = _decodeEnvelope(call_sync(opId, _encodeWireArgs([input, baseURL])));
      if (!env.ok) throw env.error;
      if (env.value === null || env.value === undefined) return null;
      const concat = env.value[0];
      const offsets = env.value[1];
      _writeU32Buf(offsetsBuf, offsets);
      return concat;
    };
  } else if (name === "op_crypto_get_random_values") {
    fn = function op_crypto_get_random_values(view) {
      if (!_isArrayBufferView(view) || _isProxy(view)) {
        throw new _TypeError("expected an integer TypedArray");
      }
      const isDv = _isDataView(view);
      const ab = _call(isDv ? _DVGetBuffer : _TAGetBuffer, view);
      const off = _call(isDv ? _DVGetByteOffset : _TAGetByteOffset, view);
      const len = _call(isDv ? _DVGetByteLength : _TAGetByteLength, view);
      const env = _decodeEnvelope(call_sync(opId, _encodeWireArgs([len])));
      if (!env.ok) throw env.error;
      _call(_TASet, new _Uint8Array(ab, off, len), env.value);
    };
  } else if (name === "op_response_complete_inline") {
    fn = function op_response_complete_inline(comp, reqCtx, memScope, status, headers, body, finish) {
      if (
        arguments.length === 7
        && _isI32(comp)
        && _isI32(reqCtx)
        && _isI32(memScope)
        && _isI32(status)
        && typeof body === "string"
        && typeof finish === "boolean"
      ) {
        const flat = _flattenHeaderPairs(headers);
        if (flat !== null && (flat.length + body.length) * 2 <= _maxOpArgBytes) {
          const resp = call_sync_resp_inline(opId, comp, reqCtx, memScope, status, flat, body, finish ? 1 : 0);
          const tag = resp[0];
          if (tag !== "F" && tag !== "B") {
            return _decodeStrValue(resp);
          }
        }
      }
      const n = arguments.length;
      const jsonArgs = _tryEncodeStrArgs(arguments, n);
      if (jsonArgs !== null) {
        const resp = call_sync_str(opId, jsonArgs);
        const tag = resp[0];
        if (tag === "B") {
          return _decodeSyncResult(call_sync(0x7fffffff, _emptyArgsWire));
        }
        if (tag !== "F") {
          return _decodeStrValue(resp);
        }
      }
      const args = new _Array(n);
      for (let i = 0; i < n; i++) args[i] = arguments[i];
      return _decodeSyncResult(call_sync(opId, _encodeWireArgs(args)));
    };
  } else {
    let useBinary = false;
    fn = function () {
      const n = arguments.length;
      if (!useBinary) {
        let resp = null;
        if (n === 0) {
          resp = call_sync_0(opId);
        } else if (n === 1) {
          const a0 = arguments[0];
          if (_isI32(a0)) {
            resp = call_sync_u32(opId, a0);
          }
        } else if (n === 2) {
          const a0 = arguments[0];
          const a1 = arguments[1];
          if (_isI32(a0) && typeof a1 === "string" && a1.length * 2 <= _maxOpArgBytes) {
            resp = call_sync_u32_str(opId, a0, a1);
          }
        } else if (n === 3) {
          const a0 = arguments[0];
          const a1 = arguments[1];
          const a2 = arguments[2];
          if (
            _isI32(a0)
            && typeof a1 === "string"
            && typeof a2 === "string"
            && (a1.length + a2.length) * 2 <= _maxOpArgBytes
          ) {
            resp = call_sync_u32_str_str(opId, a0, a1, a2);
          }
        }
        if (resp === null) {
          const jsonArgs = _tryEncodeStrArgs(arguments, n);
          if (jsonArgs !== null) {
            resp = call_sync_str(opId, jsonArgs);
          }
        }
        if (resp !== null) {
          const tag = resp[0];
          if (tag === "B") { // binary fallback (op returned Bytes/BigInt/etc.)
            useBinary = true;
            return _decodeSyncResult(call_sync(0x7fffffff, _emptyArgsWire));
          }
          if (tag !== "F") { // retry via binary (e.g. lone surrogate in string arg)
            return _decodeStrValue(resp);
          }
        }
      }
      if (n === 0) {
        return _decodeSyncResult(call_sync(opId, _emptyArgsWire));
      }
      const limit = _MathMin(4096, _maxOpArgBytes);
      const buf = _wireScratch;
      const dv = _wireScratchDV;
      _call(_DVSetUint32, dv, 0, n >>> 0, true);
      let p = 4;
      for (let i = 0; i < n; i++) {
        p = _tryFastEncodeLeaf(buf, dv, p, arguments[i], limit);
        if (p < 0) break;
      }
      if (p >= 0) {
        return _decodeSyncResult(call_sync(opId, _call(_TASubarray, buf, 0, p)));
      }
      const args = new _Array(n);
      for (let i = 0; i < n; i++) args[i] = arguments[i];
      return _decodeSyncResult(call_sync(opId, _encodeWireArgs(args)));
    };
  }
  _ObjectDefineProperty(fn, "name", { value: name, configurable: true });
  return fn;
}

function _makeAsyncOp(opId, name) {
  let useBinary = false;
  const fn = function () {
    const n = arguments.length;
    if (!useBinary) {
      let resp;
      try {
        if (n === 1) {
          const a0 = arguments[0];
          if (_isI32(a0)) {
            resp = start_async_u32(opId, a0);
          }
        } else if (n === 2) {
          const a0 = arguments[0];
          const a1 = arguments[1];
          if (typeof a0 === "string" && typeof a1 === "string" && (a0.length + a1.length) * 2 <= _maxOpArgBytes) {
            resp = start_async_str2(opId, a0, a1);
          }
        } else if (n === 3) {
          const a0 = arguments[0];
          const a1 = arguments[1];
          const a2 = arguments[2];
          if (
            _isI32(a0)
            && typeof a1 === "string"
            && typeof a2 === "string"
            && (a1.length + a2.length) * 2 <= _maxOpArgBytes
          ) {
            resp = start_async_u32_str2(opId, a0, a1, a2);
          } else if (
            typeof a0 === "string"
            && typeof a1 === "string"
            && typeof a2 === "string"
            && (a0.length + a1.length + a2.length) * 2 <= _maxOpArgBytes
          ) {
            resp = start_async_str3(opId, a0, a1, a2);
          }
        } else if (n === 5) {
          const a0 = arguments[0];
          const a1 = arguments[1];
          const a2 = arguments[2];
          const a3 = arguments[3];
          const a4 = arguments[4];
          if (
            _isI32(a0)
            && _isI32(a1)
            && typeof a2 === "string"
            && typeof a3 === "string"
            && _isI32(a4)
            && (a2.length + a3.length) * 2 <= _maxOpArgBytes
          ) {
            resp = start_async_u32_2_str2_u32(opId, a0, a1, a2, a3, a4);
          }
        }
        if (resp === undefined) {
          const jsonArgs = _tryEncodeStrArgs(arguments, n);
          if (jsonArgs !== null) {
            resp = start_async_str(opId, jsonArgs);
          }
        }
      } catch (e) {
        return _PromiseReject(e);
      }
      if (resp !== undefined) {
        const tag = resp[0];
        if (tag === "p") { // pending
          const id = _Number(_call(_StrSlice, resp, 1)) >>> 0;
          let resolveFn, rejectFn;
          const p = new _Promise((res, rej) => { resolveFn = res; rejectFn = rej; });
          _call(_MapSet, _pendingAsyncOps, id, { resolve: resolveFn, reject: rejectFn, promise: p });
          return p;
        }
        if (tag === "B") { // binary fallback
          useBinary = true;
          try {
            return _PromiseResolve(_decodeSyncResult(call_sync(0x7fffffff, _emptyArgsWire)));
          } catch (e) {
            return _PromiseReject(e);
          }
        }
        if (tag === "e") {
          return _PromiseReject(_decodeStrErr(resp));
        }
        if (tag !== "F") { // retry via binary (e.g. lone surrogate in string arg)
          try {
            return _PromiseResolve(_decodeStrValue(resp));
          } catch (e) {
            return _PromiseReject(e);
          }
        }
      }
    }
    let envBytes;
    try {
      let wire = null;
      if (n === 0) {
        wire = _emptyArgsWire;
      } else {
        const limit = _MathMin(4096, _maxOpArgBytes);
        const buf = _wireScratch;
        const dv = _wireScratchDV;
        _call(_DVSetUint32, dv, 0, n >>> 0, true);
        let p = 4;
        for (let i = 0; i < n; i++) {
          p = _tryFastEncodeLeaf(buf, dv, p, arguments[i], limit);
          if (p < 0) break;
        }
        if (p >= 0) {
          wire = _call(_TASubarray, buf, 0, p);
        } else {
          const args = new _Array(n);
          for (let i = 0; i < n; i++) args[i] = arguments[i];
          wire = _encodeWireArgs(args);
        }
      }
      envBytes = start_async(opId, wire);
    } catch (e) {
      return _PromiseReject(e);
    }
    _rInit(envBytes);
    const status = _rU8();
    if (status === ENV_PENDING) {
      const id = _rU32();
      let resolveFn, rejectFn;
      const p = new _Promise((res, rej) => { resolveFn = res; rejectFn = rej; });
      _call(_MapSet, _pendingAsyncOps, id, { resolve: resolveFn, reject: rejectFn, promise: p });
      return p;
    }
    if (status === ENV_OK) {
      return _PromiseResolve(_rValue());
    }
    if (status === ENV_ERR) {
      return _PromiseReject(_rBuildError());
    }
    return _PromiseReject(new _Error(`unknown wire envelope status ${status}`));
  };
  _ObjectDefineProperty(fn, "name", { value: name, configurable: true });
  return fn;
}

function _makeRawBufferOp(opId, name) {
  const fn = function (arg) {
    const parts = _bufferViewParts(arg);
    let envBytes;
    if (parts === null) {
      envBytes = call_sync(opId, _encodeWireArgs([0]));
    } else if (parts.len === 0 || !parts.buf) {
      envBytes = call_sync(opId, _encodeWireArgs([1, new _Uint8Array(0)]));
    } else {
      let view;
      try {
        view = new _Uint8Array(parts.buf, parts.off, parts.len);
      } catch {
        view = new _Uint8Array(0);
      }
      envBytes = call_sync(opId, _encodeWireArgs([1, view]));
    }
    const env = _decodeEnvelope(envBytes);
    if (!env.ok) throw env.error;
    const retVal = env.value[0];
    const writeback = env.value[1];
    if (writeback !== null && writeback !== undefined && parts && parts.buf && parts.len > 0) {
      try {
        const dest = new _Uint8Array(parts.buf, parts.off, parts.len);
        _call(_TASet, dest, writeback);
      } catch {}
    }
    return retVal;
  };
  _ObjectDefineProperty(fn, "name", { value: name, configurable: true });
  return fn;
}

export function __dd_init_ops(opSpecsJson, maxArgBytes, asyncContextFn) {
  if (typeof asyncContextFn === "function") _asyncContextFn = asyncContextFn;
  if (maxArgBytes > 0) _maxOpArgBytes = maxArgBytes;
  const specs = _JSONParse(opSpecsJson);
  const obj = {};
  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i];
    const name = spec[0];
    const kind = spec[1];
    // kind: 0 = sync, 1 = async, 2 = builtin_js, 3 = raw_buffer
    if (kind === 2) {
      const b = _BUILTIN_OPS[name];
      if (!b) throw new _Error(`missing JS builtin op: ${name}`);
      obj[name] = b;
    } else if (kind === 1) {
      obj[name] = _makeAsyncOp(i, name);
    } else if (kind === 3) {
      obj[name] = _makeRawBufferOp(i, name);
    } else {
      obj[name] = _makeSyncOp(i, name);
    }
  }
  _opsObject = obj;
  return obj;
}

export function __dd_set_inspector_enabled(enabled) {
  _dbgEnabled = !!enabled;
  if (_dbgEnabled) {
    const def = (k, v) => _ObjectDefineProperty(_globalThis, k, {
      value: v, writable: false, enumerable: false, configurable: true,
    });
    def("__dd_dbg_s", (sid, line, col, ev) => _dbgCheck(sid, line, col, false, ev));
    def("__dd_dbg_d", (sid, line, col, ev) => _dbgCheck(sid, line, col, true, ev));
    def("__dd_dbg_l", (sid, line, col, ev) => _dbgCheck(sid, line, col, false, ev));
  }
}

export function __dd_dbg_eval_global(instrumentedSource) {
  let remoteJson = "{\"type\":\"undefined\"}";
  let excDesc = null;
  const prev = _dbgInEval;
  _dbgInEval = true;
  try {
    const val = _eval(instrumentedSource);
    remoteJson = _JSONStringify(_toRemoteObject(val));
  } catch (err) {
    excDesc = _describeException(err);
    remoteJson = _JSONStringify(_toRemoteObject(err));
  } finally {
    _dbgInEval = prev;
  }
  return _JSONStringify([remoteJson, excDesc]);
}

export function __dd_wire_to_js(bytes) {
  const env = _decodeEnvelope(bytes);
  if (!env.ok) throw env.error;
  return env.value;
}

export function __dd_js_to_wire(val) {
  inspector_op(6, _encodeWireArgs([val]));
}

export function __dd_get_ops() { return _opsObject; }
export function __dd_set_internals(v) { _internals = v; }
export function __dd_get_internals() { return _internals; }
export function __dd_set_allow_codegen(allow) { _allowCodeGen = !!allow; }

export function __dd_settle_async(pendingId, envBytes) {
  const rec = _call(_MapGet, _pendingAsyncOps, pendingId);
  if (!rec) return;
  _call(_MapDelete, _pendingAsyncOps, pendingId);
  const env = _decodeEnvelope(envBytes);
  if (env.ok) {
    rec.resolve(env.value);
  } else {
    rec.reject(env.error);
  }
}

export function __dd_settle_async_str(pendingId, resp) {
  const rec = _call(_MapGet, _pendingAsyncOps, pendingId);
  if (!rec) return;
  _call(_MapDelete, _pendingAsyncOps, pendingId);
  if (resp[0] === "e") {
    rec.reject(_decodeStrErr(resp));
    return;
  }
  try {
    rec.resolve(_decodeStrValue(resp));
  } catch (e) {
    rec.reject(e);
  }
}

export function __dd_take_unhandled_rejection() {
  return "";
}

export function __dd_describe_error(err) {
  return _describeException(err);
}

function _describeException(err) {
  if ((typeof err === "object" || typeof err === "function") && err !== null) {
    try {
      const stack = err.stack;
      if (typeof stack === "string" && stack.length > 0) {
        const name = typeof err.name === "string" && err.name.length > 0 ? err.name : null;
        if (name && !_call(_StrStartsWith, stack, name)) {
          return `${name}: ${stack}`;
        }
        return stack;
      }
    } catch {}
  }
  try { return _String(err); } catch { return typeof err; }
}

export function __dd_eval_script(name, source, hasDynImport) {
  if (hasDynImport) {
    const dynImport = (specifier, opts) => _dynamicImport(_String(specifier), opts, name);
    let fn;
    try {
      fn = _Function("__dd_dyn_import", `return (${source});`);
    } catch {
      fn = _Function("__dd_dyn_import", source);
    }
    return fn(dynImport);
  }
  return _eval(source);
}

export function __dd_eval_with_ops(_name, source) {
  const fn = _Function("ops", source);
  return fn(_opsObject);
}

export function __dd_eval_function(_name, source, paramsJson, argsArr) {
  const params = _JSONParse(paramsJson);
  const fnArgs = new _Array(params.length + 1);
  for (let i = 0; i < params.length; i++) fnArgs[i] = params[i];
  fnArgs[params.length] = source;
  const fn = _ReflectApply(_Function, null, fnArgs);
  return _ReflectApply(fn, undefined, argsArr);
}

export function __dd_call_function(fn, argsArr) {
  return _ReflectApply(fn, undefined, argsArr);
}

export function __dd_call_u32(fn, arg) {
  return _call(fn, undefined, arg >>> 0);
}

export function __dd_call_f64_2(fn, a, b) {
  return _call(fn, undefined, a, b);
}

export function __dd_get_prop(obj, key) {
  return obj[key];
}

export function __dd_to_string(v) {
  return _String(v);
}

export function __dd_make_array() {
  return [];
}

export function __dd_push_array(arr, val) {
  arr[arr.length] = val;
}

// ---- ES Module Graph Runtime ----
const _modules = new _Map();

function _makeNamespace() {
  const ns = _ObjectCreate(null);
  _ObjectDefineProperty(ns, _SymbolToStringTag, { value: "Module" });
  return ns;
}

export function __dd_register_module(id, name, kind, isMain, payload, depsJson) {
  if (_call(_MapHas, _modules, id)) return;
  const deps = _JSONParse(depsJson);
  _call(_MapSet, _modules, id, {
    id,
    name,
    kind, // 0=js, 1=json, 2=text, 3=bytes
    isMain: !!isMain,
    payload,
    deps,
    ns: null,
    promise: null,
    evaluated: false,
  });
}

function _defineExport(ns, key, getter) {
  _ObjectDefineProperty(ns, key, {
    enumerable: true,
    configurable: true,
    get: getter,
  });
}

function _exportStar(ns, depNs) {
  const keys = _ObjectKeys(depNs);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i];
    if (k === "default" || _ObjectHasOwn(ns, k)) continue;
    _defineExport(ns, k, () => depNs[k]);
  }
}

function _evalModuleById(id) {
  const rec = _call(_MapGet, _modules, id);
  if (!rec) return _PromiseReject(new _Error(`no module with id ${id}`));
  if (rec.promise) return rec.promise;
  const ns = _makeNamespace();
  rec.ns = ns;
  const run = new _Promise((resolve, reject) => {
    try {
      if (rec.kind === 1) {
        const val = _JSONParse(rec.payload);
        _defineExport(ns, "default", () => val);
        rec.evaluated = true;
        resolve(ns);
        return;
      }
      if (rec.kind === 2 || rec.kind === 3) {
        const val = rec.payload;
        _defineExport(ns, "default", () => val);
        rec.evaluated = true;
        resolve(ns);
        return;
      }
      // JavaScript module: evaluate static deps first in declaration order.
      const depNamespaces = new _Array(rec.deps.length);
      const runBody = () => {
        try {
          const meta = { url: rec.name, main: rec.isMain };
          const imp = (idx) => depNamespaces[idx];
          const exp = (key, getter) => _defineExport(ns, key, getter);
          const expStar = (idx) => _exportStar(ns, depNamespaces[idx]);
          const dynImport = (specifier, opts) => _dynamicImport(_String(specifier), opts, rec.name);
          const fn = _ReflectApply(
            _AsyncFunction,
            null,
            ["__dd_imp", "__dd_exp", "__dd_exp_star", "__dd_meta", "__dd_dyn_import", rec.payload],
          );
          const p = _call(fn, undefined, imp, exp, expStar, meta, dynImport);
          _call(_PromiseThen, p, () => {
            rec.evaluated = true;
            resolve(ns);
          }, reject);
        } catch (err) {
          reject(err);
        }
      };
      const evalNextDep = (i) => {
        if (i >= rec.deps.length) {
          runBody();
          return;
        }
        _call(_PromiseThen, _evalModuleById(rec.deps[i]), (depNs) => {
          depNamespaces[i] = depNs;
          evalNextDep(i + 1);
        }, reject);
      };
      evalNextDep(0);
    } catch (err) {
      reject(err);
    }
  });
  rec.promise = run;
  return run;
}

function _dynamicImport(specifier, opts, referrer) {
  return new _Promise((resolve, reject) => {
    try {
      let reqType = "javascript";
      if (opts && typeof opts === "object") {
        const attrs = opts.with || opts.assert;
        if (attrs && typeof attrs === "object" && attrs.type !== undefined) {
          reqType = _String(attrs.type);
        }
      }
      const env = _decodeEnvelope(load_module(specifier, referrer, reqType));
      if (!env.ok) {
        reject(env.error);
        return;
      }
      const rootId = env.value.rootId;
      const records = env.value.records;
      for (let i = 0; i < records.length; i++) {
        const r = records[i];
        __dd_register_module(r.id, r.name, r.kind, r.isMain, r.payload, r.depsJson);
      }
      _call(_PromiseThen, _evalModuleById(rootId), resolve, reject);
    } catch (err) {
      reject(err);
    }
  });
}

export function __dd_eval_module(id) {
  return _evalModuleById(id);
}

export function __dd_module_namespace(id) {
  const rec = _call(_MapGet, _modules, id);
  if (!rec || !rec.ns) throw new _Error(`module ${id} has not been evaluated`);
  return rec.ns;
}
