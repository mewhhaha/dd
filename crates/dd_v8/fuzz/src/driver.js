// The fuzz driver. Runs once as the body of a function whose `ops`
// parameter holds the op functions, and returns the entry points the fuzz
// targets call with their input as a Uint8Array.
//
// Every op call goes through `attempt`, which accepts a result or a thrown
// Error; anything else thrown, or a copy or in-place write that disagrees
// with the JavaScript reference below, returns a "BUG: ..." report.
"use strict";

const brand = Symbol.for("Deno.core.hostObject");
const TypedArray = Object.getPrototypeOf(Uint8Array);
const getter = (proto, key) => Object.getOwnPropertyDescriptor(proto, key).get;
const intrinsics = {
  typedBuffer: getter(TypedArray.prototype, "buffer"),
  typedOffset: getter(TypedArray.prototype, "byteOffset"),
  typedLength: getter(TypedArray.prototype, "byteLength"),
  viewBuffer: getter(DataView.prototype, "buffer"),
  viewOffset: getter(DataView.prototype, "byteOffset"),
  viewLength: getter(DataView.prototype, "byteLength"),
  bufferLength: getter(ArrayBuffer.prototype, "byteLength"),
  sharedLength: getter(SharedArrayBuffer.prototype, "byteLength"),
};
const read = (f, value) => Reflect.apply(f, value, []);
const nativeWord = [...new Uint8Array(new Uint32Array([0x04030201]).buffer)];

class Bug extends Error {}

const describe = (value) => {
  try {
    if (typeof value === "bigint") return `${value}n`;
    if (typeof value === "symbol") return value.toString();
    if (value instanceof Error) return `${value.name}: ${value.message}`;
    return Object.prototype.toString.call(value);
  } catch {
    return "<undescribable>";
  }
};

/// Runs `f`; a thrown Error is fine, any other thrown value is a bug.
const attempt = (label, f) => {
  try {
    return { value: f() };
  } catch (error) {
    if (error instanceof Bug) throw error;
    if (!(error instanceof Error)) throw new Bug(`${label} threw a non-error ${describe(error)}`);
    return { error };
  }
};

/// The memory a byte copy of `value` reads, or null when ops must reject it.
const region = (value) => {
  if (ops.op_is_array_buffer(value)) {
    return { buffer: value, offset: 0, length: read(intrinsics.bufferLength, value) };
  }
  if (ops.op_is_typed_array(value)) {
    return {
      buffer: read(intrinsics.typedBuffer, value),
      offset: read(intrinsics.typedOffset, value),
      length: read(intrinsics.typedLength, value),
    };
  }
  if (ops.op_is_data_view(value)) {
    const buffer = read(intrinsics.viewBuffer, value);
    try {
      return { buffer, offset: read(intrinsics.viewOffset, value), length: read(intrinsics.viewLength, value) };
    } catch {
      return { buffer, offset: 0, length: 0 }; // detached or out of bounds
    }
  }
  return null;
};

const wholeBuffer = (buffer) => {
  const length = ops.op_is_shared_array_buffer(buffer)
    ? read(intrinsics.sharedLength, buffer)
    : read(intrinsics.bufferLength, buffer);
  return length === 0 ? [] : [...new Uint8Array(buffer, 0, length)];
};

const regionBytes = ({ buffer, offset, length }) =>
  length === 0 ? [] : [...new Uint8Array(buffer, offset, length)];

const sameBytes = (actual, expected) =>
  actual instanceof Uint8Array && actual.length === expected.length &&
  actual.every((byte, index) => byte === expected[index]);

// --- Building values from bytes -------------------------------------------

const reader = (bytes) => {
  let at = 0;
  const byte = () => (at < bytes.length ? bytes[at++] : 0);
  return {
    get done() { return at >= bytes.length; },
    byte,
    small: (n) => (n <= 0 ? 0 : byte() % n),
    pick: (list) => list[byte() % list.length],
    rest: () => bytes.slice(Math.min(at, bytes.length)),
  };
};

const NUMBERS = [0, -0, 1, -1, 0.5, -1.5, NaN, Infinity, -Infinity, 255, 256, 2 ** 31 - 1, -(2 ** 31),
  2 ** 32 - 1, 2 ** 32, 2 ** 53 - 1, 2 ** 53, -(2 ** 53), 1e300, 5e-324, Number.MAX_VALUE];
const BIGINTS = [0n, 1n, -1n, 2n ** 53n, 2n ** 63n - 1n, -(2n ** 63n), 2n ** 64n - 1n, 2n ** 64n,
  -(2n ** 63n) - 1n, 2n ** 200n];
const STRINGS = ["", "a", "type", "Point", "Mutate", "Port", "name", "data", "Stop", "Say", "Move", "Pair",
  "__proto__", "constructor", "toString", "0", "4294967295", "\ud800", "été", "两字",
  "x".repeat(300)];
const VIEWS = [Uint8Array, Int8Array, Uint8ClampedArray, Int16Array, Uint16Array, Int32Array, Uint32Array,
  Float32Array, Float64Array, BigInt64Array, BigUint64Array, DataView];
if (typeof Float16Array === "function") VIEWS.push(Float16Array);

const fill = (buffer, seed) => {
  const bytes = new Uint8Array(buffer);
  for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 31 + seed) & 0xff;
  return buffer;
};

const makeBuffer = (r) => {
  const length = r.small(48);
  switch (r.small(7)) {
    case 0: return fill(new ArrayBuffer(length), length);
    case 1: return fill(new ArrayBuffer(length, { maxByteLength: length + r.small(48) }), length);
    case 2: return fill(new SharedArrayBuffer(length), length);
    case 3: return fill(new SharedArrayBuffer(length, { maxByteLength: length + r.small(48) }), length);
    case 4: return fill(new ArrayBuffer(4096 + length), length);
    case 5: return new ArrayBuffer(0);
    default: return fill(new ArrayBuffer(length, { maxByteLength: 4096 }), length);
  }
};

/// Detaches, shrinks or grows a buffer after views were made over it.
const disturb = (r, buffer) => {
  try {
    const shared = ops.op_is_shared_array_buffer(buffer);
    switch (r.small(6)) {
      case 0: if (!shared && buffer.resizable) buffer.resize(r.small(buffer.maxByteLength + 1)); break;
      case 1: if (shared && buffer.growable) buffer.grow(buffer.byteLength + r.small(buffer.maxByteLength - buffer.byteLength + 1)); break;
      case 2: if (!shared) buffer.transfer(); break;
      case 3: if (!shared) buffer.transferToFixedLength(); break;
      default: break;
    }
  } catch {
    // Detached already, or a length the buffer refuses.
  }
};

const makeView = (r, buffer) => {
  const Kind = r.pick(VIEWS);
  const size = Kind.BYTES_PER_ELEMENT ?? 1;
  let total = 0;
  try { total = buffer.byteLength; } catch {}
  let offset = r.small(total + 1);
  offset -= offset % size;
  try {
    if (r.byte() & 1) return new Kind(buffer, offset);
    return new Kind(buffer, offset, r.small(Math.floor((total - offset) / size) + 1));
  } catch {
    return new Uint8Array(0);
  }
};

const makeValue = (r, pool, depth = 0) => {
  let value;
  try {
    value = makeValueUnchecked(r, pool, depth);
  } catch (error) {
    if (error instanceof Bug) throw error;
    value = undefined;
  }
  pool.push(value);
  return value;
};

const makeValueUnchecked = (r, pool, depth) => {
  const children = () => {
    const out = [];
    for (let i = r.small(5); i > 0 && depth < 4; i--) out.push(makeValue(r, pool, depth + 1));
    return out;
  };
  switch (r.small(depth >= 4 ? 8 : 30)) {
    case 0: return undefined;
    case 1: return null;
    case 2: return !!(r.byte() & 1);
    case 3: return r.pick(NUMBERS);
    case 4: return r.pick(BIGINTS);
    case 5: return r.pick(STRINGS);
    case 6: return pool.length ? r.pick(pool) : 0;
    case 7: return Symbol(r.pick(STRINGS));
    case 8: { const buffer = makeBuffer(r); if (r.byte() & 1) disturb(r, buffer); return buffer; }
    case 9: case 10: { const buffer = makeBuffer(r); const view = makeView(r, buffer); disturb(r, buffer); return view; }
    case 11: { const buffer = makeBuffer(r); const views = [makeView(r, buffer), makeView(r, buffer)]; disturb(r, buffer); return views; }
    case 12: return Uint8Array.of(...Array.from({ length: r.small(64) }, (_, i) => i)); // on the V8 heap
    case 13: return children();
    case 14: {
      const sparse = [];
      // serde_v8 reads every hole of an array, so lengths stay small here;
      // see `expandsSmall`.
      sparse.length = r.pick([0, 10, 1000, 4096]);
      for (const child of children()) sparse[r.small(Math.max(sparse.length, 1))] = child;
      return sparse;
    }
    case 15: {
      const object = r.byte() & 1 ? Object.create(null) : {};
      for (const child of children()) object[r.pick(STRINGS)] = child;
      return object;
    }
    case 16: return new Map(children().map((child) => [makeValue(r, pool, depth + 1), child]));
    case 17: return new Set(children());
    case 18: return r.pick([new Date(r.pick(NUMBERS)), /a+(b)?/giu, new Error("e", { cause: makeValue(r, pool, depth + 1) }),
      new RangeError("r"), new Number(r.pick(NUMBERS)), new String(r.pick(STRINGS)), Object(r.pick(BIGINTS))]);
    case 19: {
      const target = makeValue(r, pool, depth + 1);
      const object = target !== null && (typeof target === "object" || typeof target === "function") ? target : {};
      if (r.byte() & 1) return new Proxy(object, {});
      const { proxy, revoke } = Proxy.revocable(object, {});
      if (r.byte() & 1) revoke();
      return proxy;
    }
    case 20: {
      // Getters that throw, or that detach and resize buffers in the pool
      // while an op is still reading the object.
      const object = {};
      const key = r.pick(STRINGS);
      const action = r.small(3);
      const victims = pool.slice(-4);
      Object.defineProperty(object, key, {
        enumerable: true,
        get() {
          if (action === 0) throw new Error("getter");
          for (const victim of victims) {
            const target = region(victim)?.buffer ?? victim;
            if (ops.op_is_any_array_buffer(target)) disturb(reader(new Uint8Array([action * 2, 1, 7])), target);
          }
          return victims[0];
        },
      });
      for (const child of children()) object[r.pick(STRINGS)] = child;
      return object;
    }
    case 21: {
      const description = makeValue(r, pool, depth + 1);
      const shape = r.small(4);
      const host = {
        [brand]() {
          if (shape === 0) throw new Error("brand");
          if (shape === 1) return description;
          if (shape === 2) return host;
          return { type: r.pick(["Point", "Mutate", "Nope", "constructor"]), x: description, owner: pool[0] };
        },
      };
      return host;
    }
    case 22: return HOSTS[0];
    case 23: { const cycle = { children: children() }; cycle.self = cycle; return cycle; }
    case 24: return Array.from({ length: r.small(8) }, () => r.small(300));
    case 25: return new WebAssembly.Module(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]));
    case 26: { const buffer = makeBuffer(r); const view = makeView(r, buffer); return { data: view, name: r.pick(STRINGS) }; }
    case 27: return r.pick([{ Stop: null }, { Say: r.pick(STRINGS) }, { Move: { x: r.pick(NUMBERS) } }, { Pair: children() }]);
    // Never a rejected promise: nothing drains the runtime's unhandled
    // rejections between inputs.
    case 28: return r.pick([() => {}, Promise.resolve(makeValue(r, pool, depth + 1)), new Promise(() => {})]);
    default: return makeView(r, makeBuffer(r));
  }
};

class Point {
  constructor(x) { this.x = x; }
}
const HOSTS = [{ [brand]: "Port" }];
const revivers = (r, pool) => ({
  __proto__: null,
  Point: (data) => new Point(data?.x),
  Mutate: (data) => {
    // Rewrites objects V8 may still be filling in.
    const owner = data?.owner;
    if (owner !== null && typeof owner === "object") {
      switch (r.small(5)) {
        case 0: for (let i = 0; i < 64; i++) owner[`p${i}`] = i; break;
        case 1: Object.freeze(owner); break;
        case 2: if (Array.isArray(owner)) owner.length = 0; break;
        case 3: if (owner instanceof Map || owner instanceof Set) owner.clear(); break;
        default: Object.setPrototypeOf(owner, null);
      }
    }
    for (const value of pool.slice(-4)) {
      const target = region(value)?.buffer ?? value;
      if (ops.op_is_any_array_buffer(target)) disturb(r, target);
    }
    return {};
  },
});

// --- Checked op calls -------------------------------------------------------

/// Ops that copy bytes must read exactly the view, and reject non-buffers
/// (typed ops also read plain arrays of numbers).
const checkCopy = (label, value, call, acceptsArrays) => {
  const result = attempt(label, () => call(value));
  const memory = region(value);
  if (!memory) {
    if (!result.error && !(acceptsArrays && Array.isArray(value))) {
      throw new Bug(`${label} accepted ${describe(value)}`);
    }
    return;
  }
  if (result.error) throw new Bug(`${label} threw ${describe(result.error)} for a buffer`);
  const expected = regionBytes(memory);
  if (label === "op_decode") {
    if (result.value !== ops.op_decode(Uint8Array.from(expected))) throw new Bug("op_decode read the wrong bytes");
  } else if (!sameBytes(result.value, expected)) {
    throw new Bug(`${label} read [${result.value}] instead of [${expected}]`);
  }
};

/// Ops that write in place must write exactly the view's bytes.
const checkWrite = (label, value, call, written) => {
  const memory = region(value);
  const before = memory ? wholeBuffer(memory.buffer) : null;
  const result = attempt(label, () => call(value));
  if (!memory) {
    if (!result.error) throw new Bug(`${label} accepted ${describe(value)}`);
    return;
  }
  if (result.error) throw new Bug(`${label} threw ${describe(result.error)} for a buffer`);
  const after = wholeBuffer(memory.buffer);
  if (after.length !== before.length) throw new Bug(`${label} resized the buffer`);
  for (let i = 0; i < after.length; i++) {
    const inside = i >= memory.offset && i < memory.offset + memory.length;
    const expected = inside ? written(i - memory.offset, before[i], memory.length) : before[i];
    if (after[i] !== expected) {
      throw new Bug(`${label} wrote ${after[i]} at ${i}, expected ${expected} (view ${memory.offset}+${memory.length})`);
    }
  }
};

/// Whether serde_v8 can read `value` without expanding it past `budget`
/// values. It reads every hole of a sparse array and copies shared
/// references once per reference, with no overall bound, so a deserialized
/// `new Array(2 ** 32 - 1)` or a DAG of shared arrays makes a serde-typed
/// op allocate without limit. That is a known gap in serde_v8 (it needs a
/// conversion budget); the fuzzers skip such values instead of finding it
/// over and over.
const expandsSmall = (value, budget = 1 << 16) => {
  let left = budget;
  const visit = (v, depth) => {
    if (--left < 0) return false;
    if (depth > 130 || v === null || (typeof v !== "object" && typeof v !== "function")) return true;
    if (ops.op_is_proxy(v)) return false;
    if (ops.op_is_array_buffer_view(v) || ops.op_is_any_array_buffer(v)) return true;
    const keys = Array.isArray(v) ? { length: v.length } : Object.keys(v);
    const length = keys.length;
    if (length > left) return false;
    for (let i = 0; i < length; i++) {
      const key = Array.isArray(v) ? i : keys[i];
      const descriptor = Object.getOwnPropertyDescriptor(v, key);
      if (descriptor?.get || descriptor?.set) return false;
      if (!visit(descriptor?.value, depth + 1)) return false;
    }
    return true;
  };
  return visit(value, 0);
};

const mutate = (r, bytes) => {
  let out = bytes;
  for (let edits = r.small(4); edits > 0 && out.length; edits--) out[r.small(out.length)] = r.byte();
  if (r.small(4) === 0) out = out.slice(0, r.small(out.length + 1));
  return out;
};

const exercise = (r, pool, value) => {
  switch (r.small(20)) {
    case 0: return checkCopy("buffer_bytes", value, ops.op_raw_copy, false);
    case 1: return checkCopy("op_echo_bytes", value, ops.op_echo_bytes, true);
    case 2: return checkCopy("op_echo_vec", value, ops.op_echo_vec, true);
    case 3: return checkCopy("op_decode", value, ops.op_decode, false);
    case 4: return checkWrite("with_buffer_mut", value, ops.op_raw_fill, () => 0xab);
    case 5: return checkWrite("write_u32s", value, ops.op_raw_write_u32s,
      // op_raw_write_u32s writes at most 1024 words.
      (index, old, length) => (index < Math.min(Math.floor(length / 4), 1024) * 4 ? nativeWord[index % 4] : old));
    case 6: return attempt("op_wasm_streaming_feed", () => ops.op_wasm_streaming_feed(r.small(3), value));
    case 7: return attempt("op_encode", () => ops.op_encode(value));
    case 8: return attempt("op_json", () => ops.op_json(value));
    case 9: return attempt("op_shape", () => ops.op_shape(value));
    case 10: return attempt("op_command", () => ops.op_command(value));
    case 11: return attempt("op_u64", () => ops.op_u64(value));
    case 12: return attempt("op_f64", () => ops.op_f64(value));
    case 13: return attempt("op_strings", () => ops.op_strings(value));
    case 14: return attempt("op_deserialize", () => ops.op_deserialize(value, HOSTS, undefined, revivers(r, pool), !!(r.byte() & 1)));
    case 15: return attempt("op_structured_clone", () => ops.op_structured_clone(value, revivers(r, pool)));
    case 16: case 17: {
      const forStorage = !!(r.byte() & 1);
      const callback = r.byte() & 1 ? (message) => { throw new TypeError(String(message)); } : undefined;
      const written = attempt("op_serialize", () => ops.op_serialize(value, HOSTS, pool.slice(-2), forStorage, callback));
      if (written.error) return;
      const bytes = mutate(r, written.value);
      return attempt("op_deserialize", () => ops.op_deserialize(bytes, r.byte() & 1 ? HOSTS : undefined, undefined, revivers(r, pool), forStorage));
    }
    case 18: {
      const checks = ["op_is_any_array_buffer", "op_is_array_buffer_view", "op_is_typed_array", "op_is_data_view",
        "op_is_proxy", "op_is_boxed_primitive", "op_is_native_error", "op_is_map", "op_is_set", "op_is_date"];
      for (const check of checks) {
        if (typeof ops[check](value) !== "boolean") throw new Bug(`${check} returned a non-boolean`);
      }
      const details = ops.op_proxy_details(value);
      if (details !== null && !(Array.isArray(details) && details.length === 2)) throw new Bug("op_proxy_details shape");
      if ((details !== null) !== ops.op_is_proxy(value)) throw new Bug("op_proxy_details disagrees with op_is_proxy");
      const state = attempt("op_promise_state", () => ops.op_promise_state(value));
      if (!state.error && !(Array.isArray(state.value) && [0, 1, 2].includes(state.value[0]))) throw new Bug("op_promise_state shape");
      return;
    }
    default: {
      // An inherited `type` getter runs while a reviver is looked up.
      const descriptor = { configurable: true, get() { return r.pick(["Point", "Mutate", "Nope"]); } };
      Object.defineProperty(Object.prototype, "type", descriptor);
      try {
        return attempt("op_structured_clone", () => ops.op_structured_clone([value, { [brand]() { return {}; } }], revivers(r, pool)));
      } finally {
        delete Object.prototype.type;
      }
    }
  }
};

const report = (f) => {
  try {
    f();
    return "ok";
  } catch (error) {
    if (error instanceof Bug) return `BUG: ${error.message}`;
    return `BUG: the driver threw ${describe(error)}`;
  }
};

// --- Entry points ------------------------------------------------------------

/// First byte: flags. 1: storage mode; 2: pass host objects and revivers;
/// 4: prepend a version header; 8: read through a view into a larger
/// buffer; 16: read through a DataView; 32: build a value from the rest,
/// serialize it, then apply (position, byte) edits from what is left.
const deserialize = (input) => report(() => {
  const r = reader(input);
  const flags = r.byte();
  const pool = [];
  let bytes;
  if (flags & 32) {
    const value = makeValue(r, pool);
    const written = attempt("op_serialize", () => ops.op_serialize(value, HOSTS, undefined, !!(flags & 1)));
    if (written.error) return;
    bytes = written.value;
    while (!r.done && bytes.length) bytes[(r.byte() << 8 | r.byte()) % bytes.length] = r.byte();
  } else {
    bytes = r.rest();
    if (flags & 4) bytes = Uint8Array.of(0xff, 0x0f, ...bytes);
  }
  let source = bytes;
  if (flags & 8) {
    const larger = new Uint8Array(bytes.length + 16).fill(0x5c);
    larger.set(bytes, 7);
    source = larger.subarray(7, 7 + bytes.length);
  }
  if (flags & 16) source = new DataView(source.buffer, source.byteOffset, source.byteLength);
  const withHosts = !!(flags & 2);
  const result = attempt("op_deserialize", () =>
    ops.op_deserialize(source, withHosts ? HOSTS : undefined, undefined, withHosts ? revivers(r, pool) : undefined, !!(flags & 1)));
  if (result.error) return;
  // Whatever came back must also survive the other paths.
  attempt("op_serialize", () => ops.op_serialize(result.value, HOSTS, undefined, !!(flags & 1)));
  attempt("op_structured_clone", () => ops.op_structured_clone(result.value, revivers(r, pool)));
  if (expandsSmall(result.value)) attempt("op_json", () => ops.op_json(result.value));
});

/// Builds a few values, then runs ops on them until the input runs out.
const program = (input) => report(() => {
  const r = reader(input);
  const pool = [];
  for (let i = 1 + r.small(4); i > 0; i--) makeValue(r, pool);
  for (let steps = 0; !r.done && steps < 32; steps++) {
    exercise(r, pool, r.byte() & 1 ? r.pick(pool) : makeValue(r, pool));
  }
});

/// The value op_deserialize reads back from op_serialize's output (in both
/// modes, which must agree), after op_structured_clone also copied it.
const clone = (value) => {
  const cloned = ops.op_structured_clone(value, undefined);
  const message = ops.op_deserialize(ops.op_serialize(value), undefined, undefined, undefined, false);
  const stored = ops.op_deserialize(ops.op_serialize(value, undefined, undefined, true), undefined, undefined, undefined, true);
  return [cloned, message, stored];
};

return { deserialize, program, clone };
