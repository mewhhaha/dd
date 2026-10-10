//! op_serialize, op_deserialize and op_structured_clone against hostile
//! input: every truncation and many mutations of real serializer output in
//! both storage and message mode, forged host-object tags, revivers that
//! re-enter the runtime or rewrite objects still being deserialized, values
//! holding detached or out-of-bounds buffers, and nesting deep enough to
//! exhaust the stack. Storage values are read back from disk, so
//! op_deserialize has to survive any bytes at all.

mod common;

use common::{assert_checks, runtime};

/// A corpus of values covering every tag the serializer writes, plus the
/// deserializers that revive its host objects, and a structural equality
/// that knows those types.
const CORPUS: &str = r#"
globalThis.brand = Symbol.for("Deno.core.hostObject");
globalThis.Point = class Point {
  constructor(x) { this.x = x; }
  [brand]() { return { type: "Point", x: this.x }; }
};
globalThis.deserializers = { __proto__: null, Point: (data) => new Point(data.x) };
globalThis.port = { [brand]: "Port", id: 7 };
globalThis.hostObjects = [port];

globalThis.corpus = () => {
  const values = [];
  const add = (label, value, options = {}) => values.push({ label, value, ...options });
  for (const value of [undefined, null, true, false, 0, -0, NaN, 1.5, -(2 ** 31), 2 ** 53, Infinity]) {
    add(`primitive ${describe(value)}`, value);
  }
  for (const value of [0n, 1n, -(2n ** 64n), 2n ** 200n]) add(`bigint ${value}`, value);
  for (const value of ["", "ascii", "été", "两字节", "\ud800", "x".repeat(300)]) {
    add(`string of length ${value.length}`, value);
  }
  add("object", { a: 1, b: "x", c: [1, 2], d: { e: null } });
  add("object with index keys", { 1: 1, "-1": 2, 4294967295: 3 });
  add("object with doubles", { x: 1.5, y: -0, z: NaN });
  add("null-prototype object", Object.assign(Object.create(null), { a: 1 }));
  const many = {};
  for (let i = 0; i < 40; i++) many[`k${i}`] = i;
  delete many.k3;
  add("dictionary-mode object", many);
  add("dense array", [1, "two", null, undefined, { three: 3 }]);
  add("sparse array", [1, , 3]);
  const far = [];
  far[100000] = 1;
  add("far sparse array", far);
  const extras = [1, 2];
  extras.note = "x";
  add("array with properties", extras);
  add("Map", new Map([["k", 1], [2, [3]], [{ o: 1 }, new Set([4])]]));
  add("Set", new Set([1, "a", { b: 2 }]));
  add("Date", new Date(86400000));
  add("invalid Date", new Date(NaN));
  add("RegExp", /a+b?/gimsuy);
  add("RegExp with v flag", /[\p{L}--[a-z]]/v);
  add("boxed primitives", [new Number(-0), new String("s"), new Boolean(false), Object(5n)]);
  add("errors", [new Error("plain"), new TypeError("type", { cause: 1 }), new RangeError("range"), new EvalError("eval"), new SyntaxError("syntax"), new ReferenceError("ref"), new URIError("uri")]);
  add("ArrayBuffer", fresh(24).buffer);
  add("empty ArrayBuffer", new ArrayBuffer(0));
  const rab = new ArrayBuffer(8, { maxByteLength: 32 });
  new Uint8Array(rab).set(fresh(8));
  add("resizable ArrayBuffer", rab);
  add("views over a resizable buffer", [rab, new Uint8Array(rab, 2), new DataView(rab, 1, 4)]);
  const shared = fresh(64).buffer;
  const views = [new Uint8Array(shared, 1, 7)];
  for (const kind of ["Int8Array", "Uint8ClampedArray", "Int16Array", "Uint16Array", "Int32Array", "Uint32Array", "Float16Array", "Float32Array", "Float64Array", "BigInt64Array", "BigUint64Array"]) {
    if (globalThis[kind]) views.push(new globalThis[kind](shared, 8, 2));
  }
  views.push(new DataView(shared, 3, 9));
  add("typed arrays sharing a buffer", views);
  const cyclic = { name: "cycle" };
  cyclic.self = cyclic;
  cyclic.list = [cyclic, { back: cyclic }];
  add("cyclic object", cyclic);
  const sharedChild = { child: true };
  add("shared references", [sharedChild, sharedChild, { sharedChild }]);
  add("branded host object", { point: new Point(4), again: [new Point(5)] });
  add("listed host object", [port, { port }], { hostObjects });
  return values;
};

/// Structural equality for everything in the corpus, cycle-aware.
globalThis.deepEqual = (a, b, seen = new Map()) => {
  if (typeof a !== "object" || a === null || typeof b !== "object" || b === null) return Object.is(a, b);
  if (seen.has(a)) return seen.get(a) === b;
  seen.set(a, b);
  const tag = Object.prototype.toString.call(a);
  if (tag !== Object.prototype.toString.call(b)) return false;
  // Plain objects come back with Object.prototype whatever they had.
  const plain = (o) => [null, Object.prototype].includes(Object.getPrototypeOf(o));
  if (!(plain(a) && plain(b)) && Object.getPrototypeOf(a)?.constructor?.name !== Object.getPrototypeOf(b)?.constructor?.name) return false;
  if (a instanceof Date) return Object.is(a.getTime(), b.getTime());
  if (a instanceof RegExp) return a.source === b.source && a.flags === b.flags;
  if (a instanceof Error) return a.name === b.name && a.message === b.message && deepEqual(a.cause, b.cause, seen);
  if (["[object Number]", "[object String]", "[object Boolean]", "[object BigInt]"].includes(tag)) {
    return Object.is(a.valueOf(), b.valueOf());
  }
  if (a instanceof ArrayBuffer) {
    return a.byteLength === b.byteLength && a.resizable === b.resizable && a.maxByteLength === b.maxByteLength &&
      sameBytes(new Uint8Array(b), [...new Uint8Array(a)]);
  }
  if (ArrayBuffer.isView(a)) {
    return a.byteOffset === b.byteOffset && a.byteLength === b.byteLength && deepEqual(a.buffer, b.buffer, seen);
  }
  if (a instanceof Map) {
    if (a.size !== b.size) return false;
    const left = [...a], right = [...b];
    return left.every(([key, value], i) => deepEqual(key, right[i][0], seen) && deepEqual(value, right[i][1], seen));
  }
  if (a instanceof Set) {
    if (a.size !== b.size) return false;
    const right = [...b];
    return [...a].every((value, i) => deepEqual(value, right[i], seen));
  }
  if (a instanceof WebAssembly.Module) return b instanceof WebAssembly.Module;
  const keys = Reflect.ownKeys(a).filter((key) => typeof key === "string" && Object.prototype.propertyIsEnumerable.call(a, key));
  const otherKeys = Reflect.ownKeys(b).filter((key) => typeof key === "string" && Object.prototype.propertyIsEnumerable.call(b, key));
  if (keys.length !== otherKeys.length) return false;
  if (Array.isArray(a) && a.length !== b.length) return false;
  return keys.every((key) => Object.hasOwn(b, key) && deepEqual(a[key], b[key], seen));
};

globalThis.serializeEntry = (entry, forStorage) =>
  ops.op_serialize(entry.value, entry.hostObjects, undefined, forStorage);
globalThis.deserializeBytes = (bytes, forStorage, withHosts = true) =>
  ops.op_deserialize(bytes, withHosts ? hostObjects : undefined, undefined, withHosts ? deserializers : undefined, forStorage);

/// Calls `f` and checks it either returned or threw an Error object.
globalThis.survives = (label, f) => {
  const result = attempt(f);
  check(!result.error || isCleanError(result.error), `${label}: threw a non-error ${describe(result.error)}`);
  return result;
};

globalThis.fresh = (length = 16) => Uint8Array.from({ length }, (_, i) => (i * 7 + 1) & 0xff);
globalThis.varint = (n) => {
  const out = [];
  n = BigInt(n);
  do {
    let byte = Number(n & 0x7fn);
    n >>= 7n;
    if (n) byte |= 0x80;
    out.push(byte);
  } while (n);
  return out;
};
globalThis.header = [0xff, 0x0f];
"#;

#[test]
fn every_corpus_value_round_trips() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            for (const entry of corpus()) {
              for (const forStorage of [false, true]) {
                const label = `${entry.label} (${forStorage ? "storage" : "message"})`;
                const written = attempt(() => serializeEntry(entry, forStorage));
                if (written.error) {
                  check(false, `${label}: serialize threw ${describe(written.error)}`);
                  continue;
                }
                check(written.value instanceof Uint8Array && written.value[0] === 0xff, `${label}: has a header`);
                const read = attempt(() => deserializeBytes(written.value, forStorage));
                check(!read.error && deepEqual(entry.value, read.value), `${label}: round trip gave ${describe(read.error ?? read.value)}`);
              }
              if (entry.hostObjects) continue;
              const cloned = attempt(() => ops.op_structured_clone(entry.value, deserializers));
              check(!cloned.error && deepEqual(entry.value, cloned.value), `${entry.label}: structured clone gave ${describe(cloned.error ?? cloned.value)}`);
            }
            "#
        ),
    );
}

#[test]
fn every_truncation_of_serialized_values_fails_cleanly() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            let calls = 0;
            for (const entry of corpus()) {
              for (const forStorage of [false, true]) {
                const bytes = serializeEntry(entry, forStorage);
                for (let length = 0; length <= bytes.length; length++) {
                  for (const withHosts of [true, false]) {
                    // A fresh copy each time, and once as a view into a
                    // larger buffer, so reads past the end land on junk.
                    const exact = bytes.slice(0, length);
                    const padded = new Uint8Array(length + 8).fill(0x5c);
                    padded.set(exact);
                    for (const input of [exact, padded.subarray(0, length)]) {
                      survives(`${entry.label} cut to ${length} bytes`, () => deserializeBytes(input, forStorage, withHosts));
                      calls++;
                    }
                  }
                }
              }
            }
            check(calls > 5000, `only ${calls} truncations ran`);
            "#
        ),
    );
}

#[test]
fn mutated_serialized_values_fail_cleanly() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            // xorshift32, so every run mutates the same way.
            let state = 0x9e3779b9;
            const random = (n) => {
              state ^= state << 13; state >>>= 0;
              state ^= state >>> 17;
              state ^= state << 5; state >>>= 0;
              return state % n;
            };
            // Tags V8 writes, so mutations land on meaningful bytes too.
            const tags = [0x00, 0x22, 0x24, 0x26, 0x27, 0x3f, 0x40, 0x41, 0x42, 0x44, 0x49, 0x4e, 0x52, 0x53, 0x56, 0x5a,
              0x5c, 0x5e, 0x5f, 0x61, 0x63, 0x6d, 0x6f, 0x70, 0x72, 0x74, 0x75, 0x77, 0x7b, 0x7e, 0x7f, 0x80, 0xff];
            let calls = 0;
            for (const entry of corpus()) {
              for (const forStorage of [false, true]) {
                const original = serializeEntry(entry, forStorage);
                const run = (label, bytes) => {
                  survives(`${entry.label} ${label}`, () => deserializeBytes(bytes, forStorage, true));
                  survives(`${entry.label} ${label} without deserializers`, () => deserializeBytes(bytes, forStorage, false));
                  calls += 2;
                };
                for (let i = 0; i < original.length; i++) {
                  for (const value of [original[i] ^ 0x01, original[i] ^ 0x80, tags[random(tags.length)], random(256)]) {
                    const bytes = original.slice();
                    bytes[i] = value;
                    run(`byte ${i} = ${value}`, bytes);
                  }
                  const dropped = new Uint8Array([...original.subarray(0, i), ...original.subarray(i + 1)]);
                  run(`without byte ${i}`, dropped);
                  const inserted = new Uint8Array([...original.subarray(0, i), tags[random(tags.length)], ...original.subarray(i)]);
                  run(`with a byte inserted at ${i}`, inserted);
                }
                for (let round = 0; round < 40; round++) {
                  const bytes = original.slice();
                  const edits = 1 + random(4);
                  for (let edit = 0; edit < edits; edit++) bytes[random(bytes.length)] = random(2) ? random(256) : tags[random(tags.length)];
                  run(`random round ${round}`, bytes);
                }
              }
            }
            check(calls > 20000, `only ${calls} mutations ran`);
            "#
        ),
    );
}

#[test]
fn forged_host_object_tags_fail_cleanly() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            const body = (value) => [...ops.op_serialize(value)].slice(2);
            const forged = (index, description = []) => new Uint8Array([...header, 0x5c, ...varint(index), ...description]);
            const MAX = 2 ** 32 - 1;
            const read = (label, bytes, hosts, revivers, forStorage = false) =>
              survives(label, () => ops.op_deserialize(bytes, hosts, undefined, revivers, forStorage));

            // Branded descriptions are revived by `type`.
            const revived = read("valid description", forged(MAX, body({ type: "Point", x: 3 })), undefined, deserializers);
            check(revived.value instanceof Point && revived.value.x === 3, `valid description: ${describe(revived.error ?? revived.value)}`);
            for (const [label, description] of [
              ["unknown type", { type: "Nope" }],
              ["missing type", { x: 1 }],
              ["__proto__ type", { type: "__proto__" }],
              ["constructor type", { type: "constructor" }],
              ["toString type", { type: "toString" }],
              ["numeric type", { type: 1 }],
              ["null description", null],
              ["string description", "Point"],
              ["array description", ["Point"]],
            ]) {
              for (const forStorage of [false, true]) {
                const result = read(label, forged(MAX, body(description)), undefined, deserializers, forStorage);
                check(result.error instanceof Error, `${label}: expected an error, got ${describe(result.value)}`);
              }
            }
            // Ordinary objects inherit `constructor`, which revives as Object().
            const inherited = read("constructor from Object.prototype", forged(MAX, body({ type: "constructor", x: 1 })), undefined, {});
            check(inherited.value?.x === 1, `inherited reviver: ${describe(inherited.error ?? inherited.value)}`);
            for (const revivers of [undefined, null, { Point: 5 }, { Point: null }]) {
              const result = read(`revivers ${describe(revivers)}`, forged(MAX, body({ type: "Point", x: 1 })), undefined, revivers);
              check(result.error instanceof Error, `revivers ${describe(revivers)}: expected an error`);
            }
            const badRevivers = attempt(() => ops.op_deserialize(forged(MAX, body({ type: "Point" })), undefined, undefined, 5));
            check(badRevivers.error instanceof TypeError, "non-object revivers throw a TypeError");
            check(read("no description", forged(MAX), undefined, deserializers).error instanceof Error, "no description");
            check(read("no index", new Uint8Array([...header, 0x5c]), undefined, deserializers).error instanceof Error, "no index");
            // V8 keeps the low 32 bits of an oversized index.
            for (const index of [2 ** 32, 2 ** 40, 2n ** 64n - 1n]) {
              read(`index ${index}`, new Uint8Array([...header, 0x5c, ...varint(index)]), hostObjects);
            }

            // Listed host objects are looked up by index.
            const listed = read("index 0", forged(0), [port]);
            check(listed.value === port, "index 0 reads the listed object");
            for (const [label, hosts] of [
              ["no list", undefined], ["empty list", []], ["sparse list", new Array(10)], ["list of nulls", [null]],
            ]) {
              for (const index of [0, 5, MAX - 1]) {
                const result = read(`${label} index ${index}`, forged(index), hosts);
                check(result.error instanceof Error, `${label} index ${index}: expected an error, got ${describe(result.value)}`);
              }
            }
            const boxed = read("primitive in the list", forged(0), [7]);
            check(boxed.value instanceof Number, "a primitive host object comes back boxed");
            const getterHosts = [];
            Object.defineProperty(getterHosts, 0, { get() { throw new SyntaxError("host getter"); } });
            check(read("throwing list getter", forged(0), getterHosts).error instanceof Error, "a throwing list getter fails the read");
            const badList = attempt(() => ops.op_deserialize(forged(0), 5));
            check(badList.error instanceof TypeError, "a non-array list throws a TypeError");

            // Host tags nested inside containers, revived or not.
            const point = body({ type: "Point", x: 9 });
            for (const [label, bytes] of [
              ["inside an array", [...header, 0x41, 1, 0x5c, ...varint(MAX), ...point, 0x24, 0, 1]],
              ["inside an object", [...header, 0x6f, 0x22, 1, 0x61, 0x5c, ...varint(MAX), ...point, 0x7b, 1]],
              ["inside a Map", [...header, 0x3b, 0x5c, ...varint(MAX), ...point, 0x49, 2, 0x3a, 2]],
              ["as its own description", [...header, 0x5c, ...varint(MAX), 0x5c, ...varint(MAX), ...point]],
              ["describing itself", [...header, 0x5c, ...varint(MAX), 0x5e, 0]],
              ["describing a later object", [...header, 0x5c, ...varint(MAX), 0x5e, 5]],
            ]) {
              for (const revivers of [deserializers, undefined]) {
                read(`${label} ${revivers ? "with" : "without"} revivers`, new Uint8Array(bytes), hostObjects, revivers);
              }
            }
            "#
        ),
    );
}

/// V8 aborts the process if JavaScript runs while it deserializes. Finding a
/// host object's reviver used to run outside the scope that allows it, so
/// any of these, all reachable from worker code through `structuredClone`,
/// took the whole server down.
#[test]
fn scripts_run_while_finding_a_reviver_do_not_abort() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            const revivers = { __proto__: null, T: (data) => ({ revived: data.type }) };
            const typeless = { [brand]() { return {}; } };
            let reads = 0;
            Object.defineProperty(Object.prototype, "type", { configurable: true, get() { reads++; return "T"; } });
            try {
              const cloned = ops.op_structured_clone([typeless], revivers);
              check(cloned[0].revived === "T" && reads > 0, "an inherited `type` getter picks the reviver");
              for (const forStorage of [false, true]) {
                const bytes = ops.op_serialize([typeless], undefined, undefined, forStorage);
                const back = ops.op_deserialize(bytes, undefined, undefined, revivers, forStorage);
                check(back[0].revived === "T", "the getter also runs for stored bytes");
              }
            } finally {
              delete Object.prototype.type;
            }

            Object.defineProperty(Object.prototype, "type", { configurable: true, get() { throw new URIError("type getter"); } });
            try {
              const thrown = attempt(() => ops.op_structured_clone([typeless], revivers));
              check(thrown.error instanceof URIError, `a throwing \`type\` getter propagates: ${describe(thrown.error)}`);
            } finally {
              delete Object.prototype.type;
            }

            const toString = Object.prototype.toString;
            Object.prototype.toString = () => "T";
            try {
              const keyed = ops.op_structured_clone({ [brand]() { return { type: {} }; } }, revivers);
              check(typeof keyed.revived === "object", "an object `type` converts through toString");
            } finally {
              Object.prototype.toString = toString;
            }

            const lookups = new Proxy({}, { get(_target, key) { return key === "T" ? revivers.T : undefined; } });
            check(ops.op_structured_clone({ [brand]() { return { type: "T" }; } }, lookups).revived === "T", "revivers behind a proxy");
            const throwing = new Proxy({}, { get() { throw new EvalError("lookup"); } });
            check(attempt(() => ops.op_structured_clone({ [brand]() { return { type: "T" }; } }, throwing)).error instanceof EvalError,
              "a throwing reviver lookup propagates");

            const listed = { [brand]: "Port" };
            const bytes = ops.op_serialize([listed], [listed]);
            const list = [];
            Object.defineProperty(list, 0, { get() { return listed; } });
            check(ops.op_deserialize(bytes, list)[0] === listed, "an accessor in the host object list");
            "#
        ),
    );
}

#[test]
fn revivers_that_rewrite_objects_still_being_deserialized() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            // A reviver sees the description, which can refer back to the
            // objects V8 is still filling in. Whatever it does to them, the
            // deserializer must not crash.
            const host = (owner) => ({ [brand]() { return { type: "Mutate", owner }; } });
            const builds = {
              object: () => { const o = { a: 1, b: 2 }; o.h = host(o); o.c = 3.5; o.d = "x"; return o; },
              array: () => { const a = [1, 2, 3]; a.push(host(a)); a.push(4, 5, 6); return a; },
              sparse: () => { const a = []; a[5] = 1; a[10] = host(a); a[20] = 2; return a; },
              map: () => { const m = new Map(); m.set("k", host(m)); m.set("j", 1); return m; },
              set: () => { const s = new Set(); s.add(host(s)); s.add(1); return s; },
              error: () => { const e = new Error("m", { cause: null }); e.cause = host(e); return e; },
              nested: () => { const outer = { inner: { list: [] } }; outer.inner.list.push(host(outer)); outer.after = [1.5, 2.5]; return outer; },
            };
            const mutations = {
              addProps: (o) => { for (let i = 0; i < 100; i++) o["p" + i] = i; },
              dictionary: (o) => { o.zz = 1; delete o.zz; for (let i = 0; i < 2000; i++) o["q" + i] = i; },
              freeze: (o) => Object.freeze(o),
              seal: (o) => Object.seal(o),
              preventExtensions: (o) => Object.preventExtensions(o),
              nullPrototype: (o) => Object.setPrototypeOf(o, null),
              accessor: (o) => Object.defineProperty(o, "c", { get() { return 1; }, configurable: true }),
              readOnly: (o) => Object.defineProperty(o, "a", { value: 9, writable: false, configurable: false }),
              doubles: (o) => { o[0] = 1.5; o[1] = 2.5; },
              elements: (o) => { for (let i = 0; i < 10000; i++) o[i] = {}; },
              lengthZero: (o) => { o.length = 0; },
              lengthHuge: (o) => { o.length = 100000; },
              dictionaryElements: (o) => { o[100000000] = 1; },
              clear: (o) => { if (o.clear) o.clear(); },
              grow: (o) => { if (o.set) for (let i = 0; i < 10000; i++) o.set(i, i); if (o.add) for (let i = 0; i < 10000; i++) o.add(i); },
              garbage: () => { const junk = []; for (let i = 0; i < 100000; i++) junk.push({ i }); },
              reenter: (o) => { ops.op_structured_clone(o, deserializers); ops.op_serialize(o); },
            };
            let runs = 0;
            for (const [buildName, build] of Object.entries(builds)) {
              for (const [mutationName, mutate] of Object.entries(mutations)) {
                const revivers = { __proto__: null, Mutate: (data) => { try { mutate(data.owner); } catch {} return {}; } };
                const label = `${buildName}/${mutationName}`;
                survives(`${label} clone`, () => ops.op_structured_clone(build(), revivers));
                for (const forStorage of [false, true]) {
                  const bytes = ops.op_serialize(build(), undefined, undefined, forStorage);
                  survives(`${label} deserialize`, () => ops.op_deserialize(bytes, undefined, undefined, revivers, forStorage));
                }
                runs++;
              }
            }
            check(runs === Object.keys(builds).length * Object.keys(mutations).length, "every combination ran");

            // Buffers revived earlier can be detached or resized before a view
            // over them is read.
            const viewed = (buffer, View, ...args) => {
              const owner = { buffer };
              return [buffer, { [brand]() { return { type: "Buffer", buffer }; } }, new View(buffer, ...args)];
            };
            const bufferRevivers = (action) => ({ __proto__: null, Buffer: (data) => { action(data.buffer); return {}; } });
            for (const [label, value, action, expectError] of [
              ["detached before a view", viewed(fresh(16).buffer, Uint8Array, 4, 8), (b) => b.transfer(), true],
              ["detached before a DataView", viewed(fresh(16).buffer, DataView, 2, 4), (b) => b.transfer(), true],
              ["shrunk under a fixed view", viewed(new ArrayBuffer(16, { maxByteLength: 32 }), Uint8Array, 8, 8), (b) => b.resize(4), true],
              ["shrunk under a length-tracking view", viewed(new ArrayBuffer(16, { maxByteLength: 32 }), Uint16Array, 4), (b) => b.resize(6), false],
              ["grown under a view", viewed(new ArrayBuffer(16, { maxByteLength: 32 }), Uint8Array, 4, 8), (b) => b.resize(32), false],
              ["detached resizable", viewed(new ArrayBuffer(16, { maxByteLength: 32 }), Uint8Array, 2), (b) => b.transfer(), true],
            ]) {
              const result = survives(label, () => ops.op_structured_clone(value, bufferRevivers(action)));
              if (expectError) {
                check(result.error instanceof Error, `${label}: expected an error, got ${describe(result.value)}`);
              } else if (!result.error) {
                const [buffer, , view] = result.value;
                check(view.buffer === buffer && view.byteOffset + view.byteLength <= buffer.byteLength,
                  `${label}: view ${view.byteOffset}+${view.byteLength} over ${buffer.byteLength} bytes`);
              }
            }

            // Revivers that throw, return junk, or recurse.
            const once = { [brand]() { return { type: "Odd" }; } };
            for (const [label, reviver, expectError] of [
              ["throws", () => { throw new SyntaxError("reviver"); }, true],
              ["throws a primitive", () => { throw 7; }, false],
              ["returns null", () => null, true],
              ["returns undefined", () => undefined, true],
              ["returns a number", () => 5, false],
              ["returns a function", () => () => 1, false],
              ["returns a proxy", () => new Proxy({}, {}), false],
            ]) {
              const result = attempt(() => ops.op_structured_clone([once], { __proto__: null, Odd: reviver }));
              if (expectError) check(result.error instanceof Error, `reviver ${label}: expected an error`);
              else if (label === "throws a primitive") check(result.error === 7, "a thrown primitive propagates");
              else check(!result.error, `reviver ${label}: ${describe(result.error)}`);
            }
            const recursive = { [brand]() { return { type: "Again" }; } };
            let depth = 0;
            const again = { __proto__: null, Again: () => { depth++; return ops.op_structured_clone(recursive, again); } };
            const deep = attempt(() => ops.op_structured_clone(recursive, again));
            check(deep.error instanceof RangeError && depth > 10, `unbounded recursion through revivers: ${describe(deep.error)} after ${depth}`);
            "#
        ),
    );
}

#[test]
fn values_holding_unusable_buffers_fail_to_serialize() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            const detached = () => { const b = fresh().buffer; b.transfer(); return b; };
            const outOfBounds = () => {
              const b = new ArrayBuffer(8, { maxByteLength: 16 });
              const v = new Uint8Array(b, 4, 4);
              b.resize(2);
              return v;
            };
            for (const [label, make] of [
              ["detached ArrayBuffer", detached],
              ["view over a detached buffer", () => { const b = fresh().buffer; const v = new Float32Array(b, 4, 2); b.transfer(); return v; }],
              ["DataView over a detached buffer", () => { const b = fresh().buffer; const v = new DataView(b); b.transfer(); return v; }],
              ["out-of-bounds view", outOfBounds],
              ["out-of-bounds DataView", () => { const b = new ArrayBuffer(8, { maxByteLength: 16 }); const v = new DataView(b, 4, 4); b.resize(2); return v; }],
              ["SharedArrayBuffer", () => new SharedArrayBuffer(4)],
              ["view over a SharedArrayBuffer", () => new Uint8Array(new SharedArrayBuffer(4))],
              ["symbol", () => Symbol("s")],
              ["function", () => function f() {}],
              ["proxy", () => new Proxy({}, {})],
              ["WeakMap", () => new WeakMap()],
              ["Promise", () => Promise.resolve()],
              ["unbranded host-like object", () => ({ [brand]: 5 })],
            ]) {
              for (const wrap of [(v) => v, (v) => [1, v], (v) => ({ nested: { v } }), (v) => new Map([[v, 1]])]) {
                for (const forStorage of [false, true]) {
                  const result = attempt(() => ops.op_serialize(wrap(make()), undefined, undefined, forStorage));
                  check(result.error instanceof Error, `serialize ${label} (${forStorage}): ${describe(result.error ?? result.value)}`);
                }
                const cloned = attempt(() => ops.op_structured_clone(wrap(make()), deserializers));
                check(cloned.error instanceof Error, `clone ${label}: ${describe(cloned.error ?? cloned.value)}`);
              }
            }

            // The transfer list argument is not used: nothing is detached, and
            // junk in it is ignored on both sides.
            const buffer = fresh().buffer;
            const lists = [[buffer], [detached()], [1, "x", null], new Array(10), { length: 2 ** 32 - 1 }, 5];
            for (const list of lists) {
              const bytes = attempt(() => ops.op_serialize({ buffer }, undefined, list, false));
              check(!bytes.error, `transfer list ${describe(list)}: ${describe(bytes.error)}`);
              if (!bytes.error) {
                const back = attempt(() => ops.op_deserialize(bytes.value, undefined, list, undefined, false));
                check(!back.error && back.value.buffer.byteLength === 16, `transfer list ${describe(list)} on read`);
              }
            }
            check(!buffer.detached && buffer.byteLength === 16, "a listed buffer is not detached");

            // The error callback sees clone errors and may replace them.
            const messages = [];
            const replaced = attempt(() => ops.op_serialize({ f() {} }, undefined, undefined, false, (m) => { messages.push(m); throw new RangeError("replaced"); }));
            check(replaced.error instanceof RangeError && messages.length === 1, "a throwing error callback replaces the error");
            const kept = attempt(() => ops.op_serialize(detached(), undefined, undefined, false, () => {}));
            check(kept.error instanceof TypeError, "a quiet error callback keeps the TypeError");
            const notCallable = attempt(() => ops.op_serialize(1, undefined, undefined, false, 5));
            check(notCallable.error instanceof TypeError, "a non-function error callback is rejected");
            const terminal = attempt(() => ops.op_serialize({ f() {} }, undefined, undefined, false, () => { throw 1; }));
            check(terminal.error === 1, "an error callback may throw a primitive");
            "#
        ),
    );
}

#[test]
fn nesting_deep_enough_to_exhaust_the_stack_throws() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CORPUS}\n{}",
            r#"
            const nest = (depth, wrap) => { let value = 1; for (let i = 0; i < depth; i++) value = wrap(value); return value; };
            for (const [label, value] of [
              ["arrays", nest(200000, (v) => [v])],
              ["objects", nest(200000, (v) => ({ v }))],
              ["maps", nest(200000, (v) => new Map([[v, v]]))],
              ["host objects", nest(20000, (v) => ({ [brand]() { return { type: "Point", x: v }; } }))],
            ]) {
              const written = attempt(() => ops.op_serialize(value));
              check(written.error instanceof RangeError, `serialize nested ${label}: ${describe(written.error ?? written.value)}`);
              const cloned = attempt(() => ops.op_structured_clone(value, deserializers));
              check(cloned.error instanceof RangeError, `clone nested ${label}: ${describe(cloned.error ?? cloned.value)}`);
            }
            const repeat = (unit, times) => {
              const out = new Uint8Array(header.length + unit.length * times);
              out.set(header);
              for (let i = 0; i < times; i++) out.set(unit, header.length + i * unit.length);
              return out;
            };
            for (const [label, unit] of [
              ["dense arrays", [0x41, 1]],
              ["sparse arrays", [0x61, 1]],
              ["objects", [0x6f, 0x22, 1, 0x61]],
              ["maps", [0x3b]],
              ["sets", [0x27]],
              ["host objects", [0x5c, ...varint(2 ** 32 - 1)]],
              ["host object descriptions", [0x5c, ...varint(2 ** 32 - 1), 0x6f, 0x22, 4, 0x74, 0x79, 0x70, 0x65, 0x22, 5, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x22, 1, 0x78]],
              ["errors with causes", [0x72, 0x63]],
            ]) {
              for (const forStorage of [false, true]) {
                const result = attempt(() => ops.op_deserialize(repeat(unit, 1000000), hostObjects, undefined, deserializers, forStorage));
                check(result.error instanceof Error, `deserialize 1e6 nested ${label}: ${describe(result.error ?? result.value)}`);
              }
            }
            "#
        ),
    );
}
