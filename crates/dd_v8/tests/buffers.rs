//! Every op path that takes bytes from JavaScript, fed every kind of buffer
//! source and a range of non-buffers. Each call must copy exactly the view's
//! bytes, write only inside the view, or throw a clean JavaScript exception.

mod common;

use common::{assert_checks, assert_checks_async, runtime, runtime_with};
use dd_v8::RuntimeOptions;

/// Builds the cases. Each `make()` returns a fresh value plus what a byte copy
/// of it must read (`bytes`, undefined for non-buffers), what a typed op that
/// also accepts arrays of numbers reads (`typed`, null when it must throw),
/// and the memory behind the view so in-place writes can be checked.
const CASES: &str = r#"
globalThis.fresh = (length = 16) => Uint8Array.from({ length }, (_, i) => (i * 7 + 1) & 0xff);
globalThis.cases = [];
const add = (name, make) => cases.push({ name, make });
const view = (value, buffer, offset, length) => ({
  value,
  bytes: [...new Uint8Array(buffer, offset, length)],
  memory: () => new Uint8Array(buffer),
  offset,
});
const empty = (value, memory = null) => ({ value, bytes: [], memory, offset: 0 });
const resizable = (length, max) => {
  const buffer = new ArrayBuffer(length, { maxByteLength: max });
  new Uint8Array(buffer).set(fresh(length));
  return buffer;
};

add("on-heap Uint8Array", () => {
  const bytes = fresh();
  return { value: bytes, bytes: [...bytes], memory: () => bytes, offset: 0 };
});
add("off-heap Uint8Array", () => { const b = fresh(4096); return view(b, b.buffer, 0, 4096); });
add("Uint8Array at an offset", () => { const buffer = fresh(32).buffer; return view(new Uint8Array(buffer, 3, 5), buffer, 3, 5); });
add("subarray", () => { const b = fresh(32); return view(b.subarray(2, 9), b.buffer, 2, 7); });
const kinds = [
  ["Int8Array", 1, 4], ["Uint8ClampedArray", 5, 3], ["Int16Array", 2, 3], ["Uint16Array", 4, 4],
  ["Int32Array", 4, 2], ["Uint32Array", 8, 3], ["Float16Array", 2, 3], ["Float32Array", 4, 3],
  ["Float64Array", 8, 2], ["BigInt64Array", 8, 1], ["BigUint64Array", 16, 2],
];
for (const [kind, offset, length] of kinds) {
  const Kind = globalThis[kind];
  if (!Kind) continue;
  add(`${kind}(buffer, ${offset}, ${length})`, () => {
    const buffer = fresh(48).buffer;
    return view(new Kind(buffer, offset, length), buffer, offset, length * Kind.BYTES_PER_ELEMENT);
  });
}
add("DataView", () => { const buffer = fresh(16).buffer; return view(new DataView(buffer), buffer, 0, 16); });
add("DataView at an offset", () => { const buffer = fresh(32).buffer; return view(new DataView(buffer, 5, 7), buffer, 5, 7); });
add("ArrayBuffer", () => { const buffer = fresh(24).buffer; return view(buffer, buffer, 0, 24); });
add("subclass of Uint8Array", () => {
  class Bytes extends Uint8Array {}
  const buffer = fresh(16).buffer;
  return view(new Bytes(buffer, 2, 4), buffer, 2, 4);
});
add("view with lying own properties", () => {
  const buffer = fresh(16).buffer;
  const value = new Uint8Array(buffer, 2, 4);
  for (const key of ["byteLength", "length", "byteOffset"]) {
    Object.defineProperty(value, key, { value: 1e9 });
  }
  Object.defineProperty(value, "buffer", { value: new ArrayBuffer(1e6) });
  return view(value, buffer, 2, 4);
});

add("empty ArrayBuffer", () => empty(new ArrayBuffer(0)));
add("empty Uint8Array", () => empty(new Uint8Array(0)));
add("empty Float64Array", () => empty(new Float64Array(0)));
add("Uint8Array at the end of its buffer", () => { const buffer = fresh(8).buffer; return { ...view(new Uint8Array(buffer, 8), buffer, 8, 0) }; });
add("DataView at the end of its buffer", () => { const buffer = fresh(8).buffer; return view(new DataView(buffer, 8), buffer, 8, 0); });

add("detached ArrayBuffer", () => { const buffer = fresh().buffer; buffer.transfer(); return empty(buffer); });
add("ArrayBuffer detached by transferToFixedLength", () => { const buffer = fresh().buffer; buffer.transferToFixedLength(4); return empty(buffer); });
add("Uint8Array over a detached buffer", () => { const buffer = fresh().buffer; const value = new Uint8Array(buffer, 4, 8); buffer.transfer(); return empty(value); });
add("Float64Array over a detached buffer", () => { const buffer = fresh().buffer; const value = new Float64Array(buffer, 8, 1); buffer.transfer(); return empty(value); });
add("DataView over a detached buffer", () => { const buffer = fresh().buffer; const value = new DataView(buffer, 2, 6); buffer.transfer(); return empty(value); });
add("length-tracking view over a detached resizable buffer", () => { const buffer = resizable(8, 16); const value = new Uint8Array(buffer, 2); buffer.transfer(); return empty(value); });

add("resizable: fixed view in bounds after growing", () => {
  const buffer = resizable(8, 32);
  const value = new Uint8Array(buffer, 2, 4);
  buffer.resize(32);
  return view(value, buffer, 2, 4);
});
add("resizable: fixed view out of bounds after shrinking", () => {
  const buffer = resizable(8, 32);
  const value = new Uint8Array(buffer, 4, 4);
  buffer.resize(6);
  return empty(value, () => new Uint8Array(buffer));
});
add("resizable: view whose offset is past the new length", () => {
  const buffer = resizable(8, 32);
  const value = new Int16Array(buffer, 6, 1);
  buffer.resize(2);
  return empty(value, () => new Uint8Array(buffer));
});
add("resizable: length-tracking view after shrinking", () => {
  const buffer = resizable(8, 32);
  const value = new Uint8Array(buffer, 2);
  buffer.resize(5);
  return view(value, buffer, 2, 3);
});
add("resizable: length-tracking view after growing", () => {
  const buffer = resizable(4, 32);
  const value = new Uint8Array(buffer, 1);
  buffer.resize(12);
  return view(value, buffer, 1, 11);
});
add("resizable: length-tracking view shrunk below its offset", () => {
  const buffer = resizable(8, 32);
  const value = new Uint8Array(buffer, 6);
  buffer.resize(4);
  return empty(value, () => new Uint8Array(buffer));
});
add("resizable: length-tracking Uint16Array over an odd length", () => {
  const buffer = resizable(8, 32);
  const value = new Uint16Array(buffer, 2);
  buffer.resize(7);
  return view(value, buffer, 2, 4);
});
add("resizable: fixed DataView out of bounds", () => {
  const buffer = resizable(8, 32);
  const value = new DataView(buffer, 4, 4);
  buffer.resize(6);
  return empty(value, () => new Uint8Array(buffer));
});
add("resizable: length-tracking DataView", () => {
  const buffer = resizable(8, 32);
  const value = new DataView(buffer, 3);
  buffer.resize(5);
  return view(value, buffer, 3, 2);
});
add("resizable: ArrayBuffer after shrinking", () => { const buffer = resizable(8, 32); buffer.resize(3); return view(buffer, buffer, 0, 3); });
add("resizable: ArrayBuffer after growing", () => { const buffer = resizable(4, 32); buffer.resize(32); return view(buffer, buffer, 0, 32); });
add("resizable: ArrayBuffer shrunk to zero", () => { const buffer = resizable(8, 32); buffer.resize(0); return empty(buffer, () => new Uint8Array(buffer)); });

const shared = (length, options) => {
  const buffer = new SharedArrayBuffer(length, options);
  new Uint8Array(buffer).set(fresh(length));
  return buffer;
};
add("Uint8Array over a SharedArrayBuffer", () => { const buffer = shared(16); return view(new Uint8Array(buffer, 2, 6), buffer, 2, 6); });
add("DataView over a SharedArrayBuffer", () => { const buffer = shared(16); return view(new DataView(buffer, 1, 9), buffer, 1, 9); });
add("Int32Array over a SharedArrayBuffer", () => { const buffer = shared(16); return view(new Int32Array(buffer, 4, 2), buffer, 4, 8); });
add("length-tracking view over a grown SharedArrayBuffer", () => {
  const buffer = shared(4, { maxByteLength: 16 });
  const value = new Uint8Array(buffer, 1);
  buffer.grow(10);
  return view(value, buffer, 1, 9);
});
// Byte copies take views over shared memory, not the shared buffer itself.
add("SharedArrayBuffer", () => ({ value: shared(8), typed: null }));

const not = (name, make, typed = null) => add(name, () => ({ value: make(), typed }));
not("undefined", () => undefined);
not("null", () => null);
not("number", () => 42.5);
not("NaN", () => NaN);
not("string", () => "abc");
not("empty string", () => "");
not("boolean", () => true);
not("symbol", () => Symbol("bytes"));
not("bigint", () => 1n);
not("plain object", () => ({}));
not("buffer-shaped object", () => ({ byteLength: 8, byteOffset: 0, buffer: new ArrayBuffer(8) }));
not("array-like object", () => ({ length: 2, 0: 1, 1: 2 }));
not("function", () => () => {});
not("proxy of a Uint8Array", () => new Proxy(fresh(), {}));
not("object inheriting from Uint8Array.prototype", () => Object.create(Uint8Array.prototype));
not("Date", () => new Date(0));
not("Map", () => new Map([[0, 1]]));
not("Promise", () => Promise.resolve());
// Typed ops read byte arrays from plain arrays of byte values too.
not("empty array", () => [], []);
not("array of bytes", () => [1, 2, 255], [1, 2, 255]);
not("array with fractions and a bigint", () => [1.75, 2n], [1, 2]);
not("array with 256", () => [1, 256]);
not("array with -1", () => [-1]);
not("array with a string", () => ["1"]);
not("array with a hole", () => [1, , 3]);
not("array with a throwing getter", () => {
  const array = [1, 2];
  Object.defineProperty(array, 1, { get() { throw new Error("getter"); } });
  return array;
});
not("sparse array of length 2^32 - 1", () => new Array(2 ** 32 - 1));
"#;

/// Checks one call against a case: copies read exactly `bytes`, anything
/// that is not a buffer throws a TypeError.
const CHECKS: &str = r#"
globalThis.nativeWord = [...new Uint8Array(new Uint32Array([0x04030201]).buffer)];

globalThis.expectBytes = (label, result, expected) => {
  if (expected == null) {
    check(result.error instanceof TypeError, `${label}: expected a TypeError, got ${describe(result.error ?? result.value)}`);
  } else {
    check(!result.error && sameBytes(result.value, expected),
      `${label}: expected [${expected}], got ${result.error ? describe(result.error) : `[${result.value}]`}`);
  }
};

/// Runs a writing op and checks it wrote `expectedRegion` (a function of
/// the old byte at a region index) inside the view and nothing outside it.
globalThis.expectWrite = (label, built, call, writtenByte) => {
  const before = built.memory ? [...built.memory()] : null;
  const result = attempt(() => call(built.value));
  if (built.bytes === undefined) {
    check(result.error instanceof TypeError, `${label}: expected a TypeError, got ${describe(result.error ?? result.value)}`);
    return;
  }
  if (result.error) {
    check(false, `${label}: threw ${describe(result.error)}`);
    return;
  }
  if (!before) return;
  const after = [...built.memory()];
  const start = built.offset;
  const end = start + built.bytes.length;
  for (let i = 0; i < after.length; i++) {
    const inside = i >= start && i < end;
    const expected = inside ? writtenByte(i - start, before[i]) : before[i];
    if (after[i] !== expected) {
      check(false, `${label}: byte ${i} is ${after[i]}, expected ${expected} (view ${start}..${end})`);
      return;
    }
  }
};
"#;

#[tokio::test]
async fn typed_ops_copy_exactly_the_bytes_of_every_buffer_source() {
    let mut runtime = runtime();
    assert_checks_async(
        &mut runtime,
        &format!(
            "{CASES}\n{CHECKS}\n{}",
            r#"
            (async () => {
              for (const { name, make } of cases) {
                const typed = (built) => built.typed !== undefined ? built.typed : (built.bytes ?? null);
                for (const [op, call] of [
                  ["op_echo_bytes", (v) => ops.op_echo_bytes(v)],
                  ["op_echo_vec", (v) => ops.op_echo_vec(v)],
                  ["op_echo_field", (v) => ops.op_echo_field({ data: v })],
                ]) {
                  const built = make();
                  expectBytes(`${op}(${name})`, attempt(() => call(built.value)), typed(built));
                }

                let built = make();
                const optional = attempt(() => ops.op_echo_optional(built.value));
                if (built.value == null) {
                  check(optional.value === null, `op_echo_optional(${name}) should be null`);
                } else {
                  expectBytes(`op_echo_optional(${name})`, optional, typed(built));
                }

                built = make();
                const length = attempt(() => ops.op_byte_length(built.value));
                const expected = typed(built);
                if (expected == null) {
                  check(length.error instanceof TypeError, `op_byte_length(${name}): expected a TypeError`);
                } else {
                  check(length.value === expected.length, `op_byte_length(${name}) = ${describe(length.value ?? length.error)}`);
                }

                built = make();
                let result;
                try {
                  result = { value: await ops.op_echo_async(built.value) };
                } catch (error) {
                  result = { error };
                }
                expectBytes(`op_echo_async(${name})`, result, typed(built));
              }
            })()
            "#
        ),
    )
    .await;
}

#[test]
fn raw_ops_copy_exactly_the_bytes_of_every_buffer_source() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CASES}\n{CHECKS}\n{}",
            r#"
            for (const { name, make } of cases) {
              let built = make();
              expectBytes(`buffer_bytes(${name})`, attempt(() => ops.op_raw_copy(built.value)), built.bytes ?? null);

              built = make();
              const decoded = attempt(() => ops.op_decode(built.value));
              if (built.bytes === undefined) {
                check(decoded.error instanceof TypeError, `op_decode(${name}): expected a TypeError`);
              } else {
                const expected = ops.op_decode(Uint8Array.from(built.bytes));
                check(decoded.value === expected, `op_decode(${name}) = ${describe(decoded.value ?? decoded.error)}, expected ${JSON.stringify(expected)}`);
              }

              built = make();
              const fed = attempt(() => ops.op_wasm_streaming_feed(0, built.value));
              if (built.bytes === undefined) {
                check(fed.error instanceof TypeError, `op_wasm_streaming_feed(${name}): expected a TypeError`);
              } else {
                check(!fed.error, `op_wasm_streaming_feed(${name}) threw ${describe(fed.error)}`);
              }

              for (const forStorage of [false, true]) {
                built = make();
                const read = attempt(() => ops.op_deserialize(built.value, undefined, undefined, undefined, forStorage));
                if (built.bytes === undefined) {
                  check(read.error instanceof TypeError, `op_deserialize(${name}): expected a TypeError`);
                } else {
                  check(!read.error || isCleanError(read.error), `op_deserialize(${name}) threw a non-error ${describe(read.error)}`);
                }
              }
            }
            "#
        ),
    );
}

#[test]
fn in_place_writes_stay_inside_the_view() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        &format!(
            "{CASES}\n{CHECKS}\n{}",
            r#"
            for (const { name, make } of cases) {
              let built = make();
              let seen;
              expectWrite(`with_buffer_mut(${name})`, built, (v) => (seen = ops.op_raw_fill(v)), () => 0xab);
              if (built.bytes !== undefined && seen !== undefined) {
                check(seen === built.bytes.length, `with_buffer_mut(${name}) saw ${seen} bytes, expected ${built.bytes.length}`);
              }

              built = make();
              const words = built.bytes ? Math.floor(built.bytes.length / 4) * 4 : 0;
              expectWrite(`write_u32s(${name})`, built, (v) => ops.op_raw_write_u32s(v),
                (index, old) => (index < words ? nativeWord[index % 4] : old));
            }
            "#
        ),
    );
}

/// rusty_v8's `copy_contents` hands V8 the length as a C `int` and panicked
/// (aborting the process) on any view of 2 GiB or more.
#[test]
fn views_of_two_gib_or_more_copy_without_aborting() {
    // Room to copy past 2 GiB; the default budget refuses such a view first.
    let mut runtime = runtime_with(RuntimeOptions {
        max_op_argument_bytes: 3 << 30,
        ..Default::default()
    });
    assert_checks(
        &mut runtime,
        r#"
        const huge = new Uint8Array(2 ** 31 + 16);
        huge[16] = 1;
        huge[huge.length - 1] = 2;
        const view = huge.subarray(16);
        check(ops.op_byte_length(view) === 2 ** 31, "typed op copies all 2 GiB");
        const decoded = attempt(() => ops.op_decode(view));
        check(decoded.error instanceof RangeError, `op_decode of 2 GiB: ${describe(decoded.error ?? decoded.value)}`);
        "#,
    );
}

/// A sparse array's length is free to set to 2^32 - 1; reading it as bytes
/// must not reserve memory for that length up front.
#[test]
fn huge_sparse_arrays_fail_fast_as_byte_arrays() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        for (const length of [2 ** 32 - 1, 2 ** 31, 1e8]) {
          for (const op of ["op_echo_bytes", "op_echo_vec", "op_byte_length"]) {
            const result = attempt(() => ops[op](new Array(length)));
            check(result.error instanceof TypeError, `${op}(new Array(${length})): ${describe(result.error ?? result.value)}`);
          }
        }
        const filled = new Array(2 ** 32 - 1);
        filled[0] = 7;
        const result = attempt(() => ops.op_echo_bytes(filled));
        check(result.error instanceof TypeError, "a hole after the first element still throws");
        "#,
    );
}

#[tokio::test]
async fn wasm_streaming_reads_every_buffer_source() {
    let mut runtime = runtime();
    assert_checks_async(
        &mut runtime,
        r#"
        (async () => {
          // (module (func (export "add") (param i32 i32) (result i32) local.get 0 local.get 1 i32.add))
          const module = [0,97,115,109,1,0,0,0,1,7,1,96,2,127,127,1,127,3,2,1,0,7,7,1,3,97,100,100,0,0,10,9,1,7,0,32,0,32,1,106,11];
          const chunk = (from, to) => module.slice(from, to);
          const padded = (bytes, before, after) => {
            const buffer = new ArrayBuffer(before + bytes.length + after);
            new Uint8Array(buffer, before).set(bytes);
            return buffer;
          };
          const detached = () => { const buffer = new ArrayBuffer(8); buffer.transfer(); return buffer; };
          const outOfBounds = () => {
            const buffer = new ArrayBuffer(8, { maxByteLength: 16 });
            const view = new Uint8Array(buffer, 4, 4);
            buffer.resize(2);
            return view;
          };
          const pieces = [
            new Uint8Array(padded(chunk(0, 3), 5, 2), 5, 3),
            new DataView(padded(chunk(3, 8), 1, 3), 1, 5),
            detached(),
            outOfBounds(),
            (() => { const shared = new SharedArrayBuffer(7); new Uint8Array(shared).set(chunk(8, 15)); return new Uint8Array(shared); })(),
            new ArrayBuffer(0),
            (() => {
              const buffer = new ArrayBuffer(20, { maxByteLength: 64 });
              const view = new Uint8Array(buffer, 2);
              buffer.resize(2 + 10);
              view.set(chunk(15, 25));
              return view;
            })(),
            new Uint16Array(padded(chunk(25, 31), 2, 0), 2, 3),
            new Uint8Array(chunk(31, module.length)).buffer,
          ];
          let streamId;
          ops.op_set_wasm_streaming_handler((source, id) => {
            streamId = id;
            check(source === "source", `handler got ${describe(source)}`);
            for (const piece of pieces) ops.op_wasm_streaming_feed(id, piece);
            const bad = attempt(() => ops.op_wasm_streaming_feed(id, [1, 2, 3]));
            check(bad.error instanceof TypeError, "feeding a plain array throws a TypeError");
            ops.op_wasm_streaming_set_url(id, { toString() { return "mem:///add.wasm"; } });
            ops.op_wasm_streaming_finish(id);
            // The stream is gone now; these are no-ops.
            ops.op_wasm_streaming_feed(id, new Uint8Array([0xff]));
            ops.op_wasm_streaming_finish(id);
            ops.op_wasm_streaming_abort(id, new Error("late"));
          });
          const compiled = await WebAssembly.compileStreaming("source");
          const instance = new WebAssembly.Instance(compiled);
          check(instance.exports.add(19, 23) === 42, "streamed module adds");

          for (const id of [0, -1, 2 ** 32, NaN, "x", {}, 2 ** 53]) {
            const results = [
              attempt(() => ops.op_wasm_streaming_feed(id, new Uint8Array(1))),
              attempt(() => ops.op_wasm_streaming_set_url(id, "u")),
              attempt(() => ops.op_wasm_streaming_finish(id)),
              attempt(() => ops.op_wasm_streaming_abort(id, undefined)),
            ];
            check(results.every((r) => !r.error), `unknown stream ${describe(id)}: ${results.map((r) => describe(r.error)).join()}`);
          }

          ops.op_set_wasm_streaming_handler((_source, id) => {
            ops.op_wasm_streaming_feed(id, new Uint8Array([0, 97, 115]));
            ops.op_wasm_streaming_abort(id, new RangeError("stopped"));
          });
          const aborted = await WebAssembly.compileStreaming("source").then(() => null, (e) => e);
          check(aborted instanceof RangeError && aborted.message === "stopped", `abort rejects: ${describe(aborted)}`);

          ops.op_set_wasm_streaming_handler(() => { throw new SyntaxError("handler"); });
          const thrown = await WebAssembly.compileStreaming("source").then(() => null, (e) => e);
          check(thrown instanceof SyntaxError, `a throwing handler rejects: ${describe(thrown)}`);

          ops.op_set_wasm_streaming_handler((_source, id) => {
            ops.op_wasm_streaming_feed(id, new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9]));
            ops.op_wasm_streaming_finish(id);
          });
          const invalid = await WebAssembly.compileStreaming("source").then(() => null, (e) => e);
          check(invalid instanceof WebAssembly.CompileError, `garbage bytes reject: ${describe(invalid)}`);

          const notFunction = attempt(() => ops.op_set_wasm_streaming_handler(5));
          check(notFunction.error instanceof TypeError, "the handler must be a function");
          return streamId;
        })()
        "#,
    )
    .await;
}
