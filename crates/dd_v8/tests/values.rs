//! Edge values through the serde bridge: NaN and -0, integer and BigInt
//! limits, deep and cyclic nesting, proxies and getters that throw or
//! mutate, sparse arrays, and malformed structs and enums. Each conversion
//! must give the documented value or a clean TypeError.

mod common;

use common::{assert_checks, assert_checks_async, runtime};

#[test]
fn numbers_keep_nan_negative_zero_and_infinities() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        check(Number.isNaN(ops.op_f64(NaN)), "f64 NaN");
        check(Object.is(ops.op_f64(-0), -0), "f64 -0");
        check(ops.op_f64(Infinity) === Infinity && ops.op_f64(-Infinity) === -Infinity, "f64 infinities");
        check(ops.op_f64(Number.MIN_VALUE) === Number.MIN_VALUE, "f64 denormal");
        check(ops.op_f64(Number.MAX_VALUE) === Number.MAX_VALUE, "f64 max");
        check(ops.op_f64(-(2n ** 63n)) === -(2 ** 63), "f64 from an i64 BigInt");
        check(ops.op_f64(2n ** 64n - 1n) === 2 ** 64, "f64 from a u64 BigInt");
        check(Number.isNaN(ops.op_f32(NaN)) && Object.is(ops.op_f32(-0), -0), "f32 NaN and -0");
        check(ops.op_f32(1e300) === Infinity, "f32 overflow saturates");
        for (const [label, value] of [
          ["string", "1"], ["boxed number", new Number(1)], ["null", null], ["undefined", undefined],
          ["object", { valueOf: () => 1 }], ["huge BigInt", 2n ** 64n], ["symbol", Symbol()],
        ]) {
          const result = attempt(() => ops.op_f64(value));
          check(result.error instanceof TypeError, `f64(${label}) should throw a TypeError, got ${describe(result.error ?? result.value)}`);
        }

        const json = ops.op_json([NaN, -0, Infinity, -Infinity, 0.5, 2 ** 53, -(2 ** 53) - 2, 1e300]);
        check(JSON.stringify(json) === "[null,0,null,null,0.5,9007199254740992,-9007199254740994,1e+300]",
          `JSON numbers: ${JSON.stringify(json)}`);
        "#,
    );
}

#[test]
fn integers_truncate_and_reject_values_out_of_range() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        const ok = (label, actual, expected) =>
          check(Object.is(actual, expected), `${label}: got ${describe(actual)}, expected ${describe(expected)}`);
        const fails = (label, f) => {
          const result = attempt(f);
          check(result.error instanceof TypeError, `${label}: expected a TypeError, got ${describe(result.error ?? result.value)}`);
        };
        ok("u8 255", ops.op_u8(255), 255);
        ok("u8 truncates", ops.op_u8(254.9), 254);
        ok("u8 -0.5 truncates to 0", ops.op_u8(-0.5), 0);
        ok("u8 NaN is 0", ops.op_u8(NaN), 0);
        ok("u8 from BigInt", ops.op_u8(7n), 7);
        fails("u8 256", () => ops.op_u8(256));
        fails("u8 -1", () => ops.op_u8(-1));
        fails("u8 Infinity", () => ops.op_u8(Infinity));
        fails("u8 string", () => ops.op_u8("1"));
        ok("i32 min", ops.op_i32(-(2 ** 31)), -(2 ** 31));
        fails("i32 past max", () => ops.op_i32(2 ** 31));
        fails("i32 -Infinity", () => ops.op_i32(-Infinity));
        ok("u32 max", ops.op_u32(2 ** 32 - 1), 2 ** 32 - 1);
        fails("u32 past max", () => ops.op_u32(2 ** 32));
        fails("u32 1e300", () => ops.op_u32(1e300));

        ok("u64 safe max stays a Number", ops.op_u64(Number.MAX_SAFE_INTEGER), Number.MAX_SAFE_INTEGER);
        ok("u64 2^53 comes back as a BigInt", ops.op_u64(2 ** 53), 2n ** 53n);
        ok("u64 max", ops.op_u64(2n ** 64n - 1n), 2n ** 64n - 1n);
        fails("u64 2^64", () => ops.op_u64(2n ** 64n));
        fails("u64 -1n", () => ops.op_u64(-1n));
        fails("u64 2^64 as a Number", () => ops.op_u64(2 ** 64));
        ok("i64 min", ops.op_i64(-(2n ** 63n)), -(2n ** 63n));
        ok("i64 max", ops.op_i64(2n ** 63n - 1n), 2n ** 63n - 1n);
        ok("i64 small BigInt is a Number", ops.op_i64(-5n), -5);
        fails("i64 below min", () => ops.op_i64(-(2n ** 63n) - 1n));
        fails("i64 above max", () => ops.op_i64(2n ** 63n));
        fails("i64 2^63 as a Number", () => ops.op_i64(2 ** 63));
        fails("i64 BigInt past 128 bits", () => ops.op_i64(2n ** 200n));
        fails("i64 negative BigInt past 128 bits", () => ops.op_i64(-(2n ** 200n)));

        const [safe, unsafe, min, max, umax] = ops.op_big_numbers();
        ok("returned safe integer", safe, Number.MAX_SAFE_INTEGER);
        ok("returned 2^53", unsafe, 2n ** 53n);
        ok("returned i64::MIN", min, -(2n ** 63n));
        ok("returned i64::MAX", max, 2n ** 63n - 1n);
        ok("returned u64::MAX", umax, 2n ** 64n - 1n);

        ok("JSON BigInt above i64", ops.op_json(2n ** 63n), 2n ** 63n);
        ok("JSON negative BigInt", ops.op_json(-5n), -5);
        fails("JSON BigInt past u64", () => ops.op_json(2n ** 64n));
        fails("JSON BigInt below i64", () => ops.op_json(-(2n ** 63n) - 1n));

        ok("bool from a number", ops.op_bool(1), true);
        ok("bool from an object", ops.op_bool({}), true);
        ok("bool from NaN", ops.op_bool(NaN), false);
        "#,
    );
}

#[test]
fn strings_replace_lone_surrogates_and_reject_non_strings() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        check(ops.op_string("\ud800x\udfff") === "�x�", "lone surrogates become U+FFFD");
        check(ops.op_string("\u{1f600}") === "\u{1f600}", "astral characters survive");
        check(ops.op_string("") === "", "empty string");
        check(ops.op_string("a\0b") === "a\0b", "NUL survives");
        const long = "x".repeat(2 ** 24);
        check(ops.op_string(long) === long, "16 MiB string");
        for (const value of [new String("s"), 1, null, undefined, ["s"], { toString: () => "s" }, Symbol("s")]) {
          const result = attempt(() => ops.op_string(value));
          check(result.error instanceof TypeError, `string(${describe(value)}) should throw`);
        }
        const strings = ops.op_strings(["a", "b"]);
        check(strings.join() === "a,b", "Vec<String>");
        "#,
    );
}

#[test]
fn nesting_is_bounded_and_cycles_terminate() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        const nest = (depth, wrap) => { let value = 1; for (let i = 0; i < depth; i++) value = wrap(value); return value; };
        check(JSON.stringify(ops.op_json(nest(100, (v) => [v]))).length > 100, "100 nested arrays convert");
        check(ops.op_json(nest(100, (v) => ({ v }))) !== undefined, "100 nested objects convert");
        for (const [label, value] of [
          ["10000 nested arrays", nest(10000, (v) => [v])],
          ["10000 nested objects", nest(10000, (v) => ({ v }))],
          ["10000 nested enum objects", nest(10000, (v) => ({ Say: v }))],
        ]) {
          const result = attempt(() => ops.op_json(value));
          check(result.error instanceof TypeError && /nested too deeply/.test(result.error.message), `${label}: ${describe(result.error ?? result.value)}`);
        }
        const deepCommand = attempt(() => ops.op_command(nest(10000, (v) => ({ Say: v }))));
        check(deepCommand.error instanceof TypeError, `nested enum: ${describe(deepCommand.error ?? deepCommand.value)}`);

        const cyclic = { name: "loop" };
        cyclic.self = cyclic;
        const cyclicArray = [];
        cyclicArray.push(cyclicArray);
        for (const [label, value] of [["object", cyclic], ["array", cyclicArray]]) {
          const result = attempt(() => ops.op_json(value));
          check(result.error instanceof TypeError, `cyclic ${label}: ${describe(result.error ?? result.value)}`);
        }
        const shape = ops.op_shape(cyclic);
        check(shape.name === "loop", "struct ops ignore the cyclic field they do not read");
        "#,
    );
}

#[test]
fn proxies_getters_and_mutation_during_conversion() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        const typeError = (label, f, pattern) => {
          const result = attempt(f);
          check(result.error instanceof TypeError && (!pattern || pattern.test(result.error.message)),
            `${label}: ${describe(result.error ?? result.value)}`);
        };
        check(JSON.stringify(ops.op_json(new Proxy({ a: 1 }, {}))) === '{"a":1}', "a transparent proxy converts");
        const logged = [];
        const traced = new Proxy({ name: "p", size: 2 }, {
          get(target, key, receiver) { logged.push(String(key)); return Reflect.get(target, key, receiver); },
        });
        check(ops.op_shape(traced).name === "p" && logged.includes("name"), "struct reads go through get traps");

        typeError("throwing ownKeys trap", () => ops.op_json(new Proxy({}, { ownKeys() { throw new Error("keys"); } })), /keys threw/);
        typeError("throwing get trap", () => ops.op_json(new Proxy({ a: 1 }, { get() { throw new Error("get"); } })), /value threw/);
        typeError("throwing struct get trap", () => ops.op_shape(new Proxy({}, { get() { throw new Error("get"); } })), /threw/);
        typeError("ownKeys returning junk", () => ops.op_json(new Proxy({}, { ownKeys() { return [1]; } })));
        const { proxy, revoke } = Proxy.revocable({ a: 1 }, {});
        revoke();
        typeError("revoked proxy", () => ops.op_json(proxy));
        typeError("revoked proxy as a struct", () => ops.op_shape(proxy));
        typeError("getter that throws", () => ops.op_json({ get a() { throw new Error("getter"); } }), /value threw/);
        typeError("field getter that throws", () => ops.op_shape({ get name() { throw new Error("getter"); } }), /field name threw/);
        typeError("element getter that throws", () => ops.op_strings(Object.defineProperty(["a", "b"], 1, { get() { throw new Error("element"); } })), /element threw/);
        typeError("enum key getter that throws", () => ops.op_command({ get Say() { throw new Error("enum"); } }));
        typeError("getter that throws a non-error", () => ops.op_json({ get a() { throw 42; } }));

        // Getters that rewrite what is being converted.
        const shrinking = ["a", "b", "c", "d"];
        Object.defineProperty(shrinking, 0, { get() { shrinking.length = 1; return "a"; } });
        typeError("array truncated mid-read", () => ops.op_strings(shrinking));
        const growing = [1, 2];
        Object.defineProperty(growing, 0, { get() { for (let i = 0; i < 1000; i++) growing.push(i); return 0; } });
        check(ops.op_json(growing).length === 2, "array grown mid-read keeps its starting length");
        const mutating = { a: 1, b: 2, c: 3 };
        Object.defineProperty(mutating, "a", { enumerable: true, get() { delete mutating.b; mutating.z = 9; return 1; } });
        const mutated = ops.op_json(mutating);
        check(mutated.a === 1 && !("b" in mutated) && mutated.c === 3, `keys deleted mid-read are skipped: ${JSON.stringify(mutated)}`);
        let reentered = 0;
        const reentrant = { get name() { reentered = ops.op_json({ inner: ops.op_shape({ name: "inner" }).name }).inner; return "outer"; } };
        check(ops.op_shape(reentrant).name === "outer" && reentered === "inner", "ops called from a getter mid-conversion");
        "#,
    );
}

#[test]
fn sparse_arrays_holes_and_unusual_objects() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        const same = (label, value, expected) => {
          const actual = JSON.stringify(ops.op_json(value));
          check(actual === expected, `${label}: ${actual}, expected ${expected}`);
        };
        same("holes", [1, , 3], "[1,null,3]");
        same("trailing hole", [1, ,], "[1,null]");
        same("1e5 holes", new Array(1e5), JSON.stringify(new Array(1e5).fill(null)));
        same("undefined members are skipped", { a: undefined, b: 1 }, '{"b":1}');
        same("symbol keys are skipped", { [Symbol("s")]: 1, a: 2 }, '{"a":2}');
        same("numeric keys", { 2: "b", 1: "a", 1.5: "c" }, '{"1":"a","2":"b","1.5":"c"}');
        same("non-enumerable keys are skipped", Object.defineProperty({ a: 1 }, "hidden", { value: 2 }), '{"a":1}');
        same("inherited keys are skipped", Object.create({ inherited: 1 }), "{}");
        same("Map", new Map([[1, 2]]), "{}");
        same("Date", new Date(0), "{}");
        same("boxed string", new String("ab"), '{"0":"a","1":"b"}');
        same("arguments object", (function () { return arguments; })(1, 2), '{"0":1,"1":2}');
        same("null prototype", Object.assign(Object.create(null), { a: 1 }), '{"a":1}');
        const sparseWithExtras = [1];
        sparseWithExtras[5] = 2;
        sparseWithExtras.extra = "ignored";
        same("array own non-index keys are ignored", sparseWithExtras, "[1,null,null,null,null,2]");
        same("function", Object.assign(() => 1, { a: 1 }), '{"a":1}');
        const symbol = attempt(() => ops.op_json(Symbol("s")));
        check(symbol.error instanceof TypeError, "a symbol is not serializable");
        "#,
    );
}

#[test]
fn structs_and_enums_reject_malformed_shapes() {
    let mut runtime = runtime();
    assert_checks(
        &mut runtime,
        r#"
        const fails = (label, f) => {
          const result = attempt(f);
          check(result.error instanceof TypeError, `${label}: expected a TypeError, got ${describe(result.error ?? result.value)}`);
        };
        const shape = ops.op_shape({ name: "a", extra: 1, tags: undefined });
        check(shape.name === "a" && shape.tags.length === 0 && shape.size === null, `defaults apply: ${JSON.stringify(shape)}`);
        fails("missing field", () => ops.op_shape({}));
        fails("wrong field type", () => ops.op_shape({ name: 1 }));
        fails("array as struct", () => ops.op_shape(["a"]));
        fails("primitive as struct", () => ops.op_shape("a"));
        fails("null as struct", () => ops.op_shape(null));

        check(ops.op_command("Stop") === "Stop", "unit variant");
        check(ops.op_command({ Move: { x: 3 } }).Move.x === 3, "struct variant");
        check(ops.op_command({ Say: "hi" }).Say === "hi", "newtype variant");
        check(ops.op_command({ Pair: [1, 2] }).Pair.join() === "1,2", "tuple variant");
        fails("unknown variant", () => ops.op_command("Jump"));
        fails("two keys", () => ops.op_command({ Stop: null, Say: "x" }));
        fails("no keys", () => ops.op_command({}));
        fails("struct variant holding a number", () => ops.op_command({ Move: 1 }));
        fails("tuple variant too short", () => ops.op_command({ Pair: [1] }));
        fails("tuple variant with a hole", () => ops.op_command({ Pair: [1, , 3] }));
        fails("tuple variant led by a hole", () => ops.op_command({ Pair: [, 1] }));
        fails("number", () => ops.op_command(1));
        fails("symbol-keyed variant", () => ops.op_command({ [Symbol("Stop")]: null }));
        "#,
    );
}

/// `op_timer_sleep` turned any finite delay into a `Duration`, which panics
/// (aborting the process inside the op callback) past ~1.8e22 ms.
#[tokio::test]
async fn timers_accept_any_delay() {
    let mut runtime = runtime();
    assert_checks_async(
        &mut runtime,
        r#"
        (async () => {
          const ids = [];
          let id = 1000;
          for (const delay of [1e300, Number.MAX_VALUE, 2 ** 64 * 1000, Infinity, -Infinity, NaN, -1, 0]) {
            const sleeping = ops.op_timer_sleep(++id, delay);
            ids.push([id, delay, sleeping]);
          }
          for (const [id, delay, sleeping] of ids) {
            ops.op_timer_cancel(id);
            const fired = await sleeping;
            check(typeof fired === "boolean", `sleep(${delay}) settled with ${describe(fired)}`);
          }
        })()
        "#,
    )
    .await;
}
