use dd_v8::{
    JsBuffer, JsRuntime, ModuleCode, ModuleLoader, ModuleSource, ModuleType, OpDecl, OpError,
    OpState, RuntimeOptions, ToJsBuffer, op_async, op_sync,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Point {
    x: i32,
    y: i32,
    #[serde(default)]
    label: Option<String>,
}

fn op_add(_state: &mut OpState, a: u32, b: u32) -> u32 {
    a + b
}

fn op_greet(_state: &mut OpState, name: String, excited: bool) -> String {
    format!("hello {name}{}", if excited { "!" } else { "" })
}

fn op_count(state: &mut OpState) -> u32 {
    let count = state.borrow_mut::<u32>();
    *count += 1;
    *count
}

fn op_nothing(_state: &mut OpState) {}

fn op_point(_state: &mut OpState, point: Point) -> Point {
    Point {
        x: point.x * 2,
        y: point.y * 2,
        label: point.label.map(|label| label.to_uppercase()),
    }
}

fn op_sum_bytes(_state: &mut OpState, bytes: JsBuffer) -> u32 {
    bytes.iter().map(|byte| u32::from(*byte)).sum()
}

fn op_bytes(_state: &mut OpState, length: u32) -> ToJsBuffer {
    (0..length as u8).collect::<Vec<_>>().into()
}

fn op_fail(_state: &mut OpState, message: String) -> Result<u32, OpError> {
    Err(OpError::range_error(message))
}

fn op_pairs(_state: &mut OpState, pairs: Vec<(String, String)>) -> usize {
    pairs.len()
}

async fn op_ready(_state: Rc<RefCell<OpState>>, value: u32) -> u32 {
    value + 1
}

async fn op_sleep(_state: Rc<RefCell<OpState>>, millis: u32) {
    tokio::time::sleep(Duration::from_millis(u64::from(millis))).await;
}

async fn op_later(state: Rc<RefCell<OpState>>, millis: u32) -> u32 {
    tokio::time::sleep(Duration::from_millis(u64::from(millis))).await;
    *state.borrow().borrow::<u32>()
}

async fn op_reject(_state: Rc<RefCell<OpState>>, message: String) -> Result<(), OpError> {
    tokio::task::yield_now().await;
    Err(OpError::type_error(message))
}

fn test_ops() -> Vec<OpDecl> {
    vec![
        op_sync!(op_add),
        op_sync!(op_greet),
        op_sync!(op_count),
        op_sync!(op_nothing),
        op_sync!(op_point),
        op_sync!(op_sum_bytes),
        op_sync!(op_bytes),
        op_sync!(op_fail),
        op_sync!(op_pairs),
        op_async!(op_ready),
        op_async!(op_sleep),
        op_async!(op_later),
        op_async!(op_reject),
    ]
}

fn runtime() -> JsRuntime {
    let mut runtime = JsRuntime::new(RuntimeOptions {
        ops: test_ops(),
        ..Default::default()
    })
    .expect("runtime");
    runtime.op_state().borrow_mut().put(0u32);
    runtime
        .execute_with_ops("<ops>", "globalThis.ops = ops;")
        .expect("expose ops");
    runtime
}

fn string(runtime: &mut JsRuntime, value: dd_v8::v8::Global<dd_v8::v8::Value>) -> String {
    dd_v8::scope!(scope, runtime);
    let value = dd_v8::v8::Local::new(scope, value);
    value.to_rust_string_lossy(scope)
}

fn eval(runtime: &mut JsRuntime, source: &str) -> String {
    let value = runtime
        .execute_script("<test>", source)
        .expect("script runs");
    string(runtime, value)
}

async fn eval_async(runtime: &mut JsRuntime, source: &str) -> Result<String, dd_v8::Error> {
    let value = runtime.execute_script("<test>", source)?;
    let value = runtime.resolve(value).await?;
    Ok(string(runtime, value))
}

#[test]
fn scripts_return_values_and_report_exceptions() {
    let mut runtime = runtime();
    assert_eq!(eval(&mut runtime, "1 + 2"), "3");
    let error = runtime
        .execute_script(
            "file:///boom.js",
            "function boom() { throw new Error('boom'); }\nboom();",
        )
        .expect_err("script throws");
    assert!(
        error
            .message()
            .starts_with("Uncaught Error: boom\n    at boom (file:///boom.js:1:"),
        "{error}"
    );
    let error = runtime
        .execute_script("<test>", "throw 'plain'")
        .expect_err("script throws");
    assert_eq!(error.message(), "Uncaught plain");
    let error = runtime
        .execute_script("<test>", "let =")
        .expect_err("syntax error");
    assert!(
        error.message().starts_with("Uncaught SyntaxError"),
        "{error}"
    );
}

#[test]
fn sync_ops_convert_arguments_and_results() {
    let mut runtime = runtime();
    assert_eq!(eval(&mut runtime, "ops.op_add(2, 40)"), "42");
    assert_eq!(eval(&mut runtime, "ops.op_greet('dd', true)"), "hello dd!");
    assert_eq!(eval(&mut runtime, "ops.op_count() + ops.op_count()"), "3");
    assert_eq!(*runtime.op_state().borrow().borrow::<u32>(), 2);
    assert_eq!(eval(&mut runtime, "String(ops.op_nothing())"), "undefined");
    assert_eq!(
        eval(
            &mut runtime,
            "JSON.stringify(ops.op_point({ x: 1, y: 2, label: 'a' }))"
        ),
        r#"{"x":2,"y":4,"label":"A"}"#
    );
    assert_eq!(
        eval(
            &mut runtime,
            "JSON.stringify(ops.op_point({ x: 1, y: 2, label: undefined }))"
        ),
        r#"{"x":2,"y":4,"label":null}"#
    );
    assert_eq!(
        eval(
            &mut runtime,
            "ops.op_sum_bytes(new Uint8Array([1, 2, 3]).subarray(1))"
        ),
        "5"
    );
    assert_eq!(
        eval(
            &mut runtime,
            "ops.op_sum_bytes(new Uint16Array([256]).buffer)"
        ),
        "1"
    );
    assert_eq!(
        eval(
            &mut runtime,
            "const b = ops.op_bytes(4); `${b instanceof Uint8Array}:${b.join(',')}`"
        ),
        "true:0,1,2,3"
    );
    assert_eq!(
        eval(
            &mut runtime,
            "try { ops.op_fail('nope') } catch (e) { `${e.constructor.name}:${e.message}` }"
        ),
        "RangeError:nope"
    );
    assert_eq!(
        eval(
            &mut runtime,
            "try { ops.op_add('x', 1) } catch (e) { `${e.constructor.name}:${e.message}` }"
        ),
        "TypeError:op_add: argument 1: expected number, got string"
    );
    assert_eq!(
        eval(&mut runtime, "ops.op_pairs([['a', 'b'], ['c', 'd']])"),
        "2"
    );
    assert_eq!(eval(&mut runtime, "ops.op_add.name"), "op_add");
}

#[tokio::test]
async fn async_ops_resolve_through_the_event_loop() {
    let mut runtime = runtime();
    assert_eq!(
        eval_async(&mut runtime, "ops.op_ready(1)").await.unwrap(),
        "2"
    );
    assert_eq!(
        eval_async(
            &mut runtime,
            "(async () => { const order = []; const a = ops.op_sleep(20).then(() => order.push('slow')); \
             const b = ops.op_sleep(1).then(() => order.push('fast')); await Promise.all([a, b]); return order.join(); })()"
        )
        .await
        .unwrap(),
        "fast,slow"
    );
    runtime.op_state().borrow_mut().put(7u32);
    assert_eq!(
        eval_async(&mut runtime, "ops.op_later(5)").await.unwrap(),
        "7"
    );
    assert_eq!(
        eval_async(
            &mut runtime,
            "ops.op_reject('bad').catch((e) => `${e.constructor.name}:${e.message}`)"
        )
        .await
        .unwrap(),
        "TypeError:bad"
    );
    runtime.run_event_loop().await.expect("idle loop finishes");
}

#[tokio::test]
async fn unhandled_rejections_fail_the_event_loop() {
    let mut runtime = runtime();
    runtime
        .execute_script(
            "<test>",
            "Promise.reject(new Error('lost')); const p = Promise.reject(1); p.catch(() => {});",
        )
        .unwrap();
    let error = runtime
        .run_event_loop()
        .await
        .expect_err("unhandled rejection");
    assert!(
        error
            .message()
            .starts_with("Uncaught (in promise) Error: lost"),
        "{error}"
    );
    runtime
        .run_event_loop()
        .await
        .expect("rejection reported once");
}

#[test]
fn snapshots_keep_globals_and_ops() {
    static SNAPSHOT: OnceLock<Box<[u8]>> = OnceLock::new();
    let snapshot = SNAPSHOT.get_or_init(|| {
        let mut runtime = JsRuntime::new_for_snapshot(RuntimeOptions {
            ops: test_ops(),
            ..Default::default()
        })
        .expect("snapshot runtime");
        runtime
            .execute_with_ops(
                "<bootstrap>",
                "const { op_add } = ops; globalThis.add = (a, b) => op_add(a, b); globalThis.marker = 'from snapshot';",
            )
            .expect("bootstrap");
        runtime.snapshot().expect("snapshot")
    });
    let mut runtime = JsRuntime::new(RuntimeOptions {
        ops: test_ops(),
        startup_snapshot: Some(snapshot),
        ..Default::default()
    })
    .expect("restored runtime");
    assert_eq!(
        eval(&mut runtime, "`${marker}:${add(20, 22)}`"),
        "from snapshot:42"
    );
    runtime
        .execute_with_ops("<ops>", "globalThis.again = ops.op_greet('again', false);")
        .unwrap();
    assert_eq!(eval(&mut runtime, "again"), "hello again");
    drop(runtime);

    let mismatch = JsRuntime::new(RuntimeOptions {
        ops: vec![op_sync!(op_add)],
        startup_snapshot: Some(snapshot),
        ..Default::default()
    });
    assert!(mismatch.is_err());
}

struct MapLoader(HashMap<&'static str, (ModuleType, &'static str)>);

impl ModuleLoader for MapLoader {
    fn resolve(&self, specifier: &str, _referrer: &str) -> Result<String, String> {
        Ok(format!("mem:///{}", specifier.trim_start_matches("./")))
    }

    fn load(&self, name: &str, requested: ModuleType) -> Result<ModuleSource, String> {
        let (module_type, code) = self
            .0
            .get(name.trim_start_matches("mem:///"))
            .ok_or_else(|| format!("no module {name}"))?;
        if *module_type != requested {
            return Err(format!("{name} is {module_type}, not {requested}"));
        }
        Ok(ModuleSource::new(
            *module_type,
            ModuleCode::String(code.to_string()),
        ))
    }
}

fn module_runtime() -> JsRuntime {
    let loader = MapLoader(HashMap::from([
        (
            "dep.js",
            (
                ModuleType::JavaScript,
                "export const value = 40; export const meta = import.meta.url;",
            ),
        ),
        ("data.json", (ModuleType::Json, r#"{"answer": 2}"#)),
        ("note.txt", (ModuleType::Text, "a note")),
        ("blob.bin", (ModuleType::Bytes, "xyz")),
        (
            "lazy.js",
            (
                ModuleType::JavaScript,
                "await Promise.resolve(); export default 'lazy';",
            ),
        ),
        (
            "broken.js",
            (ModuleType::JavaScript, "throw new Error('broken module');"),
        ),
    ]));
    JsRuntime::new(RuntimeOptions {
        ops: test_ops(),
        module_loader: Some(Rc::new(loader)),
        ..Default::default()
    })
    .expect("runtime")
}

#[tokio::test]
async fn modules_load_imports_attributes_and_dynamic_imports() {
    let mut runtime = module_runtime();
    let main = runtime
        .load_main_module(
            "mem:///main.js",
            Some(
                r#"
                import { value, meta } from "./dep.js";
                import data from "./data.json" with { type: "json" };
                import note from "./note.txt" with { type: "text" };
                import blob from "./blob.bin" with { type: "bytes" };
                const lazy = await import("./lazy.js");
                globalThis.result = [value + data.answer, meta, note, blob.length, lazy.default, import.meta.main].join("|");
                "#
                .to_string(),
            ),
        )
        .expect("main module loads");
    runtime
        .evaluate_module(main)
        .await
        .expect("main module evaluates");
    assert_eq!(
        eval(&mut runtime, "result"),
        "42|mem:///dep.js|a note|3|lazy|true"
    );

    let side = runtime
        .load_side_module(
            "mem:///side.js",
            Some("export const main = import.meta.main;".to_string()),
        )
        .unwrap();
    runtime.evaluate_module(side).await.unwrap();
    let namespace = runtime.module_namespace(side).unwrap();
    {
        dd_v8::scope!(scope, runtime);
        let namespace = dd_v8::v8::Local::new(scope, namespace);
        let key = dd_v8::v8::String::new(scope, "main").unwrap();
        let value = namespace.get(scope, key.into()).unwrap();
        assert!(value.is_false());
    }

    assert_eq!(
        eval_async(
            &mut runtime,
            "import('./broken.js').catch((e) => e.message)"
        )
        .await
        .unwrap(),
        "broken module"
    );
    assert_eq!(
        eval_async(
            &mut runtime,
            "import('./missing.js').catch((e) => e.message)"
        )
        .await
        .unwrap(),
        "no module mem:///missing.js"
    );
}

#[tokio::test]
async fn module_errors_are_reported() {
    let mut runtime = module_runtime();
    let error = runtime
        .load_main_module("mem:///bad.js", Some("import './missing.js';".to_string()))
        .expect_err("missing import");
    assert_eq!(error.message(), "no module mem:///missing.js");

    let error = runtime
        .load_side_module("mem:///syntax.js", Some("export const = 1;".to_string()))
        .expect_err("syntax error");
    assert!(
        error.message().starts_with("Uncaught SyntaxError"),
        "{error}"
    );

    let broken = runtime.load_side_module("mem:///broken.js", None).unwrap();
    let error = runtime
        .evaluate_module(broken)
        .await
        .expect_err("evaluation throws");
    assert!(
        error.message().starts_with("Uncaught Error: broken module"),
        "{error}"
    );

    let stalled = runtime
        .load_side_module(
            "mem:///stalled.js",
            Some("await new Promise(() => {});".to_string()),
        )
        .unwrap();
    let error = runtime.evaluate_module(stalled).await.expect_err("stalled");
    assert_eq!(error.message(), "Top-level await promise never resolved");
}

#[tokio::test]
async fn foreground_tasks_finish_async_webassembly() {
    let mut runtime = runtime();
    // (module (func (export "add") (param i32 i32) (result i32) local.get 0 local.get 1 i32.add))
    let result = eval_async(
        &mut runtime,
        r#"
        const bytes = new Uint8Array([0,97,115,109,1,0,0,0,1,7,1,96,2,127,127,1,127,3,2,1,0,7,7,1,3,97,100,100,0,0,10,9,1,7,0,32,0,32,1,106,11]);
        WebAssembly.instantiate(bytes).then(({ instance }) => instance.exports.add(19, 23))
        "#,
    )
    .await
    .unwrap();
    assert_eq!(result, "42");
}

#[test]
fn heap_limit_terminates_execution() {
    let mut runtime = JsRuntime::new(RuntimeOptions {
        max_heap_bytes: 16 * 1024 * 1024,
        ..Default::default()
    })
    .unwrap();
    let handle = runtime.v8_isolate().thread_safe_handle();
    runtime.set_near_heap_limit_callback(move |current, _| {
        handle.terminate_execution();
        current + 16 * 1024 * 1024
    });
    let error = runtime
        .execute_script(
            "<oom>",
            "const keep = []; for (;;) keep.push(new Array(1e5).fill(1));",
        )
        .expect_err("terminated");
    assert!(error.is_terminated(), "{error}");
}

#[test]
fn queue_microtask_is_available() {
    let mut runtime = runtime();
    assert_eq!(eval(&mut runtime, "typeof queueMicrotask"), "function");
}

fn builtins_runtime() -> JsRuntime {
    let mut ops = dd_v8::builtins::ops();
    ops.push(op_sync!(op_fail_custom));
    let mut runtime = JsRuntime::new(RuntimeOptions {
        ops,
        ..Default::default()
    })
    .expect("runtime");
    runtime
        .execute_with_ops("<ops>", "globalThis.ops = ops;")
        .expect("expose ops");
    runtime
}

fn op_fail_custom(_state: &mut OpState) -> Result<(), OpError> {
    Err(OpError::custom("DemoError", "custom failure"))
}

#[test]
fn serialization_round_trips_values_and_branded_objects() {
    let mut runtime = builtins_runtime();
    let result = eval(
        &mut runtime,
        r#"
        const brand = Symbol.for("Deno.core.hostObject");
        class Point {
          constructor(x) { this.x = x; }
          [brand]() { return { type: "Point", x: this.x }; }
        }
        const value = { list: [1, "two", null], map: new Map([["k", 3n]]), bytes: new Uint8Array([7, 8]), point: new Point(4) };
        value.self = value;
        const bytes = ops.op_serialize(value, undefined, undefined, true);
        const back = ops.op_deserialize(bytes, undefined, undefined, { Point: (data) => new Point(data.x * 10) }, true);
        const cloned = ops.op_structured_clone({ when: new Date(5) });
        let unsupported = "none";
        try { ops.op_serialize({ f() {} }, undefined, undefined, false); } catch (e) { unsupported = e.constructor.name; }
        [
          bytes instanceof Uint8Array && bytes[0] === 0xff,
          back.list[1], back.map.get("k") === 3n, back.bytes[1], back.point instanceof Point, back.point.x, back.self === back,
          cloned.when.getTime(), unsupported,
        ].join(",")
        "#,
    );
    assert_eq!(result, "true,two,true,8,true,40,true,5,TypeError");
}

#[test]
fn custom_error_classes_use_registered_builders() {
    let mut runtime = builtins_runtime();
    assert_eq!(
        eval(
            &mut runtime,
            "try { ops.op_fail_custom() } catch (e) { `${e.name}:${e.message}:${e instanceof Error}` }"
        ),
        "DemoError:custom failure:true"
    );
    runtime
        .execute_script(
            "<test>",
            "class DemoError extends Error {}; globalThis.DemoError = DemoError; ops.op_register_error_builder('DemoError', (m) => new DemoError(m));",
        )
        .unwrap();
    assert_eq!(
        eval(
            &mut runtime,
            "try { ops.op_fail_custom() } catch (e) { `${e instanceof DemoError}:${e.message}` }"
        ),
        "true:custom failure"
    );
}

#[tokio::test]
async fn async_context_follows_continuations_and_timers_cancel() {
    let mut runtime = builtins_runtime();
    let result = eval_async(
        &mut runtime,
        r#"
        (async () => {
          ops.op_set_async_context("outer");
          const seen = [];
          const inner = (async () => {
            await null;
            seen.push(ops.op_get_async_context());
          })();
          ops.op_set_async_context(undefined);
          await inner;
          const cancelled = ops.op_timer_sleep(1, 60000);
          ops.op_timer_cancel(1);
          seen.push(await cancelled, await ops.op_timer_sleep(2, 1));
          return seen.join(",");
        })()
        "#,
    )
    .await
    .unwrap();
    assert_eq!(result, "outer,false,true");
}
