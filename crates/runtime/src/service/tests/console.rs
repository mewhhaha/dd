use super::*;

const CONSOLE_WORKER: &str = r#"
console.log("loaded");
class Point { constructor() { this.x = 1; this.y = 2; } }
export default {
  async fetch() {
    const cyclic = { name: "loop" };
    cyclic.self = cyclic;
    const error = new TypeError("bad input", { cause: "upstream" });
    error.stack = "TypeError: bad input\n    at handler (worker.js:1:1)";
    console.log("plain", 42, -0, 10n, true, null, undefined, Symbol("tag"));
    console.log({ a: 1, b: "two", "c-d": [1, 2, , 4], nested: { deep: { deeper: { deepest: { gone: { past: 1 } } } } } });
    console.log(new Map([["k", { v: 1 }]]), new Set(["s"]), new Point(), [function named() {}, class Klass {}, () => {}]);
    console.log(cyclic);
    console.log("%s is %d years and %i%% done %o", "Ada", 36.5, 99.9, { ok: true });
    console.log(Object.create(null), new Uint8Array([1, 2]), Promise.resolve(7), new Proxy({ target: 1 }, { get() { throw new Error("trap ran"); } }));
    console.log({ get computed() { throw new Error("getter ran"); } }, new Date(0), /re/g, [new String("boxed")]);
    console.warn("careful");
    console.error(error);
    console.debug("details");
    console.group("group");
    console.info("inside\nlines");
    console.groupEnd();
    console.count();
    console.count();
    console.assert(1 === 2, "math");
    console.table([{ a: 1, b: "x" }, { a: 2 }]);
    const headers = new Headers([["x-dd", "1"]]);
    console.log(headers);
    return new Response("ok");
  },
};
"#;

#[tokio::test]
#[serial]
async fn worker_console_calls_are_formatted_and_tagged() {
    let service = test_service(RuntimeConfig::default()).await;
    let mut console = service.subscribe_console();
    service
        .deploy("console".into(), CONSOLE_WORKER.into())
        .await
        .expect("console worker deploys");
    let output = service
        .invoke(
            "console".into(),
            test_invocation_with_path("/", "console-request"),
        )
        .await
        .expect("console worker responds");
    assert_eq!(output.body, b"ok");

    let mut lines = Vec::new();
    while let Ok(Ok(line)) = timeout(Duration::from_millis(500), console.recv()).await {
        lines.push(line);
    }
    let messages = lines
        .iter()
        .map(|line| format!("{:?} {}", line.level, line.message))
        .collect::<Vec<_>>();
    let expected = [
        "Info loaded",
        "Info plain 42 -0 10n true null undefined Symbol(tag)",
        "Info {\n  a: 1,\n  b: 'two',\n  'c-d': [ 1, 2, <1 empty item>, 4 ],\n  nested: { deep: { deeper: { deepest: { gone: [Object] } } } }\n}",
        "Info Map(1) { 'k' => { v: 1 } } Set(1) { 's' } Point { x: 1, y: 2 } [ [Function: named], [class Klass], [Function (anonymous)] ]",
        "Info <ref *1> { name: 'loop', self: [Circular *1] }",
        "Info Ada is 36 years and 99% done { ok: true }",
        "Info [Object: null prototype] {} Uint8Array(2) [ 1, 2 ] Promise { 7 } { target: 1 }",
        "Info { computed: [Getter] } 1970-01-01T00:00:00.000Z /re/g [ [String: 'boxed'] ]",
        "Warn careful",
        "Error TypeError: bad input\n    at handler (worker.js:1:1) {\n  [cause]: 'upstream'\n}",
        "Debug details",
        "Info group",
        "Info   inside\n  lines",
        "Info default: 1",
        "Info default: 2",
        "Error Assertion failed: math",
        "Info ┌─────────┬───┬─────┐\n│ (index) │ a │ b   │\n├─────────┼───┼─────┤\n│ 0       │ 1 │ 'x' │\n│ 1       │ 2 │     │\n└─────────┴───┴─────┘",
        "Info Headers { 'x-dd': '1' }",
    ];
    assert_eq!(messages, expected);
    assert!(lines.iter().all(|line| line.worker == "console"));
    assert_eq!(
        lines[0].request_id, "",
        "module evaluation runs outside a request"
    );
    assert!(
        lines[1..]
            .iter()
            .all(|line| line.request_id == lines[1].request_id && !line.request_id.is_empty()),
        "every call in the handler carries its request"
    );
    service.shutdown().await.expect("runtime shuts down");
}

#[tokio::test]
#[serial]
async fn worker_console_survives_replaced_builtins() {
    let service = test_service(RuntimeConfig::default()).await;
    let mut console = service.subscribe_console();
    service
        .deploy(
            "hostile-console".into(),
            r#"
export default {
  async fetch() {
    const value = { list: [1, 2], map: new Map([[1, 2]]) };
    const rows = [{ a: 1 }];
    const boom = () => { throw new Error("sabotaged"); };
    const saved = [
      [Array.prototype, "push"], [Array.prototype, "join"], [Array.prototype, "map"],
      [Array.prototype, Symbol.iterator], [Object, "keys"], [Object, "getOwnPropertyDescriptor"],
      [Reflect, "ownKeys"], [String.prototype, "replaceAll"], [Map.prototype, "forEach"],
      [globalThis, "JSON"],
    ].map(([target, key]) => [target, key, target[key]]);
    Array.prototype.push = boom;
    Array.prototype.join = boom;
    Array.prototype.map = boom;
    Array.prototype[Symbol.iterator] = boom;
    Object.keys = boom;
    Object.getOwnPropertyDescriptor = boom;
    Reflect.ownKeys = boom;
    String.prototype.replaceAll = boom;
    Map.prototype.forEach = boom;
    globalThis.JSON = undefined;
    console.log(value, "%j", { j: 1 });
    console.table(rows);
    console.group("g");
    console.log("a\nb");
    for (let i = 0; i < saved.length; i++) saved[i][0][saved[i][1]] = saved[i][2];
    return new Response("ok");
  },
};
"#
            .into(),
        )
        .await
        .expect("worker deploys");
    let output = service
        .invoke("hostile-console".into(), test_invocation())
        .await
        .expect("worker responds");
    assert_eq!(output.body, b"ok");
    let mut messages = Vec::new();
    while let Ok(Ok(line)) = timeout(Duration::from_millis(500), console.recv()).await {
        messages.push(line.message);
    }
    assert_eq!(
        messages,
        [
            "{ list: [ 1, 2 ], map: Map(1) { 1 => 2 } } %j { j: 1 }",
            "┌─────────┬───┐\n│ (index) │ a │\n├─────────┼───┤\n│ 0       │ 1 │\n└─────────┴───┘",
            "g",
            "  a\n  b",
        ]
    );
    service.shutdown().await.expect("runtime shuts down");
}

#[tokio::test]
#[serial]
async fn worker_console_prints_huge_arrays_without_listing_every_index() {
    let service = test_service(RuntimeConfig::default()).await;
    let mut console = service.subscribe_console();
    service
        .deploy(
            "huge-console".into(),
            r#"
export default {
  fetch() {
    const huge = new Array(10_000_000).fill("x");
    huge.label = "named";
    console.log(huge);
    console.table(huge);
    return new Response("ok");
  },
};
"#
            .into(),
        )
        .await
        .expect("worker deploys");
    let output = service
        .invoke("huge-console".into(), test_invocation())
        .await
        .expect("worker responds");
    assert_eq!(output.body, b"ok");
    let log = timeout(Duration::from_secs(5), console.recv())
        .await
        .expect("console.log arrives")
        .expect("console open");
    assert!(
        log.message
            .ends_with("... 9999900 more items,\n  label: 'named'\n]"),
        "{}",
        log.message.chars().rev().take(120).collect::<String>()
    );
    let table = timeout(Duration::from_secs(5), console.recv())
        .await
        .expect("console.table arrives")
        .expect("console open");
    // A thousand rows outgrow one console line, which is cut at 16 KiB.
    assert!(
        table.message.starts_with("┌"),
        "{}",
        table.message.chars().take(80).collect::<String>()
    );
    assert!(
        table.message.ends_with(" more bytes]"),
        "table output is cut, not all 10M rows"
    );
    service.shutdown().await.expect("runtime shuts down");
}
