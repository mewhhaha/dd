use super::*;

async fn deploy_memory_contract_worker(body: &str) -> RuntimeService {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy_with_config(
            "contract".to_string(),
            format!(
                "export default {{ async fetch(request, env) {{ const memory = env.STATE.get('entity'); {body} }} }};"
            ),
            DeployConfig {
                bindings: vec![DeployBinding::Memory {
                    binding: "STATE".to_string(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("transaction worker deploys");
    service
}

#[tokio::test]
#[serial]
async fn staged_values_and_deletes_remain_visible_before_and_after_commit() {
    let service = deploy_memory_contract_worker(
        r#"
const payload = new Uint8Array(64 * 1024).fill(7);
const before = await memory.atomic((tx) => {
  const putIsUndefined = tx.put("bytes", payload) === undefined;
  payload[0] = 99;
  tx.get("bytes")[1] = 100;
  tx.put("text", "first");
  tx.put("text", "last");
  tx.put("removed", 1);
  const deleted = [tx.delete("removed"), tx.delete("removed")];
  return {
    putIsUndefined, deleted,
    bytes: Array.from(tx.get("bytes").slice(0, 2)),
    text: tx.get("text"), keys: tx.list().map(({ key }) => key),
  };
});
const after = await memory.atomic((tx) => ({
  bytes: Array.from(tx.get("bytes").slice(0, 2)),
  text: tx.get("text"), keys: tx.list().map(({ key }) => key),
}));
return Response.json({ before, after });
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .expect("staged and committed reads succeed");
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).expect("json"),
        serde_json::json!({
            "before": {
                "putIsUndefined": true, "deleted": [true, false],
                "bytes": [7, 7], "text": "last", "keys": ["bytes", "text"]
            },
            "after": { "bytes": [7, 7], "text": "last", "keys": ["bytes", "text"] }
        })
    );
}

#[tokio::test]
#[serial]
async fn oversized_direct_buffers_leave_the_staged_transaction_usable() {
    let service = deploy_memory_contract_worker(
        r#"
const rejected = await memory.atomic((tx) => {
  tx.put("safe", "before");
  let rejected = false;
  try { tx.put("oversized", new Uint8Array(16 * 1024 * 1024)); }
  catch (error) { rejected = error.message.includes("staged data exceeded"); }
  tx.put("safe", "after");
  tx.put("empty", "");
  return rejected;
});
const stored = await memory.atomic((tx) => [tx.get("safe"), tx.get("empty"), tx.get("oversized")]);
return Response.json({ rejected, stored });
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::json!({
            "rejected": true, "stored": ["after", "", null]
        })
    );
}

#[tokio::test]
#[serial]
async fn unchanged_snapshots_keep_decoded_values_independent_and_refresh_after_writes() {
    let service = deploy_memory_contract_worker(
        r#"
await memory.atomic((tx) => tx.put("state", { count: 1, bytes: new Uint8Array([2, 3]) }));
const observed = [];
for (let index = 0; index < 4; index++) {
  if (index % 2 === 0) {
    for (let padding = 0; padding < 2; padding++) {
      await env.STATE.get(`padding-${padding}`).atomic(tx => tx.put("padding", "x".repeat(4 * 1024 * 1024)));
    }
  }
  observed.push(await memory.atomic((tx) => {
    const state = tx.get("state");
    const original = [state.count, ...state.bytes];
    state.count = 99;
    state.bytes[0] = 99;
    return original;
  }));
}
await memory.atomic((tx) => tx.put("state", { count: 4, bytes: new Uint8Array([5, 6]) }));
try {
  await memory.atomic((tx) => { tx.put("state", { count: 99 }); tx.delete("state"); throw new Error("rollback"); });
} catch (error) { if (!error.message.includes("rollback")) throw error; }
for (let index = 0; index < 2; index++) {
  observed.push(await memory.atomic((tx) => {
    const state = tx.get("state");
    return [state.count, ...state.bytes];
  }));
}
return Response.json(observed);
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::json!([
            [1, 2, 3],
            [1, 2, 3],
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [4, 5, 6]
        ])
    );
}

#[tokio::test]
#[serial]
async fn transactions_reuse_native_snapshots_for_large_values() {
    let service = deploy_memory_contract_worker(
        r#"
const payload = "p".repeat(1024 * 1024);
for (let index = 0; index < 10; index++) {
  await env.STATE.get(`budget-${index}`).atomic(tx => tx.put("payload", payload));
}
for (const index of [9, 9, 0]) {
  const length = await env.STATE.get(`budget-${index}`).atomic(tx => {
    if (tx.get("payload") !== payload) throw new Error("payload changed");
    return tx.get("payload").length;
  });
  if (length !== payload.length) throw new Error("wrong length");
}
const oversized = env.STATE.get("oversized-snapshot");
await oversized.atomic(tx => tx.put("payload", "q".repeat(9 * 1024 * 1024)));
for (let index = 0; index < 2; index++) {
  const length = await oversized.atomic(tx => tx.get("payload").length);
  if (length !== 9 * 1024 * 1024) throw new Error("oversized snapshot changed");
}
return new Response("ok");
"#,
    )
    .await;
    service.memory_store.set_profile_enabled(true);
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .unwrap();
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    assert_eq!(response.body, b"ok");
    let profile = service.memory_store.take_profile_snapshot_and_reset();
    assert_eq!(profile.op_snapshot.calls, 16);
    assert_eq!(
        profile.store_snapshot_cache_hit.calls, 5,
        "reads reuse shared native snapshots without an isolate byte-budget eviction"
    );
}

#[tokio::test]
#[serial]
async fn reused_snapshots_match_reloaded_unicode_keys_and_list_order() {
    let service = deploy_memory_contract_worker(
        r#"
await memory.atomic(tx => {
  tx.put("é", "composed");
  tx.put("e\u0301", "decomposed");
  tx.put("key-\ud800", "replacement");
});
const read = () => memory.atomic(tx => ({
  keys: tx.list().map(record => record.key),
  malformed: tx.get("key-\ud800"), canonical: tx.get("key-\ufffd"),
  prefix: tx.list({ prefix: "key-\ud800" }).map(record => record.key),
}));
const cached = await read();
for (let index = 0; index < 2; index++) {
  await env.STATE.get(`evict-${index}`).atomic(tx => tx.put("padding", "x".repeat(4 * 1024 * 1024)));
}
const reloaded = await read();
return Response.json({ cached, reloaded });
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .unwrap();
    let expected = serde_json::json!({
        "keys": ["e\u{301}", "é", "key-\u{fffd}"],
        "malformed": "replacement", "canonical": "replacement", "prefix": ["key-\u{fffd}"]
    });
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::json!({
            "cached": expected, "reloaded": expected
        })
    );
}

#[tokio::test]
#[serial]
async fn async_callbacks_are_rejected_before_their_body_runs() {
    let service = deploy_memory_contract_worker(
        r#"
let called = false;
let rejected = false;
try {
  await memory.atomic(async (tx) => { called = true; tx.put("value", 1); });
} catch (error) { rejected = error.message.includes("synchronous callback"); }
const value = await memory.atomic((tx) => tx.get("value"));
return Response.json({ called, rejected, value });
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .expect("request succeeds");
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).expect("json"),
        serde_json::json!({
            "called": false, "rejected": true, "value": null
        })
    );
}

#[tokio::test]
#[serial]
async fn returned_thenables_roll_back_writes_and_idempotency_results() {
    let service = deploy_memory_contract_worker(
        r#"
const cases = [Promise.resolve(1), { then() {} }, Object.assign(() => {}, { then() {} })];
const results = [];
for (let index = 0; index < cases.length; index++) {
  let calls = 0;
  let rejected = false;
  try {
    await memory.atomic((tx) => {
      calls++;
      tx.put("value", index);
      return cases[index];
    }, { idempotencyKey: `command-${index}` });
  } catch (error) { rejected = error.message.includes("synchronous"); }
  const value = await memory.atomic((tx) => tx.get("value"));
  const retry = await memory.atomic(() => "retry", { idempotencyKey: `command-${index}` });
  results.push({ calls, rejected, value, retry });
}
return Response.json(results);
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .expect("request succeeds");
    let results: Vec<Value> = serde_json::from_slice(&response.body).expect("json");
    assert_eq!(results.len(), 3);
    for result in results {
        assert_eq!(
            result,
            serde_json::json!({ "calls": 1, "rejected": true, "value": null, "retry": "retry" })
        );
    }
}

#[tokio::test]
#[serial]
async fn transaction_handles_cannot_be_used_after_the_callback_returns() {
    let service = deploy_memory_contract_worker(
        r#"
let escaped;
await memory.atomic((tx) => { escaped = tx; tx.put("value", 1); });
const rejected = [];
for (const operation of [() => escaped.get("value"), () => escaped.put("value", 2), () => escaped.delete("value"), () => escaped.list(), () => escaped.emit("event", {})]) {
  try { operation(); rejected.push(false); } catch { rejected.push(true); }
}
return Response.json({ rejected, value: await memory.atomic((tx) => tx.get("value")) });
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .expect("request succeeds");
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).expect("json"),
        serde_json::json!({
            "rejected": [true, true, true, true, true], "value": 1
        })
    );
}

#[tokio::test]
#[serial]
async fn concurrent_transactions_keep_each_entity_and_request_scope_independent() {
    let service = deploy_memory_contract_worker(
        r#"
const entities = Array.from({ length: 16 }, (_, index) => env.STATE.get(`entity-${index}`));
await Promise.all(entities.map((entity, index) => entity.atomic((tx) => tx.put("value", index))));
const values = await Promise.all(entities.map((entity) => entity.atomic((tx) => tx.get("value"))));
await Promise.all(entities.map(() => memory.atomic((tx) => tx.put("count", (tx.get("count") ?? 0) + 1))));
return Response.json({ values, count: await memory.atomic((tx) => tx.get("count")) });
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .expect("request succeeds");
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).expect("json"),
        serde_json::json!({
            "values": (0..16).collect::<Vec<_>>(), "count": 16
        })
    );
}

#[tokio::test]
#[serial]
async fn nested_transactions_fail_without_committing_the_outer_write() {
    let service = deploy_memory_contract_worker(
        r#"
let rejected = false;
try {
  await memory.atomic((tx) => {
    tx.put("value", 1);
    env.STATE.get("other").atomic(() => {});
  });
} catch (error) { rejected = error.message.includes("cannot be nested"); }
return Response.json({ rejected, value: await memory.atomic((tx) => tx.get("value")) });
"#,
    )
    .await;
    let response = service
        .invoke("contract".into(), test_invocation())
        .await
        .expect("request succeeds");
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).expect("json"),
        serde_json::json!({
            "rejected": true, "value": null
        })
    );
}

#[tokio::test]
#[serial]
async fn idempotent_results_preserve_cycles_and_shared_references() {
    let service = deploy_memory_contract_worker(
        r#"
const value = await memory.atomic(() => {
  const shared = { count: 1 };
  const result = { left: shared, right: shared, values: new Map() };
  result.self = result;
  result.values.set(result, shared);
  Object.defineProperty(result, "__proto__", { value: "stored", enumerable: true });
  return result;
}, { idempotencyKey: "cycle" });
return Response.json({ cycle: value.self === value, shared: value.left === value.right,
  map: value.values.get(value) === value.left, property: value.__proto__ });
"#,
    )
    .await;
    for _ in 0..2 {
        let response = service
            .invoke("contract".into(), test_invocation())
            .await
            .expect("cyclic command result succeeds");
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).expect("json"),
            serde_json::json!({
                "cycle": true, "shared": true, "map": true, "property": "stored"
            })
        );
    }
}

#[tokio::test]
#[serial]
async fn recording_an_idempotent_response_keeps_its_body_readable() {
    let service = deploy_memory_contract_worker(
        "return memory.atomic(() => new Response('committed body'), { idempotencyKey: 'response' });",
    ).await;
    for _ in 0..2 {
        let response = service
            .invoke("contract".into(), test_invocation())
            .await
            .expect("response command succeeds");
        assert_eq!(response.body, b"committed body");
    }
}
