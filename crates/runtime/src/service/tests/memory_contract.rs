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
