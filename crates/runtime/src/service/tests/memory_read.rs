use super::*;

async fn deploy_read_worker(body: &str) -> RuntimeService {
    let service = test_service(RuntimeConfig::default()).await;
    service.deploy_with_config(
        "read-contract".into(),
        format!("export default {{ async fetch(request, env) {{ const memory = env.STATE.get('entity'); {body} }} }};"),
        DeployConfig {
            bindings: vec![DeployBinding::Memory { binding: "STATE".into() }],
            ..DeployConfig::default()
        },
    ).await.unwrap();
    service
}

#[tokio::test]
#[serial]
async fn reads_proceed_while_the_entity_is_leased_and_do_not_commit() {
    let service = deploy_read_worker(
        "return Response.json(await memory.read(snapshot => ({ value: snapshot.get('missing'), keys: snapshot.list() })));",
    ).await;
    let namespace = crate::memory::worker_namespace("read-contract", "STATE");
    let lease = service
        .memory_store
        .acquire_lease(&namespace, "entity")
        .await
        .unwrap();
    service.memory_store.set_profile_enabled(true);
    let before = service
        .memory_store
        .state_performance_snapshot()
        .committed_commands;
    let response = timeout(
        Duration::from_secs(5),
        service.invoke("read-contract".into(), test_invocation()),
    )
    .await
    .expect("read must not wait for the entity lease")
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::json!({ "value": null, "keys": [] })
    );
    let profile = service.memory_store.take_profile_snapshot_and_reset();
    assert_eq!(profile.store_lease.calls, 0);
    assert_eq!(profile.op_apply_batch.calls, 0);
    assert_eq!(
        service
            .memory_store
            .state_performance_snapshot()
            .committed_commands,
        before
    );
    drop(lease);
}

#[tokio::test]
#[serial]
async fn captured_reads_stay_consistent_when_a_write_commits_before_the_callback() {
    let service = deploy_read_worker(
        r#"
await memory.atomic(tx => { tx.put("a", 1); tx.put("b", 1); });
const begin = Deno.core.ops.op_memory_read_begin;
let captured;
try {
  Deno.core.ops.op_memory_read_begin = async (...args) => {
    const snapshot = await begin(...args);
    await memory.atomic(tx => { tx.put("a", 2); tx.put("b", 2); tx.put("c", 2); });
    return snapshot;
  };
  captured = await memory.read(snapshot => [snapshot.get("a"), snapshot.get("b"), snapshot.list()]);
} finally {
  Deno.core.ops.op_memory_read_begin = begin;
}
const committed = await memory.read(snapshot => snapshot.list());
return Response.json({ captured, committed });
"#,
    )
    .await;
    let response = service
        .invoke("read-contract".into(), test_invocation())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::json!({
            "captured": [1, 1, [{"key":"a","value":1}, {"key":"b","value":1}]],
            "committed": [{"key":"a","value":2}, {"key":"b","value":2}, {"key":"c","value":2}]
        })
    );
}

#[tokio::test]
#[serial]
async fn reads_keep_values_independent_and_share_atomic_listing_rules() {
    let service = deploy_read_worker(r#"
const keys = ["é", "e\u0301", "E", "empty", "bytes", "gone", "object"];
await memory.atomic(tx => {
  for (const key of keys) tx.put(key, key);
  tx.put("empty", "");
  tx.put("bytes", new Uint8Array([7, 8]));
  tx.put("object", { count: 3 });
  tx.delete("gone");
});
const expected = await memory.atomic(tx => [tx.list(), tx.list({ limit: 3 }), tx.list({ prefix: "e" })]);
const observed = await memory.read(snapshot => {
  snapshot.get("bytes")[0] = 99;
  snapshot.get("object").count = 99;
  return [snapshot.list(), snapshot.list({ limit: 3 }), snapshot.list({ prefix: "e" })];
});
if (JSON.stringify(expected) !== JSON.stringify(observed)) throw new Error("read and atomic listing differ");
return Response.json(await memory.read(snapshot => [snapshot.get("empty"), snapshot.get("gone"), Array.from(snapshot.get("bytes")), snapshot.get("object")]));
"#).await;
    let response = service
        .invoke("read-contract".into(), test_invocation())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::json!(["", null, [7,8], {"count":3}])
    );
}

#[tokio::test]
#[serial]
async fn reads_reject_mutation_async_nesting_and_escaped_snapshot_access() {
    let service = deploy_read_worker(
        r#"
const errors = [];
async function rejects(action) {
  try { await action(); } catch (error) { errors.push(error.message); return; }
  throw new Error("operation unexpectedly succeeded");
}
let escaped;
const surface = await memory.read(snapshot => {
  escaped = snapshot;
  return Object.keys(snapshot).sort();
});
await rejects(() => escaped.get("count"));
await rejects(() => escaped.list());
await rejects(() => memory.read(async snapshot => snapshot.get("count")));
await rejects(() => memory.read(() => Promise.resolve(1)));
await rejects(() => memory.read(() => ({ then() {} })));
await rejects(() => memory.read(snapshot => snapshot.put("count", 1)));
await rejects(() => memory.read(snapshot => snapshot.emit("audit", {})));
await rejects(() => memory.read(() => memory.atomic(() => 1)));
await rejects(() => memory.atomic(() => memory.read(() => 1)));
await rejects(() => memory.read(() => memory.read(() => 1)));
await rejects(() => memory.read(() => { throw new Error("callback failed"); }));
const valid = await memory.read(snapshot => snapshot.get("count"));
return Response.json({ surface, errors, valid });
"#,
    )
    .await;
    let response = service
        .invoke("read-contract".into(), test_invocation())
        .await
        .unwrap();
    let result: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(result["surface"], serde_json::json!(["get", "list"]));
    let errors = result["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 11);
    for error in &errors[0..2] {
        assert!(
            error
                .as_str()
                .unwrap()
                .contains("outside its synchronous callback")
        );
    }
    for error in &errors[2..5] {
        assert!(error.as_str().unwrap().contains("synchronous"));
    }
    for error in &errors[7..10] {
        assert!(error.as_str().unwrap().contains("nested"));
    }
    assert_eq!(result["valid"], Value::Null);
}

#[tokio::test]
#[serial]
async fn concurrent_reads_never_tear_multi_key_commits_or_regress_acknowledged_writes() {
    let service = deploy_read_worker(r#"
if (request.method === "POST") {
  const expected = await memory.atomic(tx => {
    const count = (tx.get("a") ?? 0) + 1;
    tx.put("a", count); tx.put("b", count);
    return count;
  });
  const observed = await memory.read(snapshot => [snapshot.get("a"), snapshot.get("b")]);
  if (observed[0] < expected || observed[0] !== observed[1]) throw new Error("acknowledged write not visible");
  return Response.json(expected);
}
return Response.json(await memory.read(snapshot => {
  const a = snapshot.get("a") ?? 0, b = snapshot.get("b") ?? 0;
  if (a !== b) throw new Error(`torn read: ${a} vs ${b}`);
  return a;
}));
"#).await;
    for cache_entries in [4096, 0] {
        let mut memory = service.memory_store.clone();
        memory.set_snapshot_cache_limits(cache_entries, 64 * 1024 * 1024);
        let mut tasks = tokio::task::JoinSet::new();
        for caller in 0..8 {
            let service = service.clone();
            tasks.spawn(async move {
                let mut previous = 0;
                for _ in 0..12 {
                    let mut invocation = test_invocation();
                    invocation.method = if caller < 2 { "POST" } else { "GET" }.into();
                    let response = service
                        .invoke("read-contract".into(), invocation)
                        .await
                        .unwrap();
                    assert_eq!(
                        response.status,
                        200,
                        "{}",
                        String::from_utf8_lossy(&response.body)
                    );
                    let count: u64 = serde_json::from_slice(&response.body).unwrap();
                    assert!(
                        count >= previous,
                        "sequential reads regressed from {previous} to {count}"
                    );
                    previous = count;
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }
}

#[tokio::test]
#[serial]
async fn request_completion_and_cancellation_release_unclosed_read_handles() {
    let service = test_service(RuntimeConfig {
        min_isolates: 1,
        max_isolates: 1,
        max_inflight_per_isolate: 1,
        ..RuntimeConfig::default()
    })
    .await;
    service
        .deploy_with_config(
            "read-cleanup".into(),
            r#"
let captured = 0;
export default { async fetch(request, env) {
  const path = new URL(request.url).pathname;
  if (path === "/probe") {
    return Response.json(Deno.core.ops.op_memory_read_get(captured, "missing").ok);
  }
  const begin = Deno.core.ops.op_memory_read_begin;
  const close = Deno.core.ops.op_memory_read_close;
  try {
    Deno.core.ops.op_memory_read_begin = async (...args) => {
      const result = await begin(...args);
      captured = result.handle;
      return result;
    };
    Deno.core.ops.op_memory_read_close = () => {};
    await env.STATE.get("entity").read(snapshot => snapshot.get("missing"));
  } finally {
    Deno.core.ops.op_memory_read_begin = begin;
    Deno.core.ops.op_memory_read_close = close;
  }
  if (path === "/stream") {
    return new Response(new ReadableStream({ start(controller) { controller.enqueue("ready"); } }));
  }
  return Response.json(captured);
} };
"#
            .into(),
            DeployConfig {
                bindings: vec![DeployBinding::Memory {
                    binding: "STATE".into(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .unwrap();
    let response = service
        .invoke("read-cleanup".into(), test_invocation())
        .await
        .unwrap();
    assert!(serde_json::from_slice::<u32>(&response.body).unwrap() > 0);
    let probe = service
        .invoke(
            "read-cleanup".into(),
            test_invocation_with_path("/probe", "complete-probe"),
        )
        .await
        .unwrap();
    assert_eq!(probe.body, b"false");
    let mut stream = service
        .invoke_stream(
            "read-cleanup".into(),
            test_invocation_with_path("/stream", "read-stream"),
        )
        .await
        .unwrap();
    let ready = timeout(Duration::from_secs(3), stream.body.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ready.as_ref(), b"ready");
    drop(stream);
    let probe = timeout(
        Duration::from_secs(3),
        service.invoke(
            "read-cleanup".into(),
            test_invocation_with_path("/probe", "cancel-probe"),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(probe.body, b"false");
}

#[tokio::test]
#[serial]
async fn functional_commands_commit_changes_once_and_reject_async_transitions() {
    let service = test_service(RuntimeConfig::default()).await;
    let wrapper = include_str!("../../../../../packages/dd-vite/src/memory.js");
    let worker = r#"
export default { async fetch(request, env) {
  const memory = env.STATE.get("entity");
  await memory.atomic(tx => tx.put("removed", 1));
  let calls = 0;
  const increment = memoryCommand(memory, (snapshot, { by }) => {
    calls++;
    if (snapshot.put !== undefined) throw new Error("transition can mutate snapshot");
    const count = (snapshot.get("count") ?? 0) + by;
    return { writes: [{ key: "count", value: count }], deletes: ["removed"], result: count };
  });
  const first = await increment({ by: 2 }, { idempotencyKey: "once" });
  const replay = await increment({ by: 2 }, { idempotencyKey: "once" });
  let rejected = 0;
  try { memoryCommand(memory, async () => ({ writes: [], result: 1 })); } catch { rejected++; }
  try { await memoryCommand(memory, () => Promise.resolve({ writes: [], result: 1 }))(); } catch { rejected++; }
  return Response.json({ first, replay, calls, rejected, stored: await memory.read(snapshot => snapshot.list()) });
} };
"#;
    service
        .deploy_with_config(
            "functional".into(),
            format!("{wrapper}\n{worker}"),
            DeployConfig {
                bindings: vec![DeployBinding::Memory {
                    binding: "STATE".into(),
                }],
                ..DeployConfig::default()
            },
        )
        .await
        .unwrap();
    let response = service
        .invoke("functional".into(), test_invocation())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap(),
        serde_json::json!({
            "first": 2, "replay": 2, "calls": 1, "rejected": 2, "stored": [{"key":"count","value":2}]
        })
    );
}
