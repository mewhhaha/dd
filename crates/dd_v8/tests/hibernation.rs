use dd_v8::{
    HibernationCompression, JsRuntime, ModuleCode, ModuleLoader, ModuleSource, ModuleType, OpDecl,
    OpError, OpState, RuntimeOptions, WireValue, op_async,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Instant;
use tokio::sync::oneshot;

struct GateState {
    channels: HashMap<String, oneshot::Receiver<Result<String, String>>>,
}

async fn op_slow_io(state: Rc<RefCell<OpState>>, key: String) -> Result<String, OpError> {
    let rx = state
        .borrow_mut()
        .try_borrow_mut::<GateState>()
        .and_then(|g| g.channels.remove(&key));
    let Some(rx) = rx else {
        // Never resolves unless externally settled after hibernation.
        return std::future::pending().await;
    };
    match rx.await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(msg)) => Err(OpError::type_error(msg)),
        Err(_) => Err(OpError::new("gate sender dropped")),
    }
}

async fn op_slow_bytes(_state: Rc<RefCell<OpState>>, tag: String) -> Result<Vec<u8>, OpError> {
    let _ = tag;
    std::future::pending().await
}

fn test_ops() -> Vec<OpDecl> {
    vec![op_async!(op_slow_io), op_async!(op_slow_bytes)]
}

struct MapLoader(HashMap<String, String>);

impl ModuleLoader for MapLoader {
    fn resolve(&self, specifier: &str, _referrer: &str) -> Result<String, String> {
        Ok(specifier.to_string())
    }

    fn load(&self, name: &str, requested_type: ModuleType) -> Result<ModuleSource, String> {
        let src = self
            .0
            .get(name)
            .ok_or_else(|| format!("missing module {name}"))?;
        Ok(ModuleSource::new(
            requested_type,
            ModuleCode::String(src.clone()),
        ))
    }
}

#[tokio::test]
async fn hibernate_wake_multi_hop_async_closures_weakmap_and_modules() {
    let mut modules = HashMap::new();
    modules.insert(
        "file:///helper.js".to_string(),
        r#"
        export const prefix = "deck";
        export function combine(a, b, c) {
          return `${prefix}:${a}:${b}:${c}`;
        }
        "#
        .to_string(),
    );
    modules.insert(
        "file:///main.js".to_string(),
        r#"
        import { combine } from "file:///helper.js";
        export async function runWorkflow(ops, seed) {
          const token = { id: seed };
          const wm = new WeakMap();
          wm.set(token, { stage: 1, buf: new Uint8Array([7, 11, 13]) });
          let localCount = seed * 10;

          // First async I/O hop — isolate will hibernate to bytes and drop here!
          const first = await ops.op_slow_io("hop-1");
          localCount += 5;
          const meta1 = wm.get(token);

          // Second async I/O hop (returns binary Uint8Array via WireValue::Bytes)
          const raw = await ops.op_slow_bytes("hop-2");
          let sum = 0;
          for (let i = 0; i < raw.length; i++) sum += raw[i];

          // Third async I/O hop — will reject and be caught in JS try/catch!
          let caught = "none";
          try {
            await ops.op_slow_io("hop-3-err");
          } catch (err) {
            caught = `${err.name}:${err.message}`;
          }

          return combine(
            `${first}#${localCount}#${meta1.buf[2]}`,
            `bytes=${sum}`,
            caught,
          );
        }
        "#
        .to_string(),
    );
    let loader: Rc<dyn ModuleLoader> = Rc::new(MapLoader(modules));

    let mut rt1 = JsRuntime::new(RuntimeOptions {
        ops: test_ops(),
        module_loader: Some(Rc::clone(&loader)),
        ..Default::default()
    })
    .expect("create runtime 1");

    let mod_id = rt1
        .load_main_module("file:///main.js", None)
        .expect("load main module");
    rt1.evaluate_module(mod_id).await.expect("eval main module");
    let ns = rt1.module_namespace(mod_id).expect("namespace");
    let run_fn = rt1.get_property(&ns, "runWorkflow").expect("runWorkflow");

    let ops_val = rt1
        .execute_with_ops("<ops>", "return ops;")
        .expect("get ops object");
    let seed_val = rt1.to_js(&4i32).expect("seed");

    let promise1 = rt1
        .call_function(&run_fn, &[ops_val, seed_val])
        .expect("start workflow");
    rt1.drain_microtasks().expect("drain to hop 1");
    assert!(
        rt1.try_resolve_settled(&promise1)
            .expect("check pending")
            .is_none(),
        "workflow should be suspended at hop 1"
    );

    // ---- Sleep #1: hibernate to portable bytes and drop Runtime 1 completely ----
    let t_sleep1 = Instant::now();
    let hib1 = rt1.hibernate(&[&promise1]).expect("hibernate #1");
    let sleep1_ms = t_sleep1.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(hib1.pending_ops().len(), 1);
    assert_eq!(hib1.pending_ops()[0].op_name, "op_slow_io");
    assert_eq!(
        hib1.pending_ops()[0].args,
        vec![WireValue::String("hop-1".to_string())]
    );
    let ticket1_id = hib1.pending_ops()[0].id;
    let bytes1 = hib1.into_bytes();
    assert!(!bytes1.is_empty());

    // Tampered and mismatched-op payloads must be rejected cleanly.
    let mut tampered = bytes1.clone();
    let mid = tampered.len() / 2;
    tampered[mid] ^= 0x5A;
    assert!(
        JsRuntime::wake(
            RuntimeOptions {
                ops: test_ops(),
                ..Default::default()
            },
            &tampered,
        )
        .is_err(),
        "tampered bytes must fail SHA-256 verification"
    );
    assert!(
        JsRuntime::wake(RuntimeOptions::default(), &bytes1).is_err(),
        "mismatched op table must fail wake"
    );

    // ---- Wake #1 in brand-new Runtime 2 ----
    let t_wake1 = Instant::now();
    let woken2 = JsRuntime::wake(
        RuntimeOptions {
            ops: test_ops(),
            module_loader: Some(Rc::clone(&loader)),
            ..Default::default()
        },
        &bytes1,
    )
    .expect("wake #1");
    let wake1_ms = t_wake1.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(woken2.retained.len(), 1);
    assert_eq!(woken2.pending_ops.len(), 1);
    let mut rt2 = woken2.runtime;
    let promise2 = woken2.retained.into_iter().next().unwrap();

    // Settle hop #1 -> JS runs past first await and parks at op_slow_bytes("hop-2")!
    rt2.settle_pending_op(ticket1_id, Ok(WireValue::String("alpha".to_string())))
        .expect("settle hop 1");
    let tickets2 = rt2.pending_io_tickets();
    assert_eq!(tickets2.len(), 1);
    assert_eq!(tickets2[0].op_name, "op_slow_bytes");
    assert_eq!(
        tickets2[0].args,
        vec![WireValue::String("hop-2".to_string())]
    );
    let ticket2_id = tickets2[0].id;

    // ---- Sleep #2 -> Wake #2 in Runtime 3 ----
    let hib2 = rt2.hibernate(&[&promise2]).expect("hibernate #2");
    let bytes2 = hib2.into_bytes();
    let woken3 = JsRuntime::wake(
        RuntimeOptions {
            ops: test_ops(),
            module_loader: Some(Rc::clone(&loader)),
            ..Default::default()
        },
        &bytes2,
    )
    .expect("wake #2");
    let mut rt3 = woken3.runtime;
    let promise3 = woken3.retained.into_iter().next().unwrap();

    // Settle hop #2 with binary Uint8Array payload -> advances to hop #3!
    rt3.settle_pending_op(ticket2_id, Ok(WireValue::Bytes(vec![10, 20, 30, 40])))
        .expect("settle hop 2");
    let tickets3 = rt3.pending_io_tickets();
    assert_eq!(tickets3.len(), 1);
    assert_eq!(tickets3[0].op_name, "op_slow_io");
    assert_eq!(
        tickets3[0].args,
        vec![WireValue::String("hop-3-err".to_string())]
    );
    let ticket3_id = tickets3[0].id;

    // Settle hop #3 with a TypeError rejection -> caught in JS try/catch!
    rt3.settle_pending_op(ticket3_id, Err(OpError::type_error("upstream timeout")))
        .expect("settle hop 3 error");
    assert!(rt3.pending_io_tickets().is_empty());

    let final_val = rt3
        .try_resolve_settled(&promise3)
        .expect("resolve final promise")
        .expect("promise should now be fulfilled");
    let text = rt3.value_to_string(&final_val).expect("to_string");
    assert_eq!(
        text,
        "deck:alpha#45#13:bytes=100:TypeError:upstream timeout"
    );

    eprintln!(
        "[dd_v8 hibernation] bytes={} ({:.2} KiB), sleep={:.2}ms, wake={:.2}ms",
        bytes1.len(),
        bytes1.len() as f64 / 1024.0,
        sleep1_ms,
        wake1_ms
    );
}

#[tokio::test]
async fn hibernate_detaches_and_reattaches_live_futures() {
    let (tx, rx) = oneshot::channel::<Result<String, String>>();
    let mut rt1 = JsRuntime::new(RuntimeOptions {
        ops: test_ops(),
        ..Default::default()
    })
    .expect("create runtime");
    let mut channels = HashMap::new();
    channels.insert("live-io".to_string(), rx);
    rt1.op_state().borrow_mut().put(GateState { channels });

    let promise1 = rt1
        .execute_with_ops(
            "<live>",
            r#"
            return (async () => {
              const x = 40;
              const resp = await ops.op_slow_io("live-io");
              return `${x + 2}:${resp}`;
            })();
            "#,
        )
        .expect("start async script");
    rt1.drain_microtasks().expect("drain");

    let mut hib = rt1.hibernate(&[&promise1]).expect("hibernate");
    let detached = hib.take_detached_futures();
    assert_eq!(detached.len(), 1);
    assert_eq!(detached[0].op_name(), "op_slow_io");
    let bytes = hib.into_bytes();

    // Runtime 1 is completely dropped now; send the I/O response while asleep!
    tx.send(Ok("woke-up".to_string())).expect("send I/O reply");

    // Wake into Runtime 2, reattach the detached future, and resolve normally!
    let woken = JsRuntime::wake(
        RuntimeOptions {
            ops: test_ops(),
            ..Default::default()
        },
        &bytes,
    )
    .expect("wake");
    let mut rt2 = woken.runtime;
    let promise2 = woken.retained.into_iter().next().unwrap();
    for fut in detached {
        rt2.reattach_pending_op(fut).expect("reattach");
    }
    let resolved = rt2.resolve(promise2).await.expect("resolve");
    assert_eq!(
        rt2.value_to_string(&resolved).expect("stringify"),
        "42:woke-up"
    );
}

#[tokio::test]
async fn hibernate_zstd_compresses_sharply_and_v1_raw_remains_decodable() {
    let make_rt = || {
        let mut rt = JsRuntime::new(RuntimeOptions {
            ops: test_ops(),
            ..Default::default()
        })
        .expect("create runtime");
        let p = rt
            .execute_with_ops(
                "<cmp>",
                r#"
                return (async () => {
                  const arr = new Array(256).fill("dd-hibernation-compressible-payload");
                  const r = await ops.op_slow_io("cmp");
                  return `${arr.length}:${r}`;
                })();
                "#,
            )
            .expect("run");
        rt.drain_microtasks().expect("drain");
        (rt, p)
    };

    let (rt_raw, p_raw) = make_rt();
    let hib_raw = rt_raw
        .hibernate_with_compression(&[&p_raw], HibernationCompression::None)
        .expect("raw hibernate");
    let ticket_raw = hib_raw.pending_ops()[0].id;
    let raw_bytes = hib_raw.into_bytes();
    assert_eq!(&raw_bytes[8..12], &1u32.to_le_bytes());

    let (rt_zstd, p_zstd) = make_rt();
    let hib_zstd = rt_zstd
        .hibernate_with_compression(&[&p_zstd], HibernationCompression::Zstd { level: 1 })
        .expect("zstd hibernate");
    let ticket_zstd = hib_zstd.pending_ops()[0].id;
    let zstd_bytes = hib_zstd.into_bytes();
    assert_eq!(&zstd_bytes[8..12], &2u32.to_le_bytes());

    assert!(
        zstd_bytes.len() * 2 < raw_bytes.len(),
        "expected zstd ({} B) to be <50% of raw ({} B)",
        zstd_bytes.len(),
        raw_bytes.len()
    );

    // Both v1 raw and v2 zstd must wake cleanly.
    for (bytes, ticket_id, tag) in [
        (raw_bytes, ticket_raw, "raw"),
        (zstd_bytes, ticket_zstd, "zstd"),
    ] {
        let mut woken = JsRuntime::wake(
            RuntimeOptions {
                ops: test_ops(),
                ..Default::default()
            },
            &bytes,
        )
        .expect("wake");
        let p = woken.retained.into_iter().next().unwrap();
        woken
            .runtime
            .settle_pending_op(ticket_id, Ok(WireValue::String(tag.to_string())))
            .expect("settle");
        let val = woken
            .runtime
            .try_resolve_settled(&p)
            .expect("resolve")
            .expect("settled");
        assert_eq!(
            woken.runtime.value_to_string(&val).expect("str"),
            format!("256:{tag}")
        );
    }
}

#[tokio::test]
async fn wake_with_op_state_preserves_concurrent_in_flight_ops() {
    struct SharedLog(Vec<String>);

    async fn op_logged_gate(state: Rc<RefCell<OpState>>, key: String) -> Result<String, OpError> {
        let rx = state
            .borrow_mut()
            .try_borrow_mut::<GateState>()
            .and_then(|g| g.channels.remove(&key))
            .expect("missing gate");
        let val = rx.await.expect("gate send").expect("gate ok");
        state
            .borrow_mut()
            .borrow_mut::<SharedLog>()
            .0
            .push(format!("{key}={val}"));
        Ok(val)
    }

    let ops_fn = || vec![op_async!(op_logged_gate)];
    let (tx1, rx1) = oneshot::channel::<Result<String, String>>();
    let (tx2, rx2) = oneshot::channel::<Result<String, String>>();

    let mut rt1 = JsRuntime::new(RuntimeOptions {
        ops: ops_fn(),
        ..Default::default()
    })
    .expect("rt1");
    {
        let mut channels = HashMap::new();
        channels.insert("a".to_string(), rx1);
        channels.insert("b".to_string(), rx2);
        let os = rt1.op_state();
        let mut os = os.borrow_mut();
        os.put(GateState { channels });
        os.put(SharedLog(Vec::new()));
    }

    let p1 = rt1
        .execute_with_ops(
            "<par>",
            r#"
            return Promise.all([ops.op_logged_gate("a"), ops.op_logged_gate("b")])
              .then(([a, b]) => `${a}+${b}`);
            "#,
        )
        .expect("start");
    rt1.drain_microtasks().expect("drain");
    assert_eq!(rt1.pending_io_count(), 2);

    // Hibernate while BOTH ops are in-flight.
    let (bytes, mut detached, op_state, _) = rt1.hibernate(&[&p1]).expect("hib").into_parts();
    assert_eq!(detached.len(), 2);

    // Complete ONLY op "a" while asleep!
    tx1.send(Ok("first".to_string())).expect("send a");
    let first_fut = detached.remove(0);
    let (id1, res1) = first_fut.wait().await;

    // Wake reusing the EXACT SAME OpState and reattach op "b" (which still holds Rc<RefCell<OpState>>).
    let mut woken = JsRuntime::wake_with_op_state(
        RuntimeOptions {
            ops: ops_fn(),
            ..Default::default()
        },
        &bytes,
        Rc::clone(&op_state),
    )
    .expect("wake_with_op_state");
    let p2 = woken.retained.into_iter().next().unwrap();
    for rem in detached {
        woken.runtime.reattach_pending_op(rem).expect("reattach b");
    }
    woken
        .runtime
        .settle_pending_op(id1, res1)
        .expect("settle a");

    // Now complete op "b" AFTER wake — it must write into the exact same live OpState!
    tx2.send(Ok("second".to_string())).expect("send b");
    let resolved = woken.runtime.resolve(p2).await.expect("resolve");
    assert_eq!(
        woken.runtime.value_to_string(&resolved).expect("str"),
        "first+second"
    );
    let logged = woken
        .runtime
        .op_state()
        .borrow()
        .borrow::<SharedLog>()
        .0
        .clone();
    assert_eq!(logged, vec!["a=first".to_string(), "b=second".to_string()]);
}
