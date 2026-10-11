//! Fixed request workload for marginal instruction-count comparisons.

use common::{DeployBinding, DeployConfig, WorkerInvocation};
use runtime::{RuntimeConfig, RuntimeService, RuntimeServiceConfig, RuntimeStorageConfig};
use std::sync::Arc;
use std::time::Instant;

const HELLO: &str = r#"export default { async fetch() { return new Response("ok"); } };"#;
const HEADERS: &str = r#"export default { async fetch(request) { const u = new URL(request.url); return new Response(u.pathname, { headers: { "content-type": "text/plain", "x-a": "1" } }); } };"#;
const KV: &str = r#"export default { async fetch(request, env) { await env.KV.put("k", "v"); return new Response(String(await env.KV.get("k"))); } };"#;
const MEM: &str = r#"
function rw(tx) { const v = Number(tx.get("payload")?.value ?? 0); tx.put("payload", String(v + 1)); return v; }
export default { async fetch(request, env) {
  const id = env.M.idFromName("k" + (new URL(request.url).searchParams.get("i") % 64));
  return new Response(String(await env.M.get(id).atomic(rw)));
} };"#;

#[tokio::main]
async fn main() {
    let kind = std::env::var("REQ_KIND").unwrap_or("hello".into());
    let n: usize = std::env::var("REQ_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    let conc: usize = std::env::var("REQ_CONC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let root = std::env::temp_dir().join(format!("dd-reqmicro-{}", std::process::id()));
    std::fs::create_dir_all(root.join("store")).unwrap();
    let storage = RuntimeStorageConfig {
        store_dir: root.join("store"),
        ..RuntimeStorageConfig::default()
    };
    let runtime = RuntimeConfig {
        min_isolates: 0,
        max_isolates_per_worker: 2,
        max_inflight_per_isolate: 4,
        ..RuntimeConfig::default()
    };
    let service = Arc::new(
        RuntimeService::start(RuntimeServiceConfig { runtime, storage })
            .await
            .unwrap(),
    );
    let (src, bindings) = match kind.as_str() {
        "hello" => (HELLO, vec![]),
        "headers" => (HEADERS, vec![]),
        "kv" => (
            KV,
            vec![DeployBinding::Kv {
                binding: "KV".into(),
            }],
        ),
        "mem" => (
            MEM,
            vec![DeployBinding::Memory {
                binding: "M".into(),
            }],
        ),
        _ => panic!("kind"),
    };
    service
        .deploy_with_config(
            "w".to_string(),
            src.to_string(),
            DeployConfig {
                bindings,
                ..DeployConfig::default()
            },
        )
        .await
        .unwrap();
    let t = Instant::now();
    let mut handles = Vec::new();
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for _ in 0..conc {
        let service = service.clone();
        let next = next.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let out = service
                    .invoke(
                        "w".to_string(),
                        WorkerInvocation {
                            method: "GET".into(),
                            url: format!("http://worker/p?i={i}"),
                            headers: vec![],
                            body: vec![],
                            request_id: format!("r-{i}"),
                        },
                    )
                    .await
                    .expect("invoke");
                assert_eq!(out.status, 200, "{}", String::from_utf8_lossy(&out.body));
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    eprintln!("{kind} n={n} elapsed={:?}", t.elapsed());
    let _ = std::fs::remove_dir_all(&root);
    std::process::exit(0);
}
