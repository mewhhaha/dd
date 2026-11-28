use super::*;
use crate::IoHibernationStorage;
use tokio::sync::oneshot;

fn hibernation_test_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("dd-{tag}-{}", Uuid::new_v4()))
}

async fn spawn_gated_http_server(
    responses: Vec<(&'static str, &'static str)>,
) -> (
    std::net::SocketAddr,
    Vec<(oneshot::Receiver<()>, oneshot::Sender<()>)>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gated test server");
    let addr = listener.local_addr().expect("local addr");
    let mut gates = Vec::with_capacity(responses.len());
    let mut server_gates = Vec::with_capacity(responses.len());
    for (expected_path, body) in responses {
        let (arrived_tx, arrived_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        gates.push((arrived_rx, release_tx));
        server_gates.push((expected_path, body, arrived_tx, release_rx));
    }

    let handle = tokio::spawn(async move {
        let mut conns = Vec::new();
        for (expected_path, body, arrived_tx, release_rx) in server_gates {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            conns.push(tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.contains(expected_path),
                    "expected request path {expected_path}, got: {req}"
                );
                let _ = arrived_tx.send(());
                if release_rx.await.is_err() {
                    return;
                }
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.shutdown().await;
            }));
        }
        for c in conns {
            let _ = c.await;
        }
    });

    (addr, gates, handle)
}

async fn wait_for_stats(
    service: &RuntimeService,
    worker: &str,
    predicate: impl Fn(&crate::WorkerStats) -> bool,
) -> crate::WorkerStats {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(stats) = service.stats(worker.to_string()).await
            && predicate(&stats)
        {
            return stats;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for worker {worker} stats condition; last={:?}",
            service.stats(worker.to_string()).await
        );
        sleep(Duration::from_millis(15)).await;
    }
}

#[tokio::test]
async fn automatic_io_hibernation_sleeps_and_wakes_on_slow_fetch_in_memory() {
    let (addr, mut gates, server) =
        spawn_gated_http_server(vec![("/slow", "upstream-payload")]).await;
    let (arrived_rx, release_tx) = gates.remove(0);

    let service = test_service(RuntimeConfig {
        max_global_isolates: 4,
        max_isolates_per_worker: 2,
        io_hibernate_after: Some(Duration::from_millis(40)),
        io_hibernate_storage: IoHibernationStorage::Memory,
        ..RuntimeConfig::default()
    })
    .await;

    let worker = "auto-hib-mem";
    let source = format!(
        r#"
        export default {{
          async fetch(request) {{
            const token = {{ path: new URL(request.url).pathname }};
            const wm = new WeakMap();
            wm.set(token, {{ tag: "auto-sleep", bytes: new Uint8Array([4, 8, 15]) }});
            const res = await fetch("http://{addr}/slow");
            const body = await res.text();
            const saved = wm.get(token);
            return new Response(`${{saved.tag}}|${{saved.bytes[2]}}|${{token.path}}|${{body}}`, {{
              status: 201,
            }});
          }},
        }};
        "#
    );
    service
        .deploy_with_config(
            worker.to_string(),
            source,
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{addr}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy");

    let svc_invoke = service.clone();
    let invoke_task = tokio::spawn(async move {
        svc_invoke
            .invoke(
                worker.to_string(),
                test_invocation_with_path("/go", "req-auto-mem"),
            )
            .await
    });

    timeout(Duration::from_secs(5), arrived_rx)
        .await
        .expect("fetch should reach server")
        .expect("arrived");

    // While server holds response open, isolate must automatically hibernate!
    let slept_stats = wait_for_stats(&service, worker, |s| {
        s.hibernated_isolates == 1 && s.hibernate_count >= 1
    })
    .await;
    assert!(
        slept_stats.hibernated_bytes > 0 && slept_stats.hibernated_bytes < 2 * 1024 * 1024,
        "expected zstd-compressed hibernation image < 2 MiB, got {} bytes",
        slept_stats.hibernated_bytes
    );

    // Release upstream response -> isolate must transparently wake and finish!
    release_tx.send(()).expect("release slow fetch");
    let output = timeout(Duration::from_secs(10), invoke_task)
        .await
        .expect("invoke join timeout")
        .expect("invoke join")
        .expect("invoke result");
    assert_eq!(output.status, 201);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        "auto-sleep|15|/go|upstream-payload"
    );

    let woke_stats = wait_for_stats(&service, worker, |s| {
        s.hibernated_isolates == 0 && s.wake_count >= 1
    })
    .await;
    assert_eq!(woke_stats.hibernate_count, woke_stats.wake_count);

    server.await.expect("server clean");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn automatic_io_hibernation_spills_to_disk_and_cleans_up_on_wake() {
    let (addr, mut gates, server) =
        spawn_gated_http_server(vec![("/disk-hop", "woken-from-nvme")]).await;
    let (arrived_rx, release_tx) = gates.remove(0);

    let store_dir = hibernation_test_dir("hibernation-disk");
    let service = RuntimeService::start(RuntimeServiceConfig {
        runtime: RuntimeConfig {
            max_global_isolates: 4,
            max_isolates_per_worker: 2,
            io_hibernate_after: Some(Duration::from_millis(40)),
            io_hibernate_storage: IoHibernationStorage::Disk,
            ..RuntimeConfig::default()
        },
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            ..RuntimeStorageConfig::default()
        },
    })
    .await
    .expect("start disk-hibernation service");

    let worker = "auto-hib-disk";
    let source = format!(
        r#"
        export default {{
          async fetch() {{
            const res = await fetch("http://{addr}/disk-hop");
            return new Response("disk:" + await res.text());
          }},
        }};
        "#
    );
    service
        .deploy_with_config(
            worker.to_string(),
            source,
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{addr}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy");

    let svc_invoke = service.clone();
    let invoke_task = tokio::spawn(async move {
        svc_invoke
            .invoke(
                worker.to_string(),
                test_invocation_with_path("/", "req-auto-disk"),
            )
            .await
    });

    timeout(Duration::from_secs(5), arrived_rx)
        .await
        .expect("fetch arrived")
        .expect("arrived");

    let _slept = wait_for_stats(&service, worker, |s| s.hibernated_isolates == 1).await;
    let hib_dir = store_dir.join("hibernation");
    let ddhb_files_while_asleep: Vec<_> = std::fs::read_dir(&hib_dir)
        .expect("hibernation dir should exist")
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "ddhb"))
        .collect();
    assert_eq!(
        ddhb_files_while_asleep.len(),
        1,
        "expected exactly 1 spilled .ddhb image on disk while asleep"
    );

    release_tx.send(()).expect("release disk hop");
    let output = timeout(Duration::from_secs(10), invoke_task)
        .await
        .expect("invoke timeout")
        .expect("invoke join")
        .expect("invoke ok");
    assert_eq!(output.status, 200);
    assert_eq!(
        String::from_utf8(output.body).expect("utf8"),
        "disk:woken-from-nvme"
    );

    let _woke = wait_for_stats(&service, worker, |s| {
        s.hibernated_isolates == 0 && s.wake_count >= 1
    })
    .await;
    let ddhb_files_after_wake: Vec<_> = std::fs::read_dir(&hib_dir)
        .expect("hibernation dir readable")
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "ddhb"))
        .collect();
    assert!(
        ddhb_files_after_wake.is_empty(),
        "spilled .ddhb file must be unlinked immediately on wake"
    );

    server.await.expect("server clean");
    service.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_dir_all(store_dir);
}

#[tokio::test]
async fn automatic_io_hibernation_handles_concurrent_promise_all_across_two_sleep_cycles() {
    let (addr, mut gates, server) =
        spawn_gated_http_server(vec![("/a", "alpha"), ("/b", "beta")]).await;
    let (arrived_a, release_a) = gates.remove(0);
    let (arrived_b, release_b) = gates.remove(0);

    let service = test_service(RuntimeConfig {
        max_global_isolates: 4,
        max_isolates_per_worker: 1,
        io_hibernate_after: Some(Duration::from_millis(40)),
        io_hibernate_storage: IoHibernationStorage::Memory,
        ..RuntimeConfig::default()
    })
    .await;

    let worker = "auto-hib-par";
    let source = format!(
        r#"
        export default {{
          async fetch() {{
            const [ra, rb] = await Promise.all([
              fetch("http://{addr}/a").then((r) => r.text()),
              fetch("http://{addr}/b").then((r) => r.text()),
            ]);
            return new Response(`${{ra}}+${{rb}}`);
          }},
        }};
        "#
    );
    service
        .deploy_with_config(
            worker.to_string(),
            source,
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{addr}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy");

    let svc_invoke = service.clone();
    let invoke_task = tokio::spawn(async move {
        svc_invoke
            .invoke(
                worker.to_string(),
                test_invocation_with_path("/", "req-par-hib"),
            )
            .await
    });

    timeout(Duration::from_secs(5), arrived_a)
        .await
        .expect("a arrived")
        .expect("a");
    timeout(Duration::from_secs(5), arrived_b)
        .await
        .expect("b arrived")
        .expect("b");

    // Sleep #1 while both /a and /b are parked.
    let _s1 = wait_for_stats(&service, worker, |s| {
        s.hibernated_isolates == 1 && s.hibernate_count >= 1
    })
    .await;

    // Release ONLY /a -> isolate wakes, settles /a, keeps /b attached on the same OpState,
    // then goes back to sleep (#2) waiting for /b!
    release_a.send(()).expect("release a");
    let _s2 = wait_for_stats(&service, worker, |s| {
        s.hibernated_isolates == 1 && s.hibernate_count >= 2 && s.wake_count >= 1
    })
    .await;

    // Release /b -> isolate wakes a second time and completes Promise.all!
    release_b.send(()).expect("release b");
    let output = timeout(Duration::from_secs(10), invoke_task)
        .await
        .expect("invoke timeout")
        .expect("invoke join")
        .expect("invoke ok");
    assert_eq!(output.status, 200);
    assert_eq!(String::from_utf8(output.body).expect("utf8"), "alpha+beta");

    let final_stats = wait_for_stats(&service, worker, |s| {
        s.hibernated_isolates == 0 && s.wake_count >= 2
    })
    .await;
    assert_eq!(final_stats.hibernate_count, final_stats.wake_count);

    server.await.expect("server clean");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn incoming_request_wakes_hibernated_isolate_and_undeploy_cleans_disk_image() {
    // Part A: incoming request wakes a sleeping single-isolate worker without dropping slow fetch.
    let (addr, mut gates, server) = spawn_gated_http_server(vec![("/hold", "held-ok")]).await;
    let (arrived_rx, release_tx) = gates.remove(0);

    let service = test_service(RuntimeConfig {
        max_global_isolates: 4,
        max_isolates_per_worker: 1,
        max_inflight_per_isolate: 4,
        io_hibernate_after: Some(Duration::from_millis(40)),
        io_hibernate_storage: IoHibernationStorage::Memory,
        ..RuntimeConfig::default()
    })
    .await;

    let worker = "auto-hib-wake-cmd";
    let source = format!(
        r#"
        export default {{
          async fetch(request) {{
            const path = new URL(request.url).pathname;
            if (path === "/fast") {{
              return new Response("fast-ok");
            }}
            const res = await fetch("http://{addr}/hold");
            return new Response(await res.text());
          }},
        }};
        "#
    );
    service
        .deploy_with_config(
            worker.to_string(),
            source,
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{addr}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy");

    let svc_slow = service.clone();
    let slow_task = tokio::spawn(async move {
        svc_slow
            .invoke(
                worker.to_string(),
                test_invocation_with_path("/slow", "req-slow-1"),
            )
            .await
    });
    timeout(Duration::from_secs(5), arrived_rx)
        .await
        .expect("hold arrived")
        .expect("arrived");
    let _slept = wait_for_stats(&service, worker, |s| s.hibernated_isolates == 1).await;

    // Dispatch a fast request to the sleeping isolate -> must wake and answer immediately!
    let fast_out = timeout(
        Duration::from_secs(5),
        service.invoke(
            worker.to_string(),
            test_invocation_with_path("/fast", "req-fast-2"),
        ),
    )
    .await
    .expect("fast timeout")
    .expect("fast ok");
    assert_eq!(fast_out.status, 200);
    assert_eq!(String::from_utf8(fast_out.body).expect("utf8"), "fast-ok");

    // Slow request must still succeed once upstream responds.
    release_tx.send(()).expect("release hold");
    let slow_out = timeout(Duration::from_secs(10), slow_task)
        .await
        .expect("slow timeout")
        .expect("slow join")
        .expect("slow ok");
    assert_eq!(String::from_utf8(slow_out.body).expect("utf8"), "held-ok");
    server.await.expect("server clean");
    service.shutdown().await.expect("shutdown");

    // Part B: undeploying a disk-hibernated worker must unlink its .ddhb file cleanly.
    let (addr2, mut gates2, server2) = spawn_gated_http_server(vec![("/never", "never")]).await;
    let (arrived2, _release2) = gates2.remove(0);

    let store_dir = hibernation_test_dir("hibernation-undeploy-disk");
    let service2 = RuntimeService::start(RuntimeServiceConfig {
        runtime: RuntimeConfig {
            max_global_isolates: 4,
            max_isolates_per_worker: 1,
            io_hibernate_after: Some(Duration::from_millis(40)),
            io_hibernate_storage: IoHibernationStorage::Disk,
            ..RuntimeConfig::default()
        },
        storage: RuntimeStorageConfig {
            store_dir: store_dir.clone(),
            ..RuntimeStorageConfig::default()
        },
    })
    .await
    .expect("start service2");

    let worker2 = "auto-hib-undeploy";
    service2
        .deploy_with_config(
            worker2.to_string(),
            format!(
                r#"
                export default {{
                  async fetch() {{
                    const res = await fetch("http://{addr2}/never");
                    return new Response(await res.text());
                  }},
                }};
                "#
            ),
            DeployConfig {
                egress_allow_hosts: vec![format!("private:{addr2}")],
                ..DeployConfig::default()
            },
        )
        .await
        .expect("deploy2");

    let svc2 = service2.clone();
    let pending_req = tokio::spawn(async move {
        svc2.invoke(
            worker2.to_string(),
            test_invocation_with_path("/", "req-undeploy"),
        )
        .await
    });
    timeout(Duration::from_secs(5), arrived2)
        .await
        .expect("arrived2")
        .expect("arrived2 ok");
    let _slept2 = wait_for_stats(&service2, worker2, |s| s.hibernated_isolates == 1).await;

    service2
        .undeploy(worker2.to_string())
        .await
        .expect("undeploy");
    let res = timeout(Duration::from_secs(5), pending_req)
        .await
        .expect("pending req finishes on undeploy")
        .expect("join");
    assert!(res.is_err(), "undeployed worker request must fail cleanly");

    // Wait briefly for isolate thread drop to unlink the disk file.
    let hib_dir2 = store_dir.join("hibernation");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let count = std::fs::read_dir(&hib_dir2)
            .map(|rd| {
                rd.filter_map(std::result::Result::ok)
                    .filter(|e| e.path().extension().is_some_and(|ext| ext == "ddhb"))
                    .count()
            })
            .unwrap_or(0);
        if count == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "leaked {count} .ddhb file(s) after undeploy"
        );
        sleep(Duration::from_millis(20)).await;
    }

    drop(_release2);
    server2.await.expect("server2 clean");
    service2.shutdown().await.expect("shutdown2");
    let _ = std::fs::remove_dir_all(store_dir);
}
