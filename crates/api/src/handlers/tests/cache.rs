//! Front-cache representations and deployment isolation.
use super::fixtures::TestState;
use crate::handlers::{deploy_worker, invoke_worker_public};
use bytes::Bytes;
use common::{DeployBinding, DeployCacheConfig, DeployConfig, DeployRequest};
use http::Request;
use http_body_util::{BodyExt, Empty};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn opt_in_front_cache_bypasses_worker_after_first_response() {
    let state = TestState::new("example.com").await;
    deploy_worker(
        state.app(),
        DeployRequest {
            name: "cached".to_string(),
            source: r#"
let count = 0;
export default {
  async fetch() {
    count += 1;
    return new Response(String(count), {
      headers: { "cache-control": "public, max-age=60, stale-while-revalidate=30" },
    });
  },
};
"#
            .to_string(),
            config: DeployConfig {
                public: true,
                cache: DeployCacheConfig { enabled: true },
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        },
    )
    .await
    .expect("deploy");

    let invoke = || {
        Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "cached.example.com")
            .body(Empty::<Bytes>::new())
            .expect("request")
    };
    let first = invoke_worker_public(state.app(), invoke(), None)
        .await
        .expect("first invoke");
    assert_eq!(first.headers().get("x-dd-cache").expect("cache"), "MISS");
    assert_eq!(
        first.into_body().collect().await.expect("body").to_bytes(),
        Bytes::from_static(b"1")
    );

    let second = invoke_worker_public(state.app(), invoke(), None)
        .await
        .expect("second invoke");
    assert_eq!(second.headers().get("x-dd-cache").expect("cache"), "HIT");
    assert_eq!(
        second.into_body().collect().await.expect("body").to_bytes(),
        Bytes::from_static(b"1")
    );
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn redeployment_isolates_warm_cache_and_late_previous_generation_responses() {
    let state = TestState::new("example.com").await;
    let config = DeployConfig {
        public: true,
        cache: DeployCacheConfig { enabled: true },
        bindings: vec![DeployBinding::Kv {
            binding: "KV".into(),
        }],
        ..DeployConfig::default()
    };
    state
        .state
        .runtime
        .deploy_with_config(
            "cache-redeploy".into(),
            r#"
export default { async fetch(request, env) {
  const path = new URL(request.url).pathname;
  if (path === '/ready') return new Response(await env.KV.get('started') ?? '');
  if (path === '/late') {
    await env.KV.put('started', 'yes');
    while (await env.KV.get('release') !== 'yes') await new Promise(r => setTimeout(r, 10));
  }
  return new Response('old', { headers: { 'cache-control': 'public, max-age=60' } });
} };
"#
            .into(),
            config.clone(),
        )
        .await
        .unwrap();
    let old_namespace = state
        .state
        .runtime
        .front_cache_namespace("cache-redeploy")
        .unwrap();
    let invoke = |path: &str| {
        Request::builder()
            .method("GET")
            .uri(path)
            .header("host", "cache-redeploy.example.com")
            .body(Empty::<Bytes>::new())
            .unwrap()
    };
    for cache_status in ["MISS", "HIT"] {
        let response = invoke_worker_public(state.app(), invoke("/"), None)
            .await
            .unwrap();
        assert_eq!(response.headers().get("x-dd-cache").unwrap(), cache_status);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "old"
        );
    }
    let late = tokio::spawn({
        let app = state.app();
        let request = invoke("/late");
        async move { invoke_worker_public(app, request, None).await.unwrap() }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let response = invoke_worker_public(state.app(), invoke("/ready"), None)
                .await
                .unwrap();
            if response.into_body().collect().await.unwrap().to_bytes() == "yes" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("old generation invocation starts");
    state
        .state
        .runtime
        .deploy_with_config(
            "cache-redeploy".into(),
            r#"
export default { async fetch(request, env) {
  if (new URL(request.url).pathname === '/release') await env.KV.put('release', 'yes');
  return new Response('new', { headers: { 'cache-control': 'public, max-age=60' } });
} };
"#
            .into(),
            config,
        )
        .await
        .unwrap();
    assert_ne!(
        state
            .state
            .runtime
            .front_cache_namespace("cache-redeploy")
            .unwrap(),
        old_namespace
    );
    for cache_status in ["MISS", "HIT"] {
        let response = invoke_worker_public(state.app(), invoke("/"), None)
            .await
            .unwrap();
        assert_eq!(response.headers().get("x-dd-cache").unwrap(), cache_status);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "new"
        );
    }
    invoke_worker_public(state.app(), invoke("/release"), None)
        .await
        .unwrap();
    let old_response = tokio::time::timeout(std::time::Duration::from_secs(5), late)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        old_response.into_body().collect().await.unwrap().to_bytes(),
        "old"
    );
    for cache_status in ["MISS", "HIT"] {
        let response = invoke_worker_public(state.app(), invoke("/late"), None)
            .await
            .unwrap();
        assert_eq!(response.headers().get("x-dd-cache").unwrap(), cache_status);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "new"
        );
    }
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn front_cache_head_miss_preserves_get_body_and_representation_length() {
    let state = TestState::new("example.com").await;
    deploy_worker(
        state.app(),
        DeployRequest {
            name: "cached-head".into(),
            source: r#"
export default {
  fetch(request) {
    const headers = { "cache-control": "public, max-age=60" };
    if (!new URL(request.url).pathname.includes("unknown")) headers["content-length"] = "5";
    return new Response(request.method === "HEAD" ? null : "hello", {
      headers,
    });
  },
};
"#
            .into(),
            config: DeployConfig {
                public: true,
                cache: DeployCacheConfig { enabled: true },
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        },
    )
    .await
    .expect("deploy");
    for (path, method, cache_status, expected_body, expected_length) in [
        ("/known", "HEAD", "MISS", "", Some("5")),
        ("/known", "GET", "MISS", "hello", Some("5")),
        ("/known", "HEAD", "HIT", "", Some("5")),
        ("/known", "GET", "HIT", "hello", Some("5")),
        ("/unknown", "HEAD", "MISS", "", None),
        ("/unknown", "GET", "MISS", "hello", Some("5")),
        ("/unknown", "HEAD", "HIT", "", Some("5")),
        ("/unknown", "GET", "HIT", "hello", Some("5")),
    ] {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "cached-head.example.com")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let response = invoke_worker_public(state.app(), request, None)
            .await
            .unwrap();
        assert_eq!(response.headers().get("x-dd-cache").unwrap(), cache_status);
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .map(|value| value.to_str().unwrap()),
            expected_length,
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::copy_from_slice(expected_body.as_bytes())
        );
    }
    state.shutdown().await;
}

#[tokio::test]
#[serial]
async fn front_cache_stale_head_refreshes_the_get_representation() {
    let state = TestState::new("example.com").await;
    deploy_worker(
        state.app(),
        DeployRequest {
            name: "head-revalidate".into(),
            source: r#"
export default {
  fetch(request) {
    return new Response(request.method === "HEAD" ? null : "new-body", {
      headers: { "cache-control": "public, max-age=60" },
    });
  },
};
"#
            .into(),
            config: DeployConfig {
                public: true,
                cache: DeployCacheConfig { enabled: true },
                ..DeployConfig::default()
            },
            assets: Vec::new(),
            server_modules: Vec::new(),
            asset_headers: None,
            temporary: false,
        },
    )
    .await
    .unwrap();
    state
        .state
        .runtime
        .cache_put(
            storage::cache::CacheRequest {
                cache_name: state
                    .state
                    .runtime
                    .front_cache_namespace("head-revalidate")
                    .unwrap(),
                method: "GET".into(),
                url: "https://head-revalidate.example.com/".into(),
                headers: Vec::new(),
                bypass_stale: false,
            },
            storage::cache::CacheResponse {
                status: 200,
                headers: vec![(
                    "cache-control".into(),
                    "public, max-age=1, stale-while-revalidate=30".into(),
                )],
                body: Bytes::from_static(b"old-body"),
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let invoke = |method| {
        Request::builder()
            .method(method)
            .uri("/")
            .header("host", "head-revalidate.example.com")
            .body(Empty::<Bytes>::new())
            .unwrap()
    };
    let head = invoke_worker_public(state.app(), invoke("HEAD"), None)
        .await
        .unwrap();
    assert_eq!(head.headers().get("x-dd-cache").unwrap(), "STALE");
    assert_eq!(head.headers().get("content-length").unwrap(), "8");
    assert!(
        head.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let get = invoke_worker_public(state.app(), invoke("GET"), None)
                .await
                .unwrap();
            let cache_status = get.headers().get("x-dd-cache").unwrap().clone();
            let body = get.into_body().collect().await.unwrap().to_bytes();
            if cache_status == "HIT" {
                assert_eq!(body, Bytes::from_static(b"new-body"));
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("HEAD revalidation replaces the stale GET body");
    state.shutdown().await;
}
