//! Static assets deployed beside a worker: their header rules, HEAD requests,
//! restoring them from the worker store, and the catalog requests read them
//! from.
use super::*;

#[tokio::test]
#[serial]
async fn deployed_assets_resolve_with_headers_and_head_support() {
    let service = test_service(RuntimeConfig::default()).await;
    service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some(asset_headers_file()),
        )
        .await
        .expect("deploy should succeed");

    let private_public_lookup = service
        .resolve_public_asset("assets", "GET", Some("foo.example.com"), "/a.js", &[])
        .expect("public asset lookup should succeed");
    assert!(private_public_lookup.is_none());

    let asset = service
        .resolve_asset("assets", "GET", Some("foo.example.com:443"), "/a.js", &[])
        .expect("asset lookup should succeed")
        .expect("asset should exist");
    assert_eq!(asset.status, 200);
    assert_eq!(asset.body.as_ref(), b"asset-body");
    assert!(asset.headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("cache-control") && value == "public, max-age=60"
    }));
    assert!(
        asset
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("x-host") && value == "foo")
    );

    let etag = asset
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
        .map(|(_, value)| value.clone())
        .expect("etag should be present");

    let head = service
        .resolve_asset(
            "assets",
            "HEAD",
            Some("foo.example.com"),
            "/nested/b.css",
            &[],
        )
        .expect("head lookup should succeed")
        .expect("asset should exist");
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert!(
        head.headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("x-splat") && value == "b.css")
    );

    let not_modified = service
        .resolve_asset(
            "assets",
            "GET",
            Some("foo.example.com"),
            "/a.js",
            &[("if-none-match".to_string(), etag)],
        )
        .expect("etag lookup should succeed")
        .expect("asset should exist");
    assert_eq!(not_modified.status, 304);
    assert!(not_modified.body.is_empty());
}

#[tokio::test]
#[serial]
async fn deployed_assets_restore_from_worker_store() {
    let store = TestStoreDir::new("dd-assets");

    let service = test_service_with_store(RuntimeConfig::default(), store.clone(), true).await;
    service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some(asset_headers_file()),
        )
        .await
        .expect("deploy should succeed");
    drop(service);

    let restored = test_service_with_store(RuntimeConfig::default(), store.clone(), true).await;
    let asset = restored
        .resolve_asset("assets", "GET", Some("foo.example.com"), "/a.js", &[])
        .expect("asset lookup should succeed")
        .expect("asset should exist after restore");
    assert_eq!(asset.body.as_ref(), b"asset-body");
}

#[tokio::test]
#[serial]
async fn invalid_asset_headers_fail_deploy() {
    let service = test_service(RuntimeConfig::default()).await;
    let error = service
        .deploy_with_bundle_config(
            "assets".to_string(),
            asset_worker(),
            DeployConfig::default(),
            test_assets(),
            Some("/a.js\n  BadHeader".to_string()),
        )
        .await
        .expect_err("deploy should fail");
    assert!(
        error
            .to_string()
            .contains("must be `Name: value` or `! Name`")
    );
}

#[test]
fn asset_catalog_copy_on_write_preserves_concurrent_updates_and_redeploy_snapshots() {
    let catalog = crate::service::AssetCatalog::default();
    let assets = Arc::new(crate::static_assets::AssetBundle::default());

    std::thread::scope(|scope| {
        for generation in 1..=32 {
            let catalog = catalog.clone();
            let assets = Arc::clone(&assets);
            scope.spawn(move || {
                let worker_name = format!("worker-{generation}");
                catalog.insert(
                    worker_name.clone(),
                    crate::service::AssetCatalogEntry {
                        deployment_id: format!("deployment-{generation}"),
                        worker_name,
                        generation,
                        assets,
                        public: true,
                        cache_enabled: false,
                    },
                );
            });
        }
    });

    for generation in 1..=32 {
        let worker_name = format!("worker-{generation}");
        let entry = catalog
            .get(&worker_name)
            .expect("concurrent catalog update should not be lost");
        assert_eq!(entry.worker_name, worker_name);
        assert_eq!(entry.generation, generation);
    }

    let original = catalog.get("worker-1").expect("original snapshot");
    catalog.insert(
        "worker-1".to_string(),
        crate::service::AssetCatalogEntry {
            worker_name: "worker-1".to_string(),
            deployment_id: "redeployed".to_string(),
            generation: 100,
            assets,
            public: false,
            cache_enabled: true,
        },
    );
    let redeployed = catalog.get("worker-1").expect("redeployed snapshot");
    assert_eq!(original.generation, 1);
    assert_eq!(redeployed.generation, 100);
    assert!(original.public);
    assert!(!redeployed.public);
}
