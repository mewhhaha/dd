//! Deploy requests, built from flags or a deploy config file, and sending them.
use crate::args::{DeployCmd, DeployConfigFileCmd};
use crate::client::{api_url, decode_json, with_bearer_auth};
use crate::config::{CONFIG_SCHEMA_VERSION, DeployFileConfig, validate_config_document};
use crate::package::{
    package_assets_dir, package_assets_dir_with_excludes, package_deploy_file_server_modules,
    package_server_module_specs, path_to_string, push_asset_exclude_if_within,
    resolve_config_file_path,
};
use crate::{parse_json_bytes, to_json_string};
use common::{
    DeployBinding, DeployCacheConfig, DeployConfig, DeployInternalConfig, DeployRequest,
    DeployResponse, DeployTraceDestination,
};
use std::path::{Path, PathBuf};

pub async fn deploy(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: DeployCmd,
) -> Result<(), String> {
    let request = build_deploy_request(command).await?;
    let response = with_bearer_auth(
        client.post(api_url(server, "/v1/deploy")),
        private_bearer_token,
    )
    .json(&request)
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let deployment: DeployResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deployment)?);
    Ok(())
}

pub async fn deploy_config_file(
    client: &reqwest::Client,
    server: &str,
    private_bearer_token: Option<&str>,
    command: DeployConfigFileCmd,
) -> Result<(), String> {
    let request = build_deploy_request_from_config_file(command).await?;
    let response = with_bearer_auth(
        client.post(api_url(server, "/v1/deploy")),
        private_bearer_token,
    )
    .json(&request)
    .send()
    .await
    .map_err(|error| error.to_string())?;

    let deployment: DeployResponse = decode_json(response).await?;
    println!("{}", to_json_string(&deployment)?);
    Ok(())
}

pub async fn build_deploy_request(command: DeployCmd) -> Result<DeployRequest, String> {
    let source = tokio::fs::read_to_string(&command.file)
        .await
        .map_err(|error| format!("failed to read {}: {error}", command.file))?;
    let (assets, asset_headers) = package_assets_dir(command.assets_dir.as_deref())?;
    let server_modules =
        package_server_module_specs(&command.server_modules, Path::new("."), true)?;
    let mut bindings: Vec<DeployBinding> = command
        .kv_bindings
        .into_iter()
        .map(|binding| DeployBinding::Kv { binding })
        .collect();
    bindings.extend(
        command
            .memory_bindings
            .into_iter()
            .map(parse_memory_binding)
            .collect::<Result<Vec<_>, _>>()?,
    );
    bindings.extend(
        command
            .service_bindings
            .into_iter()
            .map(parse_service_binding)
            .collect::<Result<Vec<_>, _>>()?,
    );
    let config = DeployConfig {
        egress_allow_hosts: Vec::new(),
        public: command.public,
        cache: DeployCacheConfig {
            enabled: command.cache,
        },
        bindings,
        internal: DeployInternalConfig {
            trace: command.trace_worker.map(|worker| DeployTraceDestination {
                worker,
                path: command.trace_path,
            }),
        },
    };
    Ok(DeployRequest {
        name: command.name,
        source,
        config,
        assets,
        server_modules,
        asset_headers,
        temporary: command.temporary,
    })
}

pub async fn build_deploy_request_from_config_file(
    command: DeployConfigFileCmd,
) -> Result<DeployRequest, String> {
    let config_path = PathBuf::from(&command.config_file);
    let config_bytes = tokio::fs::read(&config_path)
        .await
        .map_err(|error| format!("failed to read {}: {error}", config_path.display()))?;
    validate_config_document(&config_bytes)?;
    let deploy_file = parse_json_bytes::<DeployFileConfig>(&config_bytes)?;
    if deploy_file.schema_version != CONFIG_SCHEMA_VERSION {
        return Err(format!(
            "unsupported deploy config schema_version {}; expected {CONFIG_SCHEMA_VERSION}",
            deploy_file.schema_version
        ));
    }
    if deploy_file.name.trim().is_empty() {
        return Err("deploy config name must not be empty".to_string());
    }
    if deploy_file.entrypoint.trim().is_empty() {
        return Err("deploy config entrypoint must not be empty".to_string());
    }

    let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let entry_path = resolve_config_file_path(
        base_dir,
        &deploy_file.entrypoint,
        command.allow_outside_config_root,
        "entrypoint",
    )?;
    let source = tokio::fs::read_to_string(&entry_path)
        .await
        .map_err(|error| format!("failed to read {}: {error}", entry_path.display()))?;

    let mut asset_excludes = deploy_file.asset_excludes;
    let server_module_file_paths = deploy_file
        .server_modules
        .iter()
        .enumerate()
        .map(|(index, module)| {
            let file = module.file.as_deref().unwrap_or(&module.path);
            resolve_config_file_path(
                base_dir,
                file,
                command.allow_outside_config_root,
                &format!("server_modules[{index}].file"),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (assets, asset_headers) = if let Some(assets_dir) = deploy_file.assets_dir.as_deref() {
        let assets_path = resolve_config_file_path(
            base_dir,
            assets_dir,
            command.allow_outside_config_root,
            "assets_dir",
        )?;
        push_asset_exclude_if_within(&mut asset_excludes, &assets_path, &entry_path)?;
        push_asset_exclude_if_within(&mut asset_excludes, &assets_path, &config_path)?;
        for module_path in &server_module_file_paths {
            push_asset_exclude_if_within(&mut asset_excludes, &assets_path, module_path)?;
        }
        package_assets_dir_with_excludes(Some(&path_to_string(&assets_path)?), &asset_excludes)?
    } else {
        (Vec::new(), None)
    };
    let server_modules = package_deploy_file_server_modules(
        base_dir,
        deploy_file.server_modules,
        command.allow_outside_config_root,
    )?;
    let mut config = deploy_file.config.unwrap_or_default();
    if let Some(hosts) = deploy_file.egress_allow_hosts {
        config.egress_allow_hosts = hosts;
    }
    if let Some(public) = deploy_file.public {
        config.public = public;
    }
    if let Some(cache) = deploy_file.cache {
        config.cache = cache;
    }
    if let Some(bindings) = deploy_file.bindings {
        config.bindings = bindings;
    }
    if let Some(internal) = deploy_file.internal {
        config.internal = internal;
    }

    Ok(DeployRequest {
        name: deploy_file.name,
        source,
        config,
        assets,
        server_modules,
        asset_headers,
        temporary: command.temporary || deploy_file.temporary,
    })
}

pub fn parse_memory_binding(value: String) -> Result<DeployBinding, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("memory binding must not be empty".to_string());
    }
    if trimmed.contains('=') {
        return Err(format!(
            "invalid memory binding {trimmed:?}, expected BINDING"
        ));
    }
    let binding = trimmed.trim();
    if binding.is_empty() {
        return Err(format!(
            "invalid memory binding {trimmed:?}, expected BINDING"
        ));
    }
    Ok(DeployBinding::Memory {
        binding: binding.to_string(),
    })
}

pub fn parse_service_binding(value: String) -> Result<DeployBinding, String> {
    let trimmed = value.trim();
    let Some((binding, service)) = trimmed.split_once('=') else {
        return Err(format!(
            "invalid service binding {trimmed:?}, expected BINDING=worker"
        ));
    };
    let binding = binding.trim();
    let service = service.trim();
    if binding.is_empty() || service.is_empty() {
        return Err(format!(
            "invalid service binding {trimmed:?}, expected BINDING=worker"
        ));
    }
    Ok(DeployBinding::Service {
        binding: binding.to_string(),
        service: service.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::build_deploy_request_from_config_file;
    use crate::args::DeployConfigFileCmd;
    use crate::temp_dir;
    use base64::Engine;
    use common::DeployServerModuleKind;
    use std::fs;

    #[tokio::test]
    async fn deployment_config_file_matches_shared_runtime_contract() {
        let root = temp_dir("runtime-contract");
        fs::create_dir_all(&root).expect("create config root");
        fs::write(root.join("worker.js"), "export default { fetch() {} };").expect("write worker");
        let cases: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../../../fixtures/config/deploy-config.json"))
                .expect("shared runtime contract");
        for scenario in cases {
            let mut config = serde_json::json!({
                "schema_version": 1,
                "name": "runtime-contract",
                "entrypoint": "worker.js",
            });
            if let Some(deployment) = scenario.get("deployment") {
                config
                    .as_object_mut()
                    .expect("deployment object")
                    .extend(deployment.as_object().expect("mixed configuration").clone());
            } else {
                config["config"] = scenario["input"].clone();
            }
            fs::write(
                root.join("dd.deploy.json"),
                serde_json::to_vec(&config).expect("encode deployment config"),
            )
            .expect("write deployment config");
            let result = build_deploy_request_from_config_file(DeployConfigFileCmd {
                config_file: root.join("dd.deploy.json").display().to_string(),
                temporary: false,
                allow_outside_config_root: false,
            })
            .await;
            if scenario["reject"] == true {
                assert!(result.is_err(), "{} must be rejected", scenario["name"]);
            } else {
                let request = result.expect("supported runtime config should package");
                assert_eq!(
                    serde_json::to_value(request.config).expect("normalized runtime config"),
                    scenario["expected"],
                    "{}",
                    scenario["name"],
                );
            }
        }
        fs::remove_dir_all(root).expect("remove config root");
    }

    #[tokio::test]
    async fn package_deploy_config_uses_bundled_entrypoint_and_excludes_artifacts() {
        let root = temp_dir("deploy-config");
        fs::create_dir_all(root.join("assets")).expect("create assets");
        fs::write(root.join("worker.js"), "export default { fetch() {} };").expect("write worker");
        fs::write(root.join("index.html"), "<div id=\"root\"></div>").expect("write html");
        fs::create_dir_all(root.join("data")).expect("create data");
        fs::write(root.join("data").join("config.json"), r#"{"answer":42}"#).expect("write config");
        fs::write(root.join("assets").join("app.js"), "console.log('asset');")
            .expect("write asset");
        fs::write(
            root.join("dd.deploy.json"),
            r#"{
              "schema_version": 1,
              "name": "built-worker",
              "entrypoint": "worker.js",
              "assets_dir": ".",
              "temporary": true,
              "asset_excludes": ["worker.js", "dd.deploy.json"],
              "server_modules": [
                { "type": "Json", "path": "./data/config.json", "file": "data/config.json" }
              ],
              "config": {
                "public": true,
                "egress_allow_hosts": ["api.example.com"],
                "bindings": [{ "type": "memory", "binding": "ROOM" }]
              }
            }"#,
        )
        .expect("write deploy config");

        let request = build_deploy_request_from_config_file(DeployConfigFileCmd {
            config_file: root.join("dd.deploy.json").display().to_string(),
            temporary: false,
            allow_outside_config_root: false,
        })
        .await
        .expect("package generated deploy config");

        assert_eq!(request.name, "built-worker");
        assert!(request.source.contains("export default"));
        assert!(request.config.public);
        assert_eq!(request.config.egress_allow_hosts, vec!["api.example.com"]);
        assert!(request.temporary);
        assert_eq!(request.config.bindings.len(), 1);
        let asset_paths = request
            .assets
            .iter()
            .map(|asset| asset.path.as_str())
            .collect::<Vec<_>>();
        assert_eq!(asset_paths, vec!["/assets/app.js", "/index.html"]);
        assert_eq!(request.server_modules.len(), 1);
        assert_eq!(request.server_modules[0].path, "data/config.json");
        assert_eq!(request.server_modules[0].kind, DeployServerModuleKind::Json);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(request.server_modules[0].content_base64.as_bytes())
            .expect("server module should decode");
        assert_eq!(decoded, br#"{"answer":42}"#);

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn deploy_config_rejects_parent_relative_entrypoint_escape() {
        let root = temp_dir("deploy-config-entry-escape");
        let app = root.join("app");
        fs::create_dir_all(&app).expect("create app");
        fs::write(root.join("worker.js"), "export default { fetch() {} };").expect("write worker");
        fs::write(
            app.join("dd.deploy.json"),
            r#"{
              "schema_version": 1,
              "name": "escaped-worker",
              "entrypoint": "../worker.js",
              "config": { "public": true, "bindings": [] }
            }"#,
        )
        .expect("write deploy config");

        let error = build_deploy_request_from_config_file(DeployConfigFileCmd {
            config_file: app.join("dd.deploy.json").display().to_string(),
            temporary: false,
            allow_outside_config_root: false,
        })
        .await
        .expect_err("entrypoint escape should fail");

        assert!(error.contains("entrypoint must stay within the config directory"));
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn deploy_config_rejects_absolute_assets_dir_escape() {
        let root = temp_dir("deploy-config-assets-escape");
        let app = root.join("app");
        let assets = root.join("assets");
        fs::create_dir_all(&app).expect("create app");
        fs::create_dir_all(&assets).expect("create assets");
        fs::write(app.join("worker.js"), "export default { fetch() {} };").expect("write worker");
        fs::write(assets.join("app.js"), "console.log('asset');").expect("write asset");
        fs::write(
            app.join("dd.deploy.json"),
            format!(
                r#"{{
                  "schema_version": 1,
                  "name": "escaped-assets",
                  "entrypoint": "worker.js",
                  "assets_dir": "{}",
                  "config": {{ "public": true, "bindings": [] }}
                }}"#,
                assets.display()
            ),
        )
        .expect("write deploy config");

        let error = build_deploy_request_from_config_file(DeployConfigFileCmd {
            config_file: app.join("dd.deploy.json").display().to_string(),
            temporary: false,
            allow_outside_config_root: false,
        })
        .await
        .expect_err("assets dir escape should fail");

        assert!(error.contains("assets_dir must stay within the config directory"));
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn deploy_config_allows_outside_paths_with_explicit_flag() {
        let root = temp_dir("deploy-config-allow-outside");
        let app = root.join("app");
        let assets = root.join("assets");
        fs::create_dir_all(&app).expect("create app");
        fs::create_dir_all(&assets).expect("create assets");
        fs::write(root.join("worker.js"), "export default { fetch() {} };").expect("write worker");
        fs::write(assets.join("app.js"), "console.log('asset');").expect("write asset");
        fs::write(
            app.join("dd.deploy.json"),
            format!(
                r#"{{
                  "schema_version": 1,
                  "name": "outside-worker",
                  "entrypoint": "../worker.js",
                  "assets_dir": "{}",
                  "config": {{ "public": true, "bindings": [] }}
                }}"#,
                assets.display()
            ),
        )
        .expect("write deploy config");

        let request = build_deploy_request_from_config_file(DeployConfigFileCmd {
            config_file: app.join("dd.deploy.json").display().to_string(),
            temporary: false,
            allow_outside_config_root: true,
        })
        .await
        .expect("outside paths should be allowed with flag");

        assert_eq!(request.name, "outside-worker");
        assert!(request.source.contains("export default"));
        assert_eq!(request.assets.len(), 1);
        assert_eq!(request.assets[0].path, "/app.js");
        let _ = fs::remove_dir_all(root);
    }
}
