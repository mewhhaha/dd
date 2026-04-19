use super::{DynamicRpcBinding, RuntimeConfig};
use crate::ops::DynamicWorkerPolicy;
use common::{DeployBinding, DeployConfig, PlatformError, Result};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub(super) struct DeployBindings {
    pub(super) kv: Vec<String>,
    pub(super) memory: Vec<String>,
    pub(super) dynamic: Vec<String>,
}

pub(super) struct DynamicWorkerConfig {
    pub(super) dynamic_env: Vec<(String, String)>,
    pub(super) dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
    pub(super) secret_replacements: Vec<(String, String)>,
    pub(super) egress_allow_hosts: Vec<String>,
    pub(super) env_placeholders: HashMap<String, String>,
    pub(super) policy: ValidatedDynamicWorkerPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DynamicWorkerTier {
    IsolatedAgent,
    RpcChild,
    FullDynamic,
}

impl DynamicWorkerTier {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::IsolatedAgent => "isolated-agent",
            Self::RpcChild => "rpc-child",
            Self::FullDynamic => "full-dynamic",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ValidatedDynamicWorkerPolicy {
    pub(super) egress_allow_hosts: Vec<String>,
    pub(super) allow_host_rpc: bool,
    pub(super) allow_websocket: bool,
    pub(super) allow_transport: bool,
    pub(super) allow_state_bindings: bool,
    pub(super) max_request_bytes: usize,
    pub(super) max_response_bytes: usize,
    pub(super) max_outbound_requests: u64,
    pub(super) max_concurrency: usize,
    pub(super) tier: DynamicWorkerTier,
}

pub(super) const MAX_DYNAMIC_HOST_RPC_METHODS: usize = 64;
pub(super) const MAX_DYNAMIC_HOST_RPC_ARG_BYTES: usize = 128 * 1024;
pub(super) const MAX_DYNAMIC_HOST_RPC_REPLY_BYTES: usize = 128 * 1024;

pub(super) fn full_dynamic_internal_policy(
    egress_allow_hosts: Vec<String>,
) -> ValidatedDynamicWorkerPolicy {
    ValidatedDynamicWorkerPolicy {
        egress_allow_hosts,
        allow_host_rpc: true,
        allow_websocket: true,
        allow_transport: true,
        allow_state_bindings: true,
        max_request_bytes: usize::MAX,
        max_response_bytes: usize::MAX,
        max_outbound_requests: u64::MAX,
        max_concurrency: usize::MAX,
        tier: DynamicWorkerTier::FullDynamic,
    }
}

pub(super) fn extract_bindings(config: &DeployConfig) -> Result<DeployBindings> {
    let mut kv = Vec::new();
    let mut memory = Vec::new();
    let mut dynamic = Vec::new();
    let mut seen = HashSet::new();
    for binding in &config.bindings {
        match binding {
            DeployBinding::Kv { binding } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                kv.push(name.to_string());
            }
            DeployBinding::Memory { binding } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                memory.push(name.to_string());
            }
            DeployBinding::Dynamic { binding } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                dynamic.push(name.to_string());
            }
        }
    }
    Ok(DeployBindings {
        kv,
        memory,
        dynamic,
    })
}

pub(super) fn build_dynamic_worker_config(
    env: HashMap<String, String>,
    policy_input: DynamicWorkerPolicy,
    dynamic_rpc_bindings: Vec<DynamicRpcBinding>,
) -> Result<DynamicWorkerConfig> {
    let mut dynamic_env = Vec::new();
    let mut secret_replacements = Vec::new();
    let mut env_placeholders = HashMap::new();

    for (name, value) in env {
        let key = name.trim().to_string();
        if key.is_empty() {
            return Err(PlatformError::bad_request(
                "dynamic env variable name must not be empty",
            ));
        }
        if !is_valid_env_name(&key) {
            return Err(PlatformError::bad_request(format!(
                "invalid dynamic env variable name: {key}"
            )));
        }
        if env_placeholders.contains_key(&key) {
            return Err(PlatformError::bad_request(format!(
                "duplicate dynamic env variable name: {key}"
            )));
        }

        let placeholder = format!("__DD_SECRET_{}__", Uuid::new_v4().simple());
        dynamic_env.push((key.clone(), placeholder.clone()));
        secret_replacements.push((placeholder.clone(), value));
        env_placeholders.insert(key, placeholder);
    }

    let mut normalized_hosts = Vec::new();
    let mut seen_hosts = HashSet::new();
    for host in &policy_input.egress_allow_hosts {
        let normalized = host.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if !is_valid_egress_host(&normalized) {
            return Err(PlatformError::bad_request(format!(
                "invalid egress allow host: {normalized}"
            )));
        }
        if seen_hosts.insert(normalized.clone()) {
            normalized_hosts.push(normalized);
        }
    }

    let max_request_bytes = usize::try_from(policy_input.max_request_bytes)
        .map_err(|_| PlatformError::bad_request("dynamic max_request_bytes is too large"))?;
    let max_response_bytes = usize::try_from(policy_input.max_response_bytes)
        .map_err(|_| PlatformError::bad_request("dynamic max_response_bytes is too large"))?;
    let max_concurrency = usize::try_from(policy_input.max_concurrency)
        .map_err(|_| PlatformError::bad_request("dynamic max_concurrency is too large"))?;
    if max_request_bytes == 0 {
        return Err(PlatformError::bad_request(
            "dynamic max_request_bytes must be greater than 0",
        ));
    }
    if max_response_bytes == 0 {
        return Err(PlatformError::bad_request(
            "dynamic max_response_bytes must be greater than 0",
        ));
    }
    if policy_input.max_outbound_requests == 0 {
        return Err(PlatformError::bad_request(
            "dynamic max_outbound_requests must be greater than 0",
        ));
    }
    if max_concurrency == 0 {
        return Err(PlatformError::bad_request(
            "dynamic max_concurrency must be greater than 0",
        ));
    }

    let tier = if policy_input.allow_websocket
        || policy_input.allow_transport
        || policy_input.allow_state_bindings
    {
        DynamicWorkerTier::FullDynamic
    } else if policy_input.allow_host_rpc || !dynamic_rpc_bindings.is_empty() {
        DynamicWorkerTier::RpcChild
    } else {
        DynamicWorkerTier::IsolatedAgent
    };

    let policy = ValidatedDynamicWorkerPolicy {
        egress_allow_hosts: normalized_hosts.clone(),
        allow_host_rpc: policy_input.allow_host_rpc,
        allow_websocket: policy_input.allow_websocket,
        allow_transport: policy_input.allow_transport,
        allow_state_bindings: policy_input.allow_state_bindings,
        max_request_bytes,
        max_response_bytes,
        max_outbound_requests: policy_input.max_outbound_requests,
        max_concurrency,
        tier,
    };

    Ok(DynamicWorkerConfig {
        dynamic_env,
        dynamic_rpc_bindings,
        secret_replacements,
        egress_allow_hosts: normalized_hosts,
        env_placeholders,
        policy,
    })
}

fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|char| char == '_' || char.is_ascii_alphanumeric())
}

fn is_valid_egress_host(host: &str) -> bool {
    let host = match host.rsplit_once(':') {
        Some((left, right)) if right.chars().all(|char| char.is_ascii_digit()) => {
            let Ok(port) = right.parse::<u16>() else {
                return false;
            };
            if port == 0 {
                return false;
            }
            left
        }
        _ => host,
    };
    if host.is_empty() {
        return false;
    }
    if let Some(rest) = host.strip_prefix("*.") {
        return !rest.is_empty()
            && rest
                .chars()
                .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '.')
            && rest.contains('.');
    }
    host.chars()
        .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '.')
        && host.contains('.')
}

pub(super) fn validate_runtime_config(config: &RuntimeConfig) -> Result<()> {
    if config.max_isolates == 0 {
        return Err(PlatformError::internal(
            "max_isolates must be greater than 0",
        ));
    }
    if config.max_inflight_per_isolate == 0 {
        return Err(PlatformError::internal(
            "max_inflight_per_isolate must be greater than 0",
        ));
    }
    if config.min_isolates > config.max_isolates {
        return Err(PlatformError::internal(
            "min_isolates cannot exceed max_isolates",
        ));
    }
    if config.cache_max_entries == 0 {
        return Err(PlatformError::internal(
            "cache_max_entries must be greater than 0",
        ));
    }
    if config.cache_max_bytes == 0 {
        return Err(PlatformError::internal(
            "cache_max_bytes must be greater than 0",
        ));
    }
    if config.cache_default_ttl.is_zero() {
        return Err(PlatformError::internal(
            "cache_default_ttl must be greater than 0",
        ));
    }
    if config.kv_read_cache_max_entries == 0 {
        return Err(PlatformError::internal(
            "kv_read_cache_max_entries must be greater than 0",
        ));
    }
    if config.kv_read_cache_max_bytes == 0 {
        return Err(PlatformError::internal(
            "kv_read_cache_max_bytes must be greater than 0",
        ));
    }
    if config.kv_read_cache_hit_ttl.is_zero() {
        return Err(PlatformError::internal(
            "kv_read_cache_hit_ttl must be greater than 0",
        ));
    }
    if config.kv_read_cache_miss_ttl.is_zero() {
        return Err(PlatformError::internal(
            "kv_read_cache_miss_ttl must be greater than 0",
        ));
    }
    Ok(())
}
