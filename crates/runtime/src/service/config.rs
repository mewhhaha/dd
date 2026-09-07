use super::RuntimeConfig;
use crate::ops::parse_egress_allow_host;
use common::{DeployBinding, DeployConfig, PlatformError, Result};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub(super) struct DeployBindings {
    pub(super) kv: Vec<String>,
    pub(super) memory: Vec<String>,
    pub(super) service: Vec<ServiceBinding>,
}

#[derive(Clone, Debug)]
pub(super) struct ServiceBinding {
    pub(super) binding: String,
    pub(super) service: String,
}

pub(super) fn extract_bindings(config: &DeployConfig) -> Result<DeployBindings> {
    for host in &config.egress_allow_hosts {
        if !is_valid_egress_host(host) {
            return Err(PlatformError::bad_request(format!(
                "invalid egress allow host: {host}"
            )));
        }
    }
    let mut kv = Vec::new();
    let mut memory = Vec::new();
    let mut service = Vec::new();
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
            DeployBinding::Service {
                binding,
                service: target,
            } => {
                let name = binding.trim();
                if name.is_empty() {
                    return Err(PlatformError::bad_request("binding name must not be empty"));
                }
                let service_name = target.trim();
                if service_name.is_empty() {
                    return Err(PlatformError::bad_request(
                        "service binding target must not be empty",
                    ));
                }
                if !seen.insert(name.to_string()) {
                    return Err(PlatformError::bad_request(format!(
                        "duplicate binding name: {name}"
                    )));
                }
                service.push(ServiceBinding {
                    binding: name.to_string(),
                    service: service_name.to_string(),
                });
            }
        }
    }
    Ok(DeployBindings {
        kv,
        memory,
        service,
    })
}

fn is_valid_egress_host(host: &str) -> bool {
    let Some(parsed) = parse_egress_allow_host(host) else {
        return false;
    };
    if parsed.host.parse::<std::net::IpAddr>().is_ok() {
        return !parsed.wildcard;
    }
    parsed
        .host
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || char == '-' || char == '.')
        && parsed.host.contains('.')
}

pub(super) fn validate_runtime_config(config: &RuntimeConfig) -> Result<()> {
    if config.max_global_isolates == 0 {
        return Err(PlatformError::internal(
            "max_global_isolates must be greater than 0",
        ));
    }
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
    if config.max_queued_requests_per_worker == 0 {
        return Err(PlatformError::internal(
            "max_queued_requests_per_worker must be greater than 0",
        ));
    }
    if config.max_global_queued_requests == 0 {
        return Err(PlatformError::internal(
            "max_global_queued_requests must be greater than 0",
        ));
    }
    if config.max_global_queued_bytes == 0 {
        return Err(PlatformError::internal(
            "max_global_queued_bytes must be greater than 0",
        ));
    }
    if config.max_buffered_request_bytes == 0
        || config.max_buffered_request_bytes > tokio::sync::Semaphore::MAX_PERMITS
    {
        return Err(PlatformError::internal(format!(
            "max_buffered_request_bytes must be within 1..={}, got {}",
            tokio::sync::Semaphore::MAX_PERMITS,
            config.max_buffered_request_bytes,
        )));
    }
    if config.max_buffered_response_bytes == 0
        || config.max_buffered_response_bytes > tokio::sync::Semaphore::MAX_PERMITS
    {
        return Err(PlatformError::internal(format!(
            "max_buffered_response_bytes must be within 1..={}, got {}",
            tokio::sync::Semaphore::MAX_PERMITS,
            config.max_buffered_response_bytes,
        )));
    }
    if config.max_queue_wait.is_zero() {
        return Err(PlatformError::internal(
            "max_queue_wait must be greater than 0",
        ));
    }
    if config.request_wall_timeout.is_zero() {
        return Err(PlatformError::internal(
            "request_wall_timeout must be greater than 0",
        ));
    }
    if config.max_request_body_bytes == 0 {
        return Err(PlatformError::internal(
            "max_request_body_bytes must be greater than 0",
        ));
    }
    if config.max_response_body_bytes == 0 {
        return Err(PlatformError::internal(
            "max_response_body_bytes must be greater than 0",
        ));
    }
    if config.isolate_startup_timeout.is_zero() {
        return Err(PlatformError::internal(
            "isolate_startup_timeout must be greater than 0",
        ));
    }
    if config.min_isolates > config.max_isolates {
        return Err(PlatformError::internal(
            "min_isolates cannot exceed max_isolates",
        ));
    }
    if config.min_isolates > config.max_global_isolates {
        return Err(PlatformError::internal(
            "min_isolates cannot exceed max_global_isolates",
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
    if config.temporary_worker_ttl.is_zero() {
        return Err(PlatformError::internal(
            "temporary_worker_ttl must be greater than 0",
        ));
    }
    if config.temporary_worker_ttl.as_millis() > i64::MAX as u128 {
        return Err(PlatformError::internal("temporary_worker_ttl is too large"));
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
    Ok(())
}
