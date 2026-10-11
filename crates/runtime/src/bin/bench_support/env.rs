//! Environment settings shared by the benchmark binaries. Each binary uses
//! only some of them.
#![allow(dead_code)]

use std::time::Duration;

/// A positive integer from `name`, or `default` when it is unset, zero or
/// not a number.
pub(crate) fn env_usize(name: &str, default: usize) -> usize {
    env_optional_usize(name).unwrap_or(default)
}

/// A positive integer from `name`, if it holds one.
pub(crate) fn env_optional_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

/// A positive integer from `name`, or `default` when it is unset, zero or
/// not a number.
pub(crate) fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Milliseconds from `name`, zero included, or `default` milliseconds.
pub(crate) fn env_duration_ms(name: &str, default: u64) -> Duration {
    Duration::from_millis(
        std::env::var(name)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(default),
    )
}

/// The trimmed value of `name`, if it is set and not blank.
pub(crate) fn env_name(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Whether `name` is set to 1, true, yes or on, in any case.
pub(crate) fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}
