use super::*;

pub(crate) fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

pub(crate) fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

#[derive(Clone, Copy)]
pub(crate) enum MemoryKeyMode {
    Pool,
    Unique,
    SameShard,
    CrossShard,
}

impl MemoryKeyMode {
    pub(crate) fn from_env() -> Self {
        match std::env::var("DD_BENCH_MEMORY_KEY_MODE")
            .ok()
            .unwrap_or_else(|| "pool".to_string())
            .to_lowercase()
            .as_str()
        {
            "pool" => Self::Pool,
            "unique" => Self::Unique,
            "same-shard" => Self::SameShard,
            "cross-shard" => Self::CrossShard,
            _ => Self::Pool,
        }
    }
}

pub(crate) fn env_mode() -> Option<String> {
    std::env::var("DD_BENCH_MODE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) const MEMORY_NAMESPACE_SHARDS: usize = 16;

pub(crate) fn env_duration_ms(name: &str, default: u64) -> Duration {
    Duration::from_millis(
        std::env::var(name)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(default),
    )
}

pub(crate) fn bench_options_from_env() -> BenchOptions {
    BenchOptions {
        request_timeout: env_duration_ms("DD_BENCH_REQUEST_TIMEOUT_MS", 10_000),
        verify_timeout: env_duration_ms("DD_BENCH_VERIFY_TIMEOUT_MS", 5_000),
        watchdog_interval: env_duration_ms("DD_BENCH_WATCHDOG_INTERVAL_MS", 1_000),
        watchdog_silence_timeout: env_duration_ms("DD_BENCH_WATCHDOG_SILENCE_MS", 2_000),
        wide_key_space: env_usize("DD_BENCH_WIDE_KEY_SPACE", 256),
    }
}
