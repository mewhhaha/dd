use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryKeyMode {
    Pool,
    Unique,
    SameShard,
    CrossShard,
    SkewedHotspot,
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
            "skewed-hotspot" => Self::SkewedHotspot,
            _ => Self::Pool,
        }
    }
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
