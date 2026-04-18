use super::*;

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct TestKvProfileMetric {
    pub(crate) calls: u64,
    pub(crate) total_us: u64,
    pub(crate) total_items: u64,
    pub(crate) max_us: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct TestKvProfileSnapshot {
    pub(crate) enabled: bool,
    pub(crate) op_get: TestKvProfileMetric,
    pub(crate) js_cache_hit: TestKvProfileMetric,
    pub(crate) js_cache_miss: TestKvProfileMetric,
    pub(crate) js_cache_stale: TestKvProfileMetric,
    pub(crate) js_cache_fill: TestKvProfileMetric,
    pub(crate) js_cache_invalidate: TestKvProfileMetric,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct TestKvProfileEnvelope {
    pub(crate) ok: bool,
    pub(crate) snapshot: Option<TestKvProfileSnapshot>,
    pub(crate) error: String,
}

pub(crate) fn decode_kv_profile(output: WorkerOutput) -> TestKvProfileSnapshot {
    let envelope: TestKvProfileEnvelope = crate::json::from_string(
        String::from_utf8(output.body).expect("profile body should be utf8"),
    )
    .expect("profile response should parse");
    assert!(
        envelope.ok,
        "profile route should succeed: {}",
        envelope.error
    );
    envelope
        .snapshot
        .expect("profile snapshot should be present")
}

#[derive(Deserialize)]
pub(crate) struct LoopTraceState {
    pub(crate) total_calls: usize,
    pub(crate) trace_calls: usize,
}

#[derive(Deserialize)]
pub(crate) struct FrozenTimeState {
    pub(crate) now0: i64,
    pub(crate) now1: i64,
    pub(crate) now2: i64,
    pub(crate) perf0: f64,
    pub(crate) perf1: f64,
    pub(crate) perf2: f64,
    pub(crate) guard: i64,
}

#[derive(Deserialize)]
pub(crate) struct CryptoState {
    pub(crate) random_length: usize,
    pub(crate) random_non_zero: bool,
    pub(crate) uuid: String,
    pub(crate) digest_length: usize,
}
