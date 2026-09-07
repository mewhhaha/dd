use super::*;

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
    pub(crate) digest_hex: String,
    pub(crate) hmac_signature_length: usize,
    pub(crate) hmac_verified: bool,
    pub(crate) aes_ciphertext_length: usize,
    pub(crate) aes_roundtrip: String,
    pub(crate) asymmetric_signature_length: usize,
    pub(crate) asymmetric_verified: bool,
}
