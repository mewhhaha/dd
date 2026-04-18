use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct CryptoDigestPayload {
    algorithm: String,
    data: Vec<u8>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CryptoDigestResult {
    ok: bool,
    digest: Vec<u8>,
    error: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CryptoHmacPayload {
    hash: String,
    key: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CryptoHmacVerifyPayload {
    hash: String,
    key: Vec<u8>,
    data: Vec<u8>,
    signature: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CryptoAesGcmPayload {
    key: Vec<u8>,
    iv: Vec<u8>,
    data: Vec<u8>,
    #[serde(default)]
    additional_data: Vec<u8>,
    #[serde(default = "default_tag_length_bits")]
    tag_length: u8,
}

#[derive(Debug, Serialize)]
pub(crate) struct CryptoBytesResult {
    ok: bool,
    bytes: Vec<u8>,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CryptoBoolResult {
    ok: bool,
    value: bool,
    error: String,
}

pub(crate) enum CryptoDigestAlgorithm {
    Sha1,
    Sha256,
    Sha384,
    Sha512,
}

pub(crate) fn default_tag_length_bits() -> u8 {
    128
}

pub(crate) static PROCESS_MONO_START: OnceLock<Instant> = OnceLock::new();

#[deno_core::op2]
#[serde]
pub(crate) fn op_crypto_digest(#[string] payload: String) -> CryptoDigestResult {
    let payload = match crate::json::from_string::<CryptoDigestPayload>(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoDigestResult {
                ok: false,
                digest: Vec::new(),
                error: format!("invalid digest payload: {error}"),
            };
        }
    };

    let algorithm = match parse_crypto_digest_algorithm(&payload.algorithm) {
        Some(value) => value,
        None => {
            return CryptoDigestResult {
                ok: false,
                digest: Vec::new(),
                error: format!("unsupported digest algorithm: {}", payload.algorithm),
            };
        }
    };

    let digest = match algorithm {
        CryptoDigestAlgorithm::Sha1 => {
            let mut hasher = Sha1::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut hasher = Sha384::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut hasher = Sha512::new();
            hasher.update(&payload.data);
            hasher.finalize().to_vec()
        }
    };

    CryptoDigestResult {
        ok: true,
        digest,
        error: String::new(),
    }
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_crypto_hmac_sign(#[string] payload: String) -> CryptoBytesResult {
    let payload: CryptoHmacPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("invalid hmac sign payload: {error}"),
            };
        }
    };

    let hash = match parse_crypto_digest_algorithm(&payload.hash) {
        Some(value) => value,
        None => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("unsupported hmac hash algorithm: {}", payload.hash),
            };
        }
    };

    match compute_hmac(hash, &payload.key, &payload.data) {
        Ok(bytes) => CryptoBytesResult {
            ok: true,
            bytes,
            error: String::new(),
        },
        Err(error) => CryptoBytesResult {
            ok: false,
            bytes: Vec::new(),
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_crypto_hmac_verify(#[string] payload: String) -> CryptoBoolResult {
    let payload: CryptoHmacVerifyPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBoolResult {
                ok: false,
                value: false,
                error: format!("invalid hmac verify payload: {error}"),
            };
        }
    };

    let hash = match parse_crypto_digest_algorithm(&payload.hash) {
        Some(value) => value,
        None => {
            return CryptoBoolResult {
                ok: false,
                value: false,
                error: format!("unsupported hmac hash algorithm: {}", payload.hash),
            };
        }
    };

    match verify_hmac(hash, &payload.key, &payload.data, &payload.signature) {
        Ok(value) => CryptoBoolResult {
            ok: true,
            value,
            error: String::new(),
        },
        Err(error) => CryptoBoolResult {
            ok: false,
            value: false,
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_crypto_aes_gcm_encrypt(#[string] payload: String) -> CryptoBytesResult {
    let payload: CryptoAesGcmPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("invalid aes-gcm encrypt payload: {error}"),
            };
        }
    };

    match aes_gcm_encrypt(&payload) {
        Ok(bytes) => CryptoBytesResult {
            ok: true,
            bytes,
            error: String::new(),
        },
        Err(error) => CryptoBytesResult {
            ok: false,
            bytes: Vec::new(),
            error,
        },
    }
}

#[deno_core::op2]
#[serde]
pub(crate) fn op_crypto_aes_gcm_decrypt(#[string] payload: String) -> CryptoBytesResult {
    let payload: CryptoAesGcmPayload = match crate::json::from_string(payload) {
        Ok(value) => value,
        Err(error) => {
            return CryptoBytesResult {
                ok: false,
                bytes: Vec::new(),
                error: format!("invalid aes-gcm decrypt payload: {error}"),
            };
        }
    };

    match aes_gcm_decrypt(&payload) {
        Ok(bytes) => CryptoBytesResult {
            ok: true,
            bytes,
            error: String::new(),
        },
        Err(error) => CryptoBytesResult {
            ok: false,
            bytes: Vec::new(),
            error,
        },
    }
}

pub(crate) fn parse_crypto_digest_algorithm(value: &str) -> Option<CryptoDigestAlgorithm> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "SHA-1" | "SHA1" => Some(CryptoDigestAlgorithm::Sha1),
        "SHA-256" | "SHA256" => Some(CryptoDigestAlgorithm::Sha256),
        "SHA-384" | "SHA384" => Some(CryptoDigestAlgorithm::Sha384),
        "SHA-512" | "SHA512" => Some(CryptoDigestAlgorithm::Sha512),
        _ => None,
    }
}

pub(crate) fn compute_hmac(
    algorithm: CryptoDigestAlgorithm,
    key: &[u8],
    data: &[u8],
) -> std::result::Result<Vec<u8>, String> {
    match algorithm {
        CryptoDigestAlgorithm::Sha1 => {
            let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut mac = <Hmac<Sha384> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.finalize().into_bytes().to_vec())
        }
    }
}

pub(crate) fn verify_hmac(
    algorithm: CryptoDigestAlgorithm,
    key: &[u8],
    data: &[u8],
    signature: &[u8],
) -> std::result::Result<bool, String> {
    match algorithm {
        CryptoDigestAlgorithm::Sha1 => {
            let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha256 => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha384 => {
            let mut mac = <Hmac<Sha384> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
        CryptoDigestAlgorithm::Sha512 => {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key)
                .map_err(|error| format!("hmac init failed: {error}"))?;
            mac.update(data);
            Ok(mac.verify_slice(signature).is_ok())
        }
    }
}

pub(crate) fn aes_gcm_encrypt(
    payload: &CryptoAesGcmPayload,
) -> std::result::Result<Vec<u8>, String> {
    if payload.iv.len() != 12 {
        return Err("AES-GCM iv must be exactly 12 bytes in v1".to_string());
    }
    if payload.tag_length != 128 {
        return Err("AES-GCM tagLength must be 128 in v1".to_string());
    }

    let nonce = Nonce::from_slice(&payload.iv);
    match payload.key.len() {
        16 => {
            let cipher = Aes128Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-128-GCM key init failed: {error}"))?;
            cipher
                .encrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-128-GCM encrypt failed: {error}"))
        }
        32 => {
            let cipher = Aes256Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-256-GCM key init failed: {error}"))?;
            cipher
                .encrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-256-GCM encrypt failed: {error}"))
        }
        _ => Err("AES-GCM key length must be 16 or 32 bytes".to_string()),
    }
}

pub(crate) fn aes_gcm_decrypt(
    payload: &CryptoAesGcmPayload,
) -> std::result::Result<Vec<u8>, String> {
    if payload.iv.len() != 12 {
        return Err("AES-GCM iv must be exactly 12 bytes in v1".to_string());
    }
    if payload.tag_length != 128 {
        return Err("AES-GCM tagLength must be 128 in v1".to_string());
    }

    let nonce = Nonce::from_slice(&payload.iv);
    match payload.key.len() {
        16 => {
            let cipher = Aes128Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-128-GCM key init failed: {error}"))?;
            cipher
                .decrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-128-GCM decrypt failed: {error}"))
        }
        32 => {
            let cipher = Aes256Gcm::new_from_slice(&payload.key)
                .map_err(|error| format!("AES-256-GCM key init failed: {error}"))?;
            cipher
                .decrypt(
                    nonce,
                    Payload {
                        msg: &payload.data,
                        aad: &payload.additional_data,
                    },
                )
                .map_err(|error| format!("AES-256-GCM decrypt failed: {error}"))
        }
        _ => Err("AES-GCM key length must be 16 or 32 bytes".to_string()),
    }
}
