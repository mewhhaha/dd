// Copyright 2018-2026 the Deno authors. MIT license.

use dd_v8::OpError;
use std::cell::RefCell;
use std::num::NonZeroU32;
use std::rc::Rc;

use aes_kw::KekAes128;
use aes_kw::KekAes192;
use aes_kw::KekAes256;
use aws_lc_rs::digest;
use aws_lc_rs::hkdf;
use aws_lc_rs::hmac::Algorithm as HmacAlgorithm;
use aws_lc_rs::hmac::Key as HmacKey;
use aws_lc_rs::pbkdf2;
use base64::Engine;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use dd_v8::JsBuffer;
use dd_v8::OpState;
use dd_v8::ToJsBuffer as Uint8Array;
use p256::ecdsa::Signature as P256Signature;
use p256::ecdsa::SigningKey as P256SigningKey;
use p256::ecdsa::VerifyingKey as P256VerifyingKey;
use p256::elliptic_curve::sec1::FromEncodedPoint;
use p256::pkcs8::DecodePrivateKey;
use p384::ecdsa::Signature as P384Signature;
use p384::ecdsa::SigningKey as P384SigningKey;
use p384::ecdsa::VerifyingKey as P384VerifyingKey;
use rand::Rng;
use rand::rngs::OsRng;
use rand::rngs::StdRng;
use rand::thread_rng;
use rsa::Pss;
use rsa::RsaPrivateKey;
use rsa::RsaPublicKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::signature::SignatureEncoding;
use rsa::signature::Signer;
use rsa::signature::Verifier;
use rsa::traits::SignatureScheme;
use serde::Deserialize;
use sha1::Sha1;
use sha2_10::Digest;
use sha2_10::Sha256;
use sha2_10::Sha384;
use sha2_10::Sha512;
use signature::hazmat::PrehashSigner;
use signature::hazmat::PrehashVerifier;
use tokio::task::spawn_blocking;

mod decrypt;
mod ed25519;
mod encrypt;
mod export_key;
mod generate_key;
mod import_key;
mod key;
mod shared;
mod x25519;
mod x448;

use self::decrypt::op_crypto_decrypt;
use self::ed25519::*;
use self::encrypt::op_crypto_encrypt;
use self::export_key::op_crypto_export_key;
use self::generate_key::op_crypto_generate_key;
use self::import_key::op_crypto_import_key;
use self::key::Algorithm;
use self::key::CryptoHash;
use self::key::CryptoNamedCurve;
use self::key::HkdfOutput;
use self::shared::SharedError;
use self::shared::V8RawKeyData;
use self::x448::*;
use self::x25519::*;
use dd_v8::builtins::with_buffer_mut;
use dd_v8::{OpDecl, op_async, op_raw, op_sync, v8};

pub(crate) fn ops() -> Vec<OpDecl> {
    vec![
        op_raw!(op_crypto_get_random_values),
        op_async!(op_crypto_generate_key),
        op_async!(op_crypto_sign_key),
        op_async!(op_crypto_verify_key),
        op_async!(op_crypto_derive_bits),
        op_sync!(op_crypto_import_key),
        op_sync!(op_crypto_export_key),
        op_async!(op_crypto_encrypt),
        op_async!(op_crypto_decrypt),
        op_async!(op_crypto_subtle_digest),
        op_sync!(op_crypto_random_uuid),
        op_sync!(op_crypto_wrap_key),
        op_sync!(op_crypto_unwrap_key),
        op_sync!(op_crypto_base64url_decode),
        op_sync!(op_crypto_base64url_encode),
        op_sync!(op_crypto_generate_x25519_keypair),
        op_sync!(op_crypto_x25519_public_key),
        op_sync!(op_crypto_derive_bits_x25519),
        op_sync!(op_crypto_import_spki_x25519),
        op_sync!(op_crypto_import_pkcs8_x25519),
        op_sync!(op_crypto_export_spki_x25519),
        op_sync!(op_crypto_export_pkcs8_x25519),
        op_sync!(op_crypto_generate_x448_keypair),
        op_sync!(op_crypto_derive_bits_x448),
        op_sync!(op_crypto_import_spki_x448),
        op_sync!(op_crypto_import_pkcs8_x448),
        op_sync!(op_crypto_export_spki_x448),
        op_sync!(op_crypto_export_pkcs8_x448),
        op_sync!(op_crypto_generate_ed25519_keypair),
        op_sync!(op_crypto_import_spki_ed25519),
        op_sync!(op_crypto_import_pkcs8_ed25519),
        op_sync!(op_crypto_sign_ed25519),
        op_sync!(op_crypto_verify_ed25519),
        op_sync!(op_crypto_export_spki_ed25519),
        op_sync!(op_crypto_export_pkcs8_ed25519),
        op_sync!(op_crypto_jwk_x_ed25519),
    ]
}

fn not_supported() -> OpError {
    OpError::custom(
        "DOMExceptionNotSupportedError",
        "The operation is not supported",
    )
}

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error(transparent)]
    General(#[from] SharedError),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Der(#[from] rsa::pkcs1::der::Error),
    #[error("Missing argument hash")]
    MissingArgumentHash,
    #[error("{0}")]
    DerivationRefused(&'static str),
    #[error("Missing argument saltLength")]
    MissingArgumentSaltLength,
    #[error("unsupported algorithm")]
    UnsupportedAlgorithm,
    #[error(transparent)]
    KeyRejected(#[from] aws_lc_rs::error::KeyRejected),
    #[error(transparent)]
    Rsa(#[from] rsa::Error),
    #[error(transparent)]
    Pkcs1(#[from] rsa::pkcs1::Error),
    #[error(transparent)]
    Unspecified(#[from] aws_lc_rs::error::Unspecified),
    #[error("Invalid key format")]
    InvalidKeyFormat,
    #[error(transparent)]
    P256Ecdsa(#[from] p256::ecdsa::Error),
    #[error("Unexpected error decoding private key")]
    DecodePrivateKey,
    #[error("Missing argument publicKey")]
    MissingArgumentPublicKey,
    #[error("Missing argument namedCurve")]
    MissingArgumentNamedCurve,
    #[error("Missing argument info")]
    MissingArgumentInfo,
    #[error("The length provided for HKDF is too large")]
    HKDFLengthTooLarge,
    #[error(transparent)]
    Base64Decode(#[from] base64::DecodeError),
    #[error("Data must be multiple of 8 bytes")]
    DataInvalidSize,
    #[error("Invalid key length")]
    InvalidKeyLength,
    #[error("encryption error")]
    EncryptionError,
    #[error("decryption error - integrity check failed")]
    DecryptionError,
    #[error(
        "The ArrayBufferView's byte length ({0}) exceeds the number of bytes of entropy available via this API (65536)"
    )]
    ArrayBufferViewLengthExceeded(usize),
    #[error(transparent)]
    Other(#[from] OpError),
}

pub fn op_crypto_base64url_decode(
    _state: &mut OpState,
    data: String,
) -> Result<Uint8Array, CryptoError> {
    let data: Vec<u8> = BASE64_URL_SAFE_NO_PAD.decode(data)?;
    Ok(data.into())
}

pub fn op_crypto_base64url_encode(_state: &mut OpState, data: JsBuffer) -> String {
    let data: String = BASE64_URL_SAFE_NO_PAD.encode(data);
    data
}

/// `op_crypto_get_random_values(view)`: fills the view in place.
fn op_crypto_get_random_values<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    _rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let op_state = dd_v8::runtime_op_state(scope);
    let filled = with_buffer_mut(args.get(0), |out| {
        if out.len() > 65536 {
            return Err(CryptoError::ArrayBufferViewLengthExceeded(out.len()));
        }
        let mut op_state = op_state.borrow_mut();
        if let Some(seeded_rng) = op_state.try_borrow_mut::<StdRng>() {
            seeded_rng.fill(out);
        } else {
            thread_rng().fill(out);
        }
        Ok(())
    });
    let error = match filled {
        Some(Ok(())) => return,
        Some(Err(error)) => OpError::from(error),
        None => OpError::type_error("expected an integer TypedArray"),
    };
    let exception = error.to_exception(scope);
    scope.throw_exception(exception);
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyType {
    Secret,
    Private,
    Public,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct KeyData {
    r#type: KeyType,
    data: JsBuffer,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignArg {
    key: KeyData,
    algorithm: Algorithm,
    salt_length: Option<u32>,
    hash: Option<CryptoHash>,
    named_curve: Option<CryptoNamedCurve>,
}

pub async fn op_crypto_sign_key(
    _state: Rc<RefCell<OpState>>,
    args: SignArg,
    zero_copy: JsBuffer,
) -> Result<Uint8Array, CryptoError> {
    spawn_blocking(move || {
        let data = &*zero_copy;
        let algorithm = args.algorithm;

        let signature = match algorithm {
            Algorithm::RsassaPkcs1v15 => {
                use rsa::pkcs1v15::SigningKey;
                let private_key = RsaPrivateKey::from_pkcs1_der(&args.key.data)?;
                match args.hash.ok_or_else(|| CryptoError::MissingArgumentHash)? {
                    CryptoHash::Sha1 => {
                        let signing_key = SigningKey::<Sha1>::new(private_key);
                        signing_key.sign(data)
                    }
                    CryptoHash::Sha256 => {
                        let signing_key = SigningKey::<Sha256>::new(private_key);
                        signing_key.sign(data)
                    }
                    CryptoHash::Sha384 => {
                        let signing_key = SigningKey::<Sha384>::new(private_key);
                        signing_key.sign(data)
                    }
                    CryptoHash::Sha512 => {
                        let signing_key = SigningKey::<Sha512>::new(private_key);
                        signing_key.sign(data)
                    }
                }
                .to_vec()
            }
            Algorithm::RsaPss => {
                let private_key = RsaPrivateKey::from_pkcs1_der(&args.key.data)?;

                let salt_len = args
                    .salt_length
                    .ok_or_else(|| CryptoError::MissingArgumentSaltLength)?
                    as usize;

                let mut rng = OsRng;
                match args.hash.ok_or_else(|| CryptoError::MissingArgumentHash)? {
                    CryptoHash::Sha1 => {
                        let signing_key = Pss::new_with_salt::<Sha1>(salt_len);
                        let hashed = Sha1::digest(data);
                        signing_key.sign(Some(&mut rng), &private_key, &hashed)?
                    }
                    CryptoHash::Sha256 => {
                        let signing_key = Pss::new_with_salt::<Sha256>(salt_len);
                        let hashed = Sha256::digest(data);
                        signing_key.sign(Some(&mut rng), &private_key, &hashed)?
                    }
                    CryptoHash::Sha384 => {
                        let signing_key = Pss::new_with_salt::<Sha384>(salt_len);
                        let hashed = Sha384::digest(data);
                        signing_key.sign(Some(&mut rng), &private_key, &hashed)?
                    }
                    CryptoHash::Sha512 => {
                        let signing_key = Pss::new_with_salt::<Sha512>(salt_len);
                        let hashed = Sha512::digest(data);
                        signing_key.sign(Some(&mut rng), &private_key, &hashed)?
                    }
                }
                .to_vec()
            }
            Algorithm::Ecdsa => {
                let hash = args.hash.ok_or_else(|| CryptoError::MissingArgumentHash)?;
                let named_curve = args.named_curve.ok_or_else(not_supported)?;
                match named_curve {
                    CryptoNamedCurve::P256 => {
                        // Decode PKCS#8 private key.
                        let secret_key = p256::SecretKey::from_pkcs8_der(&args.key.data)
                            .map_err(|_| CryptoError::InvalidKeyFormat)?;
                        let signing_key = P256SigningKey::from(secret_key);
                        let prehash = match hash {
                            CryptoHash::Sha1 => sha1::Sha1::digest(data).to_vec(),
                            CryptoHash::Sha256 => sha2_10::Sha256::digest(data).to_vec(),
                            CryptoHash::Sha384 => sha2_10::Sha384::digest(data).to_vec(),
                            CryptoHash::Sha512 => sha2_10::Sha512::digest(data).to_vec(),
                        };
                        // Sign the prehashed message, producing a raw r||s signature.
                        let signature: P256Signature = signing_key.sign_prehash(&prehash)?;
                        signature.to_bytes().to_vec()
                    }
                    CryptoNamedCurve::P384 => {
                        let secret_key = p384::SecretKey::from_pkcs8_der(&args.key.data)
                            .map_err(|_| CryptoError::InvalidKeyFormat)?;
                        let signing_key = P384SigningKey::from(secret_key);
                        let prehash = match hash {
                            CryptoHash::Sha1 => sha1::Sha1::digest(data).to_vec(),
                            CryptoHash::Sha256 => sha2_10::Sha256::digest(data).to_vec(),
                            CryptoHash::Sha384 => sha2_10::Sha384::digest(data).to_vec(),
                            CryptoHash::Sha512 => sha2_10::Sha512::digest(data).to_vec(),
                        };
                        let signature: P384Signature = signing_key.sign_prehash(&prehash)?;
                        signature.to_bytes().to_vec()
                    }
                }
            }
            Algorithm::Hmac => {
                let hash: HmacAlgorithm = args.hash.ok_or_else(not_supported)?.into();

                let key = HmacKey::new(hash, &args.key.data);

                let signature = aws_lc_rs::hmac::sign(&key, data);
                signature.as_ref().to_vec()
            }
            _ => return Err(CryptoError::UnsupportedAlgorithm),
        };

        Ok(signature.into())
    })
    .await?
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifyArg {
    key: KeyData,
    algorithm: Algorithm,
    salt_length: Option<u32>,
    hash: Option<CryptoHash>,
    signature: JsBuffer,
    named_curve: Option<CryptoNamedCurve>,
}

pub async fn op_crypto_verify_key(
    _state: Rc<RefCell<OpState>>,
    args: VerifyArg,
    zero_copy: JsBuffer,
) -> Result<bool, CryptoError> {
    spawn_blocking(move || {
        let data = &*zero_copy;
        let algorithm = args.algorithm;

        let verification = match algorithm {
            Algorithm::RsassaPkcs1v15 => {
                use rsa::pkcs1v15::Signature;
                use rsa::pkcs1v15::VerifyingKey;
                let public_key = read_rsa_public_key(args.key)?;
                let signature: Signature = (&*args.signature).try_into()?;
                match args.hash.ok_or_else(|| CryptoError::MissingArgumentHash)? {
                    CryptoHash::Sha1 => {
                        let verifying_key = VerifyingKey::<Sha1>::new(public_key);
                        verifying_key.verify(data, &signature).is_ok()
                    }
                    CryptoHash::Sha256 => {
                        let verifying_key = VerifyingKey::<Sha256>::new(public_key);
                        verifying_key.verify(data, &signature).is_ok()
                    }
                    CryptoHash::Sha384 => {
                        let verifying_key = VerifyingKey::<Sha384>::new(public_key);
                        verifying_key.verify(data, &signature).is_ok()
                    }
                    CryptoHash::Sha512 => {
                        let verifying_key = VerifyingKey::<Sha512>::new(public_key);
                        verifying_key.verify(data, &signature).is_ok()
                    }
                }
            }
            Algorithm::RsaPss => {
                let public_key = read_rsa_public_key(args.key)?;
                let signature = args.signature.as_ref();

                let salt_len = args
                    .salt_length
                    .ok_or_else(|| CryptoError::MissingArgumentSaltLength)?
                    as usize;

                match args.hash.ok_or_else(|| CryptoError::MissingArgumentHash)? {
                    CryptoHash::Sha1 => {
                        let pss = Pss::new_with_salt::<Sha1>(salt_len);
                        let hashed = Sha1::digest(data);
                        pss.verify(&public_key, &hashed, signature).is_ok()
                    }
                    CryptoHash::Sha256 => {
                        let pss = Pss::new_with_salt::<Sha256>(salt_len);
                        let hashed = Sha256::digest(data);
                        pss.verify(&public_key, &hashed, signature).is_ok()
                    }
                    CryptoHash::Sha384 => {
                        let pss = Pss::new_with_salt::<Sha384>(salt_len);
                        let hashed = Sha384::digest(data);
                        pss.verify(&public_key, &hashed, signature).is_ok()
                    }
                    CryptoHash::Sha512 => {
                        let pss = Pss::new_with_salt::<Sha512>(salt_len);
                        let hashed = Sha512::digest(data);
                        pss.verify(&public_key, &hashed, signature).is_ok()
                    }
                }
            }
            Algorithm::Hmac => {
                let hash: HmacAlgorithm = args.hash.ok_or_else(not_supported)?.into();
                let key = HmacKey::new(hash, &args.key.data);
                aws_lc_rs::hmac::verify(&key, data, &args.signature).is_ok()
            }
            Algorithm::Ecdsa => {
                let hash = args.hash.ok_or_else(|| CryptoError::MissingArgumentHash)?;
                let named_curve = args.named_curve.ok_or_else(not_supported)?;
                match named_curve {
                    CryptoNamedCurve::P256 => {
                        let verifying_key = match args.key.r#type {
                            KeyType::Public => P256VerifyingKey::from_sec1_bytes(&args.key.data)
                                .map_err(|_| CryptoError::InvalidKeyFormat)?,
                            KeyType::Private => {
                                let secret_key = p256::SecretKey::from_pkcs8_der(&args.key.data)
                                    .map_err(|_| CryptoError::InvalidKeyFormat)?;
                                let signing_key = P256SigningKey::from(secret_key);
                                *signing_key.verifying_key()
                            }
                            _ => return Err(CryptoError::InvalidKeyFormat),
                        };
                        match P256Signature::from_slice(&args.signature) {
                            Ok(signature) => {
                                let prehash = match hash {
                                    CryptoHash::Sha1 => sha1::Sha1::digest(data).to_vec(),
                                    CryptoHash::Sha256 => sha2_10::Sha256::digest(data).to_vec(),
                                    CryptoHash::Sha384 => sha2_10::Sha384::digest(data).to_vec(),
                                    CryptoHash::Sha512 => sha2_10::Sha512::digest(data).to_vec(),
                                };
                                verifying_key.verify_prehash(&prehash, &signature).is_ok()
                            }
                            _ => false,
                        }
                    }
                    CryptoNamedCurve::P384 => {
                        let verifying_key = match args.key.r#type {
                            KeyType::Public => P384VerifyingKey::from_sec1_bytes(&args.key.data)
                                .map_err(|_| CryptoError::InvalidKeyFormat)?,
                            KeyType::Private => {
                                let secret_key = p384::SecretKey::from_pkcs8_der(&args.key.data)
                                    .map_err(|_| CryptoError::InvalidKeyFormat)?;
                                let signing_key = P384SigningKey::from(secret_key);
                                *signing_key.verifying_key()
                            }
                            _ => return Err(CryptoError::InvalidKeyFormat),
                        };
                        match P384Signature::from_slice(&args.signature) {
                            Ok(signature) => {
                                let prehash = match hash {
                                    CryptoHash::Sha1 => sha1::Sha1::digest(data).to_vec(),
                                    CryptoHash::Sha256 => sha2_10::Sha256::digest(data).to_vec(),
                                    CryptoHash::Sha384 => sha2_10::Sha384::digest(data).to_vec(),
                                    CryptoHash::Sha512 => sha2_10::Sha512::digest(data).to_vec(),
                                };
                                verifying_key.verify_prehash(&prehash, &signature).is_ok()
                            }
                            _ => false,
                        }
                    }
                }
            }
            _ => return Err(CryptoError::UnsupportedAlgorithm),
        };

        Ok(verification)
    })
    .await?
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeriveKeyArg {
    key: KeyData,
    algorithm: Algorithm,
    hash: Option<CryptoHash>,
    length: usize,
    iterations: Option<u32>,
    // ECDH
    public_key: Option<KeyData>,
    named_curve: Option<CryptoNamedCurve>,
    // HKDF
    info: Option<JsBuffer>,
}

/// The most PBKDF2 may derive in one call, in bytes.
const PBKDF2_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
/// The most HMAC rounds (iterations times output blocks) one PBKDF2 call
/// may run: seconds of CPU, far above any real use (OWASP suggests 600,000
/// iterations for one block). Derivation runs on a blocking thread that
/// nothing can stop once it starts, so the work is bounded up front.
const PBKDF2_MAX_ROUNDS: u64 = 50_000_000;

fn pbkdf2_within_limits(
    output_bytes: usize,
    hash_bytes: usize,
    iterations: u32,
) -> Result<(), CryptoError> {
    if output_bytes > PBKDF2_MAX_OUTPUT_BYTES {
        return Err(CryptoError::DerivationRefused(
            "PBKDF2 length exceeds the runtime's 1 MiB limit",
        ));
    }
    let blocks = output_bytes.div_ceil(hash_bytes) as u64;
    if blocks.saturating_mul(u64::from(iterations)) > PBKDF2_MAX_ROUNDS {
        return Err(CryptoError::DerivationRefused(
            "PBKDF2 iterations times output blocks exceeds the runtime's limit of 50,000,000",
        ));
    }
    Ok(())
}

pub async fn op_crypto_derive_bits(
    _state: Rc<RefCell<OpState>>,
    args: DeriveKeyArg,
    zero_copy: Option<JsBuffer>,
) -> Result<Uint8Array, CryptoError> {
    spawn_blocking(move || {
        let algorithm = args.algorithm;
        match algorithm {
            Algorithm::Pbkdf2 => {
                let zero_copy = zero_copy.ok_or_else(not_supported)?;
                let salt = &*zero_copy;
                if args.length == 0 || !args.length.is_multiple_of(8) {
                    return Err(CryptoError::DerivationRefused(
                        "PBKDF2 length must be a non-zero multiple of 8",
                    ));
                }

                let (algorithm, hash_bytes) = match args.hash.ok_or_else(not_supported)? {
                    CryptoHash::Sha1 => (pbkdf2::PBKDF2_HMAC_SHA1, 20),
                    CryptoHash::Sha256 => (pbkdf2::PBKDF2_HMAC_SHA256, 32),
                    CryptoHash::Sha384 => (pbkdf2::PBKDF2_HMAC_SHA384, 48),
                    CryptoHash::Sha512 => (pbkdf2::PBKDF2_HMAC_SHA512, 64),
                };

                let iterations = NonZeroU32::new(args.iterations.ok_or_else(not_supported)?)
                    .ok_or(CryptoError::DerivationRefused(
                        "PBKDF2 iterations must not be zero",
                    ))?;
                pbkdf2_within_limits(args.length / 8, hash_bytes, iterations.get())?;
                let secret = args.key.data;
                let mut out = vec![0; args.length / 8];
                pbkdf2::derive(algorithm, iterations, salt, &secret, &mut out);
                Ok(out.into())
            }
            Algorithm::Ecdh => {
                let named_curve = args
                    .named_curve
                    .ok_or_else(|| CryptoError::MissingArgumentNamedCurve)?;

                let public_key = args
                    .public_key
                    .ok_or_else(|| CryptoError::MissingArgumentPublicKey)?;

                match named_curve {
                    CryptoNamedCurve::P256 => {
                        let secret_key = p256::SecretKey::from_pkcs8_der(&args.key.data)
                            .map_err(|_| CryptoError::DecodePrivateKey)?;

                        let public_key = match public_key.r#type {
                            KeyType::Private => p256::SecretKey::from_pkcs8_der(&public_key.data)
                                .map_err(|_| CryptoError::DecodePrivateKey)?
                                .public_key(),
                            KeyType::Public => {
                                let point = p256::EncodedPoint::from_bytes(public_key.data)
                                    .map_err(|_| CryptoError::DecodePrivateKey)?;

                                let pk = p256::PublicKey::from_encoded_point(&point);
                                // pk is a constant time Option.
                                if pk.is_some().into() {
                                    pk.unwrap()
                                } else {
                                    return Err(CryptoError::DecodePrivateKey);
                                }
                            }
                            _ => unreachable!(),
                        };

                        let shared_secret = p256::elliptic_curve::ecdh::diffie_hellman(
                            secret_key.to_nonzero_scalar(),
                            public_key.as_affine(),
                        );

                        // raw serialized x-coordinate of the computed point
                        Ok(shared_secret.raw_secret_bytes().to_vec().into())
                    }
                    CryptoNamedCurve::P384 => {
                        let secret_key = p384::SecretKey::from_pkcs8_der(&args.key.data)
                            .map_err(|_| CryptoError::DecodePrivateKey)?;

                        let public_key = match public_key.r#type {
                            KeyType::Private => p384::SecretKey::from_pkcs8_der(&public_key.data)
                                .map_err(|_| CryptoError::DecodePrivateKey)?
                                .public_key(),
                            KeyType::Public => {
                                let point = p384::EncodedPoint::from_bytes(public_key.data)
                                    .map_err(|_| CryptoError::DecodePrivateKey)?;

                                let pk = p384::PublicKey::from_encoded_point(&point);
                                // pk is a constant time Option.
                                if pk.is_some().into() {
                                    pk.unwrap()
                                } else {
                                    return Err(CryptoError::DecodePrivateKey);
                                }
                            }
                            _ => unreachable!(),
                        };

                        let shared_secret = p384::elliptic_curve::ecdh::diffie_hellman(
                            secret_key.to_nonzero_scalar(),
                            public_key.as_affine(),
                        );

                        // raw serialized x-coordinate of the computed point
                        Ok(shared_secret.raw_secret_bytes().to_vec().into())
                    }
                }
            }
            Algorithm::Hkdf => {
                let zero_copy = zero_copy.ok_or_else(not_supported)?;
                let salt = &*zero_copy;
                let algorithm = match args.hash.ok_or_else(not_supported)? {
                    CryptoHash::Sha1 => hkdf::HKDF_SHA1_FOR_LEGACY_USE_ONLY,
                    CryptoHash::Sha256 => hkdf::HKDF_SHA256,
                    CryptoHash::Sha384 => hkdf::HKDF_SHA384,
                    CryptoHash::Sha512 => hkdf::HKDF_SHA512,
                };

                let info = args.info.ok_or(CryptoError::MissingArgumentInfo)?;
                // IKM
                let secret = args.key.data;
                // L
                let length = args.length / 8;

                let salt = hkdf::Salt::new(algorithm, salt);
                let prk = salt.extract(&secret);
                let info = &[&*info];
                let okm = prk
                    .expand(info, HkdfOutput(length))
                    .map_err(|_e| CryptoError::HKDFLengthTooLarge)?;
                let mut r = vec![0u8; length];
                okm.fill(&mut r)?;
                Ok(r.into())
            }
            _ => Err(CryptoError::UnsupportedAlgorithm),
        }
    })
    .await?
}

fn read_rsa_public_key(key_data: KeyData) -> Result<RsaPublicKey, CryptoError> {
    let public_key = match key_data.r#type {
        KeyType::Private => RsaPrivateKey::from_pkcs1_der(&key_data.data)?.to_public_key(),
        KeyType::Public => RsaPublicKey::from_pkcs1_der(&key_data.data)?,
        KeyType::Secret => unreachable!("unexpected KeyType::Secret"),
    };
    Ok(public_key)
}

pub fn op_crypto_random_uuid(state: &mut OpState) -> Result<String, CryptoError> {
    let maybe_seeded_rng = state.try_borrow_mut::<StdRng>();
    let uuid = if let Some(seeded_rng) = maybe_seeded_rng {
        let mut bytes = [0u8; 16];
        seeded_rng.fill(&mut bytes);
        fast_uuid_v4(&mut bytes)
    } else {
        let mut rng = thread_rng();
        let mut bytes = [0u8; 16];
        rng.fill(&mut bytes);
        fast_uuid_v4(&mut bytes)
    };

    Ok(uuid)
}

pub async fn op_crypto_subtle_digest(
    _state: Rc<RefCell<OpState>>,
    algorithm: CryptoHash,
    data: JsBuffer,
) -> Result<Uint8Array, CryptoError> {
    let output = spawn_blocking(move || {
        digest::digest(algorithm.into(), &data)
            .as_ref()
            .to_vec()
            .into()
    })
    .await?;

    Ok(output)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrapUnwrapKeyArg {
    key: V8RawKeyData,
    algorithm: Algorithm,
}

pub fn op_crypto_wrap_key(
    _state: &mut OpState,
    args: WrapUnwrapKeyArg,
    data: JsBuffer,
) -> Result<Uint8Array, CryptoError> {
    let algorithm = args.algorithm;

    match algorithm {
        Algorithm::AesKw => {
            let key = args.key.as_secret_key()?;

            if !data.len().is_multiple_of(8) {
                return Err(CryptoError::DataInvalidSize);
            }

            let wrapped_key = match key.len() {
                16 => KekAes128::new(key.into()).wrap_vec(&data),
                24 => KekAes192::new(key.into()).wrap_vec(&data),
                32 => KekAes256::new(key.into()).wrap_vec(&data),
                _ => return Err(CryptoError::InvalidKeyLength),
            }
            .map_err(|_| CryptoError::EncryptionError)?;

            Ok(wrapped_key.into())
        }
        _ => Err(CryptoError::UnsupportedAlgorithm),
    }
}

pub fn op_crypto_unwrap_key(
    _state: &mut OpState,
    args: WrapUnwrapKeyArg,
    data: JsBuffer,
) -> Result<Uint8Array, CryptoError> {
    let algorithm = args.algorithm;
    match algorithm {
        Algorithm::AesKw => {
            let key = args.key.as_secret_key()?;

            if !data.len().is_multiple_of(8) {
                return Err(CryptoError::DataInvalidSize);
            }

            let unwrapped_key = match key.len() {
                16 => KekAes128::new(key.into()).unwrap_vec(&data),
                24 => KekAes192::new(key.into()).unwrap_vec(&data),
                32 => KekAes256::new(key.into()).unwrap_vec(&data),
                _ => return Err(CryptoError::InvalidKeyLength),
            }
            .map_err(|_| CryptoError::DecryptionError)?;

            Ok(unwrapped_key.into())
        }
        _ => Err(CryptoError::UnsupportedAlgorithm),
    }
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

fn fast_uuid_v4(bytes: &mut [u8; 16]) -> String {
    // Set UUID version to 4 and variant to 1.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let buf = [
        HEX_CHARS[(bytes[0] >> 4) as usize],
        HEX_CHARS[(bytes[0] & 0x0f) as usize],
        HEX_CHARS[(bytes[1] >> 4) as usize],
        HEX_CHARS[(bytes[1] & 0x0f) as usize],
        HEX_CHARS[(bytes[2] >> 4) as usize],
        HEX_CHARS[(bytes[2] & 0x0f) as usize],
        HEX_CHARS[(bytes[3] >> 4) as usize],
        HEX_CHARS[(bytes[3] & 0x0f) as usize],
        b'-',
        HEX_CHARS[(bytes[4] >> 4) as usize],
        HEX_CHARS[(bytes[4] & 0x0f) as usize],
        HEX_CHARS[(bytes[5] >> 4) as usize],
        HEX_CHARS[(bytes[5] & 0x0f) as usize],
        b'-',
        HEX_CHARS[(bytes[6] >> 4) as usize],
        HEX_CHARS[(bytes[6] & 0x0f) as usize],
        HEX_CHARS[(bytes[7] >> 4) as usize],
        HEX_CHARS[(bytes[7] & 0x0f) as usize],
        b'-',
        HEX_CHARS[(bytes[8] >> 4) as usize],
        HEX_CHARS[(bytes[8] & 0x0f) as usize],
        HEX_CHARS[(bytes[9] >> 4) as usize],
        HEX_CHARS[(bytes[9] & 0x0f) as usize],
        b'-',
        HEX_CHARS[(bytes[10] >> 4) as usize],
        HEX_CHARS[(bytes[10] & 0x0f) as usize],
        HEX_CHARS[(bytes[11] >> 4) as usize],
        HEX_CHARS[(bytes[11] & 0x0f) as usize],
        HEX_CHARS[(bytes[12] >> 4) as usize],
        HEX_CHARS[(bytes[12] & 0x0f) as usize],
        HEX_CHARS[(bytes[13] >> 4) as usize],
        HEX_CHARS[(bytes[13] & 0x0f) as usize],
        HEX_CHARS[(bytes[14] >> 4) as usize],
        HEX_CHARS[(bytes[14] & 0x0f) as usize],
        HEX_CHARS[(bytes[15] >> 4) as usize],
        HEX_CHARS[(bytes[15] & 0x0f) as usize],
    ];

    // Safety: the buffer is all valid UTF-8.
    unsafe { String::from_utf8_unchecked(buf.to_vec()) }
}

#[test]
fn test_fast_uuid_v4_correctness() {
    let mut rng = thread_rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    let uuid = fast_uuid_v4(&mut bytes.clone());
    let uuid_lib = uuid::Builder::from_bytes(bytes)
        .set_variant(uuid::Variant::RFC4122)
        .set_version(uuid::Version::Random)
        .as_uuid()
        .to_string();
    assert_eq!(uuid, uuid_lib);
}

impl From<CryptoError> for OpError {
    fn from(error: CryptoError) -> Self {
        #[allow(unused_variables)]
        let message = error.to_string();
        match error {
            CryptoError::General(inner) => inner.into(),
            CryptoError::JoinError(..) => OpError::new(message),
            CryptoError::Der(..) => OpError::new(message),
            CryptoError::MissingArgumentHash => OpError::type_error(message),
            CryptoError::DerivationRefused(..) => {
                OpError::custom("DOMExceptionOperationError", message)
            }
            CryptoError::MissingArgumentSaltLength => OpError::type_error(message),
            CryptoError::UnsupportedAlgorithm => OpError::type_error(message),
            CryptoError::KeyRejected(..) => OpError::new(message),
            CryptoError::Rsa(..) => OpError::new(message),
            CryptoError::Pkcs1(..) => OpError::new(message),
            CryptoError::Unspecified(..) => OpError::new(message),
            CryptoError::InvalidKeyFormat => OpError::type_error(message),
            CryptoError::P256Ecdsa(..) => OpError::new(message),
            CryptoError::DecodePrivateKey => OpError::type_error(message),
            CryptoError::MissingArgumentPublicKey => OpError::type_error(message),
            CryptoError::MissingArgumentNamedCurve => OpError::type_error(message),
            CryptoError::MissingArgumentInfo => OpError::type_error(message),
            CryptoError::HKDFLengthTooLarge => {
                OpError::custom("DOMExceptionOperationError", message)
            }
            CryptoError::Base64Decode(..) => OpError::new(message),
            CryptoError::DataInvalidSize => OpError::type_error(message),
            CryptoError::InvalidKeyLength => OpError::type_error(message),
            CryptoError::EncryptionError => OpError::custom("DOMExceptionOperationError", message),
            CryptoError::DecryptionError => OpError::custom("DOMExceptionOperationError", message),
            CryptoError::ArrayBufferViewLengthExceeded(..) => {
                OpError::custom("DOMExceptionQuotaExceededError", message)
            }
            CryptoError::Other(inner) => inner,
        }
    }
}
