// Copyright 2018-2026 the Deno authors. MIT license.

use aws_lc_rs::signature::Ed25519KeyPair;
use aws_lc_rs::signature::KeyPair;
use base64::Engine;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use dd_v8::ToJsBuffer;
use dd_v8::{JsBuffer, OpError, OpState};
use elliptic_curve::pkcs8::PrivateKeyInfoRef;
use rand::Rng;
use spki::der::Decode;
use spki::der::Encode;
use spki::der::asn1::BitString;
use spki::der::asn1::OctetStringRef;

use super::shared::{operation_error, os_rng};

#[derive(Debug, thiserror::Error)]
pub enum Ed25519Error {
    #[error("Failed to export key")]
    FailedExport,
    #[error(transparent)]
    Der(#[from] rsa::pkcs1::der::Error),
    #[error(transparent)]
    KeyRejected(#[from] aws_lc_rs::error::KeyRejected),
}

/// `[privateKey, publicKey]`, or `null` if no key pair could be made.
pub fn op_crypto_generate_ed25519_keypair(
    _state: &mut OpState,
) -> Option<(ToJsBuffer, ToJsBuffer)> {
    let mut pkey = vec![0; 32];
    os_rng().fill_bytes(&mut pkey);
    let pair = Ed25519KeyPair::from_seed_unchecked(&pkey).ok()?;
    let pubkey = pair.public_key().as_ref().to_vec();
    Some((pkey.into(), pubkey.into()))
}

/// The signature, or `null` if `key` is no valid private key.
pub fn op_crypto_sign_ed25519(
    _state: &mut OpState,
    key: JsBuffer,
    data: JsBuffer,
) -> Option<ToJsBuffer> {
    let pair = Ed25519KeyPair::from_seed_unchecked(&key).ok()?;
    Some(pair.sign(&data).as_ref().to_vec().into())
}

pub fn op_crypto_verify_ed25519(
    _state: &mut OpState,
    pubkey: JsBuffer,
    data: JsBuffer,
    signature: JsBuffer,
) -> bool {
    aws_lc_rs::signature::UnparsedPublicKey::new(&aws_lc_rs::signature::ED25519, &*pubkey)
        .verify(&data, &signature)
        .is_ok()
}

// id-Ed25519 OBJECT IDENTIFIER ::= { 1 3 101 112 }
pub const ED25519_OID: const_oid::ObjectIdentifier =
    const_oid::ObjectIdentifier::new_unwrap("1.3.101.112");

/// The public key in SPKI `key_data`, or `null` if it is no Ed25519 key.
pub fn op_crypto_import_spki_ed25519(
    _state: &mut OpState,
    key_data: JsBuffer,
) -> Option<ToJsBuffer> {
    // 2-3.
    let pk_info = spki::SubjectPublicKeyInfoRef::try_from(&*key_data).ok()?;
    // 4.
    if pk_info.algorithm.oid != ED25519_OID {
        return None;
    }
    // 5.
    if pk_info.algorithm.parameters.is_some() {
        return None;
    }
    let key = pk_info.subject_public_key.raw_bytes();
    (key.len() == 32).then(|| key.to_vec().into())
}

/// The private key in PKCS#8 `key_data`, or `null` if it is no Ed25519 key.
pub fn op_crypto_import_pkcs8_ed25519(
    _state: &mut OpState,
    key_data: JsBuffer,
) -> Option<ToJsBuffer> {
    // 2-3.
    // This should probably use OneAsymmetricKey instead
    let pk_info = PrivateKeyInfoRef::from_der(&key_data).ok()?;
    let private_key = pk_info.private_key.as_bytes();
    // 4.
    if pk_info.algorithm.oid != ED25519_OID {
        return None;
    }
    // 5.
    if pk_info.algorithm.parameters.is_some() {
        return None;
    }
    // 6.
    // CurvePrivateKey ::= OCTET STRING
    if private_key.len() != 34 {
        return None;
    }
    Some(private_key[2..].to_vec().into())
}

pub fn op_crypto_export_spki_ed25519(
    _state: &mut OpState,
    pubkey: JsBuffer,
) -> Result<ToJsBuffer, Ed25519Error> {
    let key_info = spki::SubjectPublicKeyInfo {
        algorithm: spki::AlgorithmIdentifierOwned {
            // id-Ed25519
            oid: ED25519_OID,
            parameters: None,
        },
        subject_public_key: BitString::from_bytes(&pubkey)?,
    };
    Ok(key_info
        .to_der()
        .map_err(|_| Ed25519Error::FailedExport)?
        .into())
}

pub fn op_crypto_export_pkcs8_ed25519(
    _state: &mut OpState,
    pkey: JsBuffer,
) -> Result<ToJsBuffer, Ed25519Error> {
    // This should probably use OneAsymmetricKey instead
    let pk_info = PrivateKeyInfoRef::new(
        spki::AlgorithmIdentifierRef {
            // id-Ed25519
            oid: ED25519_OID,
            parameters: None,
        },
        OctetStringRef::new(&pkey)?,
    );

    let mut buf = Vec::new();
    pk_info.encode_to_vec(&mut buf)?;
    Ok(buf.into())
}

// 'x' from Section 2 of RFC 8037
// https://www.rfc-editor.org/rfc/rfc8037#section-2
pub fn op_crypto_jwk_x_ed25519(
    _state: &mut OpState,
    pkey: JsBuffer,
) -> Result<String, Ed25519Error> {
    let pair = Ed25519KeyPair::from_seed_unchecked(&pkey)?;
    Ok(BASE64_URL_SAFE_NO_PAD.encode(pair.public_key().as_ref()))
}

impl From<Ed25519Error> for OpError {
    fn from(error: Ed25519Error) -> Self {
        match error {
            Ed25519Error::FailedExport => operation_error(error.to_string()),
            Ed25519Error::Der(..) | Ed25519Error::KeyRejected(..) => {
                OpError::new(error.to_string())
            }
        }
    }
}
