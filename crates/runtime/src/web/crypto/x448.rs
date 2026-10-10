// Copyright 2018-2026 the Deno authors. MIT license.

use dd_v8::ToJsBuffer as Uint8Array;
use dd_v8::{JsBuffer, OpError, OpState};
use ed448_goldilocks::EdwardsScalar;
use ed448_goldilocks::MontgomeryPoint;
use ed448_goldilocks::subtle::ConstantTimeEq;
use elliptic_curve::pkcs8::PrivateKeyInfo;
use rand::RngCore;
use rand::rngs::OsRng;
use spki::der::Decode;
use spki::der::Encode;
use spki::der::asn1::BitString;

#[derive(Debug, thiserror::Error)]
pub enum X448Error {
    #[error("Failed to export key")]
    FailedExport,
    #[error(transparent)]
    Der(#[from] spki::der::Error),
}

/// `[privateKey, publicKey]`.
pub fn op_crypto_generate_x448_keypair(_state: &mut OpState) -> (Uint8Array, Uint8Array) {
    let mut pkey = [0u8; 56];
    OsRng.fill_bytes(&mut pkey);

    // x448(pkey, 5)
    let mut scalar_bytes = [0u8; 57];
    scalar_bytes[..56].copy_from_slice(&pkey);
    let scalar = EdwardsScalar::from_bytes_mod_order(&scalar_bytes.into());
    let point = &MontgomeryPoint::GENERATOR * &scalar;
    (pkey.to_vec().into(), point.0.to_vec().into())
}

static MONTGOMERY_IDENTITY: MontgomeryPoint = MontgomeryPoint([0; 56]);

/// The shared secret, or `null` when it is the identity point (or a key has
/// the wrong length).
pub fn op_crypto_derive_bits_x448(
    _state: &mut OpState,
    k: JsBuffer,
    u: JsBuffer,
) -> Option<Uint8Array> {
    let k: [u8; 56] = (*k).try_into().ok()?;
    let u: [u8; 56] = (*u).try_into().ok()?;

    // x448(k, u)
    let mut scalar_bytes = [0u8; 57];
    scalar_bytes[..56].copy_from_slice(&k);
    let scalar = EdwardsScalar::from_bytes_mod_order(&scalar_bytes.into());
    let point = &MontgomeryPoint(u) * &scalar;
    if point.ct_eq(&MONTGOMERY_IDENTITY).unwrap_u8() == 1 {
        return None;
    }
    Some(point.0.to_vec().into())
}

// id-X448 OBJECT IDENTIFIER ::= { 1 3 101 111 }
const X448_OID: const_oid::ObjectIdentifier =
    const_oid::ObjectIdentifier::new_unwrap("1.3.101.111");

pub fn op_crypto_export_spki_x448(
    _state: &mut OpState,
    pubkey: JsBuffer,
) -> Result<Uint8Array, X448Error> {
    let key_info = spki::SubjectPublicKeyInfo {
        algorithm: spki::AlgorithmIdentifierRef {
            oid: X448_OID,
            parameters: None,
        },
        subject_public_key: BitString::from_bytes(&pubkey)?,
    };
    Ok(key_info
        .to_der()
        .map_err(|_| X448Error::FailedExport)?
        .into())
}

pub fn op_crypto_export_pkcs8_x448(
    _state: &mut OpState,
    pkey: JsBuffer,
) -> Result<Uint8Array, X448Error> {
    use rsa::pkcs1::der::Encode;

    let pk_info = rsa::pkcs8::PrivateKeyInfo {
        public_key: None,
        algorithm: rsa::pkcs8::AlgorithmIdentifierRef {
            oid: X448_OID,
            parameters: None,
        },
        private_key: &pkey, // OCTET STRING
    };

    let mut buf = Vec::new();
    pk_info.encode_to_vec(&mut buf)?;
    Ok(buf.into())
}

/// The public key in SPKI `key_data`, or `null` if it is no X448 key.
pub fn op_crypto_import_spki_x448(_state: &mut OpState, key_data: JsBuffer) -> Option<Uint8Array> {
    // 2-3.
    let pk_info = spki::SubjectPublicKeyInfoRef::try_from(&*key_data).ok()?;
    // 4.
    if pk_info.algorithm.oid != X448_OID {
        return None;
    }
    // 5.
    if pk_info.algorithm.parameters.is_some() {
        return None;
    }
    let key = pk_info.subject_public_key.raw_bytes();
    (key.len() == 56).then(|| key.to_vec().into())
}

/// The private key in PKCS#8 `key_data`, or `null` if it is no X448 key.
pub fn op_crypto_import_pkcs8_x448(_state: &mut OpState, key_data: JsBuffer) -> Option<Uint8Array> {
    // 2-3.
    let pk_info = PrivateKeyInfo::from_der(&key_data).ok()?;
    // 4.
    if pk_info.algorithm.oid != X448_OID {
        return None;
    }
    // 5.
    if pk_info.algorithm.parameters.is_some() {
        return None;
    }
    // 6.
    // CurvePrivateKey ::= OCTET STRING, two header bytes then 56 key bytes.
    if pk_info.private_key.len() != 58 {
        return None;
    }
    Some(pk_info.private_key[2..].to_vec().into())
}

impl From<X448Error> for OpError {
    fn from(error: X448Error) -> Self {
        #[allow(unused_variables)]
        let message = error.to_string();
        match error {
            X448Error::FailedExport => OpError::custom("DOMExceptionOperationError", message),
            X448Error::Der(..) => OpError::new(message),
        }
    }
}
