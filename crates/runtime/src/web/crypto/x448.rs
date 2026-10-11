// Copyright 2018-2026 the Deno authors. MIT license.

use dd_v8::ToJsBuffer;
use dd_v8::{JsBuffer, OpError, OpState};
use ed448_goldilocks::EdwardsScalar;
use ed448_goldilocks::MontgomeryPoint;
use ed448_goldilocks::subtle::ConstantTimeEq;
use elliptic_curve::pkcs8::PrivateKeyInfoRef;
use rand::Rng;
use spki::der::Decode;
use spki::der::Encode;
use spki::der::asn1::BitString;
use spki::der::asn1::OctetStringRef;

use super::shared::{operation_error, os_rng};

#[derive(Debug, thiserror::Error)]
pub enum X448Error {
    #[error("Failed to export key")]
    FailedExport,
    #[error(transparent)]
    Der(#[from] spki::der::Error),
}

/// `[privateKey, publicKey]`.
pub fn op_crypto_generate_x448_keypair(_state: &mut OpState) -> (ToJsBuffer, ToJsBuffer) {
    let mut pkey = [0u8; 56];
    os_rng().fill_bytes(&mut pkey);

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
) -> Option<ToJsBuffer> {
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
) -> Result<ToJsBuffer, X448Error> {
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
) -> Result<ToJsBuffer, X448Error> {
    let pk_info = PrivateKeyInfoRef::new(
        spki::AlgorithmIdentifierRef {
            oid: X448_OID,
            parameters: None,
        },
        OctetStringRef::new(&pkey)?,
    );

    let mut buf = Vec::new();
    pk_info.encode_to_vec(&mut buf)?;
    Ok(buf.into())
}

/// The public key in SPKI `key_data`, or `null` if it is no X448 key.
pub fn op_crypto_import_spki_x448(_state: &mut OpState, key_data: JsBuffer) -> Option<ToJsBuffer> {
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
pub fn op_crypto_import_pkcs8_x448(_state: &mut OpState, key_data: JsBuffer) -> Option<ToJsBuffer> {
    // 2-3.
    let pk_info = PrivateKeyInfoRef::from_der(&key_data).ok()?;
    let private_key = pk_info.private_key.as_bytes();
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
    if private_key.len() != 58 {
        return None;
    }
    Some(private_key[2..].to_vec().into())
}

impl From<X448Error> for OpError {
    fn from(error: X448Error) -> Self {
        match error {
            X448Error::FailedExport => operation_error(error.to_string()),
            X448Error::Der(..) => OpError::new(error.to_string()),
        }
    }
}
