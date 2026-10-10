// Copyright 2018-2026 the Deno authors. MIT license.

use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use curve25519_dalek::montgomery::MontgomeryPoint;
use dd_v8::ToJsBuffer as Uint8Array;
use dd_v8::{JsBuffer, OpError, OpState};
use elliptic_curve::pkcs8::PrivateKeyInfo;
use elliptic_curve::subtle::ConstantTimeEq;
use rand::RngCore;
use rand::rngs::OsRng;
use spki::der::Decode;
use spki::der::Encode;
use spki::der::asn1::BitString;

#[derive(Debug, thiserror::Error)]
pub enum X25519Error {
    #[error("Failed to export key")]
    FailedExport,
    #[error(transparent)]
    Der(#[from] spki::der::Error),
}
// u-coordinate of the base point.
const X25519_BASEPOINT_BYTES: [u8; 32] = [
    9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
/// `[privateKey, publicKey]`.
pub fn op_crypto_generate_x25519_keypair(_state: &mut OpState) -> (Uint8Array, Uint8Array) {
    let mut pkey = [0u8; 32];
    OsRng.fill_bytes(&mut pkey);
    // https://www.rfc-editor.org/rfc/rfc7748#section-6.1
    // pubkey = x25519(a, 9) which is constant-time Montgomery ladder.
    //   https://eprint.iacr.org/2014/140.pdf page 4
    //   https://eprint.iacr.org/2017/212.pdf algorithm 8
    // pubkey is in LE order.
    let pubkey = x25519_dalek::x25519(pkey, X25519_BASEPOINT_BYTES);
    (pkey.to_vec().into(), pubkey.to_vec().into())
}

pub fn op_crypto_x25519_public_key(_state: &mut OpState, private_key: JsBuffer) -> Option<String> {
    use base64::Engine;

    let private_key: [u8; 32] = (*private_key).try_into().ok()?;
    Some(BASE64_URL_SAFE_NO_PAD.encode(x25519_dalek::x25519(private_key, X25519_BASEPOINT_BYTES)))
}

const MONTGOMERY_IDENTITY: MontgomeryPoint = MontgomeryPoint([0; 32]);

/// The shared secret, or `null` when it is all zeroes (or a key has the
/// wrong length).
pub fn op_crypto_derive_bits_x25519(
    _state: &mut OpState,
    k: JsBuffer,
    u: JsBuffer,
) -> Option<Uint8Array> {
    let k: [u8; 32] = (*k).try_into().ok()?;
    let u: [u8; 32] = (*u).try_into().ok()?;
    let sh_sec = x25519_dalek::x25519(k, u);
    let point = MontgomeryPoint(sh_sec);
    if point.ct_eq(&MONTGOMERY_IDENTITY).unwrap_u8() == 1 {
        return None;
    }
    Some(sh_sec.to_vec().into())
}

// id-X25519 OBJECT IDENTIFIER ::= { 1 3 101 110 }
pub const X25519_OID: const_oid::ObjectIdentifier =
    const_oid::ObjectIdentifier::new_unwrap("1.3.101.110");

/// The public key in SPKI `key_data`, or `null` if it is no X25519 key.
pub fn op_crypto_import_spki_x25519(
    _state: &mut OpState,
    key_data: JsBuffer,
) -> Option<Uint8Array> {
    // 2-3.
    let pk_info = spki::SubjectPublicKeyInfoRef::try_from(&*key_data).ok()?;
    // 4.
    if pk_info.algorithm.oid != X25519_OID {
        return None;
    }
    // 5.
    if pk_info.algorithm.parameters.is_some() {
        return None;
    }
    let key = pk_info.subject_public_key.raw_bytes();
    (key.len() == 32).then(|| key.to_vec().into())
}

/// The private key in PKCS#8 `key_data`, or `null` if it is no X25519 key.
pub fn op_crypto_import_pkcs8_x25519(
    _state: &mut OpState,
    key_data: JsBuffer,
) -> Option<Uint8Array> {
    // 2-3.
    // This should probably use OneAsymmetricKey instead
    let pk_info = PrivateKeyInfo::from_der(&key_data).ok()?;
    // 4.
    if pk_info.algorithm.oid != X25519_OID {
        return None;
    }
    // 5.
    if pk_info.algorithm.parameters.is_some() {
        return None;
    }
    // 6.
    // CurvePrivateKey ::= OCTET STRING
    if pk_info.private_key.len() != 34 {
        return None;
    }
    Some(pk_info.private_key[2..].to_vec().into())
}

pub fn op_crypto_export_spki_x25519(
    _state: &mut OpState,
    pubkey: JsBuffer,
) -> Result<Uint8Array, X25519Error> {
    let key_info = spki::SubjectPublicKeyInfo {
        algorithm: spki::AlgorithmIdentifierRef {
            // id-X25519
            oid: X25519_OID,
            parameters: None,
        },
        subject_public_key: BitString::from_bytes(&pubkey)?,
    };
    Ok(key_info
        .to_der()
        .map_err(|_| X25519Error::FailedExport)?
        .into())
}

pub fn op_crypto_export_pkcs8_x25519(
    _state: &mut OpState,
    pkey: JsBuffer,
) -> Result<Uint8Array, X25519Error> {
    use rsa::pkcs1::der::Encode;

    // This should probably use OneAsymmetricKey instead
    let pk_info = rsa::pkcs8::PrivateKeyInfo {
        public_key: None,
        algorithm: rsa::pkcs8::AlgorithmIdentifierRef {
            // id-X25519
            oid: X25519_OID,
            parameters: None,
        },
        private_key: &pkey, // OCTET STRING
    };

    let mut buf = Vec::new();
    pk_info.encode_to_vec(&mut buf)?;
    Ok(buf.into())
}

impl From<X25519Error> for OpError {
    fn from(error: X25519Error) -> Self {
        #[allow(unused_variables)]
        let message = error.to_string();
        match error {
            X25519Error::FailedExport => OpError::custom("DOMExceptionOperationError", message),
            X25519Error::Der(..) => OpError::new(message),
        }
    }
}
