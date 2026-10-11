// Copyright 2018-2026 the Deno authors. MIT license.

use aes::cipher::BlockModeEncrypt;
use aes::cipher::KeyIvInit;
use aes::cipher::StreamCipher;
use aes::cipher::block_padding::Pkcs7;
use aes_gcm::AeadInOut;
use aes_gcm::KeyInit;
use aes_gcm::Nonce;
use aes_gcm::aead::array::ArraySize;
use aes_gcm::aead::consts::U12;
use aes_gcm::aead::consts::U16;
use aes_gcm::aes::Aes128;
use aes_gcm::aes::Aes192;
use aes_gcm::aes::Aes256;
use ctr::Ctr32BE;
use ctr::Ctr64BE;
use ctr::Ctr128BE;
use dd_v8::JsBuffer;
use dd_v8::ToJsBuffer;
use dd_v8::{OpError, OpState};
use rsa::Oaep;
use rsa::pkcs1::DecodeRsaPublicKey;
use serde::Deserialize;
use sha1::Sha1;
use sha2::Sha256;
use sha2::Sha384;
use sha2::Sha512;
use sha3::Sha3_256;
use sha3::Sha3_384;
use sha3::Sha3_512;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::task::spawn_blocking;

use super::shared::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptOptions {
    key: V8RawKeyData,
    #[serde(flatten)]
    algorithm: EncryptAlgorithm,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", tag = "algorithm")]
pub enum EncryptAlgorithm {
    #[serde(rename = "RSA-OAEP")]
    RsaOaep {
        hash: ShaHash,
        #[serde(with = "serde_bytes")]
        label: Vec<u8>,
    },
    #[serde(rename = "AES-CBC", rename_all = "camelCase")]
    AesCbc {
        #[serde(with = "serde_bytes")]
        iv: Vec<u8>,
        length: usize,
    },
    #[serde(rename = "AES-GCM", rename_all = "camelCase")]
    AesGcm {
        #[serde(with = "serde_bytes")]
        iv: Vec<u8>,
        #[serde(with = "serde_bytes")]
        additional_data: Option<Vec<u8>>,
        length: usize,
        tag_length: usize,
    },
    #[serde(rename = "AES-OCB", rename_all = "camelCase")]
    AesOcb {
        #[serde(with = "serde_bytes")]
        iv: Vec<u8>,
        #[serde(with = "serde_bytes")]
        additional_data: Option<Vec<u8>>,
        length: usize,
        tag_length: usize,
    },
    #[serde(rename = "AES-CTR", rename_all = "camelCase")]
    AesCtr {
        #[serde(with = "serde_bytes")]
        counter: Vec<u8>,
        ctr_length: usize,
        key_length: usize,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum EncryptError {
    #[error(transparent)]
    General(#[from] SharedError),
    #[error("invalid length")]
    InvalidLength,
    #[error("invalid key or iv")]
    InvalidKeyOrIv,
    #[error("iv length not equal to 12 or 16")]
    InvalidIvLength,
    #[error("AES-OCB nonces must be 12 bytes")]
    UnsupportedOcbNonce,
    #[error("invalid counter length. Currently supported 32/64/128 bits")]
    InvalidCounterLength,
    #[error("tried to encrypt too much data")]
    TooMuchData,
    #[error("Encryption failed")]
    Failed,
}

pub async fn op_crypto_encrypt(
    _state: Rc<RefCell<OpState>>,
    opts: EncryptOptions,
    data: JsBuffer,
) -> Result<ToJsBuffer, EncryptError> {
    let key = opts.key;
    let fun = move || match opts.algorithm {
        EncryptAlgorithm::RsaOaep { hash, label } => encrypt_rsa_oaep(key, hash, label, &data),
        EncryptAlgorithm::AesCbc { iv, length } => encrypt_aes_cbc(key, length, iv, &data),
        EncryptAlgorithm::AesGcm {
            iv,
            additional_data,
            length,
            tag_length,
        } => encrypt_aes_gcm(key, length, tag_length, iv, additional_data, &data),
        EncryptAlgorithm::AesOcb {
            iv,
            additional_data,
            length,
            tag_length,
        } => encrypt_aes_ocb(key, length, tag_length, iv, additional_data, &data),
        EncryptAlgorithm::AesCtr {
            counter,
            ctr_length,
            key_length,
        } => encrypt_aes_ctr(key, key_length, &counter, ctr_length, &data),
    };
    let buf = spawn_blocking(fun).await.unwrap()?;
    Ok(buf.into())
}

fn encrypt_rsa_oaep(
    key: V8RawKeyData,
    hash: ShaHash,
    label: Vec<u8>,
    data: &[u8],
) -> Result<Vec<u8>, EncryptError> {
    let public_key = key.as_rsa_public_key()?;
    let public_key = rsa::RsaPublicKey::from_pkcs1_der(&public_key)
        .map_err(|_| SharedError::FailedDecodePublicKey)?;
    let mut rng = os_rng();
    let encrypted = match hash {
        ShaHash::Sha1 => public_key.encrypt(&mut rng, Oaep::<Sha1>::new_with_label(label), data),
        ShaHash::Sha256 => {
            public_key.encrypt(&mut rng, Oaep::<Sha256>::new_with_label(label), data)
        }
        ShaHash::Sha384 => {
            public_key.encrypt(&mut rng, Oaep::<Sha384>::new_with_label(label), data)
        }
        ShaHash::Sha512 => {
            public_key.encrypt(&mut rng, Oaep::<Sha512>::new_with_label(label), data)
        }
        ShaHash::Sha3_256 => {
            public_key.encrypt(&mut rng, Oaep::<Sha3_256>::new_with_label(label), data)
        }
        ShaHash::Sha3_384 => {
            public_key.encrypt(&mut rng, Oaep::<Sha3_384>::new_with_label(label), data)
        }
        ShaHash::Sha3_512 => {
            public_key.encrypt(&mut rng, Oaep::<Sha3_512>::new_with_label(label), data)
        }
    }
    .map_err(|_| EncryptError::Failed)?;
    Ok(encrypted)
}

fn encrypt_aes_cbc(
    key: V8RawKeyData,
    length: usize,
    iv: Vec<u8>,
    data: &[u8],
) -> Result<Vec<u8>, EncryptError> {
    let key = key.as_secret_key()?;
    let ciphertext = match length {
        128 => {
            // Section 10.3 Step 2 of RFC 2315 https://www.rfc-editor.org/rfc/rfc2315
            type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;

            let cipher = Aes128CbcEnc::new_from_slices(key, &iv)
                .map_err(|_| EncryptError::InvalidKeyOrIv)?;
            cipher.encrypt_padded_vec::<Pkcs7>(data)
        }
        192 => {
            // Section 10.3 Step 2 of RFC 2315 https://www.rfc-editor.org/rfc/rfc2315
            type Aes192CbcEnc = cbc::Encryptor<aes::Aes192>;

            let cipher = Aes192CbcEnc::new_from_slices(key, &iv)
                .map_err(|_| EncryptError::InvalidKeyOrIv)?;
            cipher.encrypt_padded_vec::<Pkcs7>(data)
        }
        256 => {
            // Section 10.3 Step 2 of RFC 2315 https://www.rfc-editor.org/rfc/rfc2315
            type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;

            let cipher = Aes256CbcEnc::new_from_slices(key, &iv)
                .map_err(|_| EncryptError::InvalidKeyOrIv)?;
            cipher.encrypt_padded_vec::<Pkcs7>(data)
        }
        _ => return Err(EncryptError::InvalidLength),
    };
    Ok(ciphertext)
}

fn encrypt_aes_gcm_general<N: ArraySize>(
    key: &[u8],
    iv: Vec<u8>,
    length: usize,
    ciphertext: &mut [u8],
    additional_data: Vec<u8>,
) -> Result<aes_gcm::Tag, EncryptError> {
    let nonce = <&Nonce<N>>::try_from(iv.as_slice()).map_err(|_| EncryptError::InvalidIvLength)?;
    let tag = match length {
        128 => {
            let cipher = aes_gcm::AesGcm::<Aes128, N>::new_from_slice(key)
                .map_err(|_| EncryptError::Failed)?;
            cipher
                .encrypt_inout_detached(nonce, &additional_data, ciphertext.into())
                .map_err(|_| EncryptError::Failed)?
        }
        192 => {
            let cipher = aes_gcm::AesGcm::<Aes192, N>::new_from_slice(key)
                .map_err(|_| EncryptError::Failed)?;
            cipher
                .encrypt_inout_detached(nonce, &additional_data, ciphertext.into())
                .map_err(|_| EncryptError::Failed)?
        }
        256 => {
            let cipher = aes_gcm::AesGcm::<Aes256, N>::new_from_slice(key)
                .map_err(|_| EncryptError::Failed)?;
            cipher
                .encrypt_inout_detached(nonce, &additional_data, ciphertext.into())
                .map_err(|_| EncryptError::Failed)?
        }
        _ => return Err(EncryptError::InvalidLength),
    };

    Ok(tag)
}

fn encrypt_aes_gcm(
    key: V8RawKeyData,
    length: usize,
    tag_length: usize,
    iv: Vec<u8>,
    additional_data: Option<Vec<u8>>,
    data: &[u8],
) -> Result<Vec<u8>, EncryptError> {
    let key = key.as_secret_key()?;
    let additional_data = additional_data.unwrap_or_default();

    let mut ciphertext = data.to_vec();
    // Fixed 96-bit OR 128-bit nonce
    let tag = match iv.len() {
        12 => encrypt_aes_gcm_general::<U12>(key, iv, length, &mut ciphertext, additional_data)?,
        16 => encrypt_aes_gcm_general::<U16>(key, iv, length, &mut ciphertext, additional_data)?,
        _ => return Err(EncryptError::InvalidIvLength),
    };

    // Truncated tag to the specified tag length.
    // `tag` is fixed to be 16 bytes long and (tag_length / 8) is always <= 16
    let tag = &tag[..(tag_length / 8)];

    // C | T
    ciphertext.extend_from_slice(tag);

    Ok(ciphertext)
}

fn encrypt_aes_ocb(
    key: V8RawKeyData,
    length: usize,
    tag_length: usize,
    iv: Vec<u8>,
    additional_data: Option<Vec<u8>>,
    data: &[u8],
) -> Result<Vec<u8>, EncryptError> {
    use ocb3::Ocb3;
    use ocb3::aead::AeadInOut as Ocb3AeadInOut;
    use ocb3::aead::KeyInit as Ocb3KeyInit;

    let key = key.as_secret_key()?;
    let additional_data = additional_data.unwrap_or_default();

    let mut ciphertext = data.to_vec();

    // OCB allows nonces of 1 to 15 bytes, but `Ocb3` below takes its default
    // 12-byte nonce.
    let nonce = <&ocb3::Nonce<U12>>::try_from(iv.as_slice())
        .map_err(|_| EncryptError::UnsupportedOcbNonce)?;

    let tag = match length {
        128 => {
            let cipher =
                Ocb3::<aes::Aes128>::new_from_slice(key).map_err(|_| EncryptError::Failed)?;
            cipher
                .encrypt_inout_detached(nonce, &additional_data, ciphertext.as_mut_slice().into())
                .map_err(|_| EncryptError::Failed)?
        }
        192 => {
            let cipher =
                Ocb3::<aes::Aes192>::new_from_slice(key).map_err(|_| EncryptError::Failed)?;
            cipher
                .encrypt_inout_detached(nonce, &additional_data, ciphertext.as_mut_slice().into())
                .map_err(|_| EncryptError::Failed)?
        }
        256 => {
            let cipher =
                Ocb3::<aes::Aes256>::new_from_slice(key).map_err(|_| EncryptError::Failed)?;
            cipher
                .encrypt_inout_detached(nonce, &additional_data, ciphertext.as_mut_slice().into())
                .map_err(|_| EncryptError::Failed)?
        }
        _ => return Err(EncryptError::InvalidLength),
    };

    // Truncate tag to the specified tag length
    // OCB tag is 16 bytes by default
    let tag = &tag[..(tag_length / 8)];

    // C | T
    ciphertext.extend_from_slice(tag);

    Ok(ciphertext)
}

fn encrypt_aes_ctr_gen<B>(key: &[u8], counter: &[u8], data: &[u8]) -> Result<Vec<u8>, EncryptError>
where
    B: KeyIvInit + StreamCipher,
{
    let mut cipher = B::new_from_slices(key, counter).map_err(|_| EncryptError::InvalidKeyOrIv)?;

    let mut ciphertext = data.to_vec();
    cipher
        .try_apply_keystream(&mut ciphertext)
        .map_err(|_| EncryptError::TooMuchData)?;

    Ok(ciphertext)
}

fn encrypt_aes_ctr(
    key: V8RawKeyData,
    key_length: usize,
    counter: &[u8],
    ctr_length: usize,
    data: &[u8],
) -> Result<Vec<u8>, EncryptError> {
    let key = key.as_secret_key()?;

    match ctr_length {
        32 => match key_length {
            128 => encrypt_aes_ctr_gen::<Ctr32BE<aes::Aes128>>(key, counter, data),
            192 => encrypt_aes_ctr_gen::<Ctr32BE<aes::Aes192>>(key, counter, data),
            256 => encrypt_aes_ctr_gen::<Ctr32BE<aes::Aes256>>(key, counter, data),
            _ => Err(EncryptError::InvalidLength),
        },
        64 => match key_length {
            128 => encrypt_aes_ctr_gen::<Ctr64BE<aes::Aes128>>(key, counter, data),
            192 => encrypt_aes_ctr_gen::<Ctr64BE<aes::Aes192>>(key, counter, data),
            256 => encrypt_aes_ctr_gen::<Ctr64BE<aes::Aes256>>(key, counter, data),
            _ => Err(EncryptError::InvalidLength),
        },
        128 => match key_length {
            128 => encrypt_aes_ctr_gen::<Ctr128BE<aes::Aes128>>(key, counter, data),
            192 => encrypt_aes_ctr_gen::<Ctr128BE<aes::Aes192>>(key, counter, data),
            256 => encrypt_aes_ctr_gen::<Ctr128BE<aes::Aes256>>(key, counter, data),
            _ => Err(EncryptError::InvalidLength),
        },
        _ => Err(EncryptError::InvalidCounterLength),
    }
}

impl From<EncryptError> for OpError {
    fn from(error: EncryptError) -> Self {
        match error {
            EncryptError::General(inner) => inner.into(),
            EncryptError::InvalidLength
            | EncryptError::InvalidIvLength
            | EncryptError::InvalidCounterLength => OpError::type_error(error.to_string()),
            EncryptError::InvalidKeyOrIv | EncryptError::TooMuchData | EncryptError::Failed => {
                operation_error(error.to_string())
            }
            EncryptError::UnsupportedOcbNonce => not_supported_error(error.to_string()),
        }
    }
}
