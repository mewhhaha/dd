// Copyright 2018-2026 the Deno authors. MIT license.

use aes::cipher::BlockModeDecrypt;
use aes::cipher::KeyIvInit;
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
use ctr::cipher::StreamCipher;
use dd_v8::JsBuffer;
use dd_v8::ToJsBuffer;
use dd_v8::{OpError, OpState};
use rsa::Oaep;
use rsa::pkcs1::DecodeRsaPrivateKey;
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
pub struct DecryptOptions {
    key: V8RawKeyData,
    #[serde(flatten)]
    algorithm: DecryptAlgorithm,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", tag = "algorithm")]
pub enum DecryptAlgorithm {
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
    #[serde(rename = "AES-CTR", rename_all = "camelCase")]
    AesCtr {
        #[serde(with = "serde_bytes")]
        counter: Vec<u8>,
        ctr_length: usize,
        key_length: usize,
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
}

#[derive(Debug, thiserror::Error)]
pub enum DecryptError {
    #[error(transparent)]
    General(#[from] SharedError),
    #[error(transparent)]
    Pkcs1(#[from] rsa::pkcs1::Error),
    #[error("Decryption failed")]
    Failed,
    #[error("invalid length")]
    InvalidLength,
    #[error("invalid counter length. Currently supported 32/64/128 bits")]
    InvalidCounterLength,
    #[error("tag length not equal to 128")]
    InvalidTagLength,
    #[error("invalid key or iv")]
    InvalidKeyOrIv,
    #[error("tried to decrypt too much data")]
    TooMuchData,
    #[error("iv length not equal to 12 or 16")]
    InvalidIvLength,
    #[error("AES-OCB nonces must be 12 bytes")]
    UnsupportedOcbNonce,
    #[error("{0}")]
    Rsa(rsa::Error),
}

pub async fn op_crypto_decrypt(
    _state: Rc<RefCell<OpState>>,
    opts: DecryptOptions,
    data: JsBuffer,
) -> Result<ToJsBuffer, DecryptError> {
    let key = opts.key;
    let fun = move || match opts.algorithm {
        DecryptAlgorithm::RsaOaep { hash, label } => decrypt_rsa_oaep(key, hash, label, &data),
        DecryptAlgorithm::AesCbc { iv, length } => decrypt_aes_cbc(key, length, iv, &data),
        DecryptAlgorithm::AesCtr {
            counter,
            ctr_length,
            key_length,
        } => decrypt_aes_ctr(key, key_length, &counter, ctr_length, &data),
        DecryptAlgorithm::AesGcm {
            iv,
            additional_data,
            length,
            tag_length,
        } => decrypt_aes_gcm(key, length, tag_length, iv, additional_data, &data),
        DecryptAlgorithm::AesOcb {
            iv,
            additional_data,
            length,
            tag_length,
        } => decrypt_aes_ocb(key, length, tag_length, iv, additional_data, &data),
    };
    let buf = spawn_blocking(fun).await.unwrap()?;
    Ok(buf.into())
}

fn decrypt_rsa_oaep(
    key: V8RawKeyData,
    hash: ShaHash,
    label: Vec<u8>,
    data: &[u8],
) -> Result<Vec<u8>, DecryptError> {
    let key = key.as_rsa_private_key()?;

    let private_key = rsa::RsaPrivateKey::from_pkcs1_der(key)?;

    match hash {
        ShaHash::Sha1 => private_key.decrypt(Oaep::<Sha1>::new_with_label(label), data),
        ShaHash::Sha256 => private_key.decrypt(Oaep::<Sha256>::new_with_label(label), data),
        ShaHash::Sha384 => private_key.decrypt(Oaep::<Sha384>::new_with_label(label), data),
        ShaHash::Sha512 => private_key.decrypt(Oaep::<Sha512>::new_with_label(label), data),
        ShaHash::Sha3_256 => private_key.decrypt(Oaep::<Sha3_256>::new_with_label(label), data),
        ShaHash::Sha3_384 => private_key.decrypt(Oaep::<Sha3_384>::new_with_label(label), data),
        ShaHash::Sha3_512 => private_key.decrypt(Oaep::<Sha3_512>::new_with_label(label), data),
    }
    .map_err(DecryptError::Rsa)
}

fn decrypt_aes_cbc(
    key: V8RawKeyData,
    length: usize,
    iv: Vec<u8>,
    data: &[u8],
) -> Result<Vec<u8>, DecryptError> {
    let key = key.as_secret_key()?;

    // 2.
    let plaintext = match length {
        128 => {
            // Section 10.3 Step 2 of RFC 2315 https://www.rfc-editor.org/rfc/rfc2315
            type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
            let cipher = Aes128CbcDec::new_from_slices(key, &iv)
                .map_err(|_| DecryptError::InvalidKeyOrIv)?;

            cipher
                .decrypt_padded_vec::<Pkcs7>(data)
                .map_err(|_| DecryptError::Failed)?
        }
        192 => {
            // Section 10.3 Step 2 of RFC 2315 https://www.rfc-editor.org/rfc/rfc2315
            type Aes192CbcDec = cbc::Decryptor<aes::Aes192>;
            let cipher = Aes192CbcDec::new_from_slices(key, &iv)
                .map_err(|_| DecryptError::InvalidKeyOrIv)?;

            cipher
                .decrypt_padded_vec::<Pkcs7>(data)
                .map_err(|_| DecryptError::Failed)?
        }
        256 => {
            // Section 10.3 Step 2 of RFC 2315 https://www.rfc-editor.org/rfc/rfc2315
            type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;
            let cipher = Aes256CbcDec::new_from_slices(key, &iv)
                .map_err(|_| DecryptError::InvalidKeyOrIv)?;

            cipher
                .decrypt_padded_vec::<Pkcs7>(data)
                .map_err(|_| DecryptError::Failed)?
        }
        _ => unreachable!(),
    };

    // 6.
    Ok(plaintext)
}

fn decrypt_aes_ctr_gen<B>(key: &[u8], counter: &[u8], data: &[u8]) -> Result<Vec<u8>, DecryptError>
where
    B: KeyIvInit + StreamCipher,
{
    let mut cipher = B::new_from_slices(key, counter).map_err(|_| DecryptError::InvalidKeyOrIv)?;

    let mut plaintext = data.to_vec();
    cipher
        .try_apply_keystream(&mut plaintext)
        .map_err(|_| DecryptError::TooMuchData)?;

    Ok(plaintext)
}

fn decrypt_aes_gcm_gen<N: ArraySize>(
    key: &[u8],
    tag: &aes_gcm::Tag,
    nonce: &[u8],
    length: usize,
    additional_data: Vec<u8>,
    plaintext: &mut [u8],
) -> Result<(), DecryptError> {
    let nonce = <&Nonce<N>>::try_from(nonce).map_err(|_| DecryptError::InvalidIvLength)?;
    match length {
        128 => {
            let cipher = aes_gcm::AesGcm::<Aes128, N>::new_from_slice(key)
                .map_err(|_| DecryptError::Failed)?;
            cipher
                .decrypt_inout_detached(nonce, additional_data.as_slice(), plaintext.into(), tag)
                .map_err(|_| DecryptError::Failed)?
        }
        192 => {
            let cipher = aes_gcm::AesGcm::<Aes192, N>::new_from_slice(key)
                .map_err(|_| DecryptError::Failed)?;
            cipher
                .decrypt_inout_detached(nonce, additional_data.as_slice(), plaintext.into(), tag)
                .map_err(|_| DecryptError::Failed)?
        }
        256 => {
            let cipher = aes_gcm::AesGcm::<Aes256, N>::new_from_slice(key)
                .map_err(|_| DecryptError::Failed)?;
            cipher
                .decrypt_inout_detached(nonce, additional_data.as_slice(), plaintext.into(), tag)
                .map_err(|_| DecryptError::Failed)?
        }
        _ => return Err(DecryptError::InvalidLength),
    };

    Ok(())
}

fn decrypt_aes_ctr(
    key: V8RawKeyData,
    key_length: usize,
    counter: &[u8],
    ctr_length: usize,
    data: &[u8],
) -> Result<Vec<u8>, DecryptError> {
    let key = key.as_secret_key()?;

    match ctr_length {
        32 => match key_length {
            128 => decrypt_aes_ctr_gen::<Ctr32BE<aes::Aes128>>(key, counter, data),
            192 => decrypt_aes_ctr_gen::<Ctr32BE<aes::Aes192>>(key, counter, data),
            256 => decrypt_aes_ctr_gen::<Ctr32BE<aes::Aes256>>(key, counter, data),
            _ => Err(DecryptError::InvalidLength),
        },
        64 => match key_length {
            128 => decrypt_aes_ctr_gen::<Ctr64BE<aes::Aes128>>(key, counter, data),
            192 => decrypt_aes_ctr_gen::<Ctr64BE<aes::Aes192>>(key, counter, data),
            256 => decrypt_aes_ctr_gen::<Ctr64BE<aes::Aes256>>(key, counter, data),
            _ => Err(DecryptError::InvalidLength),
        },
        128 => match key_length {
            128 => decrypt_aes_ctr_gen::<Ctr128BE<aes::Aes128>>(key, counter, data),
            192 => decrypt_aes_ctr_gen::<Ctr128BE<aes::Aes192>>(key, counter, data),
            256 => decrypt_aes_ctr_gen::<Ctr128BE<aes::Aes256>>(key, counter, data),
            _ => Err(DecryptError::InvalidLength),
        },
        _ => Err(DecryptError::InvalidCounterLength),
    }
}

fn decrypt_aes_gcm(
    key: V8RawKeyData,
    length: usize,
    tag_length: usize,
    iv: Vec<u8>,
    additional_data: Option<Vec<u8>>,
    data: &[u8],
) -> Result<Vec<u8>, DecryptError> {
    let key = key.as_secret_key()?;
    let additional_data = additional_data.unwrap_or_default();

    // The `aes_gcm` crate only supports 128 bits tag length.
    //
    // Note that encryption won't fail, it instead truncates the tag
    // to the specified tag length as specified in the spec.
    if tag_length != 128 {
        return Err(DecryptError::InvalidTagLength);
    }

    let sep = data.len() - (tag_length / 8);
    let tag = &data[sep..];

    let tag = <&aes_gcm::Tag>::try_from(tag).map_err(|_| DecryptError::InvalidTagLength)?;

    // The actual ciphertext, called plaintext because it is reused in place.
    let mut plaintext = data[..sep].to_vec();

    // Fixed 96-bit or 128-bit nonce
    match iv.len() {
        12 => decrypt_aes_gcm_gen::<U12>(key, tag, &iv, length, additional_data, &mut plaintext)?,
        16 => decrypt_aes_gcm_gen::<U16>(key, tag, &iv, length, additional_data, &mut plaintext)?,
        _ => return Err(DecryptError::InvalidIvLength),
    }

    Ok(plaintext)
}

fn decrypt_aes_ocb(
    key: V8RawKeyData,
    length: usize,
    tag_length: usize,
    iv: Vec<u8>,
    additional_data: Option<Vec<u8>>,
    data: &[u8],
) -> Result<Vec<u8>, DecryptError> {
    use ocb3::Ocb3;
    use ocb3::aead::AeadInOut as Ocb3AeadInOut;
    use ocb3::aead::KeyInit as Ocb3KeyInit;

    let key = key.as_secret_key()?;
    let additional_data = additional_data.unwrap_or_default();

    // The `ocb3` crate only supports 128 bits tag length.
    //
    // Note that encryption won't fail, it instead truncates the tag
    // to the specified tag length as specified in the spec.
    if tag_length != 128 {
        return Err(DecryptError::InvalidTagLength);
    }

    // OCB allows nonces of 1 to 15 bytes, but `Ocb3` below takes its default
    // 12-byte nonce.
    let nonce = <&ocb3::Nonce<U12>>::try_from(iv.as_slice())
        .map_err(|_| DecryptError::UnsupportedOcbNonce)?;

    let sep = data.len() - (tag_length / 8);
    let tag =
        <&ocb3::Tag<U16>>::try_from(&data[sep..]).map_err(|_| DecryptError::InvalidTagLength)?;

    // The actual ciphertext, called plaintext because it is reused in place.
    let mut plaintext = data[..sep].to_vec();

    match length {
        128 => {
            let cipher =
                Ocb3::<aes::Aes128>::new_from_slice(key).map_err(|_| DecryptError::Failed)?;
            cipher
                .decrypt_inout_detached(
                    nonce,
                    &additional_data,
                    plaintext.as_mut_slice().into(),
                    tag,
                )
                .map_err(|_| DecryptError::Failed)?;
        }
        192 => {
            let cipher =
                Ocb3::<aes::Aes192>::new_from_slice(key).map_err(|_| DecryptError::Failed)?;
            cipher
                .decrypt_inout_detached(
                    nonce,
                    &additional_data,
                    plaintext.as_mut_slice().into(),
                    tag,
                )
                .map_err(|_| DecryptError::Failed)?;
        }
        256 => {
            let cipher =
                Ocb3::<aes::Aes256>::new_from_slice(key).map_err(|_| DecryptError::Failed)?;
            cipher
                .decrypt_inout_detached(
                    nonce,
                    &additional_data,
                    plaintext.as_mut_slice().into(),
                    tag,
                )
                .map_err(|_| DecryptError::Failed)?;
        }
        _ => return Err(DecryptError::InvalidLength),
    }

    Ok(plaintext)
}

impl From<DecryptError> for OpError {
    fn from(error: DecryptError) -> Self {
        match error {
            DecryptError::General(inner) => inner.into(),
            DecryptError::Pkcs1(..) => OpError::new(error.to_string()),
            DecryptError::InvalidLength
            | DecryptError::InvalidCounterLength
            | DecryptError::InvalidTagLength
            | DecryptError::InvalidIvLength => OpError::type_error(error.to_string()),
            DecryptError::Failed
            | DecryptError::InvalidKeyOrIv
            | DecryptError::TooMuchData
            | DecryptError::Rsa(..) => operation_error(error.to_string()),
            DecryptError::UnsupportedOcbNonce => not_supported_error(error.to_string()),
        }
    }
}
