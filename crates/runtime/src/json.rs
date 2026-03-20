use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn from_str<T>(input: &str) -> Result<T, simd_json::Error>
where
    T: DeserializeOwned,
{
    let mut bytes = input.as_bytes().to_vec();
    simd_json::serde::from_slice(&mut bytes)
}

pub fn to_string<T>(value: &T) -> Result<String, simd_json::Error>
where
    T: Serialize + ?Sized,
{
    simd_json::serde::to_string(value)
}

pub fn to_vec<T>(value: &T) -> Result<Vec<u8>, simd_json::Error>
where
    T: Serialize + ?Sized,
{
    simd_json::serde::to_vec(value)
}
