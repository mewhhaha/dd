//! Serde data model to and from V8 values.
//!
//! Structs and maps become plain objects, sequences and tuples become arrays,
//! `None` and `()` become `null`, unit variants become their name, and other
//! variants become `{ Variant: value }`, as with `serde_json`. Integers past
//! `Number.MAX_SAFE_INTEGER` become BigInts. [`ToJsBuffer`] and serde's
//! `serialize_bytes` become `Uint8Array`s; byte arrays deserialize from any
//! array buffer view.
//!
//! Reading a struct skips fields whose value is `undefined`, so serde defaults
//! apply to them.

use serde::de::{self, DeserializeSeed, IntoDeserializer, Visitor};
use serde::ser::{self, Serialize};
use std::fmt;

const MAX_SAFE_INTEGER: i64 = (1 << 53) - 1;
const MIN_SAFE_INTEGER: i64 = -MAX_SAFE_INTEGER;
const MAX_DEPTH: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    fn expected(what: &str, value: v8::Local<v8::Value>) -> Self {
        Self(format!("expected {what}, got {}", value.type_repr()))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Bytes that serialize as a `Uint8Array` that owns them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToJsBuffer(Box<[u8]>);

impl From<Vec<u8>> for ToJsBuffer {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into_boxed_slice())
    }
}

impl From<Box<[u8]>> for ToJsBuffer {
    fn from(value: Box<[u8]>) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for ToJsBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for ToJsBuffer {
    fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

pub fn to_v8<'s, T: Serialize + ?Sized>(
    scope: &v8::PinScope<'s, '_>,
    value: &T,
) -> Result<v8::Local<'s, v8::Value>> {
    value.serialize(Serializer { scope })
}

pub fn from_v8<'s, T: de::DeserializeOwned>(
    scope: &v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
) -> Result<T> {
    T::deserialize(Deserializer {
        scope,
        input: value,
        depth: 0,
    })
}

/// A `Uint8Array` that owns a copy of `bytes`.
pub fn uint8_array<'s>(
    scope: &v8::PinScope<'s, '_>,
    bytes: Vec<u8>,
) -> v8::Local<'s, v8::Uint8Array> {
    let length = bytes.len();
    let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
    let buffer = v8::ArrayBuffer::with_backing_store(scope, &store);
    v8::Uint8Array::new(scope, buffer, 0, length).expect("Uint8Array within its maximum length")
}

pub(crate) fn key<'s>(scope: &v8::PinScope<'s, '_, ()>, name: &str) -> v8::Local<'s, v8::String> {
    v8::String::new_from_utf8(scope, name.as_bytes(), v8::NewStringType::Internalized)
        .expect("property name within V8's string length limit")
}

fn string<'s>(scope: &v8::PinScope<'s, '_>, value: &str) -> Result<v8::Local<'s, v8::Value>> {
    v8::String::new(scope, value)
        .map(Into::into)
        .ok_or_else(|| Error("string exceeds V8's maximum length".to_string()))
}

struct Serializer<'a, 's, 'i> {
    scope: &'a v8::PinScope<'s, 'i>,
}

impl<'a, 's, 'i> Serializer<'a, 's, 'i> {
    fn wrap_variant(
        &self,
        variant: &'static str,
        value: v8::Local<'s, v8::Value>,
    ) -> v8::Local<'s, v8::Value> {
        let object = v8::Object::new(self.scope);
        object.create_data_property(self.scope, key(self.scope, variant).into(), value);
        object.into()
    }
}

impl<'a, 's, 'i> ser::Serializer for Serializer<'a, 's, 'i> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;
    type SerializeSeq = ArraySerializer<'a, 's, 'i>;
    type SerializeTuple = ArraySerializer<'a, 's, 'i>;
    type SerializeTupleStruct = ArraySerializer<'a, 's, 'i>;
    type SerializeTupleVariant = ArraySerializer<'a, 's, 'i>;
    type SerializeMap = ObjectSerializer<'a, 's, 'i>;
    type SerializeStruct = ObjectSerializer<'a, 's, 'i>;
    type SerializeStructVariant = ObjectSerializer<'a, 's, 'i>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        Ok(v8::Boolean::new(self.scope, v).into())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        self.serialize_i32(i32::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        Ok(v8::Integer::new(self.scope, v).into())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        if (MIN_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&v) {
            Ok(v8::Number::new(self.scope, v as f64).into())
        } else {
            Ok(v8::BigInt::new_from_i64(self.scope, v).into())
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        self.serialize_u32(u32::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        self.serialize_u32(u32::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        Ok(v8::Integer::new_from_unsigned(self.scope, v).into())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        if v <= MAX_SAFE_INTEGER as u64 {
            Ok(v8::Number::new(self.scope, v as f64).into())
        } else {
            Ok(v8::BigInt::new_from_u64(self.scope, v).into())
        }
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        Ok(v8::Number::new(self.scope, v).into())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        self.serialize_str(v.encode_utf8(&mut [0; 4]))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        string(self.scope, v)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        Ok(uint8_array(self.scope, v.to_vec()).into())
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        Ok(v8::null(self.scope).into())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Ok(v8::null(self.scope).into())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        Ok(key(self.scope, variant).into())
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        let value = value.serialize(Serializer { scope: self.scope })?;
        Ok(self.wrap_variant(variant, value))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(ArraySerializer {
            scope: self.scope,
            elements: Vec::with_capacity(len.unwrap_or(0)),
            variant: None,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(ArraySerializer {
            scope: self.scope,
            elements: Vec::with_capacity(len),
            variant: Some(variant),
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(ObjectSerializer::new(self.scope, None))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(ObjectSerializer::new(self.scope, None))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(ObjectSerializer::new(self.scope, Some(variant)))
    }
}

struct ArraySerializer<'a, 's, 'i> {
    scope: &'a v8::PinScope<'s, 'i>,
    elements: Vec<v8::Local<'s, v8::Value>>,
    variant: Option<&'static str>,
}

impl<'a, 's, 'i> ArraySerializer<'a, 's, 'i> {
    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let value = value.serialize(Serializer { scope: self.scope })?;
        self.elements.push(value);
        Ok(())
    }

    fn finish(self) -> v8::Local<'s, v8::Value> {
        let array: v8::Local<v8::Value> =
            v8::Array::new_with_elements(self.scope, &self.elements).into();
        match self.variant {
            Some(variant) => Serializer { scope: self.scope }.wrap_variant(variant, array),
            None => array,
        }
    }
}

impl<'s> ser::SerializeSeq for ArraySerializer<'_, 's, '_> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.finish())
    }
}

impl<'s> ser::SerializeTuple for ArraySerializer<'_, 's, '_> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.finish())
    }
}

impl<'s> ser::SerializeTupleStruct for ArraySerializer<'_, 's, '_> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.finish())
    }
}

impl<'s> ser::SerializeTupleVariant for ArraySerializer<'_, 's, '_> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.push(value)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.finish())
    }
}

struct ObjectSerializer<'a, 's, 'i> {
    scope: &'a v8::PinScope<'s, 'i>,
    object: v8::Local<'s, v8::Object>,
    pending_key: Option<v8::Local<'s, v8::Name>>,
    variant: Option<&'static str>,
}

impl<'a, 's, 'i> ObjectSerializer<'a, 's, 'i> {
    fn new(scope: &'a v8::PinScope<'s, 'i>, variant: Option<&'static str>) -> Self {
        Self {
            scope,
            object: v8::Object::new(scope),
            pending_key: None,
            variant,
        }
    }

    fn set_field<T: Serialize + ?Sized>(&mut self, name: &'static str, value: &T) -> Result<()> {
        let value = value.serialize(Serializer { scope: self.scope })?;
        self.object
            .create_data_property(self.scope, key(self.scope, name).into(), value);
        Ok(())
    }

    fn finish(self) -> v8::Local<'s, v8::Value> {
        match self.variant {
            Some(variant) => {
                Serializer { scope: self.scope }.wrap_variant(variant, self.object.into())
            }
            None => self.object.into(),
        }
    }
}

impl<'s> ser::SerializeMap for ObjectSerializer<'_, 's, '_> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        let key = key.serialize(Serializer { scope: self.scope })?;
        let key = if key.is_name() {
            v8::Local::<v8::Name>::try_from(key).expect("name")
        } else {
            key.to_string(self.scope)
                .ok_or_else(|| Error("map key is not convertible to a string".to_string()))?
                .into()
        };
        self.pending_key = Some(key);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| Error("map value serialized before its key".to_string()))?;
        let value = value.serialize(Serializer { scope: self.scope })?;
        self.object.create_data_property(self.scope, key, value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.finish())
    }
}

impl<'s> ser::SerializeStruct for ObjectSerializer<'_, 's, '_> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        self.set_field(name, value)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.finish())
    }
}

impl<'s> ser::SerializeStructVariant for ObjectSerializer<'_, 's, '_> {
    type Ok = v8::Local<'s, v8::Value>;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        self.set_field(name, value)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(self.finish())
    }
}

struct Deserializer<'a, 's, 'i> {
    scope: &'a v8::PinScope<'s, 'i>,
    input: v8::Local<'s, v8::Value>,
    depth: usize,
}

impl<'a, 's, 'i> Deserializer<'a, 's, 'i> {
    fn child(&self, input: v8::Local<'s, v8::Value>) -> Result<Self> {
        if self.depth >= MAX_DEPTH {
            return Err(Error("value is nested too deeply".to_string()));
        }
        Ok(Self {
            scope: self.scope,
            input,
            depth: self.depth + 1,
        })
    }

    fn number(&self) -> Result<f64> {
        if let Ok(number) = v8::Local::<v8::Number>::try_from(self.input) {
            return Ok(number.value());
        }
        if let Ok(bigint) = v8::Local::<v8::BigInt>::try_from(self.input) {
            let (value, lossless) = bigint.i64_value();
            if lossless {
                return Ok(value as f64);
            }
            let (value, lossless) = bigint.u64_value();
            if lossless {
                return Ok(value as f64);
            }
        }
        Err(Error::expected("number", self.input))
    }

    fn integer(&self) -> Result<i128> {
        if let Ok(bigint) = v8::Local::<v8::BigInt>::try_from(self.input) {
            let (value, lossless) = bigint.i64_value();
            if lossless {
                return Ok(i128::from(value));
            }
            let (value, lossless) = bigint.u64_value();
            if lossless {
                return Ok(i128::from(value));
            }
            return Err(Error("bigint is out of the 64-bit range".to_string()));
        }
        let number = self.number()?;
        if number.is_finite() {
            Ok(number.trunc() as i128)
        } else if number.is_nan() {
            Ok(0)
        } else {
            Err(Error(format!("expected a finite number, got {number}")))
        }
    }

    fn string(&self) -> Result<String> {
        let string = v8::Local::<v8::String>::try_from(self.input)
            .map_err(|_| Error::expected("string", self.input))?;
        Ok(string.to_rust_string_lossy(self.scope))
    }

    fn bytes(&self) -> Option<Vec<u8>> {
        crate::builtins::buffer_bytes(self.input)
    }
}

macro_rules! deserialize_integer {
    ($method:ident, $visit:ident, $ty:ty) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
            let value = self.integer()?;
            let value = <$ty>::try_from(value)
                .map_err(|_| Error(format!("{value} is out of range for {}", stringify!($ty))))?;
            visitor.$visit(value)
        }
    };
}

impl<'de> de::Deserializer<'de> for Deserializer<'_, '_, '_> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let input = self.input;
        if input.is_null_or_undefined() {
            visitor.visit_unit()
        } else if input.is_boolean() {
            visitor.visit_bool(input.is_true())
        } else if input.is_number() {
            let number = self.number()?;
            if number.fract() == 0.0
                && (MIN_SAFE_INTEGER as f64..=MAX_SAFE_INTEGER as f64).contains(&number)
            {
                visitor.visit_i64(number as i64)
            } else {
                visitor.visit_f64(number)
            }
        } else if input.is_big_int() {
            match self.integer()? {
                value if value < 0 => visitor.visit_i64(value as i64),
                value => visitor.visit_u64(value as u64),
            }
        } else if input.is_string() {
            visitor.visit_string(self.string()?)
        } else if input.is_array() {
            self.deserialize_seq(visitor)
        } else if let Some(bytes) = self.bytes() {
            visitor.visit_byte_buf(bytes)
        } else if input.is_object() {
            self.deserialize_map(visitor)
        } else {
            Err(Error::expected("a serializable value", input))
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.input.boolean_value(self.scope))
    }

    deserialize_integer!(deserialize_i8, visit_i8, i8);
    deserialize_integer!(deserialize_i16, visit_i16, i16);
    deserialize_integer!(deserialize_i32, visit_i32, i32);
    deserialize_integer!(deserialize_i64, visit_i64, i64);
    deserialize_integer!(deserialize_u8, visit_u8, u8);
    deserialize_integer!(deserialize_u16, visit_u16, u16);
    deserialize_integer!(deserialize_u32, visit_u32, u32);
    deserialize_integer!(deserialize_u64, visit_u64, u64);

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(self.number()? as f32)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(self.number()?)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_string(self.string()?)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.bytes() {
            Some(bytes) => visitor.visit_byte_buf(bytes),
            None if self.input.is_array() => self.deserialize_seq(visitor),
            None => Err(Error::expected("an array buffer or view", self.input)),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if self.input.is_null_or_undefined() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if let Ok(array) = v8::Local::<v8::Array>::try_from(self.input) {
            return visitor.visit_seq(ArrayAccess {
                parent: &self,
                array,
                index: 0,
                length: array.length(),
            });
        }
        if let Some(bytes) = self.bytes() {
            return visitor.visit_seq(de::value::SeqDeserializer::new(bytes.into_iter()));
        }
        Err(Error::expected("array", self.input))
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let object = v8::Local::<v8::Object>::try_from(self.input)
            .map_err(|_| Error::expected("object", self.input))?;
        let names = object
            .get_own_property_names(self.scope, v8::GetPropertyNamesArgs::default())
            .ok_or_else(|| Error("reading object keys threw".to_string()))?;
        visitor.visit_map(ObjectAccess {
            parent: &self,
            object,
            names,
            index: 0,
            length: names.length(),
            value: None,
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        if fields.is_empty() {
            return self.deserialize_map(visitor);
        }
        let object = v8::Local::<v8::Object>::try_from(self.input)
            .map_err(|_| Error::expected("object", self.input))?;
        visitor.visit_map(StructAccess {
            parent: &self,
            object,
            fields: fields.iter(),
            value: None,
        })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        if self.input.is_string() {
            return visitor.visit_enum(self.string()?.into_deserializer());
        }
        let object = v8::Local::<v8::Object>::try_from(self.input)
            .map_err(|_| Error::expected("string or single-key object", self.input))?;
        let names = object
            .get_own_property_names(self.scope, v8::GetPropertyNamesArgs::default())
            .ok_or_else(|| Error("reading object keys threw".to_string()))?;
        if names.length() != 1 {
            return Err(Error(format!(
                "expected an enum object with one key, got {} keys",
                names.length()
            )));
        }
        let name = names
            .get_index(self.scope, 0)
            .ok_or_else(|| Error("reading enum key threw".to_string()))?;
        let value = object
            .get(self.scope, name)
            .ok_or_else(|| Error("reading enum value threw".to_string()))?;
        let variant = name.to_rust_string_lossy(self.scope);
        visitor.visit_enum(EnumAccess {
            variant,
            value: self.child(value)?,
        })
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }
}

struct ArrayAccess<'p, 'a, 's, 'i> {
    parent: &'p Deserializer<'a, 's, 'i>,
    array: v8::Local<'s, v8::Array>,
    index: u32,
    length: u32,
}

impl<'de> de::SeqAccess<'de> for ArrayAccess<'_, '_, '_, '_> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.index >= self.length {
            return Ok(None);
        }
        let value = self
            .array
            .get_index(self.parent.scope, self.index)
            .ok_or_else(|| Error("reading array element threw".to_string()))?;
        self.index += 1;
        seed.deserialize(self.parent.child(value)?).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some((self.length - self.index) as usize)
    }
}

struct ObjectAccess<'p, 'a, 's, 'i> {
    parent: &'p Deserializer<'a, 's, 'i>,
    object: v8::Local<'s, v8::Object>,
    names: v8::Local<'s, v8::Array>,
    index: u32,
    length: u32,
    value: Option<v8::Local<'s, v8::Value>>,
}

impl<'de> de::MapAccess<'de> for ObjectAccess<'_, '_, '_, '_> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        while self.index < self.length {
            let scope = self.parent.scope;
            let name = self
                .names
                .get_index(scope, self.index)
                .ok_or_else(|| Error("reading object key threw".to_string()))?;
            self.index += 1;
            let value = self
                .object
                .get(scope, name)
                .ok_or_else(|| Error("reading object value threw".to_string()))?;
            if value.is_undefined() {
                continue;
            }
            self.value = Some(value);
            let name = name
                .to_string(scope)
                .ok_or_else(|| Error("object key is not a string".to_string()))?
                .to_rust_string_lossy(scope);
            return seed.deserialize(name.into_deserializer()).map(Some);
        }
        Ok(None)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let value = self
            .value
            .take()
            .ok_or_else(|| Error("map value read before its key".to_string()))?;
        seed.deserialize(self.parent.child(value)?)
    }
}

struct StructAccess<'p, 'a, 's, 'i> {
    parent: &'p Deserializer<'a, 's, 'i>,
    object: v8::Local<'s, v8::Object>,
    fields: std::slice::Iter<'static, &'static str>,
    value: Option<v8::Local<'s, v8::Value>>,
}

impl<'de> de::MapAccess<'de> for StructAccess<'_, '_, '_, '_> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        let scope = self.parent.scope;
        for field in self.fields.by_ref() {
            let value = self
                .object
                .get(scope, key(scope, field).into())
                .ok_or_else(|| Error(format!("reading field {field} threw")))?;
            if value.is_undefined() {
                continue;
            }
            self.value = Some(value);
            return seed.deserialize((*field).into_deserializer()).map(Some);
        }
        Ok(None)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let value = self
            .value
            .take()
            .ok_or_else(|| Error("struct field read before its name".to_string()))?;
        seed.deserialize(self.parent.child(value)?)
    }
}

struct EnumAccess<'a, 's, 'i> {
    variant: String,
    value: Deserializer<'a, 's, 'i>,
}

impl<'de, 'a, 's, 'i> de::EnumAccess<'de> for EnumAccess<'a, 's, 'i> {
    type Error = Error;
    type Variant = Deserializer<'a, 's, 'i>;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(self.variant.into_deserializer())?;
        Ok((variant, self.value))
    }
}

impl<'de> de::VariantAccess<'de> for Deserializer<'_, '_, '_> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        de::Deserializer::deserialize_seq(self, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        de::Deserializer::deserialize_struct(self, "", fields, visitor)
    }
}
