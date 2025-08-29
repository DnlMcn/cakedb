//! Bincode-backed wrapper types for use with `redb`.

use std::any::type_name;
use std::cmp::Ordering;
use std::fmt::Debug;

use bincode::{config, Decode, Encode};
use redb::{Key, TypeName, Value};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Wrapper type to handle keys and values using bincode serialization.
///
/// Wrap your types in this when creating your `TableDefinition`s.
#[derive(Debug)]
pub struct Bincode<T>(pub T);

impl<T> Value for Bincode<T>
where
    T: Debug + Serialize + for<'a> Deserialize<'a> + Decode<()> + Encode,
{
    type SelfType<'a>
        = T
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::decode_from_slice(data, config::standard())
            // TODO: replace `expect` with proper error handling.
            .expect("failed to deserialize bincode value")
            .0
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a + 'b,
    {
        bincode::encode_to_vec(value, config::standard())
            // TODO: replace `expect` with proper error handling.
            .expect("failed to serialize bincode value")
    }

    fn type_name() -> TypeName {
        TypeName::new(&format!("Bincode<{}>", type_name::<T>()))
    }
}

impl<T> Key for Bincode<T>
where
    T: Debug + Serialize + DeserializeOwned + Ord + Decode<()> + Encode,
{
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}
