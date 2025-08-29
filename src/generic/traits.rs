use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Types that can act as keys in CakeDb tables.
///
/// Any type implementing the required traits automatically implements `DbKey`.
///
/// # Examples
/// ```
/// use cakedb::prelude::*;
///
/// #[derive(Serialize, Deserialize, Encode, Decode, Debug, Ord, PartialOrd, Eq, PartialEq)]
/// struct MyKey(u32);
///
/// fn assert_db_key<K: DbKey>() {}
/// assert_db_key::<MyKey>();
/// ```
pub trait DbKey: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug + Ord {}
impl<T> DbKey for T where
    T: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug + Ord
{
}

/// Types that can be stored as values in CakeDb tables.
///
/// # Examples
/// ```
/// use cakedb::prelude::*;
///
/// #[derive(Serialize, Deserialize, Encode, Decode, Debug)]
/// struct MyValue(String);
///
/// fn assert_db_value<V: DbValue>() {}
/// assert_db_value::<MyValue>();
/// ```
pub trait DbValue: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug {}
impl<T> DbValue for T where T: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug {}
