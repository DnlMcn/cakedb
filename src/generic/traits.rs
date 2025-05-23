use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub trait DbKey: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug + Ord {}
impl<T> DbKey for T where
    T: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug + Ord
{
}

pub trait DbValue: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug {}
impl<T> DbValue for T where T: Serialize + for<'de> Deserialize<'de> + Decode<()> + Encode + Debug {}
