//! Convenience re-exports for building applications with CakeDb.

pub use crate::{bincode_wrapper::Bincode, CakeDb, DbKey, DbValue};
pub use bincode::{Decode, Encode};
pub use redb::TableDefinition;
pub use serde_derive::{Deserialize, Serialize};
