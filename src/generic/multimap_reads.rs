use std::collections::{BTreeMap, BTreeSet};

use redb::{MultimapTableDefinition, ReadableMultimapTable};

use crate::{bincode_wrapper::Bincode, CakeDb};

use super::traits::{DbKey, DbValue};

impl CakeDb {
    /// Returns all values mapped to the given key.
    pub fn multimap_get<K, V>(
        &self,
        table: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
    ) -> Result<BTreeSet<V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        Ok(self
            .read_multimap_table(table)?
            .get(key)?
            .flatten()
            .map(|vg| vg.value())
            .collect())
    }

    /// Returns all key-value mappings in the given table.
    pub fn multimap_table<K, V>(
        &self,
        table: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<BTreeMap<K, BTreeSet<V>>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        Ok(self
            .read_multimap_table(table)?
            .iter()?
            .flatten()
            .map(|(key_ag, value_ag)| {
                (
                    key_ag.value(),
                    value_ag.flatten().map(|v| v.value()).collect(),
                )
            })
            .collect())
    }
}
