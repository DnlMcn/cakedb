//! Read operations on key-value tables.

use std::{collections::BTreeMap, ops::RangeBounds};

use redb::{ReadableTable, TableDefinition};

use crate::{bincode_wrapper::Bincode, CakeDb};

use super::traits::{DbKey, DbValue};

// TODO: replace `Box<dyn std::error::Error>` with a structured error type.

impl CakeDb {
    /// Returns the value if it exists.
    pub fn get<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
    ) -> Result<Option<V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self.read_table(table_def)?.get(key)?.map(|g| g.value()))
    }

    /// Returns `true` if the table contains the given key.
    #[must_use]
    pub fn contains_key<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self.read_table(table_def)?.get(key)?.is_some())
    }

    /// Returns the first key-value pair matching the given predicate.
    pub fn find<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        predicate: impl Fn(&K, &V) -> bool,
    ) -> Result<Option<(K, V)>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .table(table_def)?
            .into_iter()
            .find(|(k, v)| predicate(k, v)))
    }

    /// Returns the last key-value pair matching the given predicate.
    pub fn rfind<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        predicate: impl Fn(&K, &V) -> bool,
    ) -> Result<Option<(K, V)>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .table(table_def)?
            .into_iter()
            .rfind(|(k, v)| predicate(k, v)))
    }

    /// Returns the 'nth' key-value pair matching the given predicate.
    ///
    /// If there are less than `n` matching pairs, returns `None`.
    ///
    /// This function assumes zero-indexing for `n` (e.g. to get the third k-v pair, `n` should be 2).
    pub fn find_nth<K, V>(
        &self,
        n: usize,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        predicate: impl Fn(&K, &V) -> bool,
    ) -> Result<Option<(K, V)>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let mut count = 0;
        for (k, v) in self.table(table_def)? {
            if predicate(&k, &v) {
                if count == n {
                    return Ok(Some((k, v)));
                }
                count += 1;
            }
        }
        Ok(None)
    }

    /// Counts how many key-value pairs return `true` for the given predicate.
    pub fn count_matches<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        predicate: impl Fn(&K, &V) -> bool,
    ) -> Result<usize, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .table(table_def)?
            .into_iter()
            .filter(|(k, v)| predicate(k, v))
            .count())
    }

    /// Returns all key-value pairs that match the given predicate.
    pub fn filter<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        predicate: impl Fn(&K, &V) -> bool,
    ) -> Result<BTreeMap<K, V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .table(table_def)?
            .into_iter()
            .filter(|(k, v)| predicate(k, v))
            .collect())
    }

    /// Returns all keys of the key-value pairs that match the given predicate.
    pub fn filter_keys<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        predicate: impl Fn(&K, &V) -> bool,
    ) -> Result<Vec<K>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .table(table_def)?
            .into_iter()
            .filter(|(k, v)| predicate(k, v))
            .map(|(k, _)| k)
            .collect())
    }

    /// Returns all the key-value pairs in the given table.
    pub fn table<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<BTreeMap<K, V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .read_table(table_def)?
            .iter()?
            .filter_map(Result::ok)
            .map(|(kg, vg)| (kg.value(), vg.value()))
            .collect())
    }

    /// Returns the first pair in the table.
    pub fn first<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<Option<(K, V)>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .read_table(table_def)?
            .first()?
            .map(|(kg, vg)| (kg.value(), vg.value())))
    }

    /// Returns the last pair in the table.
    pub fn last<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<Option<(K, V)>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .read_table(table_def)?
            .last()?
            .map(|(kg, vg)| (kg.value(), vg.value())))
    }

    /// Returns the first key in the given table.
    pub fn first_key<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<Option<K>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .read_table(table_def)?
            .first()?
            .map(|(kg, _)| kg.value()))
    }

    /// Returns the last key in the given table.
    pub fn last_key<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<Option<K>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .read_table(table_def)?
            .last()?
            .map(|(kg, _)| kg.value()))
    }

    /// Returns all key-value pairs in the given range of keys
    pub fn range<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        range: impl RangeBounds<K>,
    ) -> Result<BTreeMap<K, V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        Ok(self
            .read_table(table_def)?
            .range(range)?
            .flatten()
            .map(|(kg, vg)| (kg.value(), vg.value()))
            .collect())
    }
}
