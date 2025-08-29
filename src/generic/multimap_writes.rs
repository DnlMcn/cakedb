//! Write operations for multimap tables.

use redb::{MultimapTableDefinition, ReadableMultimapTable};

use crate::{bincode_wrapper::Bincode, CakeDb};

use super::traits::{DbKey, DbValue};

// TODO: replace `Box<dyn std::error::Error>` with a structured error type.

impl CakeDb {
    /// Adds a given value to the mapping of the key.
    ///
    /// Returns `true` if the key-value pair was present.
    pub fn multimap_insert<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
        value: V,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        let existed: bool;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_multimap_table(table_def)?;
            existed = table.insert(key, value)?;
        }
        transaction.commit()?;

        Ok(existed)
    }

    /// Adds the given values to the mapping of the key.
    ///
    /// Returns `true` if the key already had at least one value mapped.
    pub fn multimap_insert_values<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
        values: impl IntoIterator<Item = V>,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey + Clone,
        V: DbValue + Ord,
    {
        let mut existed = false;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_multimap_table(table_def)?;
            if !table.get(key)?.is_empty() {
                existed = true;
            }

            for v in values.into_iter() {
                table.insert(key, v)?;
            }
        }
        transaction.commit()?;

        Ok(existed)
    }

    /// Inserts each value of each key into the table.
    ///
    /// `data` can be any data structure that can be iterated in the same way as a `Vec<(K, Vec<V>)>` or a `BTreeMap<K, Vec<V>>`.
    pub fn multimap_batch_insert<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
        data: impl IntoIterator<Item = (K, impl IntoIterator<Item = V>)>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        K: DbKey + Clone,
        V: DbValue + Ord,
    {
        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_multimap_table(table_def)?;
            for (k, v) in data {
                for v in v {
                    table.insert(&k, v)?;
                }
            }
        }
        transaction.commit()?;

        Ok(())
    }

    /// Assigns `values` to the mappings of the key, overwriting any previous values.
    ///
    /// Regardless of overlap with new values, all old values will be removed.
    ///
    /// Returns `true` if the key had at least one value mapped.
    pub fn multimap_assign<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
        values: impl IntoIterator<Item = V>,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey + Clone,
        V: DbValue + Ord,
    {
        let mut existed = false;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_multimap_table(table_def)?;

            if !table.remove_all(key)?.is_empty() {
                existed = true;
            };

            for v in values.into_iter() {
                table.insert(key, v)?;
            }
        }
        transaction.commit()?;

        Ok(existed)
    }

    /// Removes a given value from the mapping of the key.
    ///
    /// Returns `true` if the value was present.
    pub fn multimap_remove<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
        value: V,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        let existed: bool;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_multimap_table(table_def)?;
            existed = table.remove(key, value)?;
        }
        transaction.commit()?;

        Ok(existed)
    }

    /// Removes all values from a key in the table.
    ///
    /// Returns the removed values in ascending order.
    pub fn multimap_remove_all<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
    ) -> Result<Vec<V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        let values: Vec<V>;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_multimap_table(table_def)?;
            values = table
                .remove_all(key)?
                .flatten()
                .map(|v| v.value())
                .collect();
        }
        transaction.commit()?;

        Ok(values)
    }

    /// Clears the contents of the given table, removing all key-value mappings.
    pub fn clear_multimap_table<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        let transaction = self.inner.begin_write()?;
        {
            let reference = self.multimap_table(table_def)?;
            let mut table = transaction.open_multimap_table(table_def)?;

            for k in reference.keys() {
                table.remove_all(k)?;
            }
        }
        transaction.commit()?;

        Ok(())
    }

    /// Deletes the given multimap table.
    ///
    /// Returns `true` if the table existed.
    pub fn delete_multimap_table<K, V>(
        &mut self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        let existed: bool;

        let transaction = self.inner.begin_write()?;
        {
            existed = transaction.delete_multimap_table(table_def)?;
        }
        transaction.commit()?;

        Ok(existed)
    }
}
