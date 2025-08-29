//! Batch write helpers for working with multiple entries at once.

use redb::{ReadableTable, TableDefinition};

use crate::{bincode_wrapper::Bincode, CakeDb};

use super::traits::{DbKey, DbValue};

// TODO: replace `Box<dyn std::error::Error>` with a structured error type.

impl CakeDb {
    /// Inserts all key-value pairs into the given table.
    ///
    /// Overwrites any values whose keys were already present.
    pub fn batch_insert<K, V, I>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        data: I,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
        I: IntoIterator<Item = (K, V)>,
    {
        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;

            for (key, value) in data {
                table.insert(&key, value)?;
            }
        }
        transaction.commit()?;

        Ok(())
    }

    /// Edits the values of all given keys in the given table, according to the given `edit` closure.
    pub fn batch_update<'a, K, V, I>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        keys: I,
        edit: impl Fn(&K, &mut V),
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        K: DbKey + Clone,
        V: DbValue,
        I: IntoIterator<Item = &'a K>,
    {
        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;

            for key in keys {
                let mut edited: V;
                {
                    let Some(value) = table.get(key)? else {
                        continue;
                    };
                    edited = value.value();
                    edit(key, &mut edited)
                };

                table.insert(key, edited)?;
            }
        }
        transaction.commit()?;

        Ok(())
    }

    /// Clears the contents of the given table, removing all key-value pairs.
    pub fn clear_table<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;
            table.retain(|_, _| false)?;
        }
        transaction.commit()?;

        Ok(())
    }

    /// Deletes the given table.
    ///
    /// Returns `true` if the table existed.
    #[must_use]
    pub fn delete_table<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let existed: bool;

        let transaction = self.inner.begin_write()?;
        {
            existed = transaction.delete_table(table_def)?;
        }
        transaction.commit()?;

        Ok(existed)
    }
}
