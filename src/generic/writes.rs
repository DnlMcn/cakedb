use redb::{ReadableTable, TableDefinition};

use crate::{bincode_wrapper::Bincode, CakeDb};

use super::traits::{DbKey, DbValue};

impl CakeDb {
    /// Tries to add a key-value pair to the table.
    ///
    /// Returns whether the key was newly added. That is:
    /// - If this key **wasn't** present, adds the key-value pair and returns `true`.
    /// - If this key **was** present, returns `false` and does not modify the table.
    pub fn try_add<K, V>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
        value: V,
    ) -> Result<bool, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let newly_added: bool;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;

            if table.get(key)?.is_none() {
                table.insert(key, value)?;
                newly_added = true;
            } else {
                newly_added = false;
            }
        }
        transaction.commit()?;

        Ok(newly_added)
    }

    /// Inserts a key-value pair into the table.
    ///
    /// If the map had this key present, its value will be overwritten by the new value.
    ///
    /// Returns the old value.
    pub fn insert<K, V>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
        value: V,
    ) -> Result<Option<V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let old_value: Option<V>;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;
            old_value = table.insert(key, value)?.map(|guard| guard.value());
        }
        transaction.commit()?;

        Ok(old_value)
    }

    /// Applies `edit` to the given entry, replacing the old value.
    ///
    /// Returns the old value.
    ///
    /// Returns an `Err` if the key isn't found in the given table.
    pub fn update<K, V>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
        mut edit: impl FnMut(&mut V),
    ) -> Result<V, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let key_not_found_error = anyhow::anyhow!("edit error: key not found in table");
        let old_value: V;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;

            let mut edited: V;
            {
                let Some(value) = table.get(key)? else {
                    return Err(key_not_found_error.into());
                };
                edited = value.value();
                edit(&mut edited)
            };

            let insert = table.insert(key, edited)?;
            old_value = match insert {
                Some(value) => value.value(),
                None => return Err(key_not_found_error.into()),
            }
        }
        transaction.commit()?;

        Ok(old_value)
    }

    /// Removes the given key.
    ///
    /// If it was present, its value is returned.
    pub fn remove<K, V>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        key: &K,
    ) -> Result<Option<V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let old_value: Option<V>;

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;
            old_value = table.remove(key)?.map(|guard| guard.value());
        }
        transaction.commit()?;

        Ok(old_value)
    }
}
