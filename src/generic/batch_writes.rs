use std::collections::BTreeMap;

use redb::{ReadableTable, TableDefinition};

use crate::{bincode_wrapper::Bincode, CakeDb};

use super::traits::{DbKey, DbValue};

impl CakeDb {
    /// Tries to add all key-value pairs to the given table.
    ///
    /// Values whose keys are already present are not modified.
    ///
    /// Returns a vector with the keys that were already present.
    pub fn batch_try_add<K, V, I>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        pairs: I,
    ) -> Result<Vec<K>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut existing: Vec<K> = Vec::new();

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;

            for (key, value) in pairs {
                if table.get(&key)?.is_none() {
                    table.insert(key, value)?;
                } else {
                    existing.push(key);
                }
            }
        }
        transaction.commit()?;

        Ok(existing)
    }

    /// Inserts all key-value pairs into the given table.
    ///
    /// Overwrites any values whose keys were already present.
    ///
    /// Returns a map with all the key-value pairs that were overwritten.
    pub fn batch_insert<K, V, I>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        pairs: I,
    ) -> Result<BTreeMap<K, V>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut old_pairs = BTreeMap::new();

        let transaction = self.inner.begin_write()?;
        {
            let mut table = transaction.open_table(table_def)?;

            for (key, value) in pairs {
                if let Some(value) = table.insert(&key, value)? {
                    old_pairs.insert(key, value.value());
                }
            }
        }
        transaction.commit()?;

        Ok(old_pairs)
    }

    /// Edits the values of all given keys in the given table, according to the given `edit` closure.
    ///
    /// Returns a map with the previous key-value pairs for all edited ones.
    pub fn batch_edit<'a, K, V, I>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        keys: I,
        edit: impl Fn(&K, &mut V),
    ) -> Result<BTreeMap<K, V>, Box<dyn std::error::Error>>
    where
        K: DbKey + Clone,
        V: DbValue,
        I: IntoIterator<Item = &'a K>,
    {
        let mut old_pairs = BTreeMap::new();

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

                if let Some(old_value) = table.insert(key, edited)? {
                    old_pairs.insert(key.clone(), old_value.value());
                }
            }
        }
        transaction.commit()?;

        Ok(old_pairs)
    }

    /// Edits the values of all given keys in the given table, according to the given `edit` closure.
    ///
    /// Doesn't return old key-value pairs, which requires cloning keys, which is why this is the fast version.
    ///
    /// Use this instead of `batch_edit` if you have no use for the old values.
    pub fn batch_edit_fast<'a, K, V, I>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        keys: I,
        edit: impl Fn(&K, &mut V),
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        K: DbKey,
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

    /// Edits the values of all given keys in the given table, according to the given `edit` closure.
    ///
    /// Returns how many values were changed (not necessarily equal to how many keys were given).
    pub fn batch_edit_count<'a, K, V, I>(
        &mut self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
        keys: I,
        edit: impl Fn(&K, &mut V),
    ) -> Result<usize, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + PartialEq + Clone,
        I: IntoIterator<Item = &'a K>,
    {
        let mut count = 0;

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

                if let Some(old_value) = table.insert(key, edited.clone())? {
                    if old_value.value() != edited {
                        count += 1;
                    }
                }
            }
        }
        transaction.commit()?;

        Ok(count)
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
