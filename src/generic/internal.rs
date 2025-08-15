use anyhow::anyhow;
use redb::{
    MultimapTableDefinition, ReadOnlyMultimapTable, ReadOnlyTable, ReadableDatabase,
    TableDefinition, TableError,
};

use crate::{bincode_wrapper::Bincode, CakeDb};

use super::traits::{DbKey, DbValue};

impl CakeDb {
    /// Opens the given table as read-only and returns it.
    pub(super) fn read_table<K, V>(
        &self,
        table_def: TableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<ReadOnlyTable<Bincode<K>, Bincode<V>>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue,
    {
        let read = self
            .inner
            .begin_read()
            .map_err(|e| anyhow!("failed to begin read for '{table_def}': {e}"))?;
        match read.open_table(table_def) {
            Err(TableError::TableDoesNotExist(outer_err)) => {
                // `open_table` from a `ReadTransaction` doesn't create the table if it doesn't exist,
                // so create it with a `WriteTransaction` here.
                let write = self.inner.begin_write().map_err(|e| anyhow!("Failed to begin write transaction to create a table: {e} (Tried creating a table because of this error: {outer_err})"))?;
                write.open_table(table_def).map_err(|e| anyhow!("Failed to open table: {e} (Tried creating a table because of this error: {outer_err})"))?;
                write.commit().map_err(|e| anyhow!("Failed to commit write transaction creating table: {e} (Tried creating a table because of this error: {outer_err})"))?;

                let read = self
                    .inner
                    .begin_read()
                    .map_err(|e| anyhow!("failed to begin read for '{table_def}': {e}"))?;

                let table = read.open_table(table_def).map_err(|e| {
                    anyhow!(
                        "failed to open table for '{table_def}': {e} (initial error: {outer_err}"
                    )
                })?;

                Ok(table)
            }
            Err(e) => Err(anyhow!("Failed to open table for '{table_def}': {e}").into()),
            Ok(table) => Ok(table),
        }
    }

    /// Opens the given multimap table as read-only and returns it.
    pub(super) fn read_multimap_table<K, V>(
        &self,
        table_def: MultimapTableDefinition<Bincode<K>, Bincode<V>>,
    ) -> Result<ReadOnlyMultimapTable<Bincode<K>, Bincode<V>>, Box<dyn std::error::Error>>
    where
        K: DbKey,
        V: DbValue + Ord,
    {
        Ok(self
            .inner
            .begin_read()
            .map_err(|e| anyhow!("failed to begin read for '{table_def}': {e}"))?
            .open_multimap_table(table_def)
            .map_err(|e| anyhow!("failed to open table for '{table_def}': {e}"))?)
    }
}
