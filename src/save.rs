use std::collections::BTreeMap;

use redb::Savepoint;
use time::UtcDateTime;

use crate::CakeDb;

pub struct CakeSavepoint {
    pub savepoint: Savepoint,
    pub creation_time: UtcDateTime,
}

impl CakeDb {
    /// Creates a new savepoint and returns its key. The savepoint is stored in-memory inside the struct itself, not in the database.
    ///
    /// These savepoints are ephemeral; they will become invalid if the `CakeDb` reference is dropped.
    pub fn savepoint(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let write = self.inner.begin_write()?;
        let savepoint = write.ephemeral_savepoint()?;
        write.commit()?;

        let key: usize;
        if let Some((max_key, _)) = self.savepoints.last_key_value() {
            key = max_key + 1;
        } else {
            key = 0
        }

        self.savepoints.insert(
            key,
            CakeSavepoint {
                savepoint,
                creation_time: UtcDateTime::now(),
            },
        );

        Ok(key)
    }

    /// Loads a savepoint from its `key`.
    ///
    /// Returns an error if there's no savepoint with a matching `key`.
    pub fn load_savepoint(&mut self, key: usize) -> Result<(), Box<dyn std::error::Error>> {
        let Some(save) = self.savepoints.get(&key) else {
            return Err(anyhow::anyhow!("failed to get specified savepoint: {key}").into());
        };

        let mut transaction = self.inner.begin_write()?;
        transaction.restore_savepoint(&save.savepoint)?;
        transaction.commit()?;

        // After loading a savepoint, savepoints created after it are invalidated; remove them.
        self.savepoints.retain(|k, _| k <= &key);

        Ok(())
    }

    /// Returns a map of the currently stored savepoints.
    pub const fn savepoints(&self) -> &BTreeMap<usize, CakeSavepoint> {
        &self.savepoints
    }

    /// Frees all currently stored savepoints.
    pub fn clear_savepoints(&mut self) {
        self.savepoints.clear();
    }
}
