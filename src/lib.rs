#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! CakeDb provides a lightweight wrapper around the `redb` key-value store
//! with conveniences such as automatic serialization and in-memory savepoints.

pub mod error;
pub mod bincode_wrapper;
mod generic;
pub mod prelude;
mod save;
mod test;

// TODO: introduce a structured error type instead of using `Box<dyn std::error::Error>`.

pub use generic::traits::{DbKey, DbValue};
pub use save::CakeSavepoint;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use tempfile::NamedTempFile;

/// Represents a high-level database encapsulation that handles interactions with the underlying storage.
///
/// Usage of the provided methods is encouraged, but if you need more control,
/// the [`database`](Self::database) and [`mut_database`](Self::mut_database)
/// methods expose the underlying `redb::Database`.
///
/// # Examples
/// ```
/// use cakedb::prelude::*;
/// use std::fmt::Debug;
///
/// #[derive(Serialize, Deserialize, Encode, Decode, Debug)]
/// pub struct TestStruct {
///     a: u32,
///     b: String,
/// }
///
/// impl TestStruct {
///     pub fn new(a: u32, b: String) -> Self {
///         Self { a, b }
///     }
/// }
///
/// const TABLE: TableDefinition<Bincode<u32>, Bincode<TestStruct>> =
///         TableDefinition::new("test_table");
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // In production, use the `new` method instead of `new_temp`.
///     let mut db = CakeDb::new_temp()?;
///
///     // The savepoint is stored inside the struct, we only receive its key.
///     let save_key = db.savepoint()?;
///
///     let var = TestStruct::new(2, "two".to_string());
///     db.insert(TABLE, &1, var)?;
///     assert!(db.get(TABLE, &1)?.is_some());
///
///     // Updates and predicates use closures.
///     db.update(TABLE, &1, |v| v.b.push_str(" (edited)"))?;
///     assert_eq!(db.get(TABLE, &1)?.unwrap().b, "two (edited)");
///
///     let pairs = vec![
///         (4, TestStruct::new(15, "fifteen".into())),
///         (7, TestStruct::new(12, "twelve".into())),
///     ];
///
///     // These batch functions take any data structure that can turn into an iter over `(K, V)` for `pairs`
///     // A `BTreeMap` would also work here, for example
///     db.batch_insert(TABLE, pairs)?;
///     assert_eq!(db.filter(TABLE, |_, v| v.b.contains("e"))?.len(), 3);
///
///     // Reloads state back to before all the operations
///     db.load_savepoint(save_key)?;
///     assert!(db.get(TABLE, &1)?.is_none());
///
///     Ok(())
/// }
/// ```
pub struct CakeDb {
    inner: redb::Database,
    savepoints: BTreeMap<usize, CakeSavepoint>,
    tempfile_path: Option<PathBuf>,
}

impl CakeDb {
    /// Initializes the database, or creates it if it doesn't exist.
    ///
    /// If you're just testing the crate, consider the [`new_test_db`](Self::new_test_db)
    /// method, or get your machine's default data path with
    /// [`data_local_path`](crate::data_local_path).
    pub fn new(path: impl AsRef<Path>) -> Result<Self, redb::DatabaseError> {
        Ok(Self {
            inner: redb::Database::create(path)?,
            savepoints: BTreeMap::new(),
            tempfile_path: None,
        })
    }

    /// Initializes a fresh database in a tempfile.
    pub fn new_temp() -> Result<Self, redb::DatabaseError> {
        // TODO: handle potential errors from `NamedTempFile::with_suffix` instead of unwrapping.
        let path = NamedTempFile::with_suffix(".redb")
            .unwrap()
            .path()
            .to_path_buf();

        Ok(Self {
            inner: redb::Database::create(&path)?,
            savepoints: BTreeMap::new(),
            tempfile_path: Some(path),
        })
    }

    /// Provides a reference to the inner `Database` struct. Use this if you need finer control.
    pub fn database(&self) -> &redb::Database {
        &self.inner
    }

    /// Provides a mutable reference to the inner `Database` struct. Use this if you need finer control.
    pub fn mut_database(&mut self) -> &mut redb::Database {
        &mut self.inner
    }

    /// Compacts the database file.
    ///
    /// Returns `true` if compaction was performed, and `false` if no further compaction was possible.
    ///
    /// If you get an error due to a transaction in progress, it's probably because you have savepoints active.
    /// Clear them and try again.
    pub fn compact(&mut self) -> Result<bool, redb::CompactionError> {
        self.inner.compact()
    }

    /// Returns the path to the tempfile this database is stored in.
    ///
    /// Should only return `Some` for instances created with `new_temp`.
    pub fn tempfile_path(&self) -> Option<&PathBuf> {
        self.tempfile_path.as_ref()
    }
}

/// Returns the path to your computer's local data directory.
///
/// | Platform | Value                                                             | Example                                               |
/// | ------- | ----------------------------------------------------------------- | ----------------------------------------------------- |
/// | Linux   | `$XDG_DATA_HOME`/`cakedb` or `$HOME`/.local/share/`cakedb`         | `/home/alice/.local/share/cakedb`                     |
/// | macOS   | `$HOME`/Library/Application Support/`cakedb`                       | `/Users/Alice/Library/Application Support/cakedb`     |
/// | Windows | `{FOLDERID_LocalAppData}`\`cakedb`\data                          | `C:\Users\Alice\AppData\Local\cakedb\data` |
pub fn data_local_path() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "", "cakedb")
        .map(|db_dir| db_dir.data_local_dir().to_path_buf())
}
