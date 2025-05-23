pub mod save;
pub mod bincode_wrapper;
pub mod generic;
pub mod prelude;

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use redb::{Database};
use save::EzSavepoint;
use tempfile::NamedTempFile;


/// Represents a high-level database encapsulation that handles interactions with the underlying storage.
///
/// Usage of the provided methods is encouraged, but if you need to do anything more advanced, you can use the `database` and `mut_database` methods to access the `redb` database directly.
///
/// # Examples
/// ```
/// use ezdb::prelude::*;
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
///     // In production, use the `new` method instead of `new_test_db`.
///     let mut db = EzDb::new_test_db()?;
///
///     // The savepoint is stored inside the struct so it's not dropped;
///     // we only receive the key.
///     let save_key = db.savepoint()?;
///
///     let var = TestStruct::new(2, "two".to_string());
///     db.insert(TABLE, &1, var)?;
///     assert!(db.get(TABLE, &1)?.is_some());
///
///     // Edits and predicates use closures.
///     db.edit(TABLE, &1, |v| v.b.push_str(" (edited)"))?;
///     assert_eq!(db.get(TABLE, &1)?.unwrap().b, "two (edited)");
///
///     let pairs = vec![
///         (4, TestStruct::new(15, "fifteen".into())),
///         (7, TestStruct::new(12, "twelve".into())),
///     ];
///
///     // These batch functions take any data structure that can turn into an iter over `(K, V)`
///     // A `BTreeMap<K, V>` would also work here, for example
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
pub struct EzDb {
    inner: Database,
    savepoints: BTreeMap<usize, EzSavepoint>,
    tempfile_path: Option<PathBuf>,
}

impl EzDb {
    /// Initializes the database, or creates it if it doesn't exist.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, redb::DatabaseError> {
        Ok(Self {
            inner: Database::create(path)?,
            savepoints: BTreeMap::new(),
            tempfile_path: None,
        })
    }

    /// Initializes a new database in a tempfile.
    pub fn new_test_db() -> Result<Self, redb::DatabaseError> {
        let path = NamedTempFile::with_suffix(".redb")
            .unwrap()
            .path()
            .to_path_buf();

        Ok(Self {
            inner: Database::create(&path)?,
            savepoints: BTreeMap::new(),
            tempfile_path: Some(path),
        })
    }

    /// Provides a reference to the inner `Database` struct.
    ///
    /// This method is unnecessary for most use cases; using the methods provided for `EzDb` is encouraged.
    pub fn database(&self) -> &Database {
        &self.inner
    }

    /// Provides a mutable reference to the inner `Database` struct.
    ///
    /// This method is unnecessary for most use cases; using the methods provided for `EzDb` is encouraged.
    pub fn mut_database(&mut self) -> &mut Database {
        &mut self.inner
    }

    /// Compacts the database file.
    ///
    /// Returns `true` if compaction was performed, and `false` if no further compaction was possible.
    pub fn compact(&mut self) -> Result<bool, redb::CompactionError> {
        self.inner.compact()
    }

    /// Returns the path to the file this database is stored in.
    ///
    /// Should only return `Some` for test instances.
    pub fn tempfile_path(&self) -> Option<&PathBuf> {
        self.tempfile_path.as_ref()
    }
}

/// Returns the path to your computer's local data directory.
///
/// |Platform | Value                                                                      | Example                                                       |
/// | ------- | -------------------------------------------------------------------------- | ------------------------------------------------------------- |
/// | Linux   | `$XDG_DATA_HOME`/`_project_path_` or `$HOME`/.local/share/`_project_path_` | /home/alice/.local/share/ezdb                               |
/// | macOS   | `$HOME`/Library/Application Support/`_project_path_`                       | /Users/Alice/Library/Application Support/EzDb |
/// | Windows | `{FOLDERID_LocalAppData}`\\`_project_path_`\\data                          | C:\Users\Alice\AppData\Local\EzDb\data            |
pub fn data_local_path() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "", "EzDb")
        .map(|db_dir| db_dir.data_local_dir().to_path_buf())
}
