# EzDb

A small wrapper over [redb](https://crates.io/crates/redb) with a super simple API so you can work on your business logic without worrying about the database.

## Features

- Abstracts over transactions, commits are automatic and immediate; in-function failures roll back transactions
- Dozens of utility methods for batch insertions, in-place edits, filtering, and more
- Savepoints for safe rollbacks
- Simple, friction-free API

## Examples

```rust
use ezdb::prelude::*;
use std::fmt::Debug;

#[derive(Serialize, Deserialize, Encode, Decode, Debug)]
pub struct TestStruct {
    a: u32,
    b: String,
}

impl TestStruct {
    pub fn new(a: u32, b: String) -> Self {
        Self { a, b }
    }
}

const TABLE: TableDefinition<Bincode<u32>, Bincode<TestStruct>> =
        TableDefinition::new("test_table");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = EzDb::new_test_db()?;

    // The savepoint is stored inside the struct so it's not dropped;
    // we only receive the key.
    let save_key = db.savepoint()?;

    let var = TestStruct::new(2, "two".to_string());
    db.insert(TABLE, &1, var)?;
    assert!(db.get(TABLE, &1)?.is_some());

    // Edits and predicates use closures.
    db.edit(TABLE, &1, |v| v.b.push_str(" (edited)"))?;
    assert_eq!(db.get(TABLE, &1)?.unwrap().b, "two (edited)");

    let pairs = vec![
        (4, TestStruct::new(15, "fifteen".into())),
        (7, TestStruct::new(12, "twelve".into())),
    ];

    // These batch functions take any data structure that can turn into an iter over `(K, V)`
    // A `BTreeMap<K, V>` would also work here, for example
    db.batch_insert(TABLE, pairs)?;
    assert_eq!(db.filter(TABLE, |_, v| v.b.contains("e"))?.len(), 3);

    // Reloads state back to before all the operations
    db.load_savepoint(save_key)?;
    assert!(db.get(TABLE, &1)?.is_none());

    Ok(())
}
```

## License

Licensed under MIT or Apache 2.0, at your choice.
