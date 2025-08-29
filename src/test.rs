#![cfg(test)]

use crate::prelude::*;

#[derive(Serialize, Deserialize, Encode, Decode, Debug, PartialEq)]
struct TestStruct {
    a: u32,
    b: String,
}

impl TestStruct {
    fn new(a: u32, b: &str) -> Self {
        Self { a, b: b.to_string() }
    }
}

const TABLE: TableDefinition<Bincode<u32>, Bincode<TestStruct>> =
    TableDefinition::new("test_table");

#[test]
fn insert_and_get() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_test_db()?;
    db.insert(TABLE, &1, TestStruct::new(10, "ten"))?;
    let value = db.get(TABLE, &1)?.unwrap();
    assert_eq!(value.a, 10);
    assert_eq!(value.b, "ten");
    Ok(())
}

#[test]
fn batch_insert_and_filter() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_test_db()?;
    let data = vec![
        (1, TestStruct::new(1, "one")),
        (2, TestStruct::new(2, "two")),
        (3, TestStruct::new(3, "three")),
    ];
    db.batch_insert(TABLE, data)?;
    let result = db.filter(TABLE, |_, v| v.a % 2 == 1)?;
    assert_eq!(result.len(), 2);
    Ok(())
}

#[test]
fn savepoint_and_restore() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_test_db()?;
    let key = db.savepoint()?;
    db.insert(TABLE, &1, TestStruct::new(4, "four"))?;
    assert!(db.get(TABLE, &1)?.is_some());
    db.load_savepoint(key)?;
    assert!(db.get(TABLE, &1)?.is_none());
    Ok(())
}

