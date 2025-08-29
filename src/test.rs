#![cfg(test)]

use std::collections::{BTreeMap, BTreeSet};

use crate::prelude::*;
use redb::MultimapTableDefinition;

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

#[derive(
    Serialize,
    Deserialize,
    Encode,
    Decode,
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
struct ComplexRecord {
    id: u32,
    name: String,
    tags: Vec<String>,
    meta: BTreeMap<String, u64>,
}

impl ComplexRecord {
    fn new(id: u32, name: &str, tags: &[&str]) -> Self {
        let tags_vec = tags.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let meta = tags
            .iter()
            .enumerate()
            .map(|(i, t)| (t.to_string(), i as u64))
            .collect();
        Self {
            id,
            name: name.to_string(),
            tags: tags_vec,
            meta,
        }
    }
}

const TABLE: TableDefinition<Bincode<u32>, Bincode<TestStruct>> =
    TableDefinition::new("test_table");
const COMPLEX_TABLE: TableDefinition<Bincode<u32>, Bincode<ComplexRecord>> =
    TableDefinition::new("complex_table");
const MULTI_TABLE: MultimapTableDefinition<Bincode<String>, Bincode<ComplexRecord>> =
    MultimapTableDefinition::new("complex_multimap");

#[test]
fn insert_and_get() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_temp()?;
    db.insert(TABLE, &1, TestStruct::new(10, "ten"))?;
    let value = db.get(TABLE, &1)?.unwrap();
    assert_eq!(value.a, 10);
    assert_eq!(value.b, "ten");
    Ok(())
}

#[test]
fn batch_insert_and_filter() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_temp()?;
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
    let mut db = CakeDb::new_temp()?;
    let key = db.savepoint()?;
    db.insert(TABLE, &1, TestStruct::new(4, "four"))?;
    assert!(db.get(TABLE, &1)?.is_some());
    db.load_savepoint(key)?;
    assert!(db.get(TABLE, &1)?.is_none());
    Ok(())
}

#[test]
fn try_add_contains_update_remove() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_temp()?;
    let rec = ComplexRecord::new(1, "alpha", &["x", "y"]);
    assert!(db.try_add(COMPLEX_TABLE, &1, rec.clone())?);
    assert!(!db.try_add(COMPLEX_TABLE, &1, rec.clone())?);
    assert!(db.contains_key(COMPLEX_TABLE, &1)?);
    assert!(!db.contains_key(COMPLEX_TABLE, &2)?);
    let old = db.update(COMPLEX_TABLE, &1, |v| v.name = "beta".into())?;
    assert_eq!(old.name, "alpha");
    assert_eq!(db.get(COMPLEX_TABLE, &1)?.unwrap().name, "beta");
    assert!(db.update(COMPLEX_TABLE, &99, |_| {}).is_err());
    assert!(db.remove(COMPLEX_TABLE, &1)?.is_some());
    assert!(db.remove(COMPLEX_TABLE, &1)?.is_none());
    Ok(())
}

#[test]
fn query_helpers() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_temp()?;
    let records = vec![
        (1, ComplexRecord::new(1, "one", &["red", "blue"])),
        (2, ComplexRecord::new(2, "two", &["green"])),
        (3, ComplexRecord::new(3, "three", &["red"])),
        (4, ComplexRecord::new(4, "four", &["blue", "yellow"])),
    ];
    db.batch_insert(COMPLEX_TABLE, records)?;

    let red = |_: &u32, v: &ComplexRecord| v.tags.contains(&"red".to_string());
    assert_eq!(db.find(COMPLEX_TABLE, red)?.unwrap().0, 1);
    assert_eq!(db.rfind(COMPLEX_TABLE, red)?.unwrap().0, 3);
    assert_eq!(db.find_nth(1, COMPLEX_TABLE, red)?.unwrap().0, 3);
    assert!(db.find_nth(5, COMPLEX_TABLE, red)?.is_none());
    let blue = |_: &u32, v: &ComplexRecord| v.tags.contains(&"blue".to_string());
    assert_eq!(db.count_matches(COMPLEX_TABLE, blue)?, 2);
    let single = db.filter_keys(COMPLEX_TABLE, |_, v| v.tags.len() == 1)?;
    assert_eq!(single, vec![2, 3]);
    let table = db.table(COMPLEX_TABLE)?;
    assert_eq!(table.len(), 4);
    assert_eq!(db.first(COMPLEX_TABLE)?.unwrap().0, 1);
    assert_eq!(db.last(COMPLEX_TABLE)?.unwrap().0, 4);
    assert_eq!(db.first_key(COMPLEX_TABLE)?, Some(1));
    assert_eq!(db.last_key(COMPLEX_TABLE)?, Some(4));
    let range_map = db.range(COMPLEX_TABLE, 2..4)?;
    assert_eq!(range_map.keys().cloned().collect::<Vec<_>>(), vec![2, 3]);
    Ok(())
}

#[test]
fn batch_update_clear_delete() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_temp()?;
    let records = vec![
        (1, ComplexRecord::new(1, "one", &[])),
        (2, ComplexRecord::new(2, "two", &[])),
    ];
    db.batch_insert(COMPLEX_TABLE, records)?;
    let keys = vec![&1u32, &2u32];
    db.batch_update(COMPLEX_TABLE, keys, |_, v| v.tags.push("batch".into()))?;
    assert_eq!(db.get(COMPLEX_TABLE, &1)?.unwrap().tags, vec!["batch"]);
    db.clear_table(COMPLEX_TABLE)?;
    assert!(db.table(COMPLEX_TABLE)?.is_empty());
    assert!(db.delete_table(COMPLEX_TABLE)?);
    assert!(!db.delete_table(COMPLEX_TABLE)?);
    Ok(())
}

#[test]
fn multimap_operations() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_temp()?;
    let r1 = ComplexRecord::new(1, "one", &["a"]);
    let r2 = ComplexRecord::new(2, "two", &["b"]);
    let r3 = ComplexRecord::new(3, "three", &["c"]);
    assert!(!db.multimap_insert(MULTI_TABLE, &"k1".to_string(), r1.clone())?);
    assert!(db.multimap_insert(MULTI_TABLE, &"k1".to_string(), r1.clone())?);
    assert!(db.multimap_insert_values(MULTI_TABLE, &"k1".to_string(), vec![r2.clone(), r3.clone()])?);
    let mut expected = BTreeSet::new();
    expected.insert(r1.clone());
    expected.insert(r2.clone());
    expected.insert(r3.clone());
    assert_eq!(db.multimap_get(MULTI_TABLE, &"k1".to_string())?, expected);
    let batch = vec![
        ("k2".to_string(), vec![r1.clone()]),
        ("k3".to_string(), vec![r2.clone(), r3.clone()]),
    ];
    db.multimap_batch_insert(MULTI_TABLE, batch)?;
    let table = db.multimap_table(MULTI_TABLE)?;
    assert_eq!(table.len(), 3);
    assert!(db.multimap_assign(MULTI_TABLE, &"k1".to_string(), vec![r1.clone()])?);
    assert_eq!(db.multimap_get(MULTI_TABLE, &"k1".to_string())?.len(), 1);
    assert!(db.multimap_remove(MULTI_TABLE, &"k1".to_string(), r1.clone())?);
    assert!(!db.multimap_remove(MULTI_TABLE, &"k1".to_string(), r1.clone())?);
    let removed = db.multimap_remove_all(MULTI_TABLE, &"k3".to_string())?;
    assert_eq!(removed, vec![r2.clone(), r3.clone()]);
    db.clear_multimap_table(MULTI_TABLE)?;
    assert!(db.multimap_table(MULTI_TABLE)?.is_empty());
    assert!(db.delete_multimap_table(MULTI_TABLE)?);
    assert!(!db.delete_multimap_table(MULTI_TABLE)?);
    Ok(())
}

#[test]
fn savepoint_clear_and_compact() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = CakeDb::new_temp()?;
    assert!(db.tempfile_path().is_some());
    db.compact()?;
    let _ = db.savepoint()?;
    let _ = db.savepoint()?;
    assert_eq!(db.savepoints().len(), 2);
    db.clear_savepoints();
    assert!(db.savepoints().is_empty());
    Ok(())
}


