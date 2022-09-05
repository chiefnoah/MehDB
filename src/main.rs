#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
extern crate pretty_env_logger;


pub mod directory;
pub mod meh;
pub mod segment;
pub mod serializer;

use crate::meh::MehDB;
use crate::serializer::{ByteKey, ByteValue};
use anyhow::{Result, Context};
use log::info;


fn main() -> Result<()>{
    pretty_env_logger::init();
    let mut db: MehDB = MehDB::init(None).unwrap();
    const RECORDS: usize = 1_000_000;
    for i in 0..RECORDS {
        info!("i: {}", i);
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let value = ByteValue(i * 2);
        db.put(key, value)
            .context("Error inserting record")
            .expect("Unable to insert record!");
        let test_key = ByteKey((0 as u64).to_le_bytes().to_vec());
        db.get(test_key).with_context(|| {
            format!("Testing for when key 0 disappears. i: {}", i)
        }).unwrap();
    }
    info!("Done putting, trying to read now...");
    for i in 0..RECORDS {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let r = db.get(key).expect(&format!("Missing record for {}", i));
        println!("k: {} v: {:?}", i, r);
        assert_eq!(r.value, i * 2);
    }
    Ok(())
}
