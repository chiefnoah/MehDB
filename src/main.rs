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
    for i in 0..662 {
        info!("i: {}", i);
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let value = ByteValue(i * 2);
        db.put(key, value)
            .context("Error inserting record")
            .expect("Unable to insert record");
    }
    info!("Done putting, trying to read now...");
    for i in 0..662 {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let r = db.get(key).expect(&format!("Missing record for {}", i));
        info!("k: {} v: {:?}", i, r);
        assert_eq!(r.value, i * 2);
    }
    Ok(())
}
