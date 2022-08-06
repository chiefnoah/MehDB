#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

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
            .with_context(|| {
                format!("Error inserting record")
            })?;
    }
    info!("Done putting, trying to read now...");
    for i in 0..662 {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let e = db.get(key).expect(&format!("Missing record for {}", i));
        info!("k: {} v: {:?}", i, e);
        assert_eq!(e, ByteValue(i * 2));
    }
    Ok(())
}
