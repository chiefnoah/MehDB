#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

pub mod directory;
pub mod meh;
pub mod segment;
pub mod serializer;

use crate::meh::MehDB;
use crate::serializer::{ByteKey, ByteValue};


fn main() {
    pretty_env_logger::init();
    let mut db: MehDB = MehDB::init(None).unwrap();
    let key = ByteKey(vec![0, 0]);
    let value = ByteValue(vec![1]);
    let _ = db.put(key, value);
    for i in 0..100000 {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let value = ByteValue((i * 2).to_le_bytes().to_vec());
        match db.put(key, value) {
            Ok(()) => return,
            Err(e) => {
                println!("Error: {}", e);
                panic!("Error");
            },
        }
    }
}
