#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

pub mod meh;
pub mod serializer;
pub mod directory;

use crate::meh::MehDB;
use crate::serializer::{ByteKey, ByteValue};

fn main() {
    let db: MehDB = MehDB::new(None).unwrap();
    let key = ByteKey(vec![0, 0]);
    let value = ByteValue(vec![1]);
    let _ = db.put(key, value);
    for i in 0..100000 {
        let i = i as u64;
        let key = ByteKey(i.to_le_bytes().to_vec());
        let value = ByteValue((i*2).to_le_bytes().to_vec());
        db.put(key, value).unwrap();
    }
}
