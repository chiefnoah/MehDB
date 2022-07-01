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
}
