pub mod meh;
pub mod serializer;

use crate::meh::MehDB;
use crate::serializer::{ByteKey, ByteValue};

fn main() {
    let db: MehDB = MehDB::new(None).unwrap();
    let key = ByteKey(vec!(b"00"));
    let value = ByteValue(vec!(b"1"));
    db.put(key, value);
}
