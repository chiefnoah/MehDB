pub mod meh;
pub mod serializer;

use crate::meh::{Map, MehDB};

fn main() {
    let db: MehDB = MehDB::new(None).unwrap();
    db.put(b"00", b"1");
}
