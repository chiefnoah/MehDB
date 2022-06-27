
mod meh;

use crate::meh::{Map, MehDB};

fn main() {
    let db: MehDB = MehDB::new(None);
    db.put(b"00", b"1");
}
