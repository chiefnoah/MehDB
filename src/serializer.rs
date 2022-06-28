use std::io::{self, File, Result};
use std::time::{Instant, UNIX_EPOCH};

trait Serializable {
    fn pack(&self, file: Option<&File>) -> Result<u64, io::Error>;
    fn unpack(file: &File) -> Self;
}

struct Key([u8]);
struct Value([u8]);

struct KeyValue {
    key: Key,
    value: Value,
}

pub struct Transaction {
    instant: u128,
    data: Vec<KeyValue>,
}

impl Serializable for Transaction {
    fn pack(&self, file: Option<&File>) -> Result<u64, io::Error> {}
}

trait Transactor {
    fn begin(&self, keys: [Key]) -> Transaction;
    fn write(&self, transaction: Transaction) -> Result<u64>;
}

struct FileTransactor {
    file: File,
    // The first time this transactor was used with this file
    epoch: Instant,
}
