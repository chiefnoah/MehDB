use std::io::{self, Result};
use std::fs::File;
use std::time::{Instant, UNIX_EPOCH};

pub trait Serializable {
    fn pack(&self, file: Option<&File>) -> Result<u64>;
    fn unpack(file: &File) -> Self;
}

pub struct Key(Vec<u8>);
pub struct Value(Vec<u8>);

struct KeyValue {
    key: Box<Key>,
    value: Box<Value>,
}

pub struct Transaction {
    instant: u128,
    data: Vec<KeyValue>,
}

impl Serializable for Transaction {
    fn pack(&self, file: Option<&File>) -> Result<u64> {
        todo!("Implement this.");
    }
    fn unpack(file: &File) -> Self {
        todo!("Implement this.");
    }

}

pub trait Transactor {
    fn begin(&self, keys: &Vec<Box<Key>>) -> Transaction;
    fn write(&self, transaction: Transaction) -> Result<u64>;
}

pub struct FileTransactor {
    file: File,
    // The first time this transactor was used with this file
    epoch: Instant,
}

impl FileTransactor {
    pub fn init(file: File) -> Self {
        todo!("Implement this.");
    }
}

impl Transactor for FileTransactor {
    fn begin(&self, keys: &Vec<Box<Key>>) -> Transaction {
        todo!("Implement this.");
    }
    fn write(&self, transaction: Transaction) -> Result<u64> {
        todo!("Implement this.");
    }
}
