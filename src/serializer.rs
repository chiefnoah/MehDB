use std::io::{self, Result};
use std::fs::File;
use std::time::{Instant, UNIX_EPOCH};
use log::{info};
use std::io::{Read, Write, Seek, SeekFrom};
use std::time::Duration;
use std::time::SystemTime;

pub trait Serializable {
    fn pack(&self, file: Option<&File>) -> Result<DataOrOffset>;
    fn unpack(file: &File) -> Self;
}

pub struct Key(Vec<u8>);
pub struct Value(Vec<u8>);

pub enum DataOrOffset {
    Offset(u64),
    Data(Vec<u8>),
}

struct KeyValue {
    key: Key,
    value: Value,
}

pub struct Transaction {
    instant: u128,
    data: Vec<KeyValue>,
}

impl Serializable for Transaction {
    fn pack(&self, file: Option<&File>) -> Result<DataOrOffset> {
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
    log_file: Option<File>,
    // The first time this transactor was used with this file
    epoch: Duration,  // Since Unix Epoch Time
}

impl FileTransactor {
    pub fn init(file: File, log_file: Option<File>) -> Result<Self> {
        let mut file = file;
        file.seek(SeekFrom::Start(0))?;
        let file_metadata = file.metadata()?;
        let epoch = if file_metadata.len() > 8 {
            info!("Initializing transactor from previous file.");
            let mut buf: [u8; 8] = [0; 8];
            file.read_exact(&mut buf[..])?;
            Duration::from_millis(u64::from_le_bytes(buf))
        } else {
            let start = SystemTime::now();
            let e = start.duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let b = u64::try_from(e.as_millis()).unwrap().to_le_bytes();
            file.write(&b)?;
            e
        };
        Ok(FileTransactor{
            file: file,
            epoch: epoch,
            log_file: log_file,
        })
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
