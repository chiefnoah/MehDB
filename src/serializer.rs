use std::default::Default;
use std::io::{self, Result};
use std::fs::File;
use log::{info};
use std::io::{Read, Write, Seek, SeekFrom};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Serializable<T> {
    fn pack(&self, file: Option<&mut File>) -> Result<DataOrOffset>;
    fn unpack(file: &mut File) -> Result<T>;
}

pub struct ByteKey(pub Vec<u8>);
pub struct ByteValue(pub Vec<u8>);

impl Serializable<Vec<u8>> for Vec<u8> {
    fn pack(&self, file: Option<&mut File>) -> Result<DataOrOffset> {
        todo!("Implement this.");
    }
    fn unpack(file: &mut File) -> Result<Self> {
        todo!("Implement this.");
    }
}

impl Default for ByteKey {
    fn default() -> Self {
        ByteKey(Vec::new())
    }
}

impl Default for ByteValue {
    fn default() -> Self {
        ByteValue(Vec::new())
    }
}

pub enum DataOrOffset {
    Offset(u64),
    Data(Vec<u8>),
}

struct KeyValue {
    key: ByteKey,
    value: ByteValue,
}

pub struct Transaction {
    instant: u128,
    data: Vec<KeyValue>,
}

impl Serializable<Transaction> for Transaction {
    fn pack(&self, file: Option<&mut File>) -> Result<DataOrOffset> {
        todo!("Implement this.");
    }
    fn unpack(file: &mut File) -> Result<Self> {
        todo!("Implement this.");
    }
}

pub trait Transactor {
    fn begin(&self, keys: &Vec<Box<ByteKey>>) -> Transaction;
    fn write(&self, transaction: Transaction) -> Result<u64>;
}

pub struct SimpleFileTransactor {
    file: File,
    log_file: Option<File>,
    // The first time this transactor was used with this file
    epoch: Duration,  // Since Unix Epoch Time
}

impl SimpleFileTransactor {
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
            info!("Initializing new transactor");
            let start = SystemTime::now();
            let e = start.duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let b = u64::try_from(e.as_millis()).unwrap().to_le_bytes();
            file.write(&b)?;
            e
        };
        Ok(SimpleFileTransactor{
            file,
            epoch,
            log_file,
        })
    }
}

impl Transactor for SimpleFileTransactor {
    fn begin(&self, keys: &Vec<Box<ByteKey>>) -> Transaction {
        todo!("Implement this.");
    }
    fn write(&self, transaction: Transaction) -> Result<u64> {
        todo!("Implement this.");
    }
}
