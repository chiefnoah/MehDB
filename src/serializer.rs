use log::info;
use std::default::Default;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use anyhow::Result;

pub trait Serializable: Sized {
    // TODO: no more Option or tagged result
    fn pack<W: Write + Seek>(&self, file: &mut W) -> Result<u64>;
    fn unpack<R: Read + Seek>(buffer: &mut R) -> Result<Self>;
}

#[derive(Debug, Eq, PartialEq)]
pub struct ByteKey(pub Vec<u8>);

#[derive(Debug, Eq, PartialEq)]
pub struct ByteValue(pub u64);

impl Default for ByteKey {
    fn default() -> Self {
        ByteKey(Vec::new())
    }
}

impl Default for ByteValue {
    fn default() -> Self {
        ByteValue(0)
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

impl Serializable for Transaction {
    fn pack<File>(&self, file: &mut File) -> Result<u64> {
        todo!("Implement this.");
    }
    fn unpack<File>(file: &mut File) -> Result<Self> {
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
    epoch: Duration, // Since Unix Epoch Time
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
            let e = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            let b = u64::try_from(e.as_millis()).unwrap().to_le_bytes();
            file.write(&b)?;
            e
        };
        Ok(SimpleFileTransactor {
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
