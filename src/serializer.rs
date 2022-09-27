use anyhow::{Context, Result};
use log::info;
use std::default::Default;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Types that implement `Serializable` should pack all or some of their properties into the
/// provided buffer. It is not a requirement to call `Write::flush()`, it should be assumed that it
/// will be called elsewhere to allow for case-by-case control over when the buffer gets flushed.
pub trait Serializable: Sized {
    /// p
    fn pack<W: Write + Seek>(&self, file: &mut W) -> Result<u64>;
    fn unpack<R: Read + Seek>(buffer: &mut R) -> Result<Self>;
}

#[derive(Debug, Eq, PartialEq, Clone)]
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
