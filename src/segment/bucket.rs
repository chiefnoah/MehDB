use std::mem::size_of;
use std::io::{self, Write, Read, Seek};
use crate::serializer::Serializable;

// The number of records in each bucket.
// This may be adatped to be parametrizable or dynamic in the future.
const BUCKET_RECORDS: usize = 16;
pub const BUCKET_SIZE: usize = BUCKET_RECORDS * size_of::<Record>();

pub struct Record {
    pub hash_key: u64,
    pub value: u64,
}

pub struct Bucket {
    pub offset: u64,
    buf: [u8; BUCKET_SIZE],
}

impl Serializable for Bucket {
    fn pack<W: Write + Seek>(&self, buffer: &mut W) -> io::Result<u64> {
        let offset = buffer.seek(io::SeekFrom::Current(0)).unwrap();
        buffer.write(&self.buf)?;
        Ok(offset)
    }

    fn unpack<R: Read + Seek>(buffer: &mut R) -> io::Result<Self> {
        let offset = buffer.seek(io::SeekFrom::Current(0)).unwrap();
        let mut bucket = Self {
            offset,
            buf: [0; BUCKET_SIZE],
        };
        buffer.read_exact(&mut bucket.buf)?;
        Ok(bucket)
    }
}

impl Bucket {
    fn get(&self, hk: u64) -> Option<Record> {
        for record in self.iter() {
            if record.hash_key == hk {
                return Some(record);
            }
        }
        None
    }

    pub fn put(&mut self, hk: u64, value: u64, local_depth: u64) -> Result<usize, String> {
        for (i, record) in self.iter().enumerate() {
            if record.hash_key >> local_depth & hk >> local_depth != hk >> local_depth {
                let offset = i * size_of::<Record>();
                self.buf[offset..offset+8].copy_from_slice(&hk.to_le_bytes());
                self.buf[offset + 8..offset+16].copy_from_slice(&value.to_le_bytes());
                return Ok(offset);
            }
        }
        Err(String::from("Bucket full"))
    }

    fn iter(&self) -> BucketIter {
        BucketIter {
            index: 0,
            bucket: &self,
        }
    }

    fn at(&self, index: usize) -> Record {
        let offset = index * size_of::<Record>();
        let mut buf: [u8; 8] = [0; 8];
        buf.copy_from_slice(&self.buf[offset..offset+8]);
        let hash_key = u64::from_le_bytes(buf);
        buf.copy_from_slice(&self.buf[offset + 8..offset + 16]);
        let value = u64::from_le_bytes(buf);
        Record { hash_key, value }
    }
}

struct BucketIter<'b> {
    index: u64,
    bucket: &'b Bucket,
}

impl<'b> Iterator for BucketIter<'b> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= BUCKET_RECORDS as u64 {
            return None;
        }
        Some(self.bucket.at(self.index as usize))
    }
}
