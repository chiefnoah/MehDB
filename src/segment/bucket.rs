use crate::serializer::Serializable;
use anyhow::{Result, Context};
use log::{debug, info, trace};
use std::fmt;
use std::io::{self, Read, Seek, Write};
use std::mem::size_of;
use std::error::Error;

// The number of records in each bucket.
// This may be adatped to be parametrizable or dynamic in the future.
const BUCKET_RECORDS: usize = 16;
pub const BUCKET_SIZE: usize = BUCKET_RECORDS * size_of::<Record>();

#[derive(Debug)]
pub struct Record {
    pub hash_key: u64,
    pub value: u64,
}

impl Record {
    pub const fn hash_key_size() -> usize {
        size_of::<u64>()
    }
    pub const fn value_size() -> usize {
        size_of::<u64>()
    }

    pub fn to_bytes(&self) -> [u8; size_of::<Self>()] {
        const SIZE: usize = size_of::<Record>();
        let mut buf: [u8; SIZE] = [0; SIZE];
        buf[0..Record::hash_key_size()].copy_from_slice(&self.hash_key.to_le_bytes());
        buf[Record::hash_key_size()..].copy_from_slice(&self.value.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: [u8; size_of::<Self>()]) -> Self {
        let hash_key = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let value = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Self{hash_key, value}
    }
}

pub struct Bucket {
    pub offset: u64,
    buf: [u8; BUCKET_SIZE],
}

impl Serializable for Bucket {
    fn pack<W: Write + Seek>(&self, buffer: &mut W) -> Result<u64> {
        let offset = buffer.seek(io::SeekFrom::Start(self.offset))?;
        buffer.write(&self.buf).with_context(|| {
            format!("Error packing bucket into buffer")
        })?;
        Ok(offset)
    }

    fn unpack<R: Read + Seek>(buffer: &mut R) -> Result<Self> {
        let offset = buffer.seek(io::SeekFrom::Current(0)).unwrap();
        let mut bucket = Self {
            offset,
            buf: [0; BUCKET_SIZE],
        };
        buffer.read_exact(&mut bucket.buf)
            .with_context(|| {
            format!("Error reading buffer when unpacking bucket at offset {}", offset)
        })?;
        Ok(bucket)
    }
}

/// Gets the effective key
fn normalize_key(hk: u64, local_depth: u64) -> u64 {
    if local_depth == 0 {
        return 0;
    };
    return if local_depth == 0 {
        0
    } else {
        hk >> (64 - local_depth)
    }
}

impl Bucket {
    pub fn new() -> Self {
        Bucket{offset: 0, buf: [0; BUCKET_SIZE]}
    }

    pub fn get(&self, hk: u64) -> Option<Record> {
        debug!("Searching bucket for {}", hk);
        for record in self.iter() {
            debug!(
                "Found hk: {}\tvalue: {}",
                record.hash_key, record.value
            );
            if record.hash_key == hk {
                return Some(record);
            }
        }
        None
    }

    fn _put(&mut self, index: usize, hk: u64, value: u64) {}

    fn maybe_index_to_insert(&self, hk: u64, value: u64, local_depth: u64) -> Option<usize> {
        for (i, record) in self.iter().enumerate() {
            trace!(
                "Index: {}\t hk: {}\tvalue: {}\tlocal_depth: {}",
                i, record.hash_key, record.value, local_depth
            );
            if record.hash_key == 0 && record.value == 0 {
                debug!("Found empty slot to insert record at index {}.", i);
                return Some(i);
            } else if record.hash_key == hk {
                return Some(i);
            } else if normalize_key(record.hash_key, local_depth) 
                & normalize_key(hk, local_depth)
                != normalize_key(hk, local_depth)
            {
                debug!("Replacing {} with new record", record.hash_key);
                // return the index we're inserting at
                return Some(i);
            }
        };
        None
    }

    /// Attempts to insert a record in the bucket. Returns the index it was inserted at if
    /// successful, otherwise an error indicating an overflow. In the event of an overflow, it is
    /// the responsibility of the Segmenter to split and allocate annother segment so the new
    /// record can be inserted.
    pub fn put(&mut self, hk: u64, value: u64, local_depth: u64) -> Result<usize, BucketFullError> {
        debug!(
            "Inserting hk: {}\tvalue: {}\t local depth: {}",
            hk, value, local_depth
        );
        let index = match self.maybe_index_to_insert(hk, value, local_depth) {
            None => {
                return Err(BucketFullError {
                    offset: self.offset,
                    hash_key: hk,
                    local_depth,
                })
            }
            Some(i) => i,
        };
        let record = Record {hash_key: hk, value};
        let bytes = record.to_bytes();
        trace!("Record bytes: {:?}", &bytes);
        let offset = index * size_of::<Record>();
        trace!("Record offset: {}", offset);
        let buf = &mut self.buf;
        buf[offset..offset + size_of::<Record>()].copy_from_slice(&bytes);
        Ok(index)
    }

    pub fn iter(&self) -> BucketIter {
        BucketIter {
            index: 0,
            bucket: &self,
        }
    }

    fn at(&self, index: usize) -> Record {
        let offset = index * size_of::<Record>();
        let mut buf: [u8; 8] = [0; 8];
        buf.copy_from_slice(&self.buf[offset..offset + 8]);
        let hash_key = u64::from_le_bytes(buf);
        buf.copy_from_slice(&self.buf[offset + 8..offset + 16]);
        let value = u64::from_le_bytes(buf);
        Record { hash_key, value }
    }
}

pub struct BucketIter<'b> {
    index: usize, // this could be a u16
    bucket: &'b Bucket,
}

impl<'b> Iterator for BucketIter<'b> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= BUCKET_RECORDS {
            return None;
        }
        self.index += 1;
        Some(self.bucket.at(self.index - 1 as usize))
    }
}

#[derive(Debug, Clone)]
pub struct BucketFullError {
    offset: u64,
    hash_key: u64,
    local_depth: u64,
}

impl fmt::Display for BucketFullError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Bucket at offset {} and depth {} overflowed when trying to {}.",
            self.offset, self.local_depth, self.offset
        )
    }
}

impl Error for BucketFullError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use io::{self, Cursor, Read, Seek, Write};

    #[test]
    fn bucket_can_pack() {
        let mut bucket = Bucket {
            offset: 5,
            buf: [0; BUCKET_SIZE],
        };
        // change this so we have something to check for
        bucket.buf[0] = 255;
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let res = bucket.pack::<Cursor<Vec<u8>>>(&mut buf);
        let res = match bucket.pack::<Cursor<Vec<u8>>>(&mut buf) {
            Err(e) => panic!("Unable to pack bucket: {}", e),
            Ok(r) => r,
        };
        let mut expected_buf: [u8; BUCKET_SIZE + 5] = [0; BUCKET_SIZE + 5];
        // this will only match up with the output buffer if the Seek is performed
        // propery
        expected_buf[5] = 255;
        let inner_buf = buf.into_inner();
        assert_eq!(&inner_buf.len(), &(BUCKET_SIZE + bucket.offset as usize));
        assert_eq!(&expected_buf[..], &inner_buf[..]);
    }

    #[test]
    fn bucket_can_unpack() {
        let mut fixture: Vec<u8> = Vec::from([0; 2 * BUCKET_SIZE + 5]);
        fixture[1] = 0x12;
        let mut buf = Cursor::new(fixture);
        buf.seek(io::SeekFrom::Start(1)).unwrap();
        let bucket = match Bucket::unpack(&mut buf) {
            Err(e) => panic!("Unable to unpack Bucket: {}", e),
            Ok(h) => h,
        };
        assert_eq!(bucket.offset, 1);
        assert_eq!(bucket.buf[0], 0x12);
    }

    #[test]
    fn when_buffer_is_too_small_header_unpack_fails() {
        // Too small!
        let mut fixture: Vec<u8> = Vec::from([0; 16]);
        fixture[1] = 0x12;
        let mut buf = Cursor::new(fixture);
        buf.seek(io::SeekFrom::Start(1)).unwrap();
        match Bucket::unpack(&mut buf) {
            Err(e) => (),
            Ok(h) => panic!("Improperly was able to unpack Bucket"),
        }
    }

    #[test]
    fn can_insert_and_index_bucket() {
        let mut bucket = Bucket {
            offset: 0,
            buf: [0; BUCKET_SIZE],
        };
        let index = match bucket.put(123, 456, 0) {
            Err(e) => panic!("Unable to insert record: {}", e),
            Ok(i) => i,
        };
        // Insert another record to make sure we don't upset the index of the already inserted
        // record
        bucket.put(789, 666, 0).unwrap();
        // Check that we can index it
        let record = bucket.at(index);
        assert_eq!(record.hash_key, 123);
        assert_eq!(record.value, 456);
        // Check that we can .get the value
        let record = bucket.get(123).unwrap();
        assert_eq!(record.hash_key, 123);
        assert_eq!(record.value, 456);
    }

    #[test]
    fn can_put_and_get_records_from_bucket() {
        let mut bucket = Bucket {
            offset: 0,
            buf: [0; BUCKET_SIZE],
        };
        for i in 1..=16 {
            let res = match bucket.put(i * 60, i * 2, 0) {
                Err(e) => panic!("Unable to insert record: {}", e),
                Ok(o) => o,
            };
        }
        // Bucket overflow
        match bucket.put(1234, 666, 0) {
            Ok(_) => panic!("Bucket should have overflown, but didn't"),
            Err(e) => (),
        }
        for i in 1..=16 {
            let record = match bucket.get(i * 60) {
                None => panic!("Unable to fetch record from bucket"),
                Some(r) => r,
            };
            assert_eq!(record.hash_key, i * 60);
            assert_eq!(record.value, i * 2);
        }
    }

    #[test]
    fn can_overwrite_soft_deleted_record() {
        let mut bucket = Bucket {
            offset: 0,
            buf: [0; BUCKET_SIZE],
        };
        let hash_key: u64 = 0xF000000000000000;
        let i = bucket.put(123, 456, 0).unwrap();
        assert_eq!(i, 0);
        let new_index = bucket.put(hash_key, 666, 1).unwrap();
        // Check that we overwrote the existing value because we now look at the first MSB
        // (local_depth=1 vs local_depth=0). This is because we "soft delete" during segment
        // splitting
        assert_eq!(i, new_index);
    }

    #[test]
    fn can_iterate_over_bucket() {
        let mut bucket = Bucket {
            offset: 0,
            buf: [0; BUCKET_SIZE],
        };
        for i in 1..=16 {
            let res = match bucket.put(i * 60, i * 2, 0) {
                Err(e) => panic!("Unable to insert record: {}", e),
                Ok(o) => o,
            };
        }
        for (i, r) in bucket.iter().enumerate() {
            // this is ugly lol
            let i = i + 1;
            assert_eq!((i * 60) as u64, r.hash_key);
            assert_eq!((i * 2) as u64, r.value);
        }
    }

    #[test]
    fn record_can_go_to_from_bytes() {
        let record = Record{hash_key: 0xF000000000000000, value: 1234};
        let bytes = record.to_bytes();
        let de_record = Record::from_bytes(bytes);
        assert_eq!(de_record.hash_key, record.hash_key);
        assert_eq!(de_record.value, record.value);
    }
}
