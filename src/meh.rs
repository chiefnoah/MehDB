use crate::serializer::{self, DataOrOffset, SimpleFileTransactor, Serializable, Transactor};
use crate::directory::{Directory, MemoryDirectory};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::default::Default;
use std::fs::File;
use std::hash::Hasher;
use std::io::{self, Read, Seek, Write};
use std::path::Path;
use std::mem::size_of;
use crate::segment::{self, FileSegmenter, Segmenter};

use highway::{self, HighwayHasher,HighwayHash};
use bitvec::prelude::*;

const BUCKET_RECORDS: usize = 16;
const SEGMENT_BUCKETS: usize = 64;

// My Extendible Hash Database
pub struct MehDB {
    hasher_key: highway::Key,
    directory: MemoryDirectory,
    segmenter: FileSegmenter,
    //transactor: SimpleFileTransactor,
}

const HEADER_SIZE: u64 = 16;

#[derive(Serialize, Deserialize)]
struct Header {
    global_depth: u64,
    num_segments: u64,
}

impl Serializable for Header {
    fn pack(&self, file: Option<&mut File>) -> io::Result<serializer::DataOrOffset> {
        let global_depth_bytes = self.global_depth.to_le_bytes();
        let num_segment_bytes = self.num_segments.to_le_bytes();
        match file {
            Some(file) => {
                let offset = file.seek(io::SeekFrom::Current(0)).unwrap();
                file.write(&global_depth_bytes)?;
                file.write(&num_segment_bytes)?;
                Ok(DataOrOffset::Offset(offset))
            },
            None => {
                let mut out = Vec::with_capacity(16);
                out.extend_from_slice(&global_depth_bytes);
                out.extend_from_slice(&num_segment_bytes);
                Ok(DataOrOffset::Data(out))
            }
        }
    }
    fn unpack(file: &mut File) -> io::Result<Self> {
        let mut buf: [u8; 8] = [0; 8];
        file.read_exact(&mut buf[..])?;
        let global_depth = u64::from_le_bytes(buf);
        file.read_exact(&mut buf[..])?;
        let num_segments = u64::from_le_bytes(buf);
        Ok(Header {
            global_depth,
            num_segments,
        })
    }
}

struct Record<K: Default, V: Default> {
    hash_key: u64,
    key: K,
    value: V,
    offset: u64,
}

impl<K: Default, V: Default> Record<K, V> {
    fn pack(&self) -> [u8; 16] {
        let mut out: [u8; 16] = [0; 16];
        out[0..8].copy_from_slice(&self.hash_key.to_le_bytes());
        out[8..].copy_from_slice(&self.offset.to_le_bytes());
        out
    }
}

// Bucket
struct Bucket<K: Default, V: Default> {
    records: [Record<K, V>; BUCKET_RECORDS],
}

fn initialize_segment(
    segment_file: &mut File,
    from_offset: Option<u64>,
) -> Result<(), io::Error> {
    segment_file.seek(io::SeekFrom::Start(HEADER_SIZE))?;
    for _ in 0..SEGMENT_BUCKETS {
        for _ in 0..BUCKET_RECORDS {
            let r: Record<serializer::ByteKey, serializer::ByteValue> = Record {
                hash_key: 0,
                offset: HEADER_SIZE,
                key: Default::default(),
                value: Default::default(),
            };
            segment_file.write(&r.pack())?;
        }
    }
    return Ok(());
}

impl MehDB {
    // New creates a new instance of MehDB with it's data in optional path.
    pub fn init(path: Option<&Path>) -> Result<Self, io::Error> {
        let path: &Path = match path {
            Some(path) => path,
            // Default to the current directory
            None => Path::new("."),
        };
        let segment_file_path = path.join("segments.bin");
        let mut segment_file = if !segment_file_path.exists() {
            let f = File::create(segment_file_path);
            f
        } else {
            File::open(segment_file_path)
        }?;
        // Attempt to read the header from segment_file
        // otherwse, create a new header and write it
        segment_file.seek(io::SeekFrom::Start(0))?;
        let segment_metadata = segment_file.metadata()?;
        let header = if segment_metadata.len() > 16 {
            Header::unpack(&mut segment_file)?
        } else {
            let header = Header {
                global_depth: 0,
                num_segments: 1,
            };
            header.pack(Some(&mut segment_file))?;
            //Initialize the first segment
            initialize_segment(&mut segment_file, Some(size_of::<Header>() as u64))?;
            // Initialize the directory
            header
        };
        Ok(MehDB {
            hasher_key: highway::Key([53252, 2352323, 563956259, 234832]), // TODO: change this
            directory: MemoryDirectory::init(None),
            segmenter: FileSegmenter::init(Some(&segment_file_path)).unwrap(),
        })
    }

    fn split_segments(
        &self,
        hash_key: u64,
        offset: u64,
        segment_depth: u8,
        v: serializer::ByteValue,
    ) -> io::Result<()> {
        Ok(())
    }

    fn grow_directory(&self) {}

    pub fn put(
        &self,
        key: serializer::ByteKey,
        value: serializer::ByteValue,
    ) -> Result<u64, io::Error> {
        let hasher = HighwayHasher::new(self.hasher_key);
        // We only need the first u64 of the returned value because
        // It's unlikely we have the hard drive space to support a u64 deep directory
        // and we *definitely* don't have the RAM to.
        let hash_key = hasher.hash256(&key.0)[0];
        let segment_index = self.directory.segment_offset(hash_key)?;
        let segment = self.segmenter.segment(segment_index)?;

        Ok(0)  // are we really ok though
    }
    pub fn get(
        &self,
        key: serializer::ByteKey,
    ) -> Entry<serializer::ByteKey, serializer::ByteValue> {
        todo!("Implement this");
    }
}
