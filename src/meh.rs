use crate::directory::{Directory, MemoryDirectory};
use crate::segment::{self, Segmenter, BasicSegmenter, BUCKETS_PER_SEGMENT, Bucket, Record};
use crate::segment::file_segmenter::file_segmenter;
use crate::serializer::{self, DataOrOffset, Serializable, SimpleFileTransactor, Transactor};
use log::{error, info, debug, warn};
use serde::{Deserialize, Serialize};
use std::default::Default;
use std::fs::File;
use std::hash::Hasher;
use std::io::{self, Read, Seek, Write};
use std::mem::size_of;
use std::path::Path;
use anyhow::{Result, anyhow, Context};

use bitvec::prelude::*;
use highway::{self, HighwayHash, HighwayHasher};

// My Extendible Hash Database
pub struct MehDB {
    hasher_key: highway::Key,
    directory: MemoryDirectory,
    segmenter: BasicSegmenter<File>,
    //transactor: SimpleFileTransactor,
}

const HEADER_SIZE: u64 = 16;

impl MehDB {
    // New creates a new instance of MehDB with it's data in optional path.
    pub fn init(path: Option<&Path>) -> Result<Self> {
        let path: &Path = match path {
            Some(path) => path,
            // Default to the current directory
            None => Path::new("."),
        };
        let segment_file_path = path.join("segments.bin");
        let segmenter = file_segmenter(Some(&segment_file_path)).with_context(|| {
            format!("Unable to initialize file_segmenter.")
        })?;
        let mehdb = MehDB {
            hasher_key: highway::Key([53252, 2352323, 563956259, 234832]), // TODO: change this
            directory: MemoryDirectory::init(None, 0),
            segmenter,
        };
        Ok(mehdb)
    }

    fn bucket_for_key(&mut self, key: &[u64; 4]) -> Result<Bucket> {
        let segment_offset = self.directory.segment_index(key[0])
            .with_context(|| {
                format!("Unable to get segment offset for {:?}", key)
            })?;
        debug!("Segment index {}", segment_offset);
        let segment = self.segmenter.segment(segment_offset)
            .with_context(|| {
                format!("Unable to read segment at offset {}", segment_offset)
            })?;
        let bucket_index = (BUCKETS_PER_SEGMENT - 1) as u64 & key[3];
        debug!("Reading bucket at index: {}", bucket_index);
        self.segmenter.bucket(&segment, bucket_index)
    }

    pub fn put(
        &mut self,
        key: serializer::ByteKey,
        value: serializer::ByteValue,
    ) -> Result<()> {
        let hasher = HighwayHasher::new(self.hasher_key);
        // We only need the first u64 of the returned value because
        // It's unlikely we have the hard drive space to support a u64 deep directory
        // and we *definitely* don't have the RAM to.
        // TODO: support the full 256 bit keyspace for magical distributed system support
        let hash_key = hasher.hash256(&key.0);
        info!("hash_key: {:?}", hash_key);
        let segment_offset = self.directory.segment_index(hash_key[0])
            .with_context(|| {
                format!("Unable to get segment offset for {:?}", hash_key)
            })?;
        debug!("Segment index {}", segment_offset);
        let segment = self.segmenter.segment(segment_offset)
            .with_context(|| {
                format!("Unable to read segment at offset {}", segment_offset)
            })?;
        let bucket_index = (BUCKETS_PER_SEGMENT - 1) as u64 & hash_key[3];
        debug!("Reading bucket at index: {}", bucket_index);
        let mut bucket = self.segmenter.bucket(&segment, bucket_index)?;
        debug!("Inserting record into bucket...");
        let overflow = bucket.put(hash_key[0], value.0, segment.depth);
        match overflow {
            Err(e) => {
                info!("Bucket overflowed. Allocating new segment and splitting.");
                return Err(anyhow!(e));
            },
            _ => {debug!("Successfully inserted record to bucket.");},
        }
        info!("Writing bucket to segment.");
        self.segmenter.write_bucket(&bucket)
            .with_context(|| {
                format!("Saving updated bucket at offset {}", bucket.offset)
            })
    }
    pub fn get(
        &mut self,
        key: serializer::ByteKey,
    ) -> Option<serializer::ByteValue> {
        let hasher = HighwayHasher::new(self.hasher_key);
        // We only need the first u64 of the returned value because
        // It's unlikely we have the hard drive space to support a u64 deep directory
        // and we *definitely* don't have the RAM to.
        // TODO: support the full 256 bit keyspace for magical distributed system support
        let hash_key = hasher.hash256(&key.0);
        info!("hash_key: {:?}", hash_key);
        let bucket = match self.bucket_for_key(&hash_key) {
            Ok(b) => b,
            Err(e) => {
                warn!("No bucket found for key {:?}", key);
                return None;
            }
        };
        match bucket.get(hash_key[0]) {
            Some(v) => Some(serializer::ByteValue(v.value)),
            None => None
        }
    }
}
