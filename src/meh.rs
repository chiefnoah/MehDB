use crate::directory::{Directory, MemoryDirectory};
use crate::segment::{self, Segmenter, BasicSegmenter, BUCKETS_PER_SEGMENT};
use crate::segment::file_segmenter::file_segmenter;
use crate::serializer::{self, DataOrOffset, Serializable, SimpleFileTransactor, Transactor};
use log::{error, info, debug, warn};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
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
        let msb_hash_key = hash_key[0];
        info!("msb_hash_key: {}", msb_hash_key);
        let segment_offset = self.directory.segment_index(msb_hash_key)
            .with_context(|| {
                format!("Unable to get segment offset for {}", msb_hash_key)
            })?;
        debug!("Segment index {}", segment_offset);
        let segment = self.segmenter.segment(segment_offset)
            .with_context(|| {
                format!("Unable to read segment at offset {}", segment_offset)
            })?;
        let bucket_index = (BUCKETS_PER_SEGMENT - 1) as u64 & hash_key[1];
        debug!("Bucket index: {}", bucket_index);
        let mut bucket = self.segmenter.bucket(&segment, bucket_index)?;
        let overflow = bucket.put(msb_hash_key, 1234, segment.depth);
        match overflow {
            Err(e) => {
                info!("Bucket overflowed. Allocating new segment and splitting.");
                return Err(anyhow!(e));
            },
            _ => (),
        }
        self.segmenter.write_bucket(&bucket)
            .with_context(|| {
                format!("Saving updated bucket at offset {}", bucket.offset)
            })
    }
    pub fn get(
        &self,
        key: serializer::ByteKey,
    ) -> Entry<serializer::ByteKey, serializer::ByteValue> {
        todo!("Implement this");
    }
}
