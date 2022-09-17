use crate::directory::{Directory, MMapDirectory, MMapDirectoryConfig};
use crate::segment::file_segmenter::file_segmenter;
use crate::segment::{
    self, BasicSegmenter, Bucket, Record, Segment, Segmenter, SegmenterProvider,
    SegmenterProviderConfig, BUCKETS_PER_SEGMENT, LockedSegmenter
};
use crate::serializer::{self, DataOrOffset, Serializable, SimpleFileTransactor, Transactor};
use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn, trace};
use std::default::Default;
use std::fs::File;
use std::hash::Hasher;
use std::io::{self, Read, Seek, Write};
use std::mem::size_of;
use std::path::Path;

use bitvec::prelude::*;
use highway::{self, HighwayHash, HighwayHasher};

use crate::serializer::{ByteKey, ByteValue};

// My Extendible Hash Database
pub struct MehDB {
    hasher_key: highway::Key,
    directory: MMapDirectory,
    segmenter_provider: SegmenterProvider,
}

const HEADER_SIZE: u64 = 16;

/// A Extendible hashing implementation that does not support multithreading.
impl MehDB {
    // New creates a new instance of MehDB with it's data in optional path.
    pub fn init(path: Option<&Path>) -> Result<Self> {
        let path: &Path = match path {
            Some(path) => path,
            // Default to the current directory
            None => Path::new("."),
        };
        let segment_file_path = path.join("segments.bin");
        let directory_file_path = path.join("directory.bin");
        let segmenter_provider = SegmenterProvider::init(Default::default())
            .context("Initializing thread-safe segmenter")?;
        let mmap_dir_config = MMapDirectoryConfig {
            path: directory_file_path,
        };
        let mehdb = MehDB {
            hasher_key: highway::Key([53252, 2352323, 563956259, 234832]), // TODO: change this
            directory: MMapDirectory::init(mmap_dir_config).context("Creating mmap dir.")?,
            segmenter_provider,
        };
        Ok(mehdb)
    }

    fn bucket_for_key(&self, key: &[u64; 4]) -> Result<Bucket> {
        let segment_index = self
            .directory
            .segment_index(key[0])
            .with_context(|| format!("Unable to get segment offset for {:?}", key))?;
        let segmenter = self.segmenter_provider.ro_segmenter_for(segment_index);
        debug!("Segment index {}", segment_index);
        let segment = segmenter
            .segment(segment_index)
            .with_context(|| format!("Unable to read segment at offset {}", segment_index))?;
        let bucket_index = ((BUCKETS_PER_SEGMENT - 1) as u64 & key[3]) as u32;
        debug!("Reading bucket at index: {}", bucket_index);
        segmenter.bucket(&segment, bucket_index)
    }

    pub fn put(&self, key: serializer::ByteKey, value: serializer::ByteValue) -> Result<()> {
        let hasher = HighwayHasher::new(self.hasher_key);
        // We only need the first u64 of the returned value because
        // It's unlikely we have the hard drive space to support a u64 deep directory
        // and we *definitely* don't have the RAM to.
        // TODO: support the full 256 bit keyspace for magical distributed system support
        let hash_key = hasher.hash256(&key.0);
        trace!("hash_key: {:?}\tvalue: {}", hash_key, value.0);
        // TODO: I think we need to keep a RO lock on the directory for the duration of this block
        let segment_index = self
            .directory
            .segment_index(hash_key[0])
            .with_context(|| format!("Unable to get segment index for {:?}", hash_key))?;
        debug!("Segment index {}", segment_index);
        let segmenter = self.segmenter_provider.rw_segmenter_for(segment_index);
        let segment = segmenter
            .segment(segment_index)
            .with_context(|| format!("Unable to read segment with index {}", segment_index))?;
        let bucket_index = ((BUCKETS_PER_SEGMENT - 1) as u64 & hash_key[3]) as u32;
        debug!("Reading bucket at index: {}", bucket_index);
        let mut bucket = segmenter
            .bucket(&segment, bucket_index)
            .with_context(|| format!("Reading bucket at index {}", bucket_index))?;
        debug!("Inserting record into bucket...");
        match bucket.put(hash_key[0], value.0, segment.depth) {
            // Overflowed the bucket!
            Err(e) => {
                info!("Bucket overflowed. Allocating new segment and splitting.");
                let offset = segment.offset;
                self.split_segment(segmenter, segment, hash_key[0])
                    .with_context(|| format!("Splitting segement at offset {}", offset))?;
                // TODO: don't be so inneficient. We already know the hash_key!
                // Call put again, it may end up in a new bucket or in the same one that's now had
                // some records migrated to a new segment.
                debug!("Re-inserting record.");
                return self.put(key, value);
            }
            _ => {
                debug!("Successfully inserted record to bucket.");
            }
        }
        info!("Writing bucket to segment.");
        segmenter
            .write_bucket(&segment, &bucket)
            .with_context(|| format!("Saving updated bucket at offset {}", bucket.offset))
    }
    pub fn get(&self, key: serializer::ByteKey) -> Option<Record> {
        let hasher = HighwayHasher::new(self.hasher_key);
        // We only need the first u64 of the returned value because
        // It's unlikely we have the hard drive space to support a u64 deep directory
        // and we *definitely* don't have the RAM to.
        // TODO: support the full 256 bit keyspace for magical distributed system support
        let hash_key = hasher.hash256(&key.0);
        trace!("hash_key: {:?}", hash_key);
        let bucket = match self.bucket_for_key(&hash_key) {
            Ok(b) => b,
            Err(e) => {
                warn!("No bucket found for key {:?}", key);
                return None;
            }
        };
        bucket.get(hash_key[0])
    }

    fn split_segment(&self, segmenter: LockedSegmenter, segment: Segment, hk: u64) -> Result<()> {
        info!("Splitting segment");
        let mut segment = segment;
        let mut global_depth = self
            .directory
            .global_depth()
            .context("Reading current global depth")?;
        // If we need to expand the directory size
        if segment.depth == global_depth {
            match self.directory.grow() {
                Ok(i) => {
                    // Re-read the global_depth post-grow call. For most implementations, it should be
                    // previous value + 1
                    global_depth = self.directory.global_depth()?;
                    debug!(
                        "Grew directory. New depth: {}\tNew size: {}",
                        global_depth, i
                    );
                }
                Err(e) => {
                    return Err(anyhow!(e));
                }
            };
        }
        info!("gobal_depth: {}", global_depth);
        let new_depth = segment.depth + 1;
        // The buckets that are being allocated to the new segment
        let mut new_buckets = Vec::<Bucket>::with_capacity(BUCKETS_PER_SEGMENT);
        let mask = (hk >> (64 - new_depth)) | 1;
        for bi in 0..BUCKETS_PER_SEGMENT {
            let old_bucket = segmenter
                .bucket(&segment, bi as u32)
                .context("Reading old bucket for segment split")?;
            let mut new_bucket = Bucket::new();
            for record in old_bucket.iter() {
                if record.hash_key >> (64 - new_depth) == mask {
                    debug!(
                        "Insering record with hk {} into new bucket",
                        record.hash_key
                    );
                    new_bucket
                        .put(record.hash_key, record.value, new_depth)
                        .context("Inserting record in new bucket.")?;
                }
            }
            new_buckets.insert(bi, new_bucket);
        }

        //TODO: refactor this to be more idiomatic
        warn!("Allocating new segment with depth {}", new_depth);
        let (new_segment_index, new_segment) =
            match segmenter.allocate_with_buckets(new_buckets, new_depth) {
                Ok(s) => s,
                Err(e) => return Err(e.context("Allocating new segment with populated buckets.")),
            };
        let s = hk >> 64 - global_depth;
        let step = 1 << (global_depth - new_depth);
        let mut start_dir_entry = if segment.depth == 0 {
            0
        } else {
            hk >> 64 - segment.depth
        };
        start_dir_entry = start_dir_entry << (global_depth - segment.depth);
        start_dir_entry = start_dir_entry - (start_dir_entry % 2);
        for i in 0..step {
            self.directory
                .set_segment_index(start_dir_entry + i + step, new_segment_index)?;
        }
        // Update the original
        segment.depth += 1;
        segmenter
            .update_segment(segment)
            .context("Updating existing segment depth")?;
        Ok(())
    }
}
