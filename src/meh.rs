use crate::directory::{Directory, MMapDirectory};
use crate::locking::{SegmentNode, StripedLock};
use crate::segment::{
    Bucket, Record, Segment, Segmenter, ThreadSafeFileSegmenter, BUCKETS_PER_SEGMENT,
};
use crate::serializer;
use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::path::Path;
use std::sync::Arc;

use highway::{self, HighwayHash, HighwayHasher};
use parking_lot::{RwLockUpgradableReadGuard, RwLockWriteGuard};

// My Extendible Hash Database
#[derive(Clone)]
pub struct MehDB {
    // TODO: make an init or something so we don't have to deal with this
    pub hasher_key: highway::Key,
    pub directory: Arc<MMapDirectory>,
    pub segmenter: ThreadSafeFileSegmenter,
    pub lock: Arc<StripedLock<SegmentNode>>,
}

/// A Extendible hashing implementation that does not support multithreading.
impl MehDB {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let lock = StripedLock::init(1024);
        let segmenter = ThreadSafeFileSegmenter::init(dir.as_ref().join("segment.bin"))?;
        let directory = MMapDirectory::init(dir.as_ref().join("directory.bin"))?;
        Ok(MehDB {
            hasher_key: highway::Key([53252, 2352323, 563956259, 234832]),
            directory: Arc::new(directory),
            segmenter: segmenter.clone(),
            lock: Arc::new(lock),
        })
    }

    fn bucket_for_key(&mut self, key: &[u64; 4]) -> Result<Bucket> {
        let mut segment_index = self
            .directory
            .segment_index(key[0])
            .with_context(|| format!("Unable to get segment offset for {:?}", key))?;
        let mut segment_locker = self.lock.get(segment_index);
        let mut segment_node = segment_locker.read();
        let mut segment_index_double_check = self
            .directory
            .segment_index(key[0])
            .with_context(|| format!("Unable to get segment offset for {:?}", key))?;

        // While the directory doesn't agree with what we grabbed last...
        while segment_index != segment_index_double_check {
            debug!(
                "Segment changed after acquiring lock. {} -> {}. Retrying lock.",
                segment_index, segment_index_double_check
            );
            // Drop the existing lock (we might lose our place here 🥲)
            drop(segment_node);
            segment_index = segment_index_double_check;
            segment_locker = self.lock.get(segment_index);
            // Acquire a new upgradable_read lock
            segment_node = segment_locker.read();
            segment_index_double_check = self
                .directory
                .segment_index(key[0])
                .with_context(|| format!("Unable to get segment offset for {:?}", key))?;
        }
        //TODO: remove this assert
        assert_eq!(segment_index_double_check, segment_index);
        let bucket_index = ((BUCKETS_PER_SEGMENT - 1) as u64 & key[3]) as u32;
        // Acquire a read lock on the bucket
        let _bucket_lock = segment_node
            .get_bucket_lock(bucket_index)
            .context("Getting bucket lock")?
            .read();
        let segment = self
            .segmenter
            .segment(segment_index)
            .with_context(|| format!("Unable to read segment at offset {}", segment_index))?;
        debug!("Reading bucket at index: {}", bucket_index);
        self.segmenter.bucket(&segment, bucket_index)
    }

    pub fn put(&mut self, key: serializer::ByteKey, value: serializer::ByteValue) -> Result<()> {
        let hasher = HighwayHasher::new(self.hasher_key);
        // We only need the first u64 of the returned value because
        // It's unlikely we have the hard drive space to support a u64 deep directory
        // and we *definitely* don't have the RAM to.
        // TODO: support the full 256 bit keyspace for magical distributed system support
        let hash_key = hasher.hash256(&key.0);
        info!("hash_key: {:?}\tvalue: {}", hash_key, value.0);
        let mut segment_index = self
            .directory
            .segment_index(hash_key[0])
            .with_context(|| format!("Unable to get segment offset for {:?}", hash_key))?;
        let mut segment_locker = self.lock.get(segment_index);
        let mut segment_node = segment_locker.upgradable_read();
        let mut segment_index_double_check = self
            .directory
            .segment_index(hash_key[0])
            .with_context(|| format!("Unable to get segment offset for {:?}", hash_key))?;

        // While the directory doesn't agree with what we grabbed last...
        while segment_index != segment_index_double_check {
            debug!(
                "Segment changed after acquiring lock. {} -> {}. Retrying lock.",
                segment_index, segment_index_double_check
            );
            // Drop the existing lock (we might lose our place here 🥲)
            drop(segment_node);
            segment_index = segment_index_double_check;
            segment_locker = self.lock.get(segment_index);
            // Acquire a new upgradable_read lock
            segment_node = segment_locker.upgradable_read();
            segment_index_double_check = self
                .directory
                .segment_index(hash_key[0])
                .with_context(|| format!("Unable to get segment offset for {:?}", hash_key))?;
        }
        //TODO: remove this assert
        assert_eq!(segment_index_double_check, segment_index);

        let bucket_index = ((BUCKETS_PER_SEGMENT - 1) as u64 & hash_key[3]) as u32;
        let bucket_lock = segment_node
            .get_bucket_lock(bucket_index)
            .context("Getting bucket lock")?
            .write();
        debug!("Segment index {}", segment_index);
        let segment = self
            .segmenter
            .segment(segment_index)
            .with_context(|| format!("Unable to read segment with index {}", segment_index))?;
        debug!("Reading bucket at index: {}", bucket_index);
        let mut bucket = self
            .segmenter
            .bucket(&segment, bucket_index)
            .with_context(|| format!("Reading bucket at index {}", bucket_index))?;
        debug!("Inserting record into bucket...");
        match bucket.put(hash_key[0], value.0, segment.depth) {
            // Overflowed the bucket!
            Err(_) => {
                info!("Bucket overflowed. Allocating new segment and splitting.");
                // Drop the bucket lock before we split, we don't need it
                // segment and maybe directory
                drop(bucket_lock);
                let offset = segment.offset;
                let write_lock = RwLockUpgradableReadGuard::upgrade(segment_node);
                self.split_segment(segment, hash_key[0], write_lock)
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
        self.segmenter
            .write_bucket(&bucket)
            .with_context(|| format!("Saving updated bucket at offset {}", bucket.offset))
    }
    pub fn get(&mut self, key: serializer::ByteKey) -> Option<Record> {
        let hasher = HighwayHasher::new(self.hasher_key);
        // We only need the first u64 of the returned value because
        // It's unlikely we have the hard drive space to support a u64 deep directory
        // and we *definitely* don't have the RAM to.
        // TODO: support the full 256 bit keyspace for magical distributed system support
        let hash_key = hasher.hash256(&key.0);
        info!("hash_key: {:?}", hash_key);
        let bucket = match self.bucket_for_key(&hash_key) {
            Ok(b) => b,
            Err(_) => {
                warn!("No bucket found for key {:?}", key);
                return None;
            }
        };
        bucket.get(hash_key[0])
    }

    fn split_segment(
        &self,
        segment: Segment,
        hk: u64,
        _lock: RwLockWriteGuard<SegmentNode>,
    ) -> Result<()> {
        info!("Splitting segment");
        let mut segment = segment;
        // If we need to expand the directory size
        let global_depth = self
            .directory
            .grow_if_eq(segment.depth)
            .context("Getting global depth, growing if local_depth == global_depth")?;
        debug!("gobal_depth: {}", global_depth);
        let new_depth = segment.depth + 1;
        // The buckets that are being allocated to the new segment
        let mut new_buckets = Vec::<Bucket>::with_capacity(BUCKETS_PER_SEGMENT);
        let mask = (hk >> (64 - new_depth)) | 1;
        for bi in 0..BUCKETS_PER_SEGMENT {
            let old_bucket = self
                .segmenter
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
        info!("Allocating new segment with depth {}", new_depth);
        let (new_segment_index, _) =
            match self.segmenter.allocate_with_buckets(new_buckets, new_depth) {
                Ok(s) => s,
                Err(e) => return Err(e.context("Allocating new segment with populated buckets.")),
            };
        let mut global_depth = self
            .directory
            .global_depth()
            .context("Getting global depth + read lock.")?;
        let step = 1 << (*global_depth - new_depth);
        let mut start_dir_entry = if segment.depth == 0 {
            0
        } else {
            hk >> 64 - segment.depth
        };
        start_dir_entry = start_dir_entry << (*global_depth - segment.depth);
        start_dir_entry = start_dir_entry - (start_dir_entry % 2);
        for i in 0..step {
            self.directory.set_segment_index(
                start_dir_entry + i + step,
                new_segment_index,
                &mut global_depth,
            )?;
        }
        drop(global_depth);
        // Update the original segment
        segment.depth += 1;
        self.segmenter
            .update_segment(segment)
            .context("Updating existing segment depth")?;
        Ok(())
    }
}
