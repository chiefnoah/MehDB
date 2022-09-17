use crate::locking::{LockFileProxy, RWFilePool, StripedRWLock};
use crate::segment::bucket::{self, Bucket, BUCKET_SIZE};
use crate::segment::{Segment, Segmenter, BUCKETS_PER_SEGMENT, SEGMENT_SIZE};
use crate::serializer::{DataOrOffset, Serializable};
use anyhow::{anyhow, Context, Result};
use log::{debug, info, trace, warn};
use std::default::Default;
use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

//TODO: bucket-level locks. We need a write lock on the bucket, but only a read lock on the segment
//for normal inserts. We only need a segment-level write lock if we're migrating the segment

pub struct SegmenterProviderConfig {
    /// The number of reader file handles to open for the RW pool
    read_files: usize,
    /// The number of RW file handles to open for the RW pool
    write_files: usize,
    /// The Folder path to store segment files in
    path: PathBuf,
}

impl Default for SegmenterProviderConfig {
    fn default() -> Self {
        Self {
            read_files: 8,
            write_files: 6,
            path: PathBuf::from("./segments.bin"), // Defaults to pwd + segments.bin
        }
    }
}

/// A SegmenterProvider is a `Send`-able wrapper around `StripedRWLock` that can be used to
/// generate a temporary `Segmenter` implementation that is safe to use within that thread. It
/// retains a lock on the Segment it's requesting.
pub struct SegmenterProvider {
    file_handles: RWFilePool,
    num_segments: AtomicU32,
}

/// A `LockedSegmenter` implements `Segmenter` and represents the ability to manipulate one
/// particular segment at a time. It has
pub struct LockedSegmenter<'a> {
    file: LockFileProxy<'a>,
    // This is the segment index that was requested and represents the identity of the
    identity: u32,
    provider: &'a SegmenterProvider, // TODO: should this be a different lifetime?
}

impl SegmenterProvider {
    pub fn init(config: SegmenterProviderConfig) -> Result<Self> {
        let file_handles = RWFilePool::init(config.path, config.read_files, config.write_files)
            .context("Initializing segmenter file pool.")?;
        let mut file = file_handles.upgradeable_ro_file(0);
        let mut first_time = false;
        let num_segments = match u32::unpack(&mut *file) {
            Ok(n) => AtomicU32::new(n),
            Err(e) => {
                first_time = true;
                AtomicU32::new(0)
            }
        };
        info!("Num segments: {}", &num_segments.load(Ordering::Acquire));
        drop(file);
        let out = Self {
            num_segments,
            file_handles,
        };
        if first_time {
            let s = out.rw_segmenter_for(0);
            s.allocate_segment(0)
                .with_context(|| format!("Error initializing initial segment."))?;
        }
        Ok(out)
    }

    pub fn ro_segmenter_for(&self, segment_index: u32) -> LockedSegmenter {
        let file = self.file_handles.ro_file(segment_index);
        LockedSegmenter {
            file,
            identity: segment_index,
            provider: &self,
        }
    }
    pub fn rw_segmenter_for(&self, segment_index: u32) -> LockedSegmenter {
        let file = self.file_handles.rw_file(segment_index);
        LockedSegmenter {
            file,
            identity: segment_index,
            provider: &self,
        }
    }

    //pub fn upgradeable_segmenter_for(&self, segment_index: u32) -> LockedSegmenter {
    //    let file = self.file_handles.upgradeable_ro_file(segment_index);
    //    LockedSegmenter {
    //        file,
    //        identity: segment_index,
    //        provider: &self,
    //    }
    //}

    /// Atomically fetches the number of segments there currently are allocated
    fn num_segments(&self) -> u32 {
        self.num_segments.load(Ordering::Acquire)
    }

    /// Incremenets the number of segments by 1 and returns the *previous* number of segments.
    /// This method will also flush the updated num_segments to the header of the segments file.
    fn inc_num_segments(&self) -> Result<u32> {
        let mut f = self.file_handles.global_file.lock();
        f.seek(SeekFrom::Start(0))?;
        let cur = self.num_segments.fetch_add(1, Ordering::AcqRel);
        f.write_all(&(cur + 1).to_le_bytes())?;
        f.flush()?;
        Ok(cur)
    }

}

impl<'a> Segmenter for LockedSegmenter<'a> {
    type Header = u32;

    type Record = bucket::Record;

    fn segment(&self, index: u32) -> Result<Segment> {
        assert_eq!(self.identity, index);
        // Get a file-handle
        let mut buffer = &*self.file;
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        buffer
            .seek(SeekFrom::Start(offset))
            .context("Seeking to segment offset in buffer.")?;
        let mut buf: [u8; 1] = [0; 1];
        buffer.read_exact(&mut buf).with_context(|| {
            format!(
                "Error reading segment local depth for segment index {} with offset {}",
                index, offset
            )
        })?;
        let depth = u8::from_le_bytes(buf);
        Ok(Segment {
            depth,
            offset,
        })
    }

    fn allocate_segment(&self, depth: u8) -> Result<(u32, Segment)> {
        debug!("Allocating empty segment with depth {}", depth);
        // Seek to the proper offset
        let index = self
            .provider
            .inc_num_segments()
            .context("Incrementing number of segments.")?;
        //------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        debug!("New segment offset: {}", offset);
        let mut buffer = &*self.file;
        buffer
            .seek(io::SeekFrom::Start(offset))
            .with_context(|| format!("Seeking to new segment's offset {}", offset))?;
        let mut buf: [u8; SEGMENT_SIZE] = [0; SEGMENT_SIZE];
        buf[..1].copy_from_slice(&depth.to_le_bytes());
        buffer
            .write_all(&buf)
            .context("Writing new segment bytes")?;
        buffer
            .flush()
            .context("Flushing buffer after segment allocate.")?;
        Ok((
            index,
            Segment {
                depth,
                offset,
            },
        ))
    }

    fn allocate_with_buckets(&self, buckets: Vec<Bucket>, depth: u8) -> Result<(u32, Segment)> {
        // The number of buckets passed in *must* be the entire segment's buckets
        assert!(buckets.len() == BUCKETS_PER_SEGMENT);

        let index = self
            .provider
            .inc_num_segments()
            .context("Incrementing number of segments.")?;
        info!("New segment index: {}", &index);
        let mut buffer = &*self.file;
        // ------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        info!("New segment offset: {}", offset);
        // We create a BufWriter because we're going to be writing a lot and don't want to flush it
        // until we're done.
        buffer
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to new segment location")?;
        let mut buf: [u8; 1] = [0; 1];
        buf[..].copy_from_slice(&depth.to_le_bytes());
        buffer
            .write_all(&buf)
            .context("Writing new segment's depth.")?;
        for (i, bucket) in buckets.iter().enumerate() {
            debug!("Writing bucket with index {}", i);
            bucket
                .pack(&mut buffer)
                .with_context(|| format!("Writing bucket with index {}", i))?;
        }
        buffer.flush()?;
        Ok((
            index,
            Segment {
                offset,
                depth,
            },
        ))
    }

    fn bucket(&self, segment: &Segment, index: u32) -> Result<Bucket> {
        let mut buffer = &*self.file;
        assert!(index < BUCKETS_PER_SEGMENT as u32);
        //----------------------------------------------------------👇 for segment_depth
        let offset = segment.offset + (index as usize * BUCKET_SIZE) as u64 + 1;
        buffer
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to bucket's offset")?;
        debug!("Reading bucket at offset {}", offset);
        return Bucket::unpack(&mut buffer);
    }

    fn write_bucket(&self, segment: &Segment, bucket: &Bucket) -> Result<()> {
        //info!("Writing bucket at offset {} at segment offset {}", bucket.offset, segment.offset);
        let mut buffer = &*self.file;
        buffer
            .seek(io::SeekFrom::Start(bucket.offset))
            .context("Seeking to bucket's offset")?;
        bucket.pack(&mut buffer)?;
        buffer.flush()?;
        Ok(())
    }

    fn num_segments(&self) -> Result<u32> {
        Ok(self.provider.num_segments())
    }

    fn update_segment(&self, segment: Segment) -> Result<()> {
        let mut buffer = &*self.file;
        debug!("Updating segment depth to {}", segment.depth);
        buffer.seek(io::SeekFrom::Start(segment.offset))?;
        buffer.write_all(&segment.depth.to_le_bytes())?;
        buffer.flush()?;
        Ok(())
    }
}
