use crate::locking::{RWFilePool, StripedRWLock};
use crate::segment::bucket::{self, Bucket, BUCKET_SIZE};
use crate::segment::{Segment, Segmenter, BUCKETS_PER_SEGMENT, SEGMENT_SIZE};
use crate::serializer::{DataOrOffset, Serializable};
use anyhow::{anyhow, Context, Result};
use log::{debug, info, trace, warn};
use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::default::Default;

pub struct ThreadSafeSegmenter {
    // TODO: add a LRU cache for segment depth
    file_handles: RWFilePool,
    num_segments: AtomicU32,
}

pub struct ThreadSafeSegmenterConfig {
    /// The number of reader file handles to open for the RW pool
    read_files: usize,
    /// The number of RW file handles to open for the RW pool
    write_files: usize,
    /// The Folder path to store segment files in
    path: PathBuf,
}

impl Default for ThreadSafeSegmenterConfig {
    fn default() -> Self {
        Self {
            read_files: 8,
            write_files: 4,
            path: PathBuf::from("./segment.bin"),
        }
    }
}

impl Segmenter for ThreadSafeSegmenter {
    type Header = u32;
    type Record = bucket::Record;
    type Config = ThreadSafeSegmenterConfig;

    fn init(config: Self::Config) -> Result<Self> {
        let file_handles = RWFilePool::init(config.path, config.read_files, config.write_files)
            .context("Initializing segmenter file pool.")?;
        let mut file = file_handles.upgradeable_rw_file(0);
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
            out.allocate_segment(0)
                .with_context(|| format!("Error initializing initial segment."))?;
        }
        Ok(out)
    }

    fn segment(&self, index: u32) -> Result<Segment> {
        // Get a file-handle
        let mut buffer = self.file_handles.ro_file(index);
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
            index,
        })
    }

    fn allocate_segment(&self, depth: u8) -> Result<(u32, Segment)> {
        debug!("Allocating empty segment with depth {}", depth);
        // Seek to the proper offset
        let index = self.num_segments.fetch_add(1, Ordering::AcqRel);
        //------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        debug!("New segment offset: {}", offset);
        let mut buffer = self.file_handles.rw_file(index);
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
        // Seek to beginning of file
        buffer
            .seek(io::SeekFrom::Start(0))
            .context("Seeking to beginning of segment file.")?;
        // Write the number_of_segments, and simultaneously swap the atomic
        buffer
            .write_all(&(self.num_segments.load(Ordering::Acquire)).to_le_bytes())
            .context("Syncing num_segments")?;
        buffer.flush()?;
        Ok((
            index,
            Segment {
                depth,
                offset,
                index,
            },
        ))
    }

    fn allocate_with_buckets(&self, buckets: Vec<Bucket>, depth: u8) -> Result<(u32, Segment)> {
        // The number of buckets passed in *must* be the entire segment's buckets
        assert!(buckets.len() == BUCKETS_PER_SEGMENT);

        let index = self.num_segments.fetch_add(1, Ordering::AcqRel);
        info!("New segment index: {}", &index);
        let mut buffer = self.file_handles.rw_file(index);
        // ------------------------------------------👇 for num_segments header in segments file
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        info!("New segment offset: {}", offset);
        // We create a BufWriter because we're going to be writing a lot and don't want to flush it
        // until we're done.
        let mut buffer_w = BufWriter::with_capacity(SEGMENT_SIZE as usize, &mut *buffer);
        buffer_w
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to new segment location")?;
        let mut buf: [u8; 1] = [0; 1];
        buf[..].copy_from_slice(&depth.to_le_bytes());
        buffer_w
            .write_all(&buf)
            .context("Writing new segment's depth.")?;
        for (i, bucket) in buckets.iter().enumerate() {
            debug!("Writing bucket with index {}", i);
            bucket
                .pack(&mut buffer_w)
                .with_context(|| format!("Writing bucket with index {}", i))?;
        }
        buffer_w
            .flush()
            .context("Flushing outer buffer after segment allocate.")?;
        drop(buffer_w);
        // Write the number_of_segments, and simultaneously swap the atomic
        buffer
            .write_all(&(self.num_segments.load(Ordering::Acquire)).to_le_bytes())
            .context("Syncing num_segments")?;
        buffer.flush()?;
        drop(buffer);
        Ok((
            index,
            Segment {
                offset,
                depth,
                index,
            },
        ))
    }

    fn bucket(&self, segment: &Segment, index: u32) -> Result<Bucket> {
        let mut buffer = self.file_handles.ro_file(segment.index);
        assert!(index < BUCKETS_PER_SEGMENT as u32);
        //----------------------------------------------------------👇 for segment_depth
        let offset = segment.offset + (index as usize * BUCKET_SIZE) as u64 + 1;
        buffer
            .seek(io::SeekFrom::Start(offset))
            .context("Seeking to bucket's offset")?;
        debug!("Reading bucket at offset {}", offset);
        return Bucket::unpack(&mut (*buffer));
    }

    fn write_bucket(&self, segment: &Segment, bucket: &Bucket) -> Result<()> {
        let mut buffer = self.file_handles.rw_file(segment.index);
        buffer
            .seek(io::SeekFrom::Start(bucket.offset))
            .context("Seeking to bucket's offset")?;
        bucket.pack(&mut *buffer)?;
        buffer.flush()?;
        Ok(())
    }

    fn num_segments(&self) -> Result<u32> {
        Ok(self.num_segments.load(Ordering::Acquire))
    }

    fn update_segment(&self, segment: Segment) -> Result<()> {
        let mut buffer = self.file_handles.rw_file(segment.index);
        debug!("Updating segment depth to {}", segment.depth);
        buffer.seek(io::SeekFrom::Start(segment.offset))?;
        buffer.write_all(&segment.depth.to_le_bytes())?;
        buffer.flush()?;
        Ok(())
    }
}
