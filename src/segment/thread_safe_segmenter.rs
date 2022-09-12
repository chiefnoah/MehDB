use crate::locking::{RWFilePool, StripedRWLock};
use crate::segment::bucket::{self, Bucket};
use crate::segment::{Segment, Segmenter, BUCKETS_PER_SEGMENT, SEGMENT_SIZE};
use crate::serializer::{DataOrOffset, Serializable};
use anyhow::{anyhow, Context, Result};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use log::{debug, info, trace, warn};

struct ThreadSafeSegmenter {
    // TODO: add a LRU cache for segment depth
    file_handles: Arc<RWFilePool>,
    num_segments: AtomicU32,
}

struct ThreadSafeSegmenterConfig {
    read_files: usize,
    write_files: usize,
    path: PathBuf,
}


impl Segmenter for ThreadSafeSegmenter {
    type Header = u8;
    type Record = bucket::Record;
    type Config = ThreadSafeSegmenterConfig;

    fn init(config: Self::Config) -> Result<Self> {
        let file_handles = Arc::new(
            RWFilePool::init(config.path, config.read_files, config.write_files)
                .context("Initializing segmenter file pool.")?,
        );
        let mut file = file_handles.upgradeable_rw_file(0);
        let mut first_time = false;
        let num_segments = match u32::unpack(&mut *file) {
            Ok(n) => AtomicU32::new(n),
            Err(e) => {
                first_time = true;
                AtomicU32::new(0)
            }
        };
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
        let k = &index.to_le_bytes()[..];
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
        Ok(Segment { depth, offset })
    }

    fn allocate_segment(&self, depth: u8) -> Result<(u32, Segment)> {
        debug!("Allocating empty segment with depth {}", depth);
        // Seek to the proper offset
        let index = self.num_segments.load(Ordering::Acquire);
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
            .write_all(&(self.num_segments.fetch_add(1, Ordering::AcqRel) + 1).to_le_bytes())
            .context("Syncing num_segments")?;
        buffer.flush()?;
        Ok((index, Segment { depth, offset }))
    }

    fn allocate_with_buckets(&self, buckets: Vec<Bucket>, depth: u8) -> Result<(u32, Segment)> {
        todo!()
    }

    fn bucket(&self, segment: &Segment, index: u32) -> Result<Bucket> {
        todo!()
    }

    fn write_bucket(&self, bucket: &Bucket) -> Result<()> {
        todo!()
    }

    fn num_segments(&self) -> Result<u32> {
        todo!()
    }

    fn update_segment(&self, segment: Segment) -> Result<()> {
        todo!()
    }
}
