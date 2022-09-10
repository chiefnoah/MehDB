use crate::locking::StripedRWLock;
use crate::segment::bucket::{self, Bucket};
use crate::segment::{Segment, Segmenter, BUCKETS_PER_SEGMENT, SEGMENT_SIZE};
use crate::serializer::{DataOrOffset, Serializable};
use anyhow::{anyhow, Context, Result};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::sync::{Arc, Mutex};

struct ThreadSafeSegmenter<T: Read + Write + Seek> {
    // TODO: add a LRU cache for segment depth
    file_handles: Arc<StripedRWLock<()>>,
    num_segments: Mutex<u32>,
}

impl<T: Read + Write + Seek> Segmenter for ThreadSafeSegmenter<T> {
    type Header = u8;
    type Record = bucket::Record;

    fn segment(&self, index: u32) -> Result<Segment> {
        let buffer = self.file_handles.get(&index.to_le_bytes()[..]).read();
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
        // Get a handle on the num_segments ex. This should prevent all other allocations
        let num_segments = self.num_segments.lock();
        
        todo!()
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
