use crate::segment::bucket::{Bucket, BUCKET_SIZE};
use crate::serializer::{DataOrOffset, Serializable};
use anyhow::{Context, Result};
use log::{info, warn, debug};
use simple_error::SimpleError;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Cursor, Read, Seek, Write};
use std::mem::size_of;
use std::ops::{Index, IndexMut};
use std::path::Path;
use std::iter::Iterator;

// The number of buckets in each segment.
// This may be adapted to be parametrizable on a per-database level
// in the futere.
pub const BUCKETS_PER_SEGMENT: usize = 64;

// The size on-disk of a segment
pub const SEGMENT_SIZE: usize = (BUCKET_SIZE * BUCKETS_PER_SEGMENT) + 8;

pub struct Segment {
    pub depth: u64,
    pub offset: u64,
}

impl Serializable for Segment {
    /// packs a Segment's depth. Assumes buffer has alread Seeked to offset
    fn pack<W: Write + Seek>(&self, buffer: &mut W) -> Result<u64> {
        buffer
            .write(&self.depth.to_le_bytes())
            .with_context(|| format!("Error packing segmenter at offset {}", self.offset))?;
        Ok(self.offset)
    }

    /// unpacks a segment's depth. Assumes buffer has already Seeked to proper offset
    fn unpack<R: Read + Seek>(buffer: &mut R) -> Result<Self> {
        let offset = buffer.seek(io::SeekFrom::Current(0))?;
        let mut b: [u8; 8] = [0; 8];
        buffer
            .read_exact(&mut b)
            .with_context(|| format!("Error unpacking segment at offset {}", offset))?;
        let depth = u64::from_le_bytes(b);
        Ok(Self { offset, depth})
    }
}

/// A Segmenter is a something responsible for allocating and reading segments
pub trait Segmenter {
    /// Header is a Sized type used to identify the offset for reading and writing segments. Some
    /// implementations will have a 0-sized header. The `BasicSegmenter` will have a u64 header for
    /// storing `num_segments`. Implementations can expose a way to read the header in their API,
    /// but it is not strictly necessary.
    type Header;
    /// Attempts to read an existing segment.
    fn segment(&mut self, index: u64) -> Result<Segment>;
    /// Creates a new segment and returns a tuple containing the index of the newly created segment
    /// and an instance of `Segment`. This increments `num_segments` by one. If the implementation
    /// stores on disk, it should write the new segment then increment the persisted `num_segments`
    /// after, effectively "committing" the change.
    fn allocate_segment(&mut self, depth: u64) -> Result<(u64, Segment)>;
    /// The same as `Segmenter.allocate_segment` except instead of empty buckets it writes the
    /// provided buckets.
    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u64) -> Result<(u64, Segment)>;
    /// Retrieves a bucket from segment at index. The resulting bucket can be modified and synced
    /// back to to the Segmenter using `write_bucket`. Implementations that support concurrency are
    /// responsible for providing the appropriate locking mechanism to prevent race-conditions.
    fn bucket(&mut self, segment: &Segment, index: u64) -> Result<Bucket>;
    /// Overwrites an existing bucket.
    fn write_bucket(&mut self, bucket: &Bucket) -> Result<()>;
    /// Returns the *current* number of segements allocated. This may or may not be cached
    /// in-memory. Implementations that save segements to non-volatile storage *should* store this
    /// value along with the segments. If the implementation supports concurrency, this should
    /// only be used in a context where a read of this value is guaranteed to be correct for the
    /// duration of it's use. That is, if you support concurrent segement creation, this method
    /// should only be called inside a syncronized block.
    fn num_segments(&mut self) -> Result<u64>;
    /// Updates the local_depth of `Segment`. Implementations that allow for concurrent access
    /// should take care to prevent race conditions.
    fn set_segment_depth(&mut self, segment: Segment, new_depth: u64) -> Result<()>;
}

impl Serializable for u64 {
    fn pack<W: Write + Seek>(&self, buffer: &mut W) -> Result<u64> {
        let buf = self.to_le_bytes();
        buffer.write_all(&buf).context("Writing u64")?;
        Ok(4)
    }
    fn unpack<R: Read + Seek>(buffer: &mut R) -> Result<Self> {
        let mut buf: [u8; 8] = [0; 8];
        buffer.read_exact(&mut buf).context("Reading u64")?;
        Ok(u64::from_le_bytes(buf))
    }
}

pub struct BasicSegmenter<B: Read + Write + Seek> {
    buffer: B,
    num_segments: u64,
    header_dirty: bool,
    segment_depth_cache: HashMap<u64, u64>,
    iter_index: usize,
}

impl<B: Read + Write + Seek> BasicSegmenter<B> {
    pub fn init(mut buffer: B) -> Result<Self> {
        buffer.seek(io::SeekFrom::Start(0)).context("Seeking to beginning of segment file.")?;
        // Attempt to read the header and use it, otherwise initialize as new
        let mut first_time = false;
        let num_segments = match u64::unpack(&mut buffer) {
            Ok(n) => n,
            Err(e) => {
                first_time = true;
                0
            }
        };
        let mut out = Self {
            num_segments,
            buffer,
            header_dirty: false,
            segment_depth_cache: HashMap::new(),
            iter_index: 0,
        };
        if first_time {
            out.allocate_segment(0)
                .with_context(|| format!("Error initializing initial segment."))?;
        }
        Ok(out)
    }

    fn write_num_segments(&mut self) -> Result<()> {
        self.buffer.seek(io::SeekFrom::Start(0)).context("Seeking to beginning of segment file.")?;
        self.buffer.write_all(&self.num_segments.to_le_bytes()).context("Syncing num_segments")?;
        Ok(())
    }
}

impl<B> Segmenter for BasicSegmenter<B>
where
    B: Write + Read + Seek,
{
    type Header = u64;
    fn segment(&mut self, index: u64) -> Result<Segment> {
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Self::Header>()) as u64;
        if self.segment_depth_cache.contains_key(&offset) {
            return Ok(Segment {
                depth: *self.segment_depth_cache.get(&offset).unwrap(),
                offset,
            });
        }
        self.buffer.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 8] = [0; 8];
        self.buffer.read_exact(&mut buf).with_context(|| {
            format!(
                "Error reading segment local depth for segment index {}/{} with offset {}",
                index, self.num_segments, offset
            )
        })?;
        let depth = u64::from_le_bytes(buf);
        self.segment_depth_cache.insert(offset, depth);
        Ok(Segment { depth, offset })
    }

    fn allocate_segment(&mut self, depth: u64) -> Result<(u64, Segment)> {
        debug!("Allocating empty segment with depth {}", depth);
        // Seek to the proper offset
        let index = self.num_segments;
        let offset = (index * SEGMENT_SIZE as u64) + size_of::<Self::Header>() as u64;
        debug!("New segment offset: {}", offset);
        self.buffer.seek(io::SeekFrom::Start(offset))
            .with_context(|| format!("Seeking to new segment's offset {}", offset))?;
        let mut buf: [u8; SEGMENT_SIZE] = [0; SEGMENT_SIZE];
        buf[..8].copy_from_slice(&depth.to_le_bytes());
        self.buffer.write(&buf).context("Writing new segment bytes")?;
        self.num_segments += 1;
        self.write_num_segments().context("Syncronizing num_segments after allocating.")?;
        Ok((index, Segment { depth, offset }))
    }

    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u64) -> Result<(u64, Segment)> {
        // The number of buckets passed in *must* be the entire segment's buckets
        assert!(buckets.len() as u64 == BUCKETS_PER_SEGMENT as u64);
        let index = self.num_segments;
        let offset = (index * SEGMENT_SIZE as u64) + 8;
        let mut buffer = BufWriter::with_capacity(SEGMENT_SIZE as usize, &mut self.buffer);
        buffer.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 8] = [0; 8];
        buf[..].copy_from_slice(&depth.to_le_bytes());
        buffer.write(&buf)?;
        for bucket in buckets.iter() {
            bucket.pack(&mut buffer)?;
        }
        buffer.flush().context("Flushing new segment buffer.")?;
        // Drop so we can call sync_header
        drop(buffer);
        let index = self.num_segments; 
        self.num_segments += 1;
        self.write_num_segments().context("Syncronizing num_segments after allocating.")?;
        Ok((index, Segment { offset, depth }))
    }

    fn bucket(&mut self, segment: &Segment, index: u64) -> Result<Bucket> {
        //-----------------------------------------------------------| for segment_depth
        let offset = segment.offset + (index * BUCKET_SIZE as u64) + 8;
        self.buffer.seek(io::SeekFrom::Start(offset))?;
        info!("Reading bucket at offset {}", offset);
        return Bucket::unpack(&mut self.buffer);
    }

    fn write_bucket(&mut self, bucket: &Bucket) -> Result<()> {
        self.buffer.seek(io::SeekFrom::Start(bucket.offset))?;
        bucket.pack(&mut self.buffer)?;
        Ok(())
    }

    fn num_segments(&mut self) -> Result<u64> {
        Ok(self.num_segments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::{self, Cursor, Read, Seek, Write};

    fn inmemory_segmenter() -> BasicSegmenter<Cursor<Vec<u8>>> {
        let buffer = Cursor::new(Vec::new());
        BasicSegmenter::init(buffer).unwrap()
    }

    #[test]
    fn can_pack_segment() {
        let segment = Segment {
            depth: 5,
            offset: 3,
        };
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let result = segment.pack(&mut buf).unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn can_unpack_segment() {
        let fixture: Vec<u8> = vec![0x10, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut buf: Cursor<Vec<u8>> = Cursor::new(fixture);
        buf.seek(io::SeekFrom::Current(1)).unwrap();
        let segment = Segment::unpack(&mut buf).unwrap();
        assert_eq!(segment.depth, 51);
        assert_eq!(segment.offset, 1);
    }

    #[test]
    fn segmenter_init_syncs_num_segments() {
        let mut segmenter = inmemory_segmenter();
        let num_segments = segmenter.num_segments().unwrap();
        assert_eq!(num_segments, 1);
    }

    #[test]
    fn segmenter_init_initializes_first_segment() {
        let mut segmenter = inmemory_segmenter();
        const HEADER_SIZE: usize = size_of::<<BasicSegmenter<Cursor<Vec<u8>>> as Segmenter>::Header>();
        let first_segment = segmenter.segment(0).unwrap();
        // Get the last bucket
        let mut bucket = segmenter
            .bucket(&first_segment, BUCKETS_PER_SEGMENT as u64 - 1)
            .unwrap();
        bucket
            .put(123, 456, 0)
            .expect("Unable to insert record into bucket");
        segmenter
            .write_bucket(&bucket)
            .expect("Unable to write bucket to segment");
        let bucket = segmenter
            .bucket(&first_segment, BUCKETS_PER_SEGMENT as u64 - 1)
            .unwrap();
        let r = bucket.get(123).unwrap();
        assert_eq!(r.hash_key, 123);
        assert_eq!(r.value, 456);

        let expected_size = SEGMENT_SIZE + HEADER_SIZE;
        assert_eq!(segmenter.buffer.into_inner().len(), expected_size);
    }

    #[test]
    fn segmenter_initialize_segment_correct_size() {
        let mut segmenter = inmemory_segmenter();
        const HEADER_SIZE: usize = size_of::<<BasicSegmenter<Cursor<Vec<u8>>> as Segmenter>::Header>();
        let first_segment = segmenter.segment(0).unwrap();
        let second_segment = segmenter.allocate_segment(3).unwrap().1;
        assert_eq!(second_segment.depth, 3);
        let expected_offset = HEADER_SIZE + SEGMENT_SIZE;
        assert_eq!(second_segment.offset, expected_offset as u64);
        let another_second_segment = segmenter.segment(1).unwrap();
        assert_eq!(another_second_segment.offset, second_segment.offset);
        assert_eq!(another_second_segment.depth, second_segment.depth);
        let expected_buffer_length = HEADER_SIZE + 2 * (SEGMENT_SIZE);
        let actual_len = segmenter.buffer.into_inner().len();
        assert_eq!(actual_len, expected_buffer_length);
    }
}
