use crate::segment::bucket::{Bucket, BUCKET_SIZE};
use crate::segment::header::Header;
use crate::serializer::{DataOrOffset, Serializable};
use anyhow::{Context, Result};
use log::info;
use simple_error::SimpleError;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Cursor, Read, Seek, Write};
use std::mem::size_of;
use std::ops::{Index, IndexMut};
use std::path::Path;

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
        Ok(Self { offset, depth })
    }
}

/// A Segmenter is a something responsible for allocating and reading segments
pub trait Segmenter {
    /// Attempts to read an existing segment.
    fn segment(&mut self, index: u32) -> Result<Segment>;
    /// Creates a new segment and returns it's index.
    fn allocate_segment(&mut self, depth: u64) -> Result<Segment>;
    /// Creates a new segment, filling it iwth buckets
    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u64) -> Result<Segment>;
    /// Returns the header
    fn header(&mut self) -> Result<Header>;
    /// Sets the global_depth
    fn sync_header(&mut self) -> Result<()>;
    /// Retrieves a bucket from segment at index
    fn bucket(&mut self, segment: &Segment, index: u64) -> Result<Bucket>;
    /// Overwrites an existing bucket
    fn write_bucket(&mut self, bucket: &Bucket) -> Result<()>;
    /// IterateSegments returns an Iterator that returns segments
    fn iterate_segments(&mut self) -> SegmentIterator;
}

pub struct SegmentIterator {
    index: usize,
}

pub struct BasicSegmenter<B: Read + Write + Seek> {
    buffer: B,
    header: Header,
    header_dirty: bool,
    segment_depth_cache: HashMap<u64, u64>,
}

impl<B: Read + Write + Seek> BasicSegmenter<B> {
    pub fn init(mut buffer: B) -> Result<Self> {
        buffer.seek(io::SeekFrom::Start(0))?;
        // Attempt to read the header and use it, otherwise initialize as new
        let mut first_time = false;
        let header = match Header::unpack(&mut buffer) {
            Ok(h) => h,
            Err(e) => {
                first_time = true;
                Header {
                    global_depth: 0,
                    num_segments: 0,
                }
            }
        };
        let mut out = Self {
            buffer,
            header,
            header_dirty: false,
            segment_depth_cache: HashMap::new(),
        };
        if first_time {
            out.allocate_segment(0)
                .with_context(|| format!("Error initializing initial segment."))?;
        }
        Ok(out)
    }
}

impl<B> Segmenter for BasicSegmenter<B>
where
    B: Write + Read + Seek,
{
    fn segment(&mut self, index: u32) -> Result<Segment> {
        let offset = ((index as usize * SEGMENT_SIZE) + size_of::<Header>()) as u64;
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
                index, self.header.num_segments, offset
            )
        })?;
        let depth = u64::from_le_bytes(buf);
        self.segment_depth_cache.insert(offset, depth);
        Ok(Segment { depth, offset })
    }

    fn allocate_segment(&mut self, depth: u64) -> Result<Segment> {
        // Seek to the proper offset
        let offset = (self.header.num_segments * SEGMENT_SIZE as u64) + size_of::<Header>() as u64;
        self.buffer.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; SEGMENT_SIZE] = [0; SEGMENT_SIZE];
        buf[..8].copy_from_slice(&depth.to_le_bytes());
        self.buffer.write(&buf)?;
        self.header.num_segments += 1;
        self.sync_header()?;
        Ok(Segment { depth, offset })
    }

    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u64) -> Result<Segment> {
        // The number of buckets passed in *must* be the entire segment's buckets
        assert!(buckets.len() as u64 == BUCKETS_PER_SEGMENT as u64);
        let offset = (self.header.num_segments * SEGMENT_SIZE as u64) + size_of::<Header>() as u64;
        let mut buffer = BufWriter::with_capacity(SEGMENT_SIZE as usize, &mut self.buffer);
        buffer.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 8] = [0; 8];
        buf[..].copy_from_slice(&depth.to_le_bytes());
        buffer.write(&buf)?;
        for bucket in buckets.iter() {
            bucket.pack(&mut buffer)?;
        }
        // Drop so we can call sync_header
        drop(buffer);
        self.header.num_segments += 1;
        self.sync_header()?;
        Ok(Segment { offset, depth })
    }

    fn header(&mut self) -> Result<Header> {
        self.buffer.seek(io::SeekFrom::Start(0))?;
        Header::unpack(&mut self.buffer)
    }

    fn sync_header(&mut self) -> Result<()> {
        self.buffer.seek(io::SeekFrom::Start(0))?;
        self.header.pack(&mut self.buffer)?;
        Ok(())
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

    fn iterate_segments(&mut self) -> SegmentIterator {
        SegmentIterator { index: 0 }
    }
}

impl Iterator<Segment> for SegmentIterator {

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
    fn segmenter_init_syncs_header() {
        let mut segmenter = inmemory_segmenter();
        let header = segmenter.header().unwrap();
        assert_eq!(header.num_segments, 1);
        assert_eq!(header.global_depth, 0);
    }

    #[test]
    fn segmenter_init_initializes_first_segment() {
        let mut segmenter = inmemory_segmenter();
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

        let expected_size = SEGMENT_SIZE + size_of::<Header>();
        assert_eq!(segmenter.buffer.into_inner().len(), expected_size);
    }

    #[test]
    fn segmenter_initialize_segment_correct_size() {
        let mut segmenter = inmemory_segmenter();
        let first_segment = segmenter.segment(0).unwrap();
        let second_segment = segmenter.allocate_segment(3).unwrap();
        assert_eq!(second_segment.depth, 3);
        let expected_offset = size_of::<Header>() + SEGMENT_SIZE;
        assert_eq!(second_segment.offset, expected_offset as u64);
        let another_second_segment = segmenter.segment(1).unwrap();
        assert_eq!(another_second_segment.offset, second_segment.offset);
        assert_eq!(another_second_segment.depth, second_segment.depth);
        let expected_buffer_length = size_of::<Header>() + 2 * (SEGMENT_SIZE);
        let actual_len = segmenter.buffer.into_inner().len();
        assert_eq!(actual_len, expected_buffer_length);
    }
}
