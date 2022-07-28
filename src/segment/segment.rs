use crate::serializer::{DataOrOffset, Serializable};
use crate::segment::header::Header;
use crate::segment::bucket::{Bucket, BUCKET_SIZE};
use simple_error::SimpleError;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, Write};
use std::mem::size_of;
use std::ops::{Index, IndexMut};
use std::path::Path;
use log::info;

// The number of buckets in each segment.
// This may be adapted to be parametrizable on a per-database level
// in the futere.
const BUCKETS_PER_SEGMENT: usize = 64;

// The size on-disk of a segment
const SEGMENT_SIZE: usize = (BUCKET_SIZE * BUCKETS_PER_SEGMENT) + 8;

pub struct Segment {
    pub depth: u64,
    pub offset: u64,
}

impl Serializable for Segment {
    fn pack<File: Write + Seek>(&self, buffer: &mut File) -> io::Result<u64> {
        let offset = buffer.seek(io::SeekFrom::Current(0))?;
        buffer.write(&self.depth.to_le_bytes())?;
        Ok(offset)
    }

    fn unpack<File: Read + Seek>(buffer: &mut File) -> io::Result<Self> {
        let offset = buffer.seek(io::SeekFrom::Current(0))?;
        let mut b: [u8; 8] = [0; 8];
        buffer.read_exact(&mut b)?;
        let depth = u64::from_le_bytes(b);
        Ok(Self { offset, depth })
    }
}

// A Segmenter is a something responsible for allocating and reading segments
pub trait Segmenter {
    // Attempts to read an existing segment.
    fn segment(&mut self, index: u64) -> io::Result<Segment>;
    // Creates a new segment and returns it's index.
    fn allocate_segment(&mut self, depth: u64) -> io::Result<Segment>;
    // Creates a new segment, filling it iwth buckets
    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u64) -> io::Result<Segment>;
    // Returns the header
    fn header(&mut self) -> io::Result<Header>;
    // Sets the global_depth
    fn sync_header(&mut self) -> io::Result<()>;
    // Retrieves a bucket from segment at index
    fn bucket(&mut self, segment: &Segment, index: u64) -> io::Result<Bucket>;
    // Overwrites an existing bucket
    fn write_bucket(&mut self, bucket: &Bucket) -> io::Result<()>;
}

// A simple, file-backed Segmenter
pub struct FileSegmenter {
    file: File,
    header: Header,
    header_dirty: bool,
    segment_depth_cache: HashMap<u64, u64>,
}

impl FileSegmenter {
    pub fn init(path: Option<&Path>) -> io::Result<Self> {
        let path: &Path = match path {
            Some(path) => path,
            None => Path::new("index.bin"),
        };
        let mut file = if !path.exists() {
            let f = File::create(path);
            f
        } else {
            File::open(path)
        }?;
        let segment_metadata = file.metadata()?;
        let mut first_time = false;
        let header = if segment_metadata.len() >= size_of::<Header>() as u64 {
            Header::unpack(&mut file)?
        } else {
            let header = Header {
                global_depth: 0,
                num_segments: 1,
            };
            header.pack(&mut file)?;
            //Set flag to initialize the first segment
            first_time = true;
            // Initialize the directory
            header
        };
        let mut out = Self {
            file,
            header,
            header_dirty: false,
            segment_depth_cache: HashMap::new(),
        };
        if first_time { out.allocate_segment(size_of::<Header>() as u64)?; }
        Ok(out)
    }
}

impl Segmenter for FileSegmenter {
    fn segment(&mut self, index: u64) -> io::Result<Segment> {
        let offset = (index * SEGMENT_SIZE as u64) * size_of::<Header>() as u64;
        if self.segment_depth_cache.contains_key(&offset) {
            return Ok(Segment {
                depth: *self.segment_depth_cache.get(&offset).unwrap(),
                offset,
            });
        }
        self.file.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 8] = [0; 8];
        let depth = u64::from_le_bytes(buf);
        self.file.read_exact(&mut buf)?;
        self.segment_depth_cache.insert(offset, depth);
        Ok(Segment { depth, offset })
    }

    fn allocate_segment(&mut self, depth: u64) -> io::Result<Segment> {
        let offset = self.header.num_segments * SEGMENT_SIZE as u64;
        self.file.seek(io::SeekFrom::Start(offset))?;
        const SIZE: usize = SEGMENT_SIZE as usize;
        let mut buf: [u8; SIZE] = [0; SIZE];
        buf[..8].copy_from_slice(&depth.to_le_bytes());
        self.file.write(&buf)?;
        Ok(Segment { depth, offset })
    }

    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u64) -> io::Result<Segment> {
        // The number of buckets passed in *must* be the entire segment's buckets
        assert!(buckets.len() as u64 == BUCKETS_PER_SEGMENT as u64);
        let offset = (self.header.num_segments * SEGMENT_SIZE as u64) + size_of::<Header>() as u64;
        let mut buffer =
            BufWriter::with_capacity(SEGMENT_SIZE as usize, &mut self.file);
        buffer.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 8] = [0; 8];
        buf[..].copy_from_slice(&depth.to_le_bytes());
        buffer.write(&buf)?;
        for bucket in buckets.iter() {
            bucket.pack(&mut buffer)?;
        }
        buffer.seek(io::SeekFrom::Start(0))?;
        self.header.pack(&mut buffer)?;
        Ok(Segment { offset, depth })
    }

    fn header(&mut self) -> io::Result<Header> {
        self.file.seek(io::SeekFrom::Start(0))?;
        Header::unpack(&mut self.file)
    }

    fn sync_header(&mut self) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(0))?;
        self.header.pack(&mut self.file)?;
        Ok(())
    }

    fn bucket(&mut self, segment: &Segment, index: u64) -> io::Result<Bucket> {
        let offset = segment.offset + (index * BUCKET_SIZE as u64);
        self.file.seek(io::SeekFrom::Start(offset))?;
        info!("Reading bucket at offset {}", offset);
        return Bucket::unpack(&mut self.file);
    }

    fn write_bucket(&mut self, bucket: &Bucket) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(bucket.offset))?;
        bucket.pack(&mut self.file)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::{self, Read, Write, Cursor, Seek};


}
