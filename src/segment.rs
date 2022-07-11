use crate::serializer::{DataOrOffset, Serializable};
use simple_error::SimpleError;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, Write};
use std::mem::size_of;
use std::ops::{Index, IndexMut};
use std::path::Path;

// The number of buckets in each segment.
// This may be adapted to be parametrizable on a per-database level
// in the futere.
const BUCKETS_PER_SEGMENT: u64 = 64;
// The number of records in each bucket.
// This may be adatped to be parametrizable or dynamic in the future.
const BUCKET_RECORDS: u64 = 16;

const BUCKET_SIZE: u64 = size_of::<Record>() as u64 * BUCKET_RECORDS;
// The size on-disk of a segment
const SEGMENT_SIZE: u64 = BUCKET_SIZE * BUCKETS_PER_SEGMENT;

pub struct Header {
    global_depth: u64,
    num_segments: u64,
}

impl<W: Seek + Write> Serializable<W, File> for Header {
    fn pack(&self, buffer: &mut W) -> io::Result<u64> {
        let global_depth_bytes = self.global_depth.to_le_bytes();
        let num_segment_bytes = self.num_segments.to_le_bytes();
        let offset = buffer.seek(io::SeekFrom::Current(0)).unwrap();
        buffer.write(&global_depth_bytes)?;
        buffer.write(&num_segment_bytes)?;
        Ok(offset)
    }

    fn unpack(file: &mut File) -> io::Result<Self> {
        let mut buf: [u8; 8] = [0; 8];
        file.read_exact(&mut buf[..])?;
        let global_depth = u64::from_le_bytes(buf);
        file.read_exact(&mut buf[..])?;
        let num_segments = u64::from_le_bytes(buf);
        Ok(Header {
            global_depth,
            num_segments,
        })
    }
}

pub struct Segment {
    pub depth: u64,
    pub offset: u64,
}

impl Serializable<File, File> for Segment {
    fn pack(&self, buffer: &mut File) -> io::Result<u64> {
        let offset = buffer.seek(io::SeekFrom::Current(0))?;
        buffer.write(&self.depth.to_le_bytes())?;
        Ok(offset)
    }

    fn unpack(buffer: &mut File) -> io::Result<Self> {
        let offset = buffer.seek(io::SeekFrom::Current(0))?;
        let mut b: [u8; 8] = [0; 8];
        buffer.read_exact(&mut b)?;
        let depth = u64::from_le_bytes(b);
        Ok(Self { offset, depth })
    }
}

pub struct Bucket {
    iter_index: u64,
    offset: u64,
    buf: [u8; BUCKET_SIZE as usize],
}

impl<'a> Bucket {
    fn get(&self, hk: u64) -> Option<Record> {
        for record in self.iter() {
            if record.hash_key == hk {
                return Some(record);
            }
        }
        None
    }

    pub fn put(&mut self, hk: u64, value: u64, local_depth: u64) -> Result<usize, SimpleError> {
        for (i, record) in self.iter().enumerate() {
            if record.hash_key >> local_depth & hk >> local_depth != hk >> local_depth {
                let offset = i * size_of::<Record>();
                self.buf[offset..8].copy_from_slice(&hk.to_le_bytes());
                self.buf[offset + 8..8].copy_from_slice(&value.to_le_bytes());
                return Ok(offset);
            }
        }
        Err(SimpleError::new("Bucket full"))
    }

    fn iter(&self) -> BucketIter {
        BucketIter {
            index: 0,
            bucket: &self,
        }
    }

    fn at(&self, index: usize) -> Record {
        let offset = index * size_of::<Record>();
        let mut buf: [u8; 8] = [0; 8];
        buf.copy_from_slice(&self.buf[offset..8]);
        let hash_key = u64::from_le_bytes(buf);
        buf.copy_from_slice(&self.buf[offset + 8..8]);
        let value = u64::from_le_bytes(buf);
        Record { hash_key, value }
    }
}

impl<'a, W: Seek + Write> Serializable<W, File> for Bucket {
    fn pack(&self, buffer: &mut W) -> io::Result<u64> {
        let offset = buffer.seek(io::SeekFrom::Current(0)).unwrap();
        buffer.write(&self.buf)?;
        Ok(offset)
    }

    fn unpack(buffer: &mut File) -> io::Result<Self> {
        let offset = buffer.seek(io::SeekFrom::Current(0)).unwrap();
        let mut bucket = Self {
            iter_index: 0,
            offset,
            buf: [0; BUCKET_SIZE as usize],
        };
        buffer.read_exact(&mut bucket.buf)?;
        Ok(bucket)
    }
}

struct BucketIter<'b> {
    index: u64,
    bucket: &'b Bucket,
}

impl<'b> Iterator for BucketIter<'b> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= BUCKET_RECORDS {
            return None;
        }
        Some(self.bucket.at(self.index as usize))
    }
}

pub struct Record {
    pub hash_key: u64,
    pub value: u64,
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
            <Header as Serializable<File, File>>::unpack(&mut file)?
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
        let offset = index * SEGMENT_SIZE;
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
        let offset = self.header.num_segments * SEGMENT_SIZE;
        self.file.seek(io::SeekFrom::Start(offset))?;
        const SIZE: usize = SEGMENT_SIZE as usize + 8;
        let mut buf: [u8; SIZE] = [0; SIZE];
        buf[..8].copy_from_slice(&depth.to_le_bytes());
        self.file.write(&buf)?;
        Ok(Segment { depth, offset })
    }

    fn allocate_with_buckets(&mut self, buckets: Vec<Bucket>, depth: u64) -> io::Result<Segment> {
        // The number of buckets passed in *must* be the entire segment's buckets
        assert!(buckets.len() as u64 == BUCKETS_PER_SEGMENT);
        let offset = self.header.num_segments * SEGMENT_SIZE;
        let mut buffer =
            BufWriter::with_capacity(SEGMENT_SIZE as usize + size_of::<Header>(), &mut self.file);
        buffer.seek(io::SeekFrom::Start(offset))?;
        self.header.pack(&mut buffer)?;
        for bucket in buckets.iter() {
            bucket.pack(&mut buffer)?;
        }
        Ok(Segment { offset, depth })
    }

    fn header(&mut self) -> io::Result<Header> {
        self.file.seek(io::SeekFrom::Start(0))?;
        <Header as Serializable<File, File>>::unpack(&mut self.file)
    }

    fn sync_header(&mut self) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(0))?;
        self.header.pack(&mut self.file)?;
        Ok(())
    }

    fn bucket(&mut self, segment: &Segment, index: u64) -> io::Result<Bucket> {
        let offset = segment.offset + (index * BUCKET_SIZE);
        self.file.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; BUCKET_SIZE as usize] = [0; BUCKET_SIZE as usize];
        self.file.read_exact(&mut buf)?;
        Ok(Bucket {
            iter_index: 0,
            offset,
            buf,
        })
    }

    fn write_bucket(&mut self, bucket: &Bucket) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(bucket.offset))?;
        self.file.write(&bucket.buf)?;
        Ok(())
    }
}
