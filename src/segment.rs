use std::io::{self, Seek, Read, Write};
use std::fs::File;
use std::path::Path;
use crate::serializer::{DataOrOffset, Serializable};
use std::mem::size_of;
use std::ops::{Index, IndexMut};

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

impl Serializable for Header {
    fn pack(&self, file: Option<&mut File>) -> io::Result<DataOrOffset> {
        let global_depth_bytes = self.global_depth.to_le_bytes();
        let num_segment_bytes = self.num_segments.to_le_bytes();
        match file {
            Some(file) => {
                let offset = file.seek(io::SeekFrom::Current(0)).unwrap();
                file.write(&global_depth_bytes)?;
                file.write(&num_segment_bytes)?;
                Ok(DataOrOffset::Offset(offset))
            }
            None => {
                let mut out = Vec::with_capacity(16);
                out.extend_from_slice(&global_depth_bytes);
                out.extend_from_slice(&num_segment_bytes);
                Ok(DataOrOffset::Data(out))
            }
        }
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
    segmenter: *mut dyn Segmenter,
}

pub struct Bucket {
    iter_index: u64,
    offset: u64,
    buf: [u8; BUCKET_SIZE as usize],
}

pub struct Record {
    pub hash_key: u64,
    pub value: u64,
    index: u64
}

// A Segmenter is a something responsible for allocating and reading segments
pub trait Segmenter {
    // Attempts to read an existing segment.
    fn segment(&mut self, index: u64) -> io::Result<Segment>;
    // Creates a new segment and returns it's index.
    fn allocate_segment(&mut self, depth: u64) -> io::Result<Segment>;
    // Returns the header
    fn header(&mut self) -> io::Result<Header>;
    // Sets the global_depth
    fn sync_header(&mut self) -> io::Result<()>;
    // Retrieves a bucket from segment at index
    fn bucket(&mut self, segment: &Segment, index: u64) -> io::Result<Bucket>;
    // Overwrites an existing bucket
    fn write_bucket(&mut self, segment: &Segment, indez: u64) -> io::Result<()>;
}

// A simple, file-backed Segmenter
pub struct FileSegmenter {
    file: File,
    header: Header,
    header_dirty: bool,
}

impl FileSegmenter {
    pub fn init(path: Option<&Path>) -> io::Result<Self> {
        let path: &Path = match path {
            Some(path) => path,
            None => Path::new("index.bin")
        };
        let mut file = if !path.exists() {
            let f = File::create(path);
            f
        } else {
            File::open(path)
        }?;
        let segment_metadata = file.metadata()?;
        let header = if segment_metadata.len() >= size_of::<Header>() as u64 {
            Header::unpack(&mut file)?
        } else {
            let header = Header {
                global_depth: 0,
                num_segments: 1,
            };
            header.pack(Some(&mut file))?;
            //Initialize the first segment
            // Initialize the directory
            header
        };
        let mut out = Self {
            file,
            header,
            header_dirty: false,
        };
        out.allocate_segment(size_of::<Header>() as u64)?;
        Ok(out)
    }
}

impl Segmenter for FileSegmenter {

    fn segment(&mut self, index: u64) -> io::Result<Segment> {
        let offset = index * SEGMENT_SIZE;
        self.file.seek(io::SeekFrom::Start(offset))?;
        let mut buf: [u8; 8] = [0; 8];
        let depth = u64::from_le_bytes(buf);
        self.file.read_exact(&mut buf)?;
        Ok(Segment {
            depth,
            offset,
            segmenter: self,
        })
    }

    fn allocate_segment(&mut self, depth: u64) -> io::Result<Segment> {
        let offset = self.header.num_segments * SEGMENT_SIZE;
        self.file.seek(io::SeekFrom::Start(offset))?;
        Ok(Segment{depth, offset, segmenter: self})
    }

    fn header(&mut self) -> io::Result<Header> {
        self.file.seek(io::SeekFrom::Start(0))?;
        Header::unpack(&mut self.file)
    }

    fn sync_header(&mut self) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(0))?;
        self.header.pack(Some(& mut self.file))?;
        Ok(())
    }

}


impl Index<u64> for Segment {
    type Output = Bucket;
    fn index(&self, index: u64) -> &Self::Output {
        let offset = index * BUCKET_SIZE;
        let buf: [u8; BUCKET_SIZE as usize] = [0; BUCKET_SIZE as usize];
        self.segmenter.file.seek(io::SeekFrom::Start(offset));
        self.file.read_exact(&buf);
        &Bucket{iter_index: 0, offset, buf}
    }
}

impl Iterator for Bucket {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_index >= BUCKET_RECORDS {
            return None;
        }
        let offset = self.offset + (size_of::<Record>() as u64 * self.iter_index);

        None
    }

}
