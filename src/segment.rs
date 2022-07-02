use std::io::{self, Seek, Read, Write};
use std::fs::File;
use std::path::Path;
use crate::serializer::Serializable;

// The number of buckets in each segment.
// This may be adapted to be parametrizable on a per-database level
// in the futere.
const BUCKETS_PER_SEGMENT: usize = 64;
// The number of records in each bucket.
// This may be adatped to be parametrizable or dynamic in the future.
const BUCKET_RECORDS: usize = 16;

struct Header {
    global_depth: u64,
    num_segments: u64,
}

struct Segment {
    depth: u64,
}

// A Segmenter is a something responsible for allocating and reading segments
pub trait Segmenter {
    // Attempts to read an existing segment.
    fn segment(&self, index: u64) -> io::Result<Segment>;
    // Creates a new segment and returns it's index.
    fn allocate_segment(&self, depth: u64) -> io::Result<Segment>;
    // Returns the header
    fn header(&self) -> io::Result<Header>;
    // Sets the global_depth
    fn update_header(&self, header: Header) -> io::Result<()>;
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
        let file = if !path.exists() {
            let f = File::create(path);
            f
        } else {
            File::open(path)
        }?;
        Ok(Self {file})
    }
}

impl Segmenter for FileSegmenter {
    fn segment(&self, index: u64) -> io::Result<Segment> {
        self.file.seek(io::SeekFrom::Start(index));
        let buf: [u8; 8] = [0; 8];
        let depth = u64::from_le_bytes(buf);
        self.file.read_exact(&mut buf)?;
        Ok(Segment {
            depth,
        })
    }

    fn allocate_segment(&self, depth: u64) -> io::Result<Segment> {
        self.file.seek(io::SeekFrom::End(0))?;
        Ok(Segment{depth:123})
    }

}

impl Segment {

}


